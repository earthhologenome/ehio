"""Batch scanning and screen-session launcher for ehio."""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from ehio import config as cfg
from ehio.airtable import AirtableClient, AirtableError
from ehio.core import CoreError
from ehio.drakkar import LOGGING_DIRNAME, normalise_assembly_type

# Marker file written by 'ehio stop' in the run directory of a batch.
STOP_SENTINEL = ".ehio_stopped"

_PRIMARY_BASE = {
    "preprocessing": "EHI_BASE",
    "binning":       "EHI_BASE",
    "quantifying":   "MAG_BASE",
    "amr":           "EHI_BASE",
}
_BATCH_TABLE_KEY = {
    "preprocessing": "EHI_PPR_BATCH",
    "binning":       "EHI_ASB_BATCH",
    "quantifying":   "MAG_DMB_BATCH",
    "amr":           "EHI_AMR_BATCH",
}
_BATCH_CODE_CFG = {
    "preprocessing": "EHI_PPR_BATCH_CODE",
    "binning":       "EHI_ASB_BATCH_CODE",
    "quantifying":   "MAG_DMB_BATCH_CODE",
    "amr":           "EHI_AMR_BATCH_CODE",
}
_BATCH_STATUS_CFG = {
    "preprocessing": "EHI_PPR_BATCH_STATUS",
    "binning":       "EHI_ASB_BATCH_STATUS",
    "quantifying":   "MAG_DMB_BATCH_STATUS",
    "amr":           "EHI_AMR_BATCH_STATUS",
}
_OUTPUT_BASE_CFG = {
    "preprocessing": "EHI_PPR_OUTPUT_BASE",
    "binning":       "EHI_ASB_OUTPUT_BASE",
    "quantifying":   "MAG_DMB_OUTPUT_BASE",
    "amr":           "EHI_AMR_OUTPUT_BASE",
    "ena":           "ENA_OUTPUT_BASE",
}
_BOOST_TIME_CFG = {
    "preprocessing": "EHI_PPR_BATCH_BOOST_TIME",
    "binning":       "EHI_ASB_BATCH_BOOST_TIME",
    "quantifying":   "MAG_DMB_BATCH_BOOST_TIME",
    "amr":           "EHI_AMR_BATCH_BOOST_TIME",
}
_BOOST_MEMORY_CFG = {
    "preprocessing": "EHI_PPR_BATCH_BOOST_MEMORY",
    "binning":       "EHI_ASB_BATCH_BOOST_MEMORY",
    "quantifying":   "MAG_DMB_BATCH_BOOST_MEMORY",
    "amr":           "EHI_AMR_BATCH_BOOST_MEMORY",
}
_RUN_BASE_CFG = "RUN_BASE"

# Size of the failure report appended to the .err file when a batch fails.
_FAILURE_TAIL_LINES     = 80   # lines of the .out file
_FAILURE_LOG_FILES      = 5    # log files referenced in the .out file
_FAILURE_JOB_TAIL_LINES = 40   # lines of each of those log files

DRAKKAR_CMD = {
    "preprocessing": "preprocessing",
    "binning":       "cataloging",
    "quantifying":   "profiling",
    "amr":           "amr",
}

# ENA submissions run no drakkar workflow: 'ehio ena' deposits the reads
# itself. They live in ehi-core alone, so they have no Airtable table either.
MODULES = [*DRAKKAR_CMD, "ena"]


# ---------------------------------------------------------------------------
# screen helpers
# ---------------------------------------------------------------------------

def screen_available() -> bool:
    return shutil.which("screen") is not None


def session_exists(name: str) -> bool:
    """Return True if a screen session with exactly this name is already running."""
    result = subprocess.run(["screen", "-ls"], capture_output=True, text=True)
    return bool(re.search(rf"\d+\.{re.escape(name)}(\t| )", result.stdout))


def launch_screen(session_name: str, script_path: str, token: str = "", core_token: str = "") -> None:
    env = {**os.environ}
    if token:
        env["AIRTABLE_TOKEN"] = token
    if core_token:
        env["EHI_CORE_TOKEN"] = core_token
    subprocess.run(
        ["screen", "-dmS", session_name, "bash", script_path],
        env=env,
        check=True,
    )


# ---------------------------------------------------------------------------
# Script builder
# ---------------------------------------------------------------------------

class BatchLaunchError(Exception):
    """A batch cannot be launched — reported and marked as Error, scan continues."""


def _resolve_preprocessing_ref_flag(batch_record: dict, token: str, verbose: bool = False) -> str:
    """Return the drakkar reference flag for a preprocessing batch.

    Checks EHI_GENOME_URL_INDEXED first (-x, indexed tarball), then
    EHI_GENOME_URL_RAW (-r, plain fasta).  Returns '' if no reference
    is configured or the linked genome record cannot be found.

    Raises BatchLaunchError if the resolved reference cannot be downloaded
    (remote URL) or does not exist (local path).
    """
    from ehio.reference import genome_code, resolve_genome_record

    def _dbg(msg: str) -> None:
        if verbose:
            print(f"    [ref] {msg}", file=sys.stderr)

    genome_indexed_fld = str(cfg.get("EHI_GENOME_URL_INDEXED") or "").strip()
    genome_raw_fld     = str(cfg.get("EHI_GENOME_URL_RAW")     or "").strip()

    genome_rec = resolve_genome_record(batch_record, token, dbg=_dbg)
    if not genome_rec:
        return ""

    genome_fields = genome_rec.get("fields", {})
    ref_id        = genome_code(genome_rec) or genome_rec.get("id", "")
    indexed_url   = str(genome_fields.get(genome_indexed_fld, "") or "").strip()
    raw_url       = str(genome_fields.get(genome_raw_fld,     "") or "").strip()
    _dbg(f"EHI_GENOME_URL_INDEXED ({genome_indexed_fld}): {indexed_url!r}")
    _dbg(f"EHI_GENOME_URL_RAW     ({genome_raw_fld}):     {raw_url!r}")

    if indexed_url:
        _dbg(f"Using indexed reference: -x {indexed_url}")
        _verify_reference(indexed_url, ref_id, "EHI_GENOME_URL_INDEXED", _dbg)
        return f"-x {shlex.quote(indexed_url)}"
    if raw_url:
        _dbg(f"Using raw reference: -r {raw_url}")
        _verify_reference(raw_url, ref_id, "EHI_GENOME_URL_RAW", _dbg)
        return f"-r {shlex.quote(raw_url)}"

    print(
        f"    [ref] WARNING: genome record {ref_id!r} found but both "
        "EHI_GENOME_URL_INDEXED and EHI_GENOME_URL_RAW are empty.",
        file=sys.stderr,
    )
    return ""


def _verify_reference(reference: str, genome_id: str, field_name: str, dbg) -> None:
    """Raise BatchLaunchError if the reference genome cannot be downloaded/found."""
    from ehio.urls import check_url, is_remote_url

    if is_remote_url(reference):
        dbg(f"Checking reference URL is downloadable: {reference}")
        reason = check_url(reference)
        if reason:
            raise BatchLaunchError(
                f"reference genome URL is not downloadable: {reference} ({reason}) "
                f"— {field_name} of genome record {genome_id!r}"
            )
        dbg("Reference URL OK.")
        return

    if not Path(reference).exists():
        raise BatchLaunchError(
            f"reference genome file not found: {reference} "
            f"— {field_name} of genome record {genome_id!r}"
        )


def build_script_content(
    module: str,
    batch_name: str,
    run_dir: str,
    output_dir: str,
    profile: str,
    error_status: str = "Error",
    ref_flag: str = "",
    ehio_conda_env: str = "",
    drakkar_conda_env: str = "",
    ppr_fraction: bool = False,
    ppr_nonpareil: bool = False,
    boost_time: int | None = None,
    boost_memory: int | None = None,
    rerun: bool = False,
    resume: bool = False,
    reannotate: bool = False,
    multicoverage: bool = False,
    ani_threshold: str = "",
    profiling_type: str = "",
    annotation_type: str = "all",
) -> str:
    """Return the full content of the .sh script written into run_dir.

    The script uses bash ERR trap to set the batch status to error_status
    in Airtable if any step fails.  AIRTABLE_TOKEN is inherited from the
    environment of the launching shell.

    run_dir    — /projects/ehi/data/RUN/{batch_code}  (samples.tsv, logs, .snakemake)
    output_dir — /projects/ehi/data/{PPR|ASB|DMB|AMR}/{batch_code}  (drakkar -o target)
    ref_flag   — pre-resolved '-x url' or '-r url' for preprocessing; '' otherwise
    """
    if module not in MODULES:
        raise ValueError(f"Unknown module: {module}")

    drakkar_sub = DRAKKAR_CMD.get(module, "")
    q = shlex.quote

    tsv_file = f"{run_dir}/{batch_name}.tsv"

    out_file = f"{run_dir}/{batch_name}.out"
    err_file = f"{run_dir}/{batch_name}.err"
    # 'ehio stop' drops this file before killing the screen session, so the
    # exit trap below can tell a deliberate stop from a genuine failure.
    stop_file = f"{run_dir}/{STOP_SENTINEL}"
    # Touched right before every drakkar call, so the run metadata written by
    # that call can be told apart from the metadata of earlier runs.
    marker_file = f"{run_dir}/.ehio_drakkar_marker"
    # Where drakkar 2.5.0 and later keep the run metadata of every run.
    logging_dir = f"{output_dir}/{LOGGING_DIRNAME}"

    conda_block = ""
    if ehio_conda_env:
        conda_block = (
            'if [ -f "$(conda info --base 2>/dev/null)/etc/profile.d/conda.sh" ]; then\n'
            '    source "$(conda info --base)/etc/profile.d/conda.sh"\n'
            f"    conda activate {shlex.quote(ehio_conda_env)}\n"
            "fi\n"
        )

    if drakkar_conda_env:
        _flag = "-p" if drakkar_conda_env.startswith(("/", "~", ".")) else "-n"
        drakkar_prefix = f"conda run {_flag} {shlex.quote(drakkar_conda_env)} "
    else:
        drakkar_prefix = ""

    header = (
        "#!/usr/bin/env bash\n"
        f"# ehio-generated script — batch {batch_name} ({module})\n"
        "# Do not edit manually; re-run ehio scanning to regenerate.\n"
        "# AIRTABLE_TOKEN, and EHI_CORE_TOKEN when ehi-core is in use, must be exported\n"
        "# in the environment before launching.\n"
        "\n"
        "set -euo pipefail\n"
        f"exec >> {q(out_file)} 2>> {q(err_file)}\n"
        'echo ""\n'
        'echo "=== $(date \'+%Y-%m-%d %H:%M:%S\') ==="\n'
        'echo "=== $(date \'+%Y-%m-%d %H:%M:%S\') ===" >&2\n'
        "\n"
        + conda_block +
        # A stop marker left by an earlier 'ehio stop' must not silence the
        # error reporting of this launch.
        f"rm -f {q(stop_file)}\n"
        "_EHIO_SUCCESS=0\n"
        # Only failure reports written after this point belong to this launch.
        "_EHIO_STARTED=$(date +%s)\n"
        # drakkar merges the snakemake stderr into its stdout, so the cause of a
        # failure only ever reaches the .out file.  Copy the relevant tail (and
        # the logs of the failing snakemake jobs) into the .err file, so the
        # error report is self-contained.
        "_ehio_report_failure() {\n"
        f'    echo "" >&2\n'
        f'    echo "=== ehio: {module} of {batch_name} failed ===" >&2\n'
        f'    echo "--- last {_FAILURE_TAIL_LINES} lines of {out_file} ---" >&2\n'
        f"    tail -n {_FAILURE_TAIL_LINES} {q(out_file)} >&2 2>/dev/null || true\n"
        f"    _EHIO_LOGS=$(grep -hoE '/[^ ,:)]+\\.log' {q(out_file)} 2>/dev/null "
        f"| tail -n {_FAILURE_LOG_FILES} | sort -u || true)\n"
        '    for _EHIO_LOG in ${_EHIO_LOGS:-}; do\n'
        '        [ -f "$_EHIO_LOG" ] || continue\n'
        f'        echo "--- last {_FAILURE_JOB_TAIL_LINES} lines of $_EHIO_LOG ---" >&2\n'
        f'        tail -n {_FAILURE_JOB_TAIL_LINES} "$_EHIO_LOG" >&2 2>/dev/null || true\n'
        "    done\n"
        f'    echo "=== ehio: end of failure report (full log: {out_file}) ===" >&2\n'
        "}\n"
        "_on_exit() {\n"
        f'    if [ -f {q(stop_file)} ]; then\n'
        f'        echo "=== ehio: batch {batch_name} stopped on request ===" >&2\n'
        "        return 0\n"
        "    fi\n"
        '    if [ "$_EHIO_SUCCESS" -ne 1 ]; then\n'
        "        _ehio_report_failure || true\n"
        f"        ehio set-status --module {module} --batch {q(batch_name)} --status {q(error_status)}"
        f" --failures-dir {q(output_dir)} --failures-since \"$_EHIO_STARTED\"\n"
        "    fi\n"
        "}\n"
        "trap _on_exit EXIT\n"
        "\n"
        # drakkar reports several of its own errors — a Snakemake lock left
        # behind by a killed batch, an input file it cannot find — by printing
        # a message and returning, which leaves the exit status at 0.  Without
        # the checks below 'set -e' reads that as a successful run, the output
        # step finds no results, and the batch is marked as done although
        # nothing ran.  Two independent signals catch it: the run metadata
        # drakkar writes for every run (drakkar_<run id>.yaml, stamped
        # 'status: success' once the workflow ends) and the products the
        # workflow must have left behind.
        #
        # drakkar 2.5.0 moved that metadata into the 'logging/' subdirectory of
        # the output directory; before it, the file sat in the output root next
        # to the results.  Both are searched, so the check keeps working across
        # the upgrade — including for a batch launched under one layout and
        # resumed under the other.  The run id is a UTC timestamp, and matching
        # its shape rather than 'drakkar_*.yaml' leaves out the benchmark
        # roll-up an older drakkar wrote beside the metadata as
        # drakkar_<run id>_resources.yaml, which carries no status.
        f"_EHIO_MARKER={q(marker_file)}\n"
        "_ehio_drakkar_start() {\n"
        '    rm -f "$_EHIO_MARKER"\n'
        '    touch "$_EHIO_MARKER"\n'
        "}\n"
        # 'find' exits non-zero on a directory that does not exist — which the
        # logging directory does not under an older drakkar — and 'pipefail'
        # would turn that into a failed script, so its status is discarded.
        "_ehio_drakkar_metadata() {\n"
        f'    {{ find {q(logging_dir)} {q(output_dir)} -maxdepth 1'
        ' -name "drakkar_????????-??????.yaml" "$@" 2>/dev/null || true; } | sort\n'
        "}\n"
        "_ehio_drakkar_check() {\n"
        '    if [ -z "$(_ehio_drakkar_metadata)" ]; then\n'
        "        return 0\n"
        "    fi\n"
        '    _EHIO_META=$(_ehio_drakkar_metadata -newer "$_EHIO_MARKER" | tail -n 1)\n'
        '    if [ -z "${_EHIO_META:-}" ]; then\n'
        '        echo "=== ehio: drakkar $1 exited without starting a workflow run ===" >&2\n'
        "        exit 1\n"
        "    fi\n"
        '    if ! grep -q "^status: success" "$_EHIO_META"; then\n'
        '        echo "=== ehio: drakkar $1 did not finish successfully (see $_EHIO_META) ===" >&2\n'
        "        exit 1\n"
        "    fi\n"
        "}\n"
        "_ehio_require() {\n"
        '    if [ ! -e "$1" ]; then\n'
        '        echo "=== ehio: drakkar $2 left no output at $1 ===" >&2\n'
        "        exit 1\n"
        "    fi\n"
        "}\n"
        "\n"
        f"mkdir -p {q(run_dir)} {q(output_dir)}\n"
        f"cd {q(output_dir)}\n"
    )

    def drakkar_step(command: str, label: str) -> str:
        """A drakkar call bracketed by the run-metadata check."""
        return "_ehio_drakkar_start\n" + command + f"_ehio_drakkar_check {q(label)}\n"

    def optional_drakkar_step(condition: str, command: str, label: str) -> str:
        """A drakkar call that is skipped when its result is already there."""
        body = "".join(f"    {line}\n" for line in drakkar_step(command, label).splitlines())
        return f"if {condition}; then\n{body}fi\n"

    def input_step_of(command: str, produced: str) -> str:
        """The '--input' step; on resume it only runs if its file is missing.

        A resumed batch reuses the input file written by the original launch,
        but that file is gone whenever the run directory was cleaned, and
        drakkar exits 0 when it cannot find it — so it is rebuilt instead of
        letting the batch fail silently.
        """
        return (f"[ -s {q(produced)} ] || " if resume else "") + command

    # A batch that was killed — by 'ehio stop', or by the driver dying — leaves
    # the Snakemake lock behind in the output directory, and drakkar refuses to
    # start over a locked directory.  Clearing it is what makes a resume able to
    # continue from the checkpoint at all.
    # A re-annotation is re-runnable in the same way — it is set back to the
    # reannotate status and picks up where it stopped — so it clears the lock
    # too.  On a first run the output directory has just been created and there
    # is nothing to unlock, which is why the call is allowed to fail.
    unlock_step = (
        f"{drakkar_prefix}drakkar unlock -o {q(output_dir)} -p {q(profile)} || true\n"
        if resume or reannotate else ""
    )

    time_part   = f" --time-multiplier {boost_time}"     if boost_time   and boost_time   > 1 else ""
    memory_part = f" --memory-multiplier {boost_memory}" if boost_memory and boost_memory > 1 else ""
    boost_parts = time_part + memory_part
    rerun_flag  = " --rerun" if rerun else ""

    if module == "preprocessing":
        ref_part       = f" {ref_flag}" if ref_flag else ""
        fraction_part  = " --fraction"  if ppr_fraction  else ""
        nonpareil_part = " --nonpareil" if ppr_nonpareil else ""
        input_step = input_step_of(
            f"ehio preprocessing --input -b {q(batch_name)} -f {q(tsv_file)}\n", tsv_file,
        )
        return header + (
            input_step
            + unlock_step
            + drakkar_step(
                f"{drakkar_prefix}drakkar {drakkar_sub} -f {q(tsv_file)} -o {q(output_dir)} -p {q(profile)}{ref_part}{fraction_part}{nonpareil_part}{boost_parts}\n",
                "preprocessing",
            )
            + f"_ehio_require {q(output_dir + '/preprocessing/final')} {q('preprocessing')}\n"
            + f"ehio preprocessing --output -b {q(batch_name)} -l {q(output_dir)}{rerun_flag}\n"
            + "_EHIO_SUCCESS=1\n"
        )

    if module == "binning":
        # -c maps every sample of the batch to every individual assembly, so the
        # binners see a coverage profile per assembly instead of a single depth.
        multicoverage_part = " -c" if multicoverage else ""
        input_step = input_step_of(
            f"ehio binning --input -b {q(batch_name)} -f {q(tsv_file)}\n", tsv_file,
        )
        return header + (
            input_step
            + unlock_step
            + drakkar_step(
                f"{drakkar_prefix}drakkar {drakkar_sub} -f {q(tsv_file)} -o {q(output_dir)} -p {q(profile)}{multicoverage_part}{boost_parts}\n",
                "cataloging",
            )
            + f"_ehio_require {q(output_dir + '/cataloging/final')} {q('cataloging')}\n"
            + f"ehio binning --output -b {q(batch_name)} -l {q(output_dir)}{rerun_flag}\n"
            + "_EHIO_SUCCESS=1\n"
        )

    if module == "ena":
        # No drakkar: ehio downloads each hologenome's reads into the output
        # directory, deposits them and sets the submission's status itself.
        # Hologenomes ENA already holds are skipped, so a resume or a rerun
        # carries on where the submission stopped.
        return header + (
            f"ehio ena -b {q(batch_name)} -d {q(output_dir)}\n"
            + "_EHIO_SUCCESS=1\n"
        )

    if module == "amr":
        manifest_file  = f"{run_dir}/{batch_name}_assemblies.tsv"
        # The assemblies are downloaded next to the manifest drakkar writes for
        # itself, so a rerun that clears the output directory fetches them again
        # and a resume re-uses what is already there.
        assemblies_dir = f"{output_dir}/data/assemblies"
        qc_tsv         = f"{output_dir}/amr/amr_qc.tsv"
        input_step = input_step_of(
            f"ehio amr --input -b {q(batch_name)} -f {q(manifest_file)}"
            f" -d {q(assemblies_dir)}\n",
            manifest_file,
        )
        return header + (
            input_step
            + unlock_step
            + drakkar_step(
                f"{drakkar_prefix}drakkar {drakkar_sub} -f {q(manifest_file)}"
                f" -o {q(output_dir)} -p {q(profile)}{boost_parts}\n",
                "amr",
            )
            + f"_ehio_require {q(qc_tsv)} {q('amr')}\n"
            + f"ehio amr --output -b {q(batch_name)} -l {q(output_dir)}{rerun_flag}\n"
            + "_EHIO_SUCCESS=1\n"
        )

    if module == "quantifying":
        mags_file    = f"{run_dir}/{batch_name}_mags.tsv"
        reads_file   = f"{run_dir}/{batch_name}_reads.tsv"
        quality_file = f"{run_dir}/{batch_name}_quality.tsv"
        ani_part     = f" -a {q(ani_threshold)}"   if ani_threshold  else ""
        type_part    = f" -t {q(profiling_type)}"  if profiling_type else ""
        input_step   = input_step_of(
            f"ehio quantifying --input -b {q(batch_name)}"
            f" --mags-file {q(mags_file)}"
            f" --reads-file {q(reads_file)}"
            f" --quality-file {q(quality_file)}\n",
            mags_file,
        )
        derep_genomes_dir         = f"{output_dir}/profiling_genomes/drep/dereplicated_genomes"
        mags_info_tsv             = f"{output_dir}/profiling_genomes/final/mags.tsv"
        annotation_file           = f"{run_dir}/{batch_name}_annotation.tsv"
        annotation_clusters_file  = f"{run_dir}/{batch_name}_annotation_clusters.tsv"
        taxonomy_tsv              = f"{output_dir}/annotating/genome_taxonomy.tsv"
        # Map batch annotation type to drakkar --annotation-type flag
        _ann_drakkar_map = {"kegg": "kegg", "genes": "genes", "all": "function"}
        drakkar_ann_flag = _ann_drakkar_map.get(annotation_type.lower() if annotation_type else "all", "function")
        qfy_output_sentinel  = f"{run_dir}/.qfy_output_done"
        qfy_status           = str(cfg.get("QUANTIFYING_RUNNING_STATUS")  or "Quantifying").strip()
        ann_tax_status       = str(cfg.get("ANNOTATING_TAXONOMY_STATUS")  or "Annotating taxonomy").strip()
        ann_func_status      = str(cfg.get("ANNOTATING_FUNCTION_STATUS")  or "Annotating function").strip()
        profiling_cmd = (
            f"{drakkar_prefix}drakkar {drakkar_sub} -B {q(mags_file)} -R {q(reads_file)}{ani_part}{type_part} -q {q(quality_file)} -o {q(output_dir)} -p {q(profile)}{boost_parts}\n"
        )
        taxonomy_cmd = (
            f"{drakkar_prefix}drakkar annotating -b {q(derep_genomes_dir)} -p {q(profile)}{boost_parts} --annotation-type taxonomy\n"
        )
        function_cmd = (
            f"{drakkar_prefix}drakkar annotating -B {q(annotation_file)} -p {q(profile)}{boost_parts} --annotation-type {q(drakkar_ann_flag)}\n"
        )

        if reannotate:
            # A batch whose genomes were dereplicated and profiled long ago,
            # sent through the current drakkar's functional annotation again.
            # Everything before the annotation is skipped: no profiling, so no
            # reads and no MAG_DMB_BATCH_LIST_PPR are needed, and no 'ehio
            # quantifying --output', so the counts, the mapping rates and the
            # dereplicated MAG count already on the records are left exactly as
            # they are.
            #
            # Taxonomy is skipped as well, and deliberately: a genome's
            # classification is a property of the genome, fixed when it was
            # binned, and dereplication neither changes it nor produces a
            # better one.  Re-running GTDB-Tk here would rewrite the taxonomy
            # of every MAG in the catalogue against whichever GTDB release
            # happens to be installed, which is a different operation from
            # annotating them again and not one a DMB batch should be doing.
            #
            # What the batch does need is its genomes back on disk, which is
            # the staging step: the catalogue comes from the counts table the
            # batch left on ERDA and each genome from its own MAG record.
            #
            # 'annotating --input' runs with --rerun so every genome is
            # annotated again rather than skipped for already carrying the
            # batch's annotation level — which, after a finished batch, all of
            # them do.  That also leaves the cluster-upgrade file empty, so the
            # clusters step below stays skipped and the single 'function' run
            # covers the whole catalogue.
            return header + (
                f"ehio set-status --module quantifying -b {q(batch_name)} --status {q(ann_func_status)}\n"
                + f"ehio annotating --stage -b {q(batch_name)} -d {q(derep_genomes_dir)}\n"
                + f"_ehio_require {q(derep_genomes_dir)} {q('annotating --stage')}\n"
                + unlock_step
                + f"ehio annotating --input -b {q(batch_name)} -f {q(annotation_file)}"
                  f" -d {q(derep_genomes_dir)} --rerun\n"
                + optional_drakkar_step(
                    f"[ -s {q(annotation_file)} ]", function_cmd, "annotating function",
                )
                + f"_ehio_require {q(output_dir + '/annotating/final')} {q('annotating function')}\n"
                + optional_drakkar_step(
                    f"[ -s {q(annotation_clusters_file)} ]",
                    f"{drakkar_prefix}drakkar annotating -B {q(annotation_clusters_file)} -p {q(profile)}{boost_parts} --annotation-type clusters\n",
                    "annotating clusters",
                )
                # --rerun replaces ANN/{batch} on ERDA, whose per-genome tables
                # are the ones just superseded; --reannotate keeps the drakkar
                # version that profiled the batch on the record instead of
                # overwriting it with the annotation run's.
                + f"ehio annotating --output -b {q(batch_name)} -l {q(output_dir)} --rerun --reannotate\n"
                + "_EHIO_SUCCESS=1\n"
            )

        if resume:
            # mags.tsv (the MAG info table) was added to drakkar after the first
            # batches were finished, so a resumed batch that has its dereplicated
            # genomes but no mags.tsv still calls drakkar: snakemake then builds
            # that one missing target and leaves everything else alone.
            profiling_step = optional_drakkar_step(
                f"[ ! -d {q(derep_genomes_dir)} ] || [ ! -s {q(mags_info_tsv)} ]",
                profiling_cmd, "profiling",
            )
            # Re-run the upload when mags.tsv is newer than the last upload, so
            # a file drakkar has just produced still reaches the DMB folder.
            qfy_output_step = (
                f"if [ ! -f {q(qfy_output_sentinel)} ] || [ {q(mags_info_tsv)} -nt {q(qfy_output_sentinel)} ]; then\n"
                f"  ehio quantifying --output -b {q(batch_name)} -l {q(output_dir)}{rerun_flag}\n"
                f"  touch {q(qfy_output_sentinel)}\n"
                f"fi\n"
            )
            taxonomy_step = optional_drakkar_step(
                f"[ ! -f {q(taxonomy_tsv)} ]", taxonomy_cmd, "annotating taxonomy",
            )
            ann_input_step = (
                f"[ -s {q(annotation_file)} ] || "
                f"ehio annotating --input -b {q(batch_name)} -f {q(annotation_file)} -d {q(derep_genomes_dir)}{rerun_flag}\n"
            )
        else:
            profiling_step = drakkar_step(profiling_cmd, "profiling")
            qfy_output_step = f"ehio quantifying --output -b {q(batch_name)} -l {q(output_dir)}{rerun_flag}\n"
            taxonomy_step = drakkar_step(taxonomy_cmd, "annotating taxonomy")
            ann_input_step = f"ehio annotating --input -b {q(batch_name)} -f {q(annotation_file)} -d {q(derep_genomes_dir)}{rerun_flag}\n"
        clusters_step = optional_drakkar_step(
            f"[ -s {q(annotation_clusters_file)} ]",
            f"{drakkar_prefix}drakkar annotating -B {q(annotation_clusters_file)} -p {q(profile)}{boost_parts} --annotation-type clusters\n",
            "annotating clusters",
        ) if annotation_type.lower() == "all" else ""
        return header + (
            f"ehio set-status --module quantifying -b {q(batch_name)} --status {q(qfy_status)}\n"
            + input_step
            + unlock_step
            + profiling_step
            + f"_ehio_require {q(derep_genomes_dir)} {q('profiling')}\n"
            + qfy_output_step
            + f"ehio set-status --module quantifying -b {q(batch_name)} --status {q(ann_tax_status)}\n"
            + taxonomy_step
            + f"_ehio_require {q(taxonomy_tsv)} {q('annotating taxonomy')}\n"
            + f"ehio set-status --module quantifying -b {q(batch_name)} --status {q(ann_func_status)}\n"
            + ann_input_step
            + optional_drakkar_step(
                f"[ -s {q(annotation_file)} ]", function_cmd, "annotating function",
            )
            + clusters_step
            + f"ehio annotating --output -b {q(batch_name)} -l {q(output_dir)}{rerun_flag}\n"
            + "_EHIO_SUCCESS=1\n"
        )

    raise ValueError(f"Unknown module: {module}")


# ---------------------------------------------------------------------------
# Input-file generator (used by dry-run)
# ---------------------------------------------------------------------------

def _generate_input_files(
    module: str, batch_name: str, run_dir: str, token: str, core_token: str = ""
) -> None:
    """Run 'ehio <module> --input' to write the TSV (and bins file) into run_dir.

    Uses the same Python interpreter so the installed package is always found.
    The Airtable and ehi-core tokens are injected via the environment.
    """
    tsv_path = str(Path(run_dir) / f"{batch_name}.tsv")
    env      = {**os.environ, "AIRTABLE_TOKEN": token}
    if core_token:
        env["EHI_CORE_TOKEN"] = core_token

    if module == "preprocessing":
        cmd = [sys.executable, "-m", "ehio", "preprocessing", "--input",
               "-b", batch_name, "-f", tsv_path]
    elif module == "binning":
        cmd = [sys.executable, "-m", "ehio", "binning", "--input",
               "-b", batch_name, "-f", tsv_path]
    elif module == "amr":
        manifest_path = str(Path(run_dir) / f"{batch_name}_assemblies.tsv")
        output_base   = str(cfg.get(_OUTPUT_BASE_CFG["amr"]) or "").strip()
        cmd = [sys.executable, "-m", "ehio", "amr", "--input",
               "-b", batch_name, "-f", manifest_path,
               "-d", str(Path(output_base) / batch_name / "data" / "assemblies")]
    elif module == "ena":
        # What a dry run of the scan can check of a submission: its tables,
        # written without downloading or submitting anything.
        output_base = str(cfg.get(_OUTPUT_BASE_CFG["ena"]) or "").strip()
        cmd = [sys.executable, "-m", "ehio", "ena", "--dry-run",
               "-b", batch_name, "-d", str(Path(output_base) / batch_name)]
    elif module == "quantifying":
        mags_path    = str(Path(run_dir) / f"{batch_name}_mags.tsv")
        reads_path   = str(Path(run_dir) / f"{batch_name}_reads.tsv")
        quality_path = str(Path(run_dir) / f"{batch_name}_quality.tsv")
        cmd = [sys.executable, "-m", "ehio", "quantifying", "--input",
               "-b", batch_name,
               "--mags-file", mags_path,
               "--reads-file", reads_path,
               "--quality-file", quality_path]
    else:
        raise ValueError(f"Unknown module: {module}")

    subprocess.run(cmd, env=env, check=True)


# ---------------------------------------------------------------------------
# Where the scan finds its batches
# ---------------------------------------------------------------------------
#
# Airtable is running out of room and ehi-core is taking over from it, so the
# scan reads both batch tables and launches each batch once.  A batch both
# databases hold keeps the code Airtable gave it, because the run directory and
# the screen session on the cluster are named after that, and is otherwise
# described by the core: that is the database the EHI is moving to, and ehio
# has been writing every status it sets into it.  The status a launch sets goes
# back to whichever of the two holds the batch, so a batch created in the core
# alone is launched and reported without Airtable knowing anything about it.

# The core table behind each module's Airtable batch table.
_CORE_BATCH_TABLE = {
    "preprocessing": "preprocessing_batches",
    "binning":       "assembly_batches",
    "quantifying":   "dereplication_batches",
    "amr":           "amr_batches",
    "ena":           "ena_submissions",
}

# The core column holding what the Airtable field of this config key holds.
_CORE_COLUMN = {
    "EHI_PPR_BATCH_BOOST_TIME":      "boost_time",
    "EHI_ASB_BATCH_BOOST_TIME":      "boost_time",
    "MAG_DMB_BATCH_BOOST_TIME":      "boost_time",
    "EHI_AMR_BATCH_BOOST_TIME":      "boost_time",
    "EHI_PPR_BATCH_BOOST_MEMORY":    "boost_memory",
    "EHI_ASB_BATCH_BOOST_MEMORY":    "boost_memory",
    "MAG_DMB_BATCH_BOOST_MEMORY":    "boost_memory",
    "EHI_AMR_BATCH_BOOST_MEMORY":    "boost_memory",
    "EHI_ASB_BATCH_TYPE":            "batch_type",
    "MAG_DMB_BATCH_TYPE":            "batch_type",
    "MAG_DMB_BATCH_ANI":             "ani_threshold",
    "MAG_DMB_BATCH_ANNOTATION_TYPE": "annotation_type",
    "EHI_PPR_BATCH_REFERENCE":       "reference_genome_code",
}


class PendingBatch:
    """One batch waiting to be launched, as the databases holding it describe it.

    `kind` is what its status asks for: a plain launch (''), a 'resume' of a
    failed run, a 'rerun' from scratch, or a 'reannotate' of a finished DMB
    batch.  `record` is the Airtable record and `row` the core row; a batch in
    both has both, and then the core's values are the ones read.
    """

    def __init__(
        self,
        code: str,
        kind: str,
        status: str,
        record: dict | None = None,
        row: dict | None = None,
    ) -> None:
        self.code = code
        self.kind = kind
        self.status = status
        self.record = record
        self.row = row

    @property
    def where(self) -> str:
        held = [name for name, seen in (("Airtable", self.record), ("ehi-core", self.row)) if seen]
        return " and ".join(held) or "nowhere"

    def value(self, config_key: str) -> Any:
        """A batch's field: the core's, when the core holds it, else Airtable's."""
        column = _CORE_COLUMN.get(config_key)
        if self.row and column:
            held = self.row.get(column)
            if held not in (None, ""):
                return held
        field = str(cfg.get(config_key) or "").strip()
        if not (self.record and field):
            return None
        held = (self.record.get("fields") or {}).get(field)
        return held if held not in (None, "") else None

    def text(self, config_key: str) -> str:
        held = self.value(config_key)
        return "" if held is None else str(held).strip()

    def number(self, config_key: str) -> int | None:
        try:
            held = self.value(config_key)
            return int(held) if held is not None else None
        except (TypeError, ValueError):
            return None

    def reference_record(self) -> dict:
        """The record the reference genome is resolved from.

        The genome table stays in Airtable, so the core holds only the genome's
        code — which is one of the two things the resolver already accepts, the
        other being the record id an Airtable link cell holds.
        """
        field = str(cfg.get("EHI_PPR_BATCH_REFERENCE") or "").strip()
        code  = str((self.row or {}).get("reference_genome_code") or "").strip()
        if field and code:
            return {"id": self.code, "fields": {field: code}}
        return self.record or {}


class AirtableBatches:
    """A module's batch table in Airtable, as the scan reads and writes it."""

    name = "Airtable"

    def __init__(self, module: str, token: str) -> None:
        self.module       = module
        self.base_id      = cfg.get(_PRIMARY_BASE[module], "").strip()
        self.table        = cfg.get(_BATCH_TABLE_KEY[module], "").strip()
        self.code_field   = cfg.get(_BATCH_CODE_CFG[module], "").strip()
        self.status_field = cfg.get(_BATCH_STATUS_CFG[module], "").strip()
        self._token       = token

    def missing(self) -> list[str]:
        """The config keys this source needs and does not have."""
        return [key for key, value in (
            (_PRIMARY_BASE[self.module],     self.base_id),
            (_BATCH_TABLE_KEY[self.module],  self.table),
            (_BATCH_CODE_CFG[self.module],   self.code_field),
            (_BATCH_STATUS_CFG[self.module], self.status_field),
        ) if not value]

    @property
    def client(self) -> AirtableClient:
        return AirtableClient(api_key=self._token, base_id=self.base_id)

    def holds(self, batch: PendingBatch) -> bool:
        return batch.record is not None

    def pending(self, statuses: dict[str, str]) -> list[PendingBatch]:
        client = self.client
        found: list[PendingBatch] = []
        for kind, status in statuses.items():
            if not status:
                continue
            for record in client.fetch_pending_batches(
                batch_table=self.table,
                batch_status_field=self.status_field,
                trigger_status=status,
            ):
                code = str((record.get("fields") or {}).get(self.code_field, "")).strip()
                if code:
                    found.append(PendingBatch(code, kind, status, record=record))
        return found

    def set_status(self, batch: PendingBatch, status: str) -> None:
        self.client.update_records(
            self.table, [{"id": batch.record["id"], "fields": {self.status_field: status}}]
        )


class CoreBatches:
    """A module's batch table in ehi-core, read through the pipeline API.

    A core that cannot be reached, or one too old to know the route, is
    reported and leaves the scan to Airtable, unless EHI_CORE_REQUIRED — which
    is what CoreSession already means by required.
    """

    name = "ehi-core"

    def __init__(self, module: str, core) -> None:
        self.module = module
        self.core   = core
        self.table  = _CORE_BATCH_TABLE[module]

    def holds(self, batch: PendingBatch) -> bool:
        # A batch Airtable alone held is created in the core by the status
        # write, the way every other Airtable fact reaches it.
        return batch.row is not None or batch.record is not None

    def pending(self, statuses: dict[str, str]) -> list[PendingBatch]:
        wanted = {kind: status for kind, status in statuses.items() if status}
        if not wanted:
            return []
        rows = self.core.mirror_call(
            f"The pending {self.table}",
            lambda client: client.pending_batches(self.table, list(wanted.values())),
        )
        by_status = {status.strip().lower(): kind for kind, status in wanted.items()}
        found: list[PendingBatch] = []
        for row in rows or []:
            code   = str(row.get("code") or "").strip()
            status = str(row.get("status") or "").strip()
            kind   = by_status.get(status.lower())
            if code and kind is not None:
                found.append(PendingBatch(code, kind, status, row=row))
        return found

    def set_status(self, batch: PendingBatch, status: str) -> None:
        # Written outright rather than mirrored: the scan reads the core's
        # status and takes it over Airtable's, so a status that did not reach
        # the core is not one the next scan sees, whoever else holds the batch.
        from ehio import mirror

        self.core.write([mirror.batch(self.module, batch.code, batch.record, status=status)],
                        f"Status of batch '{batch.code}'")


def _sources(module: str, token: str, core=None, verbose: bool = False) -> list:
    """The databases this module's batches are scanned in, Airtable first.

    A missing Airtable table is no longer the end of the scan: the core may
    hold the module's batches on its own, which is where this is going.
    """
    sources: list = []
    if module in _BATCH_TABLE_KEY:
        airtable = AirtableBatches(module, token)
        missing  = airtable.missing()
        if token and not missing:
            sources.append(airtable)
        elif verbose:
            why = f"missing config: {', '.join(missing)}" if missing else "no Airtable token"
            print(f"  [{module}] not scanning Airtable — {why}", file=sys.stderr)
    if core:
        sources.append(CoreBatches(module, core))
    return sources


def _merge(module: str, sources: list, statuses: dict[str, str], verbose: bool = False) -> list[PendingBatch]:
    """Every pending batch of a module, each one once.

    When the two databases disagree about what a batch is waiting for, that is
    said and the core's answer is taken: it is worth seeing while ehio is still
    writing to both.
    """
    merged: dict[str, PendingBatch] = {}
    for source in sources:
        for batch in source.pending(statuses):
            held = merged.get(batch.code.upper())
            if held is None:
                merged[batch.code.upper()] = batch
                continue
            # Airtable is read first, so this is the core's copy of the batch.
            if held.status.strip().lower() != batch.status.strip().lower():
                print(
                    f"  [{module}] {held.code}: '{held.status}' in Airtable but "
                    f"'{batch.status}' in ehi-core — taking ehi-core's.",
                    file=sys.stderr,
                )
            held.kind, held.status, held.row = batch.kind, batch.status, batch.row

    batches = sorted(merged.values(), key=lambda batch: batch.code)
    if verbose:
        for batch in batches:
            print(f"  [{module}] {batch.code}: '{batch.status}' in {batch.where}", file=sys.stderr)
    return batches


def _set_status(
    module: str,
    batch: PendingBatch,
    status: str,
    sources: list,
    dry_run: bool = False,
    strict: bool = False,
) -> None:
    """Set a batch's status wherever the batch is held.

    A failure is reported and the scan carries on, except under `strict`: a
    batch whose screen session is already running must not be left looking
    unlaunched, or the next pass would launch it again.  That holds in the
    core as much as in Airtable, since the scan reads both.
    """
    if dry_run:
        print(f"  [{module}] {batch.code}: dry-run — status not set to '{status}'", file=sys.stderr)
        return
    written = False
    for source in sources:
        if not source.holds(batch):
            continue
        try:
            source.set_status(batch, status)
            written = True
        except (AirtableError, CoreError) as exc:
            if strict:
                raise type(exc)(
                    f"{exc}\n  The screen session for {batch.code} was launched, but its "
                    f"status in {source.name} ({source.table}) could not be set to "
                    f"'{status}'. Fix the cause above and set the status manually."
                ) from exc
            print(
                f"  [{module}] {batch.code}: WARNING — could not set the status to "
                f"'{status}' in {source.name}: {exc}",
                file=sys.stderr,
            )
    if written:
        print(f"  [{module}] {batch.code}: status → '{status}'", file=sys.stderr)


# ---------------------------------------------------------------------------
# Per-module scan
# ---------------------------------------------------------------------------

def scan_module(
    module: str,
    token: str,
    dry_run: bool = False,
    verbose: bool = False,
    core=None,
    core_token: str = "",
) -> tuple[int, int]:
    """Scan a module's batch tables for pending batches and launch them.

    Airtable and ehi-core are both scanned; `core` (an ehio.core.CoreSession)
    is the second of the two as well as where the statuses the scan sets are
    written, and `core_token` is handed to the launched batch.

    Returns (found, launched).
    """
    output_base        = cfg.get(_OUTPUT_BASE_CFG[module], "").strip()
    run_base           = cfg.get(_RUN_BASE_CFG, "").strip()
    trigger_status     = cfg.get("SCANNING_TRIGGER_STATUS", "Ready").strip()
    resume_status      = cfg.get("SCANNING_RESUME_STATUS",  "Resume").strip()
    rerun_status       = cfg.get("SCANNING_RERUN_STATUS",   "Rerun").strip()
    # Only a DMB batch can be re-annotated: it is the one module whose results
    # hold a genome catalogue that can be put back on disk and annotated again
    # without redoing the work that produced it.
    reannotate_status  = (
        cfg.get("SCANNING_REANNOTATE_STATUS", "Reannotate").strip()
        if module == "quantifying" else ""
    )
    launched_status    = cfg.get("SCANNING_LAUNCHED_STATUS", "Running").strip()
    error_status       = cfg.get("PROCESSING_ERROR_STATUS", "Error").strip()
    profile            = cfg.get("DRAKKAR_PROFILE", "slurm").strip()
    ehio_conda_env     = cfg.get("EHIO_CONDA_ENV", "").strip()
    drakkar_conda_env  = cfg.get("DRAKKAR_CONDA_ENV", "").strip()

    def _bool_cfg(key: str) -> bool:
        return str(cfg.get(key) or "false").strip().lower() not in ("false", "0", "no", "")

    ppr_fraction  = _bool_cfg("DRAKKAR_PPR_FRACTION")
    ppr_nonpareil = _bool_cfg("DRAKKAR_PPR_NONPAREIL")

    if not (output_base and run_base):
        if verbose:
            missing = [k for k, v in {
                _OUTPUT_BASE_CFG[module]: output_base,
                _RUN_BASE_CFG:            run_base,
            }.items() if not v]
            print(f"  [{module}] skipped — missing config: {', '.join(missing)}", file=sys.stderr)
        return 0, 0

    sources = _sources(module, token, core, verbose=verbose)
    if not sources:
        if verbose:
            print(f"  [{module}] skipped — no database to scan.", file=sys.stderr)
        return 0, 0

    pending = _merge(module, sources, {
        "":           trigger_status,
        "resume":     resume_status,
        "rerun":      rerun_status,
        "reannotate": reannotate_status,
    }, verbose=verbose)

    found    = len(pending)
    launched = 0

    for batch in pending:
        batch_name     = batch.code
        do_rerun       = batch.kind == "rerun"
        do_resume      = batch.kind == "resume"
        do_reannotate  = batch.kind == "reannotate"

        if session_exists(batch_name):
            print(
                f"  [{module}] {batch_name}: screen session already exists — skipping.",
                file=sys.stderr,
            )
            continue

        if batch.record is None:
            print(f"  [{module}] {batch_name}: in ehi-core only.", file=sys.stderr)

        output_dir  = str(Path(output_base) / batch_name)
        run_dir     = str(Path(run_base)    / batch_name)
        script_path = Path(run_dir) / f"{batch_name}.sh"

        ref_flag = ""
        if module == "preprocessing":
            try:
                ref_flag = _resolve_preprocessing_ref_flag(
                    batch.reference_record(), token, verbose=verbose
                )
            except BatchLaunchError as exc:
                print(f"  [{module}] {batch_name}: ERROR — {exc}", file=sys.stderr)
                _set_status(module, batch, error_status, sources, dry_run=dry_run)
                continue
            ref_desc = ref_flag if ref_flag else "(no reference)"
            print(f"  [{module}] {batch_name}: reference flag → {ref_desc}", file=sys.stderr)

        boost_time   = batch.number(_BOOST_TIME_CFG[module])   if module in _BOOST_TIME_CFG   else None
        boost_memory = batch.number(_BOOST_MEMORY_CFG[module]) if module in _BOOST_MEMORY_CFG else None
        if boost_time or boost_memory:
            print(
                f"  [{module}] {batch_name}: boost time={boost_time} memory={boost_memory}",
                file=sys.stderr,
            )

        multicoverage = False
        if module == "binning":
            assembly_type = normalise_assembly_type(batch.value("EHI_ASB_BATCH_TYPE"))
            multicoverage = assembly_type == "multicoverage"
            print(
                f"  [{module}] {batch_name}: assembly type → {assembly_type or '(unset)'}",
                file=sys.stderr,
            )

        ani_threshold   = ""
        profiling_type  = ""
        annotation_type = "all"
        if module == "quantifying":
            ani_threshold   = batch.text("MAG_DMB_BATCH_ANI")
            profiling_type  = batch.text("MAG_DMB_BATCH_TYPE").lower()
            annotation_type = batch.text("MAG_DMB_BATCH_ANNOTATION_TYPE").lower() or "all"

        script_content = build_script_content(
            module, batch_name, run_dir, output_dir, profile, error_status, ref_flag,
            ehio_conda_env=ehio_conda_env,
            drakkar_conda_env=drakkar_conda_env,
            ppr_fraction=ppr_fraction,
            ppr_nonpareil=ppr_nonpareil,
            boost_time=boost_time,
            boost_memory=boost_memory,
            rerun=do_rerun,
            resume=do_resume,
            reannotate=do_reannotate,
            multicoverage=multicoverage,
            ani_threshold=ani_threshold,
            profiling_type=profiling_type,
            annotation_type=annotation_type,
        )

        if do_rerun:
            for _d in (run_dir, output_dir):
                if Path(_d).exists():
                    shutil.rmtree(_d)
                    print(f"  [{module}] {batch_name}: deleted {_d}", file=sys.stderr)

        if dry_run:
            # Write the script and generate the input TSV, but do not launch
            # the screen session and do not update any status.
            Path(run_dir).mkdir(parents=True, exist_ok=True)
            script_path.write_text(script_content, encoding="utf-8")
            script_path.chmod(0o755)
            print(f"  [{module}] {batch_name}: script written → {script_path}")
            if do_resume:
                print(f"  [{module}] {batch_name}: resume — skipping input file generation (using existing TSV)")
            elif do_reannotate:
                # A re-annotation never calls drakkar profiling, so it needs
                # neither the bins file nor the reads file that step reads.
                print(f"  [{module}] {batch_name}: reannotate — no profiling input files needed")
            else:
                try:
                    _generate_input_files(module, batch_name, run_dir, token, core_token)
                    written = output_dir if module == "ena" else Path(run_dir) / f"{batch_name}.tsv"
                    print(f"  [{module}] {batch_name}: input file written → {written}")
                except subprocess.CalledProcessError as exc:
                    print(
                        f"  [{module}] {batch_name}: WARNING — input generation failed "
                        f"(exit {exc.returncode}); check the batch's entries and token.",
                        file=sys.stderr,
                    )
            print(f"  [{module}] {batch_name}: dry-run — screen session not launched, status unchanged")
            launched += 1
            continue

        Path(run_dir).mkdir(parents=True, exist_ok=True)
        script_path.write_text(script_content, encoding="utf-8")
        script_path.chmod(0o755)

        launch_screen(batch_name, str(script_path), token=token, core_token=core_token)
        _set_status(module, batch, launched_status, sources, strict=True)
        print(f"  [{module}] {batch_name}: launched — script at {script_path}")
        launched += 1

    return found, launched


# ---------------------------------------------------------------------------
# Full scan across all modules
# ---------------------------------------------------------------------------

def run_scan(
    token: str,
    modules: list[str] | None = None,
    dry_run: bool = False,
    verbose: bool = False,
    core=None,
    core_token: str = "",
) -> int:
    """Scan all (or selected) modules and launch any pending batches.

    Returns the total number of batches launched.
    """
    if not screen_available():
        print("Error: 'screen' is not available on PATH.", file=sys.stderr)
        return 0

    targets        = modules or MODULES
    total_found    = 0
    total_launched = 0

    for module in targets:
        found, launched = scan_module(
            module, token, dry_run=dry_run, verbose=verbose, core=core, core_token=core_token,
        )
        total_found    += found
        total_launched += launched
        if found:
            print(f"  [{module}] {found} pending, {launched} launched.")
        elif verbose:
            print(f"  [{module}] no pending batches.")

    return total_launched
