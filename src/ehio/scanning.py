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
from ehio.airtable import AirtableClient

_PRIMARY_BASE = {
    "preprocessing": "EHI_BASE",
    "binning":       "EHI_BASE",
    "quantifying":   "MAG_BASE",
}
_BATCH_TABLE_KEY = {
    "preprocessing": "EHI_PPR_BATCH",
    "binning":       "EHI_ASB_BATCH",
    "quantifying":   "MAG_DMB_BATCH",
}
_BATCH_CODE_CFG = {
    "preprocessing": "EHI_PPR_BATCH_CODE",
    "binning":       "EHI_ASB_BATCH_CODE",
    "quantifying":   "MAG_DMB_BATCH_CODE",
}
_BATCH_STATUS_CFG = {
    "preprocessing": "EHI_PPR_BATCH_STATUS",
    "binning":       "EHI_ASB_BATCH_STATUS",
    "quantifying":   "MAG_DMB_BATCH_STATUS",
}
_OUTPUT_BASE_CFG = {
    "preprocessing": "EHI_PPR_OUTPUT_BASE",
    "binning":       "EHI_ASB_OUTPUT_BASE",
    "quantifying":   "MAG_DMB_OUTPUT_BASE",
}
_BOOST_TIME_CFG = {
    "preprocessing": "EHI_PPR_BATCH_BOOST_TIME",
    "binning":       "EHI_ASB_BATCH_BOOST_TIME",
    "quantifying":   "MAG_DMB_BATCH_BOOST_TIME",
}
_BOOST_MEMORY_CFG = {
    "preprocessing": "EHI_PPR_BATCH_BOOST_MEMORY",
    "binning":       "EHI_ASB_BATCH_BOOST_MEMORY",
    "quantifying":   "MAG_DMB_BATCH_BOOST_MEMORY",
}
_RUN_BASE_CFG = "RUN_BASE"

DRAKKAR_CMD = {
    "preprocessing": "preprocessing",
    "binning":       "cataloging",
    "quantifying":   "profiling",
}

MODULES = list(DRAKKAR_CMD)


# ---------------------------------------------------------------------------
# screen helpers
# ---------------------------------------------------------------------------

def screen_available() -> bool:
    return shutil.which("screen") is not None


def session_exists(name: str) -> bool:
    """Return True if a screen session with exactly this name is already running."""
    result = subprocess.run(["screen", "-ls"], capture_output=True, text=True)
    return bool(re.search(rf"\d+\.{re.escape(name)}(\t| )", result.stdout))


def launch_screen(session_name: str, script_path: str, token: str = "") -> None:
    env = {**os.environ}
    if token:
        env["AIRTABLE_TOKEN"] = token
    subprocess.run(
        ["screen", "-dmS", session_name, "bash", script_path],
        env=env,
        check=True,
    )


# ---------------------------------------------------------------------------
# Script builder
# ---------------------------------------------------------------------------

def _resolve_preprocessing_ref_flag(batch_record: dict, token: str, verbose: bool = False) -> str:
    """Return the drakkar reference flag for a preprocessing batch.

    Checks GENOME_ENTRY_URL_INDEXED first (-x, indexed tarball), then
    GENOME_ENTRY_URL_RAW (-g, plain fasta).  Returns '' if no reference
    is configured or the linked genome record cannot be found.
    """
    from ehio.airtable import AirtableClient

    def _dbg(msg: str) -> None:
        if verbose:
            print(f"    [ref] {msg}", file=sys.stderr)

    batch_ref_field    = str(cfg.get("EHI_PPR_BATCH_REFERENCE")  or "").strip()
    ehi_base_id        = str(cfg.get("EHI_BASE")                  or "").strip()
    genome_table       = str(cfg.get("EHI_GENOME")                or "").strip()
    genome_indexed_fld = str(cfg.get("EHI_GENOME_URL_INDEXED")    or "").strip()
    genome_raw_fld     = str(cfg.get("EHI_GENOME_URL_RAW")        or "").strip()

    if not batch_ref_field:
        _dbg("EHI_PPR_BATCH_REFERENCE not configured — no reference flag.")
        return ""

    ref_value = batch_record.get("fields", {}).get(batch_ref_field)
    _dbg(f"EHI_PPR_BATCH_REFERENCE field ({batch_ref_field}) raw value: {ref_value!r}")

    if isinstance(ref_value, list):
        ref_value = ref_value[0] if ref_value else None
    if not ref_value:
        _dbg("Reference field is empty — no reference flag.")
        return ""

    ref_rec_id = str(ref_value).strip()
    _dbg(f"Resolved reference value: {ref_rec_id!r}")

    if not (ehi_base_id and genome_table):
        print("    [ref] WARNING: EHI_BASE or EHI_GENOME not configured.", file=sys.stderr)
        return ""

    genome_client = AirtableClient(api_key=token, base_id=ehi_base_id)

    if ref_rec_id.startswith("rec"):
        # Linked-record field — fetch the genome record directly by its record ID.
        _dbg(f"Looking up genome record by ID: {ref_rec_id}")
        genome_rec = genome_client.fetch_record_by_id(genome_table, ref_rec_id)
    else:
        # Text/formula field containing the genome code (e.g. "G0001") — search by code.
        genome_code_fld = str(cfg.get("EHI_GENOME_CODE") or "").strip()
        _dbg(f"Looking up genome record by code: {ref_rec_id!r} in field {genome_code_fld}")
        if not genome_code_fld:
            print("    [ref] WARNING: EHI_GENOME_CODE not configured.", file=sys.stderr)
            return ""
        formula = f'{{{genome_code_fld}}} = "{ref_rec_id}"'
        records = genome_client._table(genome_table).all(formula=formula)
        genome_rec = records[0] if records else None

    if not genome_rec:
        print(
            f"    [ref] WARNING: genome record {ref_rec_id!r} not found in "
            f"EHI_GENOME ({genome_table}).",
            file=sys.stderr,
        )
        return ""

    genome_fields = genome_rec.get("fields", {})
    indexed_url = str(genome_fields.get(genome_indexed_fld, "") or "").strip()
    raw_url     = str(genome_fields.get(genome_raw_fld,     "") or "").strip()
    _dbg(f"EHI_GENOME_URL_INDEXED ({genome_indexed_fld}): {indexed_url!r}")
    _dbg(f"EHI_GENOME_URL_RAW     ({genome_raw_fld}):     {raw_url!r}")

    if indexed_url:
        _dbg(f"Using indexed reference: -x {indexed_url}")
        return f"-x {shlex.quote(indexed_url)}"
    if raw_url:
        _dbg(f"Using raw reference: -g {raw_url}")
        return f"-g {shlex.quote(raw_url)}"

    print(
        f"    [ref] WARNING: genome record {ref_rec_id!r} found but both "
        "EHI_GENOME_URL_INDEXED and EHI_GENOME_URL_RAW are empty.",
        file=sys.stderr,
    )
    return ""


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
    ani_threshold: str = "",
    profiling_type: str = "",
) -> str:
    """Return the full content of the .sh script written into run_dir.

    The script uses bash ERR trap to set the batch status to error_status
    in Airtable if any step fails.  AIRTABLE_TOKEN is inherited from the
    environment of the launching shell.

    run_dir    — /projects/ehi/data/RUN/{batch_code}  (samples.tsv, logs, .snakemake)
    output_dir — /projects/ehi/data/{PPR|ASB|DMB}/{batch_code}  (drakkar -o target)
    ref_flag   — pre-resolved '-x url' or '-g url' for preprocessing; '' otherwise
    """
    if module not in DRAKKAR_CMD:
        raise ValueError(f"Unknown module: {module}")

    drakkar_sub = DRAKKAR_CMD[module]
    q = shlex.quote

    tsv_file = f"{run_dir}/{batch_name}.tsv"

    out_file = f"{run_dir}/{batch_name}.out"
    err_file = f"{run_dir}/{batch_name}.err"

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
        "# AIRTABLE_TOKEN must be exported in the environment before launching.\n"
        "\n"
        "set -euo pipefail\n"
        f"exec >> {q(out_file)} 2>> {q(err_file)}\n"
        'echo ""\n'
        'echo "=== $(date \'+%Y-%m-%d %H:%M:%S\') ==="\n'
        'echo "=== $(date \'+%Y-%m-%d %H:%M:%S\') ===" >&2\n'
        "\n"
        + conda_block +
        "_EHIO_SUCCESS=0\n"
        "_on_exit() {\n"
        '    if [ "$_EHIO_SUCCESS" -ne 1 ]; then\n'
        f"        ehio set-status --module {module} --batch {q(batch_name)} --status {q(error_status)}\n"
        "    fi\n"
        "}\n"
        "trap _on_exit EXIT\n"
        "\n"
        f"mkdir -p {q(run_dir)} {q(output_dir)}\n"
        f"cd {q(output_dir)}\n"
    )

    time_part   = f" --time-multiplier {boost_time}"     if boost_time   and boost_time   > 1 else ""
    memory_part = f" --memory-multiplier {boost_memory}" if boost_memory and boost_memory > 1 else ""
    boost_parts = time_part + memory_part
    rerun_flag  = " --rerun" if rerun else ""

    if module == "preprocessing":
        ref_part       = f" {ref_flag}" if ref_flag else ""
        fraction_part  = " --fraction"  if ppr_fraction  else ""
        nonpareil_part = " --nonpareil" if ppr_nonpareil else ""
        input_step = "" if resume else f"ehio preprocessing --input -b {q(batch_name)} -f {q(tsv_file)}\n"
        return header + (
            input_step
            + f"{drakkar_prefix}drakkar {drakkar_sub} -f {q(tsv_file)} -o {q(output_dir)} -p {q(profile)}{ref_part}{fraction_part}{nonpareil_part}{boost_parts}\n"
            + f"ehio preprocessing --output -b {q(batch_name)} -l {q(output_dir)}{rerun_flag}\n"
            + "_EHIO_SUCCESS=1\n"
        )

    if module == "binning":
        input_step = "" if resume else f"ehio binning --input -b {q(batch_name)} -f {q(tsv_file)}\n"
        return header + (
            input_step
            + f"{drakkar_prefix}drakkar {drakkar_sub} -f {q(tsv_file)} -o {q(output_dir)} -p {q(profile)}{boost_parts}\n"
            + f"ehio binning --output -b {q(batch_name)} -l {q(output_dir)}{rerun_flag}\n"
            + "_EHIO_SUCCESS=1\n"
        )

    if module == "quantifying":
        mags_file    = f"{run_dir}/{batch_name}_mags.tsv"
        reads_file   = f"{run_dir}/{batch_name}_reads.tsv"
        quality_file = f"{run_dir}/{batch_name}_quality.tsv"
        ani_part     = f" -a {q(ani_threshold)}"   if ani_threshold  else ""
        type_part    = f" -t {q(profiling_type)}"  if profiling_type else ""
        input_step   = "" if resume else (
            f"ehio quantifying --input -b {q(batch_name)}"
            f" --mags-file {q(mags_file)}"
            f" --reads-file {q(reads_file)}"
            f" --quality-file {q(quality_file)}\n"
        )
        derep_genomes_dir   = f"{output_dir}/profiling_genomes/drep/dereplicated_genomes"
        annotation_file     = f"{run_dir}/{batch_name}_annotation.tsv"
        qfy_status          = str(cfg.get("QUANTIFYING_RUNNING_STATUS")  or "Quantifying").strip()
        ann_tax_status      = str(cfg.get("ANNOTATING_TAXONOMY_STATUS")  or "Annotating taxonomy").strip()
        ann_func_status     = str(cfg.get("ANNOTATING_FUNCTION_STATUS")  or "Annotating function").strip()
        return header + (
            f"ehio set-status --module quantifying -b {q(batch_name)} --status {q(qfy_status)}\n"
            + input_step
            + f"{drakkar_prefix}drakkar {drakkar_sub} -B {q(mags_file)} -R {q(reads_file)}{ani_part}{type_part} -q {q(quality_file)} -o {q(output_dir)} -p {q(profile)}{boost_parts}\n"
            + f"ehio quantifying --output -b {q(batch_name)} -l {q(output_dir)}{rerun_flag}\n"
            + f"ehio set-status --module quantifying -b {q(batch_name)} --status {q(ann_tax_status)}\n"
            + f"{drakkar_prefix}drakkar annotating -b {q(derep_genomes_dir)} -p {q(profile)}{boost_parts} --annotation-type taxonomy\n"
            + f"ehio set-status --module quantifying -b {q(batch_name)} --status {q(ann_func_status)}\n"
            + f"ehio annotating --input -b {q(batch_name)} -f {q(annotation_file)} -d {q(derep_genomes_dir)}\n"
            + f"{drakkar_prefix}drakkar annotating -B {q(annotation_file)} -p {q(profile)}{boost_parts} --annotation-type function\n"
            + f"ehio annotating --output -b {q(batch_name)} -l {q(output_dir)}{rerun_flag}\n"
            + "_EHIO_SUCCESS=1\n"
        )

    raise ValueError(f"Unknown module: {module}")


# ---------------------------------------------------------------------------
# Input-file generator (used by dry-run)
# ---------------------------------------------------------------------------

def _generate_input_files(module: str, batch_name: str, run_dir: str, token: str) -> None:
    """Run 'ehio <module> --input' to write the TSV (and bins file) into run_dir.

    Uses the same Python interpreter so the installed package is always found.
    The Airtable token is injected via the environment.
    """
    tsv_path = str(Path(run_dir) / f"{batch_name}.tsv")
    env      = {**os.environ, "AIRTABLE_TOKEN": token}

    if module == "preprocessing":
        cmd = [sys.executable, "-m", "ehio", "preprocessing", "--input",
               "-b", batch_name, "-f", tsv_path]
    elif module == "binning":
        cmd = [sys.executable, "-m", "ehio", "binning", "--input",
               "-b", batch_name, "-f", tsv_path]
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
# Per-module scan
# ---------------------------------------------------------------------------

def scan_module(
    module: str,
    token: str,
    dry_run: bool = False,
    verbose: bool = False,
) -> tuple[int, int]:
    """Scan one module's batch table for pending batches and launch them.

    Returns (found, launched).
    """
    base_id            = cfg.get(_PRIMARY_BASE[module], "").strip()
    batch_table        = cfg.get(_BATCH_TABLE_KEY[module], "").strip()
    batch_code_field   = cfg.get(_BATCH_CODE_CFG[module], "").strip()
    batch_status_field = cfg.get(_BATCH_STATUS_CFG[module], "").strip()
    output_base        = cfg.get(_OUTPUT_BASE_CFG[module], "").strip()
    run_base           = cfg.get(_RUN_BASE_CFG, "").strip()
    trigger_status     = cfg.get("SCANNING_TRIGGER_STATUS", "Ready").strip()
    resume_status      = cfg.get("SCANNING_RESUME_STATUS",  "Resume").strip()
    rerun_status       = cfg.get("SCANNING_RERUN_STATUS",   "Rerun").strip()
    launched_status    = cfg.get("SCANNING_LAUNCHED_STATUS", "Running").strip()
    error_status       = cfg.get("PROCESSING_ERROR_STATUS", "Error").strip()
    profile            = cfg.get("DRAKKAR_PROFILE", "slurm").strip()
    ehio_conda_env     = cfg.get("EHIO_CONDA_ENV", "").strip()
    drakkar_conda_env  = cfg.get("DRAKKAR_CONDA_ENV", "").strip()

    def _bool_cfg(key: str) -> bool:
        return str(cfg.get(key) or "false").strip().lower() not in ("false", "0", "no", "")

    ppr_fraction  = _bool_cfg("DRAKKAR_PPR_FRACTION")
    ppr_nonpareil = _bool_cfg("DRAKKAR_PPR_NONPAREIL")

    if not all([base_id, batch_table, batch_code_field, batch_status_field, output_base, run_base]):
        if verbose:
            missing = [k for k, v in {
                _PRIMARY_BASE[module]:     base_id,
                _BATCH_TABLE_KEY[module]:  batch_table,
                _BATCH_CODE_CFG[module]:   batch_code_field,
                _BATCH_STATUS_CFG[module]: batch_status_field,
                _OUTPUT_BASE_CFG[module]:  output_base,
                _RUN_BASE_CFG:             run_base,
            }.items() if not v]
            print(
                f"  [{module}] skipped — missing config: {', '.join(missing)}",
                file=sys.stderr,
            )
        return 0, 0

    client = AirtableClient(api_key=token, base_id=base_id)

    def _fetch(status: str) -> list:
        return client.fetch_pending_batches(
            batch_table=batch_table,
            batch_status_field=batch_status_field,
            trigger_status=status,
        ) if status else []

    # (record, do_rerun, do_resume)
    pending: list[tuple[dict, bool, bool]] = (
        [(r, False, False) for r in _fetch(trigger_status)]
        + [(r, False, True)  for r in _fetch(resume_status)]
        + [(r, True,  False) for r in _fetch(rerun_status)]
    )

    found    = len(pending)
    launched = 0

    for record, do_rerun, do_resume in pending:
        batch_name = str(record.get("fields", {}).get(batch_code_field, "")).strip()
        if not batch_name:
            continue

        if session_exists(batch_name):
            print(
                f"  [{module}] {batch_name}: screen session already exists — skipping.",
                file=sys.stderr,
            )
            continue

        output_dir  = str(Path(output_base) / batch_name)
        run_dir     = str(Path(run_base)    / batch_name)
        script_path = Path(run_dir) / f"{batch_name}.sh"

        ref_flag = ""
        if module == "preprocessing":
            ref_flag = _resolve_preprocessing_ref_flag(record, token, verbose=verbose)
            ref_desc = ref_flag if ref_flag else "(no reference)"
            print(f"  [{module}] {batch_name}: reference flag → {ref_desc}", file=sys.stderr)

        def _read_boost(cfg_dict: dict) -> int | None:
            field_id = str(cfg.get(cfg_dict[module]) or "").strip()
            if not field_id:
                return None
            raw = record.get("fields", {}).get(field_id)
            try:
                return int(raw) if raw is not None else None
            except (TypeError, ValueError):
                return None

        boost_time   = _read_boost(_BOOST_TIME_CFG)
        boost_memory = _read_boost(_BOOST_MEMORY_CFG)
        if boost_time or boost_memory:
            print(
                f"  [{module}] {batch_name}: boost time={boost_time} memory={boost_memory}",
                file=sys.stderr,
            )

        ani_threshold  = ""
        profiling_type = ""
        if module == "quantifying":
            ani_field  = str(cfg.get("MAG_DMB_BATCH_ANI")  or "").strip()
            type_field = str(cfg.get("MAG_DMB_BATCH_TYPE") or "").strip()
            if ani_field:
                raw = record.get("fields", {}).get(ani_field)
                if raw is not None:
                    ani_threshold = str(raw).strip()
            if type_field:
                raw = record.get("fields", {}).get(type_field)
                if raw:
                    profiling_type = str(raw).strip().lower()

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
            ani_threshold=ani_threshold,
            profiling_type=profiling_type,
        )

        if do_rerun:
            for _d in (run_dir, output_dir):
                if Path(_d).exists():
                    shutil.rmtree(_d)
                    print(f"  [{module}] {batch_name}: deleted {_d}", file=sys.stderr)

        if dry_run:
            # Write the script and generate the input TSV, but do not launch
            # the screen session and do not update the Airtable status.
            Path(run_dir).mkdir(parents=True, exist_ok=True)
            script_path.write_text(script_content, encoding="utf-8")
            script_path.chmod(0o755)
            print(f"  [{module}] {batch_name}: script written → {script_path}")
            if do_resume:
                print(f"  [{module}] {batch_name}: resume — skipping input file generation (using existing TSV)")
            else:
                try:
                    _generate_input_files(module, batch_name, run_dir, token)
                    tsv_path = Path(run_dir) / f"{batch_name}.tsv"
                    print(f"  [{module}] {batch_name}: input file written → {tsv_path}")
                except subprocess.CalledProcessError as exc:
                    print(
                        f"  [{module}] {batch_name}: WARNING — input generation failed "
                        f"(exit {exc.returncode}); check Airtable fields and token.",
                        file=sys.stderr,
                    )
            print(f"  [{module}] {batch_name}: dry-run — screen session not launched, Airtable status unchanged")
            launched += 1
            continue

        Path(run_dir).mkdir(parents=True, exist_ok=True)
        script_path.write_text(script_content, encoding="utf-8")
        script_path.chmod(0o755)

        launch_screen(batch_name, str(script_path), token=token)
        client.update_records(
            batch_table,
            [{"id": record["id"], "fields": {batch_status_field: launched_status}}],
        )
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
        found, launched = scan_module(module, token, dry_run=dry_run, verbose=verbose)
        total_found    += found
        total_launched += launched
        if found:
            print(f"  [{module}] {found} pending, {launched} launched.")
        elif verbose:
            print(f"  [{module}] no pending batches.")

    return total_launched
