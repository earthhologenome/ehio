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


def launch_screen(session_name: str, script_path: str) -> None:
    subprocess.run(
        ["screen", "-dmS", session_name, "bash", script_path],
        check=True,
    )


# ---------------------------------------------------------------------------
# Script builder
# ---------------------------------------------------------------------------

def build_script_content(
    module: str,
    batch_name: str,
    run_dir: str,
    output_dir: str,
    profile: str,
    error_status: str = "Error",
) -> str:
    """Return the full content of the .sh script written into run_dir.

    The script uses bash ERR trap to set the batch status to error_status
    in Airtable if any step fails.  AIRTABLE_TOKEN is inherited from the
    environment of the launching shell.

    run_dir    — /projects/ehi/data/RUN/{batch_code}  (samples.tsv, logs, .snakemake)
    output_dir — /projects/ehi/data/{PPR|ASB|DMB}/{batch_code}  (drakkar -o target)
    """
    if module not in DRAKKAR_CMD:
        raise ValueError(f"Unknown module: {module}")

    drakkar_sub = DRAKKAR_CMD[module]
    q = shlex.quote

    tsv_file = f"{run_dir}/{batch_name}.tsv"

    header = (
        "#!/usr/bin/env bash\n"
        f"# ehio-generated script — batch {batch_name} ({module})\n"
        "# Do not edit manually; re-run ehio scanning to regenerate.\n"
        "# AIRTABLE_TOKEN must be exported in the environment before launching.\n"
        "\n"
        "set -euo pipefail\n"
        "\n"
        "_on_error() {\n"
        f"    ehio set-status --module {module} --batch {q(batch_name)} --status {q(error_status)}\n"
        "}\n"
        "trap _on_error ERR\n"
        "\n"
        f"mkdir -p {q(run_dir)} {q(output_dir)}\n"
    )

    if module == "preprocessing":
        ref_env_file = f"{run_dir}/{batch_name}_ref.env"
        return header + (
            f"ehio preprocessing --input -b {q(batch_name)} -f {q(tsv_file)} --ref-flag-file {q(ref_env_file)}\n"
            f"source {q(ref_env_file)}\n"
            f"drakkar {drakkar_sub} -f {q(tsv_file)} -o {q(output_dir)} -p {q(profile)} $DRAKKAR_REF_FLAG\n"
        )

    if module == "binning":
        return header + (
            f"ehio binning --input -b {q(batch_name)} -f {q(tsv_file)}\n"
            f"drakkar {drakkar_sub} -f {q(tsv_file)} -o {q(output_dir)} -p {q(profile)}\n"
        )

    if module == "quantifying":
        bins_file = f"{run_dir}/{batch_name}_bins.txt"
        return header + (
            f"ehio quantifying --input -b {q(batch_name)} -f {q(tsv_file)} --bins-file {q(bins_file)}\n"
            f"drakkar {drakkar_sub} -B {q(bins_file)} -R {q(tsv_file)} -o {q(output_dir)} -p {q(profile)}\n"
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
        bins_path = str(Path(run_dir) / f"{batch_name}_bins.txt")
        cmd = [sys.executable, "-m", "ehio", "quantifying", "--input",
               "-b", batch_name, "-f", tsv_path, "--bins-file", bins_path]
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
    launched_status    = cfg.get("SCANNING_LAUNCHED_STATUS", "Running").strip()
    error_status       = cfg.get("PROCESSING_ERROR_STATUS", "Error").strip()
    profile            = cfg.get("DRAKKAR_PROFILE", "slurm").strip()

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
    pending = client.fetch_pending_batches(
        batch_table=batch_table,
        batch_status_field=batch_status_field,
        trigger_status=trigger_status,
    )

    found    = len(pending)
    launched = 0

    for record in pending:
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

        script_content = build_script_content(
            module, batch_name, run_dir, output_dir, profile, error_status,
        )

        if dry_run:
            # Write the script and generate the input TSV, but do not launch
            # the screen session and do not update the Airtable status.
            Path(run_dir).mkdir(parents=True, exist_ok=True)
            script_path.write_text(script_content, encoding="utf-8")
            script_path.chmod(0o755)
            print(f"  [{module}] {batch_name}: script written → {script_path}")
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

        launch_screen(batch_name, str(script_path))
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
