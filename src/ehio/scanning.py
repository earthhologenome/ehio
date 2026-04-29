"""Batch scanning and screen-session launcher for ehio."""

from __future__ import annotations

import re
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
# Config keys for batch code (name) and status fields, keyed by module
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

# Maps ehio module names to the corresponding drakkar subcommand
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


def launch_screen(session_name: str, command: str) -> None:
    subprocess.run(
        ["screen", "-dmS", session_name, "bash", "-c", command],
        check=True,
    )


# ---------------------------------------------------------------------------
# Command builder
# ---------------------------------------------------------------------------

def _q(s: str) -> str:
    """Wrap a string in double quotes, escaping any embedded quotes."""
    return '"' + s.replace('"', '\\"') + '"'


def build_command(
    module: str,
    batch_name: str,
    run_dir: str,
    output_dir: str,
    profile: str,
) -> str:
    """Return the shell command to run inside the screen session.

    run_dir    — working directory for the session: samples.tsv, .snakemake, logs
                 (/projects/ehi/data/RUN/{batch_code})
    output_dir — where drakkar writes its data output (-o flag)
                 (/projects/ehi/data/{PPR|ASB|DMB}/{batch_code})
    """
    if module not in DRAKKAR_CMD:
        raise ValueError(f"Unknown module: {module}")
    drakkar_sub = DRAKKAR_CMD[module]
    run  = _q(run_dir)
    out  = _q(output_dir)
    batch = _q(batch_name)
    prof  = _q(profile)

    if module == "preprocessing":
        sample_file = _q(f"{run_dir}/samples.tsv")
        return (
            f"mkdir -p {run} {out} && "
            f"ehio preprocessing --input -b {batch} -f {sample_file} && "
            f"drakkar {drakkar_sub} -f {sample_file} -o {out} -p {prof}"
        )

    if module == "binning":
        sample_file = _q(f"{run_dir}/samples.tsv")
        return (
            f"mkdir -p {run} {out} && "
            f"ehio binning --input -b {batch} -f {sample_file} && "
            f"drakkar {drakkar_sub} -f {sample_file} -o {out} -p {prof}"
        )

    if module == "quantifying":
        bins_file   = _q(f"{run_dir}/bins.txt")
        sample_file = _q(f"{run_dir}/samples.tsv")
        return (
            f"mkdir -p {run} {out} && "
            f"ehio quantifying --input -b {batch} -f {sample_file} --bins-file {bins_file} && "
            f"drakkar {drakkar_sub} -B {bins_file} -R {sample_file} -o {out} -p {prof}"
        )

    raise ValueError(f"Unknown module: {module}")


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
    trigger_status     = cfg.get("SCANNING_TRIGGER_STATUS", "ready").strip()
    launched_status    = cfg.get("SCANNING_LAUNCHED_STATUS", "running").strip()
    profile            = cfg.get("DRAKKAR_PROFILE", "slurm").strip()

    if not all([base_id, batch_table, batch_code_field, batch_status_field, output_base, run_base]):
        if verbose:
            missing = [k for k, v in {
                _PRIMARY_BASE[module]:       base_id,
                _BATCH_TABLE_KEY[module]:    batch_table,
                _BATCH_CODE_CFG[module]:     batch_code_field,
                _BATCH_STATUS_CFG[module]:   batch_status_field,
                _OUTPUT_BASE_CFG[module]:    output_base,
                _RUN_BASE_CFG:               run_base,
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

        output_dir = str(Path(output_base) / batch_name)
        run_dir    = str(Path(run_base)    / batch_name)
        command    = build_command(module, batch_name, run_dir, output_dir, profile)

        if dry_run:
            print(f"  [{module}] {batch_name}: would launch in screen session '{batch_name}'")
            print(f"    {command}")
            launched += 1
            continue

        launch_screen(batch_name, command)
        client.update_records(
            batch_table,
            [{"id": record["id"], "fields": {batch_status_field: launched_status}}],
        )
        print(f"  [{module}] {batch_name}: launched in screen session '{batch_name}'")
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
