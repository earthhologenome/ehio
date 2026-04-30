"""Command-line interface for ehio."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from ehio import __version__
from ehio import config as cfg

ERROR = "\033[1;31m"
INFO  = "\033[1;34m"
RESET = "\033[0m"

_PRIMARY_BASE = {
    "PREPROCESSING": "EHI_BASE",
    "BINNING":       "EHI_BASE",
    "QUANTIFYING":   "MAG_BASE",
}
_SECONDARY_BASE = {
    "BINNING": "MAG_BASE",
}
_BATCH_TABLE_KEY = {
    "PREPROCESSING": "EHI_PPR_BATCH",
    "BINNING":       "EHI_ASB_BATCH",
    "QUANTIFYING":   "MAG_DMB_BATCH",
}
_ENTRY_TABLE_KEY = {
    "PREPROCESSING": "EHI_PPR_ENTRY",
    "BINNING":       "EHI_ASB_ENTRY",
    "QUANTIFYING":   "MAG_DMB_ENTRY",
}


def _die(msg: str) -> None:
    print(f"{ERROR}Error:{RESET} {msg}", file=sys.stderr)
    sys.exit(1)


def _info(msg: str) -> None:
    print(f"{INFO}Info:{RESET} {msg}", file=sys.stderr)


def _conf(args: argparse.Namespace, cli_attr: str, config_key: str, required: bool = False) -> str:
    """Return the first non-empty value from: CLI flag → config file → ''."""
    value = (getattr(args, cli_attr, None) or "").strip()
    if not value:
        value = str(cfg.get(config_key) or "").strip()
    if required and not value:
        flag = "--" + cli_attr.replace("_", "-")
        _die(
            f"{flag} is not set. "
            f"Provide it as a flag or set {config_key} in the config (ehio config --edit)."
        )
    return value


def _resolve_token(args: argparse.Namespace) -> str:
    token = (getattr(args, "airtable_token", None) or "").strip()
    if not token:
        token = os.environ.get("AIRTABLE_TOKEN", "").strip()
    if not token:
        _die(
            "Airtable token not found. "
            "Provide --airtable-token or export AIRTABLE_TOKEN."
        )
    return token


def _require_cfg(key: str) -> str:
    value = str(cfg.get(key) or "").strip()
    if not value:
        _die(f"Config key '{key}' is not set. Run: ehio config --edit")
    return value


# ---------------------------------------------------------------------------
# preprocessing
# ---------------------------------------------------------------------------

def cmd_preprocessing(args: argparse.Namespace) -> int:
    if args.input:
        return _run_preprocessing_input(args)
    return _run_preprocessing_output(args)


def _run_preprocessing_input(args: argparse.Namespace) -> int:
    """Fetch batch+entries from Airtable and write a drakkar sample TSV."""
    from ehio.airtable import AirtableClient
    from ehio.drakkar import write_sample_file

    token       = _resolve_token(args)
    base_id     = _require_cfg("EHI_BASE")
    batch_table = _require_cfg("EHI_PPR_BATCH")
    entry_table = _require_cfg("EHI_PPR_ENTRY")

    batch_code_field  = _require_cfg("EHI_PPR_BATCH_CODE")
    batch_ref_field   = str(cfg.get("EHI_PPR_BATCH_REFERENCE") or "").strip()
    entry_batch_field = _require_cfg("EHI_PPR_ENTRY_BATCH")
    entry_code_field  = _require_cfg("EHI_PPR_ENTRY_CODE")
    reads1_field      = _require_cfg("EHI_PPR_ENTRY_RAW_FILE_FORWARD")
    reads2_field      = _require_cfg("EHI_PPR_ENTRY_RAW_FILE_REVERSE")

    _info(f"Looking up batch '{args.batch}' in Airtable...")
    client = AirtableClient(api_key=token, base_id=base_id)
    batch_record, entries = client.fetch_batch_and_entries(
        batch_table=batch_table,
        batch_code_field=batch_code_field,
        batch_code=args.batch,
        entry_table=entry_table,
        entry_batch_field=entry_batch_field,
    )

    if batch_record is None:
        _die(f"Batch '{args.batch}' not found in {batch_table}.")
    _info(f"Found {len(entries)} entries for batch '{args.batch}'.")
    if not entries:
        _die(f"No entries found for batch '{args.batch}'.")

    # Resolve reference genome: batch field may hold a URL directly or a linked
    # record ID pointing to GENOME_ENTRY.  We try the field value first; if it
    # looks like a record ID (starts with "rec") we follow the link.
    reference: str | None = None
    if batch_ref_field:
        ref_value = batch_record.get("fields", {}).get(batch_ref_field)
        if isinstance(ref_value, list):
            ref_value = ref_value[0] if ref_value else None
        if ref_value:
            ref_str = str(ref_value).strip()
            if ref_str.startswith("rec"):
                # Linked record — fetch URL from GENOME_ENTRY
                genome_table   = str(cfg.get("GENOME_ENTRY") or "").strip()
                genome_url_fld = str(cfg.get("GENOME_ENTRY_URL_INDEXED") or "").strip()
                if genome_table and genome_url_fld:
                    genome_base_id = str(cfg.get("GENOME_BASE") or "").strip()
                    if genome_base_id:
                        genome_client = AirtableClient(api_key=token, base_id=genome_base_id)
                        genome_rec = genome_client.fetch_record_by_id(genome_table, ref_str)
                        if genome_rec:
                            reference = str(
                                genome_rec.get("fields", {}).get(genome_url_fld, "")
                            ).strip() or None
            else:
                reference = ref_str or None

    out_path = Path(args.sample_file)
    n = write_sample_file(
        entries,
        out_path,
        sample_field=entry_code_field,
        reads1_field=reads1_field,
        reads2_field=reads2_field,
        reference=reference,
    )
    _info(f"Wrote {n} samples to {out_path}")
    if reference:
        _info(f"Reference genome: {reference}")
    return 0


def _run_preprocessing_output(args: argparse.Namespace) -> int:
    """Parse QC metadata from drakkar output, update Airtable, transfer files."""
    from ehio.airtable import AirtableClient
    from ehio.metadata import (
        collect_preprocessing_metadata,
        build_entry_update,
        PREPROCESSING_METRIC_KEYS,
    )
    from ehio.transfer import upload_with_lftp

    token       = _resolve_token(args)
    base_id     = _require_cfg("EHI_BASE")
    batch_table = _require_cfg("EHI_PPR_BATCH")
    entry_table = _require_cfg("EHI_PPR_ENTRY")

    batch_code_field  = _require_cfg("EHI_PPR_BATCH_CODE")
    entry_batch_field = _require_cfg("EHI_PPR_ENTRY_BATCH")
    entry_code_field  = _require_cfg("EHI_PPR_ENTRY_CODE")

    local_root = Path(args.local_dir).resolve()
    if not local_root.is_dir():
        _die(f"Local directory not found: {local_root}")

    # Fetch batch + entries
    _info(f"Looking up batch '{args.batch}' in Airtable...")
    client = AirtableClient(api_key=token, base_id=base_id)
    batch_record, entries = client.fetch_batch_and_entries(
        batch_table=batch_table,
        batch_code_field=batch_code_field,
        batch_code=args.batch,
        entry_table=entry_table,
        entry_batch_field=entry_batch_field,
    )
    if batch_record is None:
        _die(f"Batch '{args.batch}' not found.")
    if not entries:
        _die(f"No entries found for batch '{args.batch}'.")

    # Build field_map: metric_key → field_id (resolved from config)
    field_map: dict[str, str] = {}
    for metric_key, config_key in PREPROCESSING_METRIC_KEYS.items():
        fld_id = str(cfg.get(config_key) or "").strip()
        if fld_id:
            field_map[metric_key] = fld_id

    # Parse QC metadata and build Airtable update payloads
    updates: list[dict] = []
    for entry in entries:
        sample = str(entry.get("fields", {}).get(entry_code_field, "")).strip()
        if not sample:
            continue
        metrics = collect_preprocessing_metadata(sample, local_root)
        payload = build_entry_update(entry["id"], metrics, field_map)
        if payload["fields"]:
            updates.append(payload)

    if updates:
        _info(f"Updating {len(updates)} entry records in Airtable...")
        client.update_records(entry_table, updates)
        _info("Airtable update complete.")
    else:
        _info("No QC metrics found to update.")

    # Transfer final preprocessed files via lftp
    final_dir = local_root / "preprocessing" / "final"
    if not final_dir.is_dir():
        _info(f"Final output directory not found ({final_dir}); skipping transfer.")
        return 0

    host     = _conf(args, "host",     "SFTP_HOST",     required=True)
    user     = _conf(args, "user",     "SFTP_USER",     required=True)
    port     = int(_conf(args, "port", "SFTP_PORT") or 22)
    identity = _conf(args, "identity", "SFTP_IDENTITY") or None

    remote_base = _conf(args, "remote_dir", "SFTP_REMOTE_BASE", required=True)
    remote_dir = f"{remote_base.rstrip('/')}/PPR/{args.batch}"

    _info(f"Transferring {final_dir} → {user}@{host}:{remote_dir} ...")
    upload_with_lftp(
        local_dir=final_dir,
        remote_dir=remote_dir,
        host=host,
        user=user,
        port=port,
        identity=identity,
        verbose=getattr(args, "verbose", False),
    )
    _info("Transfer complete.")

    # Mark the batch as done
    done_status       = str(cfg.get("PROCESSING_DONE_STATUS") or "Done").strip()
    batch_status_field = _require_cfg("EHI_PPR_BATCH_STATUS")
    client.update_records(
        batch_table,
        [{"id": batch_record["id"], "fields": {batch_status_field: done_status}}],
    )
    _info(f"Batch '{args.batch}' status → '{done_status}'.")
    return 0


# ---------------------------------------------------------------------------
# binning
# ---------------------------------------------------------------------------

def cmd_binning(args: argparse.Namespace) -> int:
    if args.input:
        return _run_binning_input(args)
    return _run_binning_output(args)


def _run_binning_input(args: argparse.Namespace) -> int:
    from ehio.airtable import AirtableClient
    from ehio.drakkar import write_sample_file

    token       = _resolve_token(args)
    base_id     = _require_cfg("EHI_BASE")
    batch_table = _require_cfg("EHI_ASB_BATCH")
    entry_table = _require_cfg("EHI_ASB_ENTRY")

    batch_code_field  = _require_cfg("EHI_ASB_BATCH_CODE")
    entry_batch_field = _require_cfg("EHI_ASB_ENTRY_BATCH")
    entry_code_field  = _require_cfg("EHI_ASB_ENTRY_CODE")

    reads1_field = _conf(args, "reads1_field", "EHI_ASB_ENTRY_READS1", required=True)
    reads2_field = _conf(args, "reads2_field", "EHI_ASB_ENTRY_READS2", required=True)

    _info(f"Looking up batch '{args.batch}'...")
    client = AirtableClient(api_key=token, base_id=base_id)
    batch_record, entries = client.fetch_batch_and_entries(
        batch_table=batch_table,
        batch_code_field=batch_code_field,
        batch_code=args.batch,
        entry_table=entry_table,
        entry_batch_field=entry_batch_field,
    )
    if batch_record is None:
        _die(f"Batch '{args.batch}' not found.")
    _info(f"Found {len(entries)} entries for batch '{args.batch}'.")
    if not entries:
        _die(f"No entries found for batch '{args.batch}'.")

    out_path = Path(args.sample_file)
    n = write_sample_file(
        entries,
        out_path,
        sample_field=entry_code_field,
        reads1_field=reads1_field,
        reads2_field=reads2_field,
    )
    _info(f"Wrote {n} samples to {out_path}")
    return 0


def _run_binning_output(args: argparse.Namespace) -> int:
    _info("Binning output wiring is not yet implemented.")
    return 1


# ---------------------------------------------------------------------------
# quantifying
# ---------------------------------------------------------------------------

def cmd_quantifying(args: argparse.Namespace) -> int:
    if args.input:
        return _run_quantifying_input(args)
    return _run_quantifying_output(args)


def _run_quantifying_input(args: argparse.Namespace) -> int:
    from ehio.airtable import AirtableClient
    from ehio.drakkar import write_bins_file, write_sample_file

    token       = _resolve_token(args)
    base_id     = _require_cfg("MAG_BASE")
    batch_table = _require_cfg("MAG_DMB_BATCH")
    entry_table = _require_cfg("MAG_DMB_ENTRY")

    batch_code_field  = _require_cfg("MAG_DMB_BATCH_CODE")
    entry_batch_field = _require_cfg("MAG_DMB_ENTRY_BATCH")
    entry_code_field  = _require_cfg("MAG_DMB_ENTRY_CODE")

    bins_field   = _conf(args, "bins_field",   "MAG_DMB_ENTRY_BINS",   required=True)
    reads1_field = _conf(args, "reads1_field", "MAG_DMB_ENTRY_READS1", required=True)
    reads2_field = _conf(args, "reads2_field", "MAG_DMB_ENTRY_READS2", required=True)

    _info(f"Looking up batch '{args.batch}'...")
    client = AirtableClient(api_key=token, base_id=base_id)
    batch_record, entries = client.fetch_batch_and_entries(
        batch_table=batch_table,
        batch_code_field=batch_code_field,
        batch_code=args.batch,
        entry_table=entry_table,
        entry_batch_field=entry_batch_field,
    )
    if batch_record is None:
        _die(f"Batch '{args.batch}' not found.")
    _info(f"Found {len(entries)} entries for batch '{args.batch}'.")
    if not entries:
        _die(f"No entries found for batch '{args.batch}'.")

    bins_path = Path(args.bins_file)
    n_bins = write_bins_file(entries, bins_path, bins_field=bins_field)
    _info(f"Wrote {n_bins} bin paths to {bins_path}")

    reads_path = Path(args.sample_file)
    n_reads = write_sample_file(
        entries,
        reads_path,
        sample_field=entry_code_field,
        reads1_field=reads1_field,
        reads2_field=reads2_field,
    )
    _info(f"Wrote {n_reads} read entries to {reads_path}")
    return 0


def _run_quantifying_output(args: argparse.Namespace) -> int:
    _info("Quantifying output wiring is not yet implemented.")
    return 1


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ehio",
        description="ehio: bridge between Airtable metadata and Drakkar workflows.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"ehio {__version__}")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    def _add_mode(p: argparse.ArgumentParser) -> None:
        mode = p.add_mutually_exclusive_group(required=True)
        mode.add_argument("--input", action="store_true",
            help="Input mode: fetch records from Airtable and write drakkar input files.")
        mode.add_argument("--output", action="store_true",
            help="Output mode: collect metadata, update Airtable, transfer files.")

    def _add_batch(p: argparse.ArgumentParser) -> None:
        p.add_argument("--batch", "-b", required=True, metavar="BATCH",
            help="Batch code used to look up the batch record in Airtable.")

    def _add_token(p: argparse.ArgumentParser) -> None:
        p.add_argument("--airtable-token", metavar="TOKEN",
            help="Airtable personal access token. Overrides $AIRTABLE_TOKEN.")

    def _add_verbose(p: argparse.ArgumentParser) -> None:
        p.add_argument("--verbose", "-v", action="store_true",
            help="Print additional progress details.")

    def _add_sftp_overrides(p: argparse.ArgumentParser) -> None:
        g = p.add_argument_group("Output / transfer options")
        g.add_argument("--host",     metavar="HOST", help="SFTP host (overrides SFTP_HOST).")
        g.add_argument("--user", "-u", metavar="USER", help="SFTP username (overrides SFTP_USER).")
        g.add_argument("--port",     metavar="PORT", help="SFTP port (overrides SFTP_PORT).")
        g.add_argument("--identity", "-k", metavar="KEY", help="SSH private key path.")
        g.add_argument("--local-dir", "-l", default=os.getcwd(), metavar="DIR",
            help="Local drakkar output directory. Default: current directory.")
        g.add_argument("--remote-dir", "-r", metavar="DIR",
            help="Remote base directory for file transfer.")

    # ------------------------------------------------------------------
    # preprocessing
    # ------------------------------------------------------------------
    p_pre = sub.add_parser(
        "preprocessing",
        help="Input/output for the preprocessing workflow.",
        description=(
            "Input mode:  fetch batch + entries from EHI_BASE/EHI_PPR_* tables,\n"
            "             resolve the reference genome, and write a drakkar sample TSV.\n"
            "Output mode: parse QC stats from drakkar output, update EHI_PPR_ENTRY,\n"
            "             and transfer preprocessing/final/ via lftp."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_mode(p_pre)
    _add_batch(p_pre)
    _add_token(p_pre)
    _add_verbose(p_pre)
    p_pre.add_argument("--sample-file", "-f", default="samples.tsv", metavar="PATH",
        help="Output sample info TSV for drakkar (input mode). Default: samples.tsv.")
    _add_sftp_overrides(p_pre)
    p_pre.set_defaults(func=cmd_preprocessing)

    # ------------------------------------------------------------------
    # binning
    # ------------------------------------------------------------------
    p_bin = sub.add_parser(
        "binning",
        help="Input/output for the assembly and binning workflow.",
        description=(
            "Input mode:  fetch batch + entries from EHI_BASE/EHI_ASB_* tables\n"
            "             and write a drakkar sample TSV.\n"
            "Output mode: transfer bins via lftp and update EHI_ASB_ENTRY (not yet implemented)."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_mode(p_bin)
    _add_batch(p_bin)
    _add_token(p_bin)
    _add_verbose(p_bin)
    p_bin.add_argument("--sample-file", "-f", default="samples.tsv", metavar="PATH",
        help="Output sample info TSV for drakkar (input mode). Default: samples.tsv.")
    p_bin.add_argument("--reads1-field", metavar="FIELD",
        help="Field ID for preprocessed R1 reads URL (overrides EHI_ASB_ENTRY_READS1).")
    p_bin.add_argument("--reads2-field", metavar="FIELD",
        help="Field ID for preprocessed R2 reads URL (overrides EHI_ASB_ENTRY_READS2).")
    _add_sftp_overrides(p_bin)
    p_bin.set_defaults(func=cmd_binning)

    # ------------------------------------------------------------------
    # quantifying
    # ------------------------------------------------------------------
    p_qnt = sub.add_parser(
        "quantifying",
        help="Input/output for the dereplication and mapping workflow.",
        description=(
            "Input mode:  fetch batch + entries from MAG_BASE/MAG_DMB_* tables\n"
            "             and write a bins file and a reads sample file.\n"
            "Output mode: transfer coverage tables via lftp (not yet implemented)."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_mode(p_qnt)
    _add_batch(p_qnt)
    _add_token(p_qnt)
    _add_verbose(p_qnt)
    p_qnt.add_argument("--sample-file", "-f", default="samples.tsv", metavar="PATH",
        help="Output reads sample file for drakkar (input mode). Default: samples.tsv.")
    p_qnt.add_argument("--bins-file", default="bins.txt", metavar="PATH",
        help="Output bins path file for drakkar (input mode). Default: bins.txt.")
    p_qnt.add_argument("--bins-field", metavar="FIELD",
        help="Field ID for bin file paths (overrides MAG_DMB_ENTRY_BINS).")
    p_qnt.add_argument("--reads1-field", metavar="FIELD",
        help="Field ID for R1 reads URL (overrides MAG_DMB_ENTRY_READS1).")
    p_qnt.add_argument("--reads2-field", metavar="FIELD",
        help="Field ID for R2 reads URL (overrides MAG_DMB_ENTRY_READS2).")
    _add_sftp_overrides(p_qnt)
    p_qnt.set_defaults(func=cmd_quantifying)

    # ------------------------------------------------------------------
    # scanning
    # ------------------------------------------------------------------
    p_scan = sub.add_parser(
        "scanning",
        help="Scan Airtable batch tables for pending batches and launch them in screen sessions.",
        description=(
            "Queries each configured batch table for records whose status matches\n"
            "SCANNING_TRIGGER_STATUS, then for each pending batch:\n"
            "  1. Creates a screen session named after the batch.\n"
            "  2. Runs: ehio <module> --input -b BATCH && drakkar <cmd> ...\n"
            "  3. Updates the batch record status to SCANNING_LAUNCHED_STATUS.\n\n"
            "Already-running screen sessions are skipped automatically."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p_scan.add_argument("--module", "-m",
        choices=["preprocessing", "binning", "quantifying"],
        metavar="MODULE",
        help="Scan only this module. Default: scan all three.")
    p_scan.add_argument("--airtable-token", metavar="TOKEN",
        help="Airtable personal access token. Overrides $AIRTABLE_TOKEN.")
    p_scan.add_argument("--dry-run", action="store_true",
        help="Print what would be launched without creating any screen sessions.")
    p_scan.add_argument("--verbose", "-v", action="store_true",
        help="Print details for modules with no pending batches too.")
    p_scan.set_defaults(func=cmd_scanning)

    # ------------------------------------------------------------------
    # set-status
    # ------------------------------------------------------------------
    p_ss = sub.add_parser(
        "set-status",
        help="Update the status of a batch record in Airtable.",
        description=(
            "Directly sets the status field of a batch record.\n"
            "Called automatically by the .sh error trap on drakkar failure;\n"
            "can also be used manually to correct a status."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p_ss.add_argument("--module", "-m", required=True,
        choices=["preprocessing", "binning", "quantifying"],
        help="Module whose batch table to update.")
    p_ss.add_argument("--batch", "-b", required=True, metavar="BATCH",
        help="Batch code to look up.")
    p_ss.add_argument("--status", "-s", required=True, metavar="STATUS",
        help="New status value to write (e.g. Error, Done, Ready).")
    p_ss.add_argument("--airtable-token", metavar="TOKEN",
        help="Airtable personal access token. Overrides $AIRTABLE_TOKEN.")
    p_ss.set_defaults(func=cmd_set_status)

    # ------------------------------------------------------------------
    # config
    # ------------------------------------------------------------------
    p_cfg = sub.add_parser("config", help="View or edit the ehio config file.")
    cfg_group = p_cfg.add_mutually_exclusive_group(required=True)
    cfg_group.add_argument("--view", action="store_true", help="Print the config file.")
    cfg_group.add_argument("--edit", action="store_true", help="Open the config file in a terminal editor.")
    p_cfg.set_defaults(func=cmd_config)

    return parser


# ---------------------------------------------------------------------------
# set-status  (called from the .sh error trap, or manually)
# ---------------------------------------------------------------------------

_SET_STATUS_CFG = {
    "preprocessing": ("EHI_BASE", "EHI_PPR_BATCH", "EHI_PPR_BATCH_CODE", "EHI_PPR_BATCH_STATUS"),
    "binning":       ("EHI_BASE", "EHI_ASB_BATCH", "EHI_ASB_BATCH_CODE", "EHI_ASB_BATCH_STATUS"),
    "quantifying":   ("MAG_BASE", "MAG_DMB_BATCH", "MAG_DMB_BATCH_CODE", "MAG_DMB_BATCH_STATUS"),
}


def cmd_set_status(args: argparse.Namespace) -> int:
    from ehio.airtable import AirtableClient

    base_cfg, table_cfg, code_cfg, status_cfg = _SET_STATUS_CFG[args.module]

    token            = _resolve_token(args)
    base_id          = _require_cfg(base_cfg)
    batch_table      = _require_cfg(table_cfg)
    batch_code_field = _require_cfg(code_cfg)
    status_field     = _require_cfg(status_cfg)

    client = AirtableClient(api_key=token, base_id=base_id)
    batch_record = client.fetch_batch_record(batch_table, batch_code_field, args.batch)
    if not batch_record:
        _die(f"Batch '{args.batch}' not found in {batch_table}.")

    client.update_records(
        batch_table,
        [{"id": batch_record["id"], "fields": {status_field: args.status}}],
    )
    _info(f"Batch '{args.batch}' status → '{args.status}'.")
    return 0


def cmd_scanning(args: argparse.Namespace) -> int:
    from ehio.scanning import run_scan, MODULES

    token = _resolve_token(args)
    modules = [args.module] if args.module else None

    print("Scanning Airtable for pending batches...")
    total = run_scan(
        token=token,
        modules=modules,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    if total == 0:
        print("No new batches launched.")
    else:
        suffix = "(dry run)" if args.dry_run else ""
        print(f"{total} batch(es) launched. {suffix}".strip())
    return 0


def cmd_config(args: argparse.Namespace) -> int:
    if args.view:
        return cfg.view_config()
    return cfg.edit_config()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 0
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
