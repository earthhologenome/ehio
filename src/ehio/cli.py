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

def _get_drakkar_version() -> str:
    import re as _re
    import subprocess as _sp
    drakkar_conda_env = str(cfg.get("DRAKKAR_CONDA_ENV") or "").strip()
    if drakkar_conda_env:
        _flag = "-p" if drakkar_conda_env.startswith(("/", "~", ".")) else "-n"
        _cmd = ["conda", "run", _flag, drakkar_conda_env, "drakkar", "--version"]
    else:
        _cmd = ["drakkar", "--version"]
    try:
        _res = _sp.run(_cmd, capture_output=True, text=True, timeout=30)
        _raw = _res.stdout.strip() or _res.stderr.strip() or ""
        _m = _re.search(r"(\d+\.\d+[\.\d]*)", _raw)
        return _m.group(1) if _m else (_raw or "unknown")
    except Exception:
        return "unknown"


def cmd_preprocessing(args: argparse.Namespace) -> int:
    if args.input:
        return _run_preprocessing_input(args)
    return _run_preprocessing_output(args)


def _run_preprocessing_input(args: argparse.Namespace) -> int:
    """Fetch batch+entries from Airtable and write a drakkar sample TSV."""
    from ehio.airtable import AirtableClient
    from ehio.drakkar import write_sample_file, verify_input_files

    token       = _resolve_token(args)
    base_id     = _require_cfg("EHI_BASE")
    batch_table = _require_cfg("EHI_PPR_BATCH")
    entry_table = _require_cfg("EHI_PPR_ENTRY")

    batch_code_field  = _require_cfg("EHI_PPR_BATCH_CODE")
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

    out_path = Path(args.sample_file)
    n = write_sample_file(
        entries,
        out_path,
        sample_field=entry_code_field,
        reads1_field=reads1_field,
        reads2_field=reads2_field,
    )
    _info(f"Wrote {n} samples to {out_path}")

    missing = verify_input_files(entries, entry_code_field, [reads1_field, reads2_field])
    if missing:
        for sample, path in missing:
            print(f"  WARNING: [{sample}] file not found: {path}", file=sys.stderr)
        _die(f"{len(missing)} input file(s) missing — fix paths in Airtable before launching drakkar.")
    return 0


def _rename_preprocessing_files(ppr_dir: Path, code_to_ehi: dict[str, str]) -> None:
    """Rename drakkar preprocessing output files from sample-code names to EHI names.

    Mapping:
      {code}.bam        → {ehi}_G.bam
      {code}_1.fq.gz    → {ehi}_M_1.fq.gz
      {code}_2.fq.gz    → {ehi}_M_2.fq.gz
      {code}_cond.tsv   → {ehi}_cond.tsv
    """
    for file_path in sorted(ppr_dir.rglob("*")):
        if not file_path.is_file():
            continue
        name = file_path.name
        for code, ehi in code_to_ehi.items():
            new_name: str | None = None
            if name == f"{code}.bam":
                new_name = f"{ehi}_G.bam"
            elif name == f"{code}_1.fq.gz":
                new_name = f"{ehi}_M_1.fq.gz"
            elif name == f"{code}_2.fq.gz":
                new_name = f"{ehi}_M_2.fq.gz"
            elif name == f"{code}_cond.tsv":
                new_name = f"{ehi}_cond.tsv"
            if new_name:
                file_path.rename(file_path.parent / new_name)
                break


def _run_preprocessing_output(args: argparse.Namespace) -> int:
    """Parse QC metadata from drakkar output, update Airtable, transfer files."""
    from ehio.airtable import AirtableClient
    from ehio.metadata import (
        parse_drakkar_stats_tsv,
        build_entry_update,
        write_output_tsv,
        PREPROCESSING_METRIC_KEYS,
    )
    from ehio.transfer import SFTPTransfer

    token       = _resolve_token(args)
    base_id     = _require_cfg("EHI_BASE")
    batch_table = _require_cfg("EHI_PPR_BATCH")
    entry_table = _require_cfg("EHI_PPR_ENTRY")

    batch_code_field  = _require_cfg("EHI_PPR_BATCH_CODE")
    entry_batch_field = _require_cfg("EHI_PPR_ENTRY_BATCH")
    entry_code_field  = _require_cfg("EHI_PPR_ENTRY_CODE")
    ehi_number_field  = _require_cfg("EHI_PPR_ENTRY_EHI_NUMBER")

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

    # Read all QC metrics from the drakkar-generated summary TSV
    stats_tsv = local_root / "preprocessing.tsv"
    sample_stats = parse_drakkar_stats_tsv(stats_tsv)
    if not sample_stats:
        print(f"  Warning: drakkar stats TSV not found or empty: {stats_tsv}", file=sys.stderr)

    # Build code→EHI mapping and Airtable update payloads
    code_to_ehi: dict[str, str] = {}
    all_metrics: dict[str, dict] = {}
    updates: list[dict] = []
    for entry in entries:
        fields = entry.get("fields", {})
        sample = str(fields.get(entry_code_field, "")).strip()
        ehi    = str(fields.get(ehi_number_field, "")).strip()
        if not sample:
            continue
        if ehi:
            code_to_ehi[sample] = ehi
        metrics = sample_stats.get(sample, {})
        if not metrics:
            print(f"  Warning: no stats found for sample '{sample}' in {stats_tsv}", file=sys.stderr)
        all_metrics[sample] = metrics
        payload = build_entry_update(entry["id"], metrics, field_map)
        if payload["fields"]:
            updates.append(payload)

    # Write summary TSV keyed by EHI number (fall back to sample code if missing)
    metrics_by_ehi = {code_to_ehi.get(s, s): m for s, m in all_metrics.items()}
    run_base = str(cfg.get("RUN_BASE") or "").strip()
    tsv_out: Path | None = None
    if run_base:
        tsv_out = Path(run_base) / args.batch / f"{args.batch}_output.tsv"
        write_output_tsv(metrics_by_ehi, tsv_out)
        _info(f"Output summary written to {tsv_out}")

    if updates:
        _info(f"Updating {len(updates)} entry records in Airtable...")
        client.update_records(entry_table, updates)
        _info("Airtable update complete.")
    else:
        _info("No QC metrics found to update.")

    # Transfer preprocessed output files via SFTP
    ppr_dir = local_root / "preprocessing"
    if not ppr_dir.is_dir():
        _info(f"Preprocessing output directory not found ({ppr_dir}); skipping transfer.")
        return 0

    host     = _conf(args, "host",     "SFTP_HOST",     required=True)
    user     = _conf(args, "user",     "SFTP_USER",     required=True)
    port     = int(_conf(args, "port", "SFTP_PORT") or 22)
    identity = _conf(args, "identity", "SFTP_IDENTITY") or None

    remote_base = _conf(args, "remote_dir", "SFTP_REMOTE_BASE", required=True)
    remote_dir = f"{remote_base.rstrip('/')}/PPR/{args.batch}"

    import shutil as _shutil

    # Rename output files from sample-code names to EHI names before archiving
    if code_to_ehi:
        _rename_preprocessing_files(ppr_dir, code_to_ehi)
        _info(f"Renamed {len(code_to_ehi)} sample file set(s) to EHI names.")

    # Collect specific files for a flat transfer (no subdirectory structure)
    files_to_transfer: list[Path] = []
    final_dir = ppr_dir / "final"
    if final_dir.is_dir():
        files_to_transfer += [
            f for f in sorted(final_dir.iterdir())
            if f.is_file() and (f.name.endswith(".bam") or f.name.endswith(".fq.gz"))
        ]
    singlem_dir = ppr_dir / "singlem"
    if singlem_dir.is_dir():
        files_to_transfer += [
            f for f in sorted(singlem_dir.iterdir())
            if f.is_file() and f.name.endswith("_cond.tsv")
        ]
    if tsv_out is not None and tsv_out.exists():
        files_to_transfer.append(tsv_out)

    if not files_to_transfer:
        _info("No output files found to transfer; skipping SFTP upload.")
    else:
        _info(f"Transferring {len(files_to_transfer)} file(s) to {user}@{host}:{remote_dir} ...")
        with SFTPTransfer(host=host, username=user, port=port, key_path=identity or None) as xfer:
            if getattr(args, "rerun", False):
                xfer.remove_remote_dir(remote_dir)
                _info(f"Deleted remote directory {remote_dir} for rerun.")
            n = xfer.upload_flat(
                files_to_transfer, remote_dir,
                verbose=getattr(args, "verbose", False),
            )
        _info(f"Transferred {n} file(s) to {remote_dir}.")

    # Delete the output directory — only the RUN/{batch} directory is kept
    cleanup = str(cfg.get("CLEANUP_OUTPUT_DIR") or "true").strip().lower()
    if cleanup not in ("false", "0", "no"):
        _shutil.rmtree(local_root, ignore_errors=True)
        _info(f"Deleted output directory {local_root}.")

    # Collect version metadata for the batch record
    batch_fields: dict = {}

    ehio_version_field   = str(cfg.get("EHI_PPR_BATCH_EHIO_VERSION")   or "").strip()
    drakkar_version_field = str(cfg.get("EHI_PPR_BATCH_DRAKKAR_VERSION") or "").strip()

    if ehio_version_field:
        batch_fields[ehio_version_field] = __version__

    if drakkar_version_field:
        batch_fields[drakkar_version_field] = _get_drakkar_version()

    # Mark the batch as done
    done_status        = str(cfg.get("PROCESSING_DONE_STATUS") or "Done").strip()
    batch_status_field = _require_cfg("EHI_PPR_BATCH_STATUS")
    batch_fields[batch_status_field] = done_status

    client.update_records(
        batch_table,
        [{"id": batch_record["id"], "fields": batch_fields}],
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
    from ehio.drakkar import write_sample_file, verify_input_files

    token       = _resolve_token(args)
    base_id     = _require_cfg("EHI_BASE")
    batch_table = _require_cfg("EHI_ASB_BATCH")
    entry_table = _require_cfg("EHI_ASB_ENTRY")

    batch_code_field     = _require_cfg("EHI_ASB_BATCH_CODE")
    entry_batch_field    = _require_cfg("EHI_ASB_ENTRY_BATCH")
    ehi_number_field     = _require_cfg("EHI_ASB_ENTRY_EHI_NUMBER")
    assembly_code_field  = _require_cfg("EHI_ASB_ENTRY_ASSEMBLY_CODE")

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
        sample_field=ehi_number_field,
        reads1_field=reads1_field,
        reads2_field=reads2_field,
        assembly_field=assembly_code_field,
    )
    _info(f"Wrote {n} samples to {out_path}")

    missing = verify_input_files(entries, ehi_number_field, [reads1_field, reads2_field])
    if missing:
        for sample, path in missing:
            print(f"  WARNING: [{sample}] file not found: {path}", file=sys.stderr)
        _die(f"{len(missing)} input file(s) missing — fix paths in Airtable before launching drakkar.")
    return 0


def _run_binning_output(args: argparse.Namespace) -> int:
    """Parse cataloging metadata from drakkar output, update Airtable, transfer files."""
    from ehio.airtable import AirtableClient
    from ehio.metadata import (
        parse_drakkar_cataloging_tsv,
        parse_sample_mapping_rates,
        parse_bin_metadata_csv,
        build_entry_update,
        write_binning_output_tsv,
        BINNING_METRIC_KEYS,
        BIN_METRIC_KEYS,
    )
    from ehio.transfer import SFTPTransfer

    token       = _resolve_token(args)
    base_id     = _require_cfg("EHI_BASE")
    batch_table = _require_cfg("EHI_ASB_BATCH")
    entry_table = _require_cfg("EHI_ASB_ENTRY")

    batch_code_field     = _require_cfg("EHI_ASB_BATCH_CODE")
    entry_batch_field    = _require_cfg("EHI_ASB_ENTRY_BATCH")
    entry_code_field     = _require_cfg("EHI_ASB_ENTRY_CODE")
    ehi_number_field     = str(cfg.get("EHI_ASB_ENTRY_EHI_NUMBER") or "").strip()
    assembly_code_field  = str(cfg.get("EHI_ASB_ENTRY_ASSEMBLY_CODE") or "").strip()

    local_root = Path(args.local_dir).resolve()
    if not local_root.is_dir():
        _die(f"Local directory not found: {local_root}")

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

    field_map: dict[str, str] = {}
    for metric_key, config_key in BINNING_METRIC_KEYS.items():
        fld_id = str(cfg.get(config_key) or "").strip()
        if fld_id:
            field_map[metric_key] = fld_id

    # Read assembly metrics from the drakkar cataloging summary (keyed by assembly code)
    stats_tsv = local_root / "cataloging.tsv"
    assembly_stats = parse_drakkar_cataloging_tsv(stats_tsv)
    if not assembly_stats:
        print(f"  Warning: drakkar stats TSV not found or empty: {stats_tsv}", file=sys.stderr)

    all_metrics: dict[str, dict] = {}
    updates: list[dict] = []
    for entry in entries:
        fields = entry.get("fields", {})
        entry_code = str(fields.get(entry_code_field, "")).strip()
        if not entry_code:
            continue
        # Use EHI number as the output-TSV key (one row per sample); fall back to entry code
        ehi_number = str(fields.get(ehi_number_field, entry_code)).strip() if ehi_number_field else entry_code
        # Metrics are keyed by assembly code in cataloging.tsv
        assembly_code = str(fields.get(assembly_code_field, entry_code)).strip() if assembly_code_field else entry_code
        assembly_metrics = assembly_stats.get(assembly_code, {})
        if not assembly_metrics:
            print(f"  Warning: no stats found for assembly '{assembly_code}' in {stats_tsv}", file=sys.stderr)
        # Override assembly-level mapping rate with this sample's individual rate
        sample_rates = parse_sample_mapping_rates(str(assembly_metrics.get("sample_mapping_rates") or ""))
        metrics = {
            **assembly_metrics,
            "assembly": assembly_code,
            "assembly_mapping_rate": sample_rates.get(ehi_number),
        }
        all_metrics[ehi_number] = metrics
        payload = build_entry_update(entry["id"], metrics, field_map)
        if payload["fields"]:
            updates.append(payload)

    run_base = str(cfg.get("RUN_BASE") or "").strip()
    tsv_out: Path | None = None
    if run_base:
        tsv_out = Path(run_base) / args.batch / f"{args.batch}_output.tsv"
        write_binning_output_tsv(all_metrics, tsv_out)
        _info(f"Output summary written to {tsv_out}")

    if updates:
        _info(f"Updating {len(updates)} entry records in Airtable...")
        client.update_records(entry_table, updates)
        _info("Airtable update complete.")
    else:
        _info("No assembly/binning metrics found to update.")

    final_dir = local_root / "cataloging" / "final"
    if not final_dir.is_dir():
        _info(f"Final output directory not found ({final_dir}); skipping transfer.")
    else:
        host     = _conf(args, "host",     "SFTP_HOST",     required=True)
        user     = _conf(args, "user",     "SFTP_USER",     required=True)
        port     = int(_conf(args, "port", "SFTP_PORT") or 22)
        identity = _conf(args, "identity", "SFTP_IDENTITY") or None

        remote_base = _conf(args, "remote_dir", "SFTP_REMOTE_BASE", required=True)
        remote_dir  = f"{remote_base.rstrip('/')}/ASB/{args.batch}"

        import shutil as _shutil
        if tsv_out is not None and tsv_out.exists():
            _shutil.copy2(tsv_out, final_dir / tsv_out.name)

        _info(f"Transferring {final_dir} → {user}@{host}:{remote_dir} ...")
        with SFTPTransfer(host=host, username=user, port=port, key_path=identity or None) as xfer:
            if getattr(args, "rerun", False):
                xfer.remove_remote_dir(remote_dir)
                _info(f"Deleted remote directory {remote_dir} for rerun.")
            n = xfer.upload_dir(final_dir, remote_dir, verbose=getattr(args, "verbose", False))
        _info(f"Transferred {n} file(s) to {remote_dir}.")

        # --- Create MAG_ENTRY records and upload FASTA files ----------------
        bin_metadata_csv = final_dir / "all_bin_metadata.csv"
        bin_paths_txt    = final_dir / "all_bin_paths.txt"
        mag_base_id      = str(cfg.get("MAG_BASE") or "").strip()

        if not bin_metadata_csv.exists():
            _info(f"No bin metadata CSV found ({bin_metadata_csv}); skipping MAG creation.")
        elif not mag_base_id:
            _info("MAG_BASE not configured; skipping MAG creation.")
        else:
            mag_table        = _require_cfg("MAG_ENTRY")
            mag_client       = AirtableClient(api_key=token, base_id=mag_base_id)
            mag_name_fld     = str(cfg.get("MAG_ENTRY_NAME")     or "").strip()
            mag_assembly_fld = str(cfg.get("MAG_ENTRY_ASSEMBLY") or "").strip()
            mag_field_map: dict[str, str] = {}
            for _mk, _ck in BIN_METRIC_KEYS.items():
                _fid = str(cfg.get(_ck) or "").strip()
                if _fid:
                    mag_field_map[_mk] = _fid

            remote_mag_dir = f"{remote_base.rstrip('/')}/MAG/{args.batch}"

            # Collect FASTA files listed in all_bin_paths.txt
            bin_files: list[Path] = []
            if bin_paths_txt.exists():
                for _line in bin_paths_txt.read_text().splitlines():
                    _line = _line.strip()
                    if _line:
                        _p = local_root / _line
                        if _p.exists():
                            bin_files.append(_p)

            # Build and create MAG_ENTRY records
            bins_data = parse_bin_metadata_csv(bin_metadata_csv)
            records_to_create: list[dict] = []
            for bin_row in bins_data:
                genome = bin_row.get("genome", "")
                if not genome:
                    continue
                genome_name   = genome.removesuffix(".fa").removesuffix(".fasta")
                assembly_code = genome_name.split("_bin_")[0] if "_bin_" in genome_name else genome_name
                rec_fields: dict = {}
                if mag_name_fld:
                    rec_fields[mag_name_fld] = genome_name
                if mag_assembly_fld:
                    rec_fields[mag_assembly_fld] = assembly_code
                for metric, fld_id in mag_field_map.items():
                    val = bin_row.get(metric)
                    if val is not None:
                        rec_fields[fld_id] = val
                if rec_fields:
                    records_to_create.append(rec_fields)

            if records_to_create:
                _info(f"Creating {len(records_to_create)} MAG_ENTRY records in Airtable...")
                mag_client.create_records(mag_table, records_to_create)
                _info("MAG_ENTRY records created.")

            # Upload FASTA files to MAG/{batch}/
            if bin_files:
                _info(f"Uploading {len(bin_files)} FASTA files to {remote_mag_dir} ...")
                with SFTPTransfer(host=host, username=user, port=port, key_path=identity or None) as xfer:
                    if getattr(args, "rerun", False):
                        xfer.remove_remote_dir(remote_mag_dir)
                        _info(f"Deleted remote MAG directory {remote_mag_dir} for rerun.")
                    n_mag = xfer.upload_flat(bin_files, remote_mag_dir,
                                             verbose=getattr(args, "verbose", False))
                _info(f"Uploaded {n_mag} FASTA files to {remote_mag_dir}.")

        cleanup = str(cfg.get("CLEANUP_OUTPUT_DIR") or "true").strip().lower()
        if cleanup not in ("false", "0", "no"):
            _shutil.rmtree(local_root, ignore_errors=True)
            _info(f"Deleted output directory {local_root}.")

    batch_fields: dict = {}
    ehio_version_field    = str(cfg.get("EHI_ASB_BATCH_EHIO_VERSION")    or "").strip()
    drakkar_version_field = str(cfg.get("EHI_ASB_BATCH_DRAKKAR_VERSION") or "").strip()
    if ehio_version_field:
        batch_fields[ehio_version_field] = __version__
    if drakkar_version_field:
        batch_fields[drakkar_version_field] = _get_drakkar_version()

    done_status        = str(cfg.get("PROCESSING_DONE_STATUS") or "Done").strip()
    batch_status_field = _require_cfg("EHI_ASB_BATCH_STATUS")
    batch_fields[batch_status_field] = done_status

    client.update_records(
        batch_table,
        [{"id": batch_record["id"], "fields": batch_fields}],
    )
    _info(f"Batch '{args.batch}' status → '{done_status}'.")
    return 0


# ---------------------------------------------------------------------------
# quantifying
# ---------------------------------------------------------------------------

def cmd_quantifying(args: argparse.Namespace) -> int:
    if args.input:
        return _run_quantifying_input(args)
    return _run_quantifying_output(args)


def _run_quantifying_input(args: argparse.Namespace) -> int:
    from ehio.airtable import AirtableClient
    from ehio.drakkar import write_bins_file, write_sample_file, verify_input_files

    token         = _resolve_token(args)
    base_id       = _require_cfg("MAG_BASE")
    batch_table   = _require_cfg("MAG_DMB_BATCH")
    entry_table   = _require_cfg("MAG_DMB_ENTRY")
    mag_table     = _require_cfg("MAG_ENTRY")

    batch_code_field  = _require_cfg("MAG_DMB_BATCH_CODE")
    entry_batch_field = _require_cfg("MAG_DMB_ENTRY_BATCH")
    entry_code_field  = _require_cfg("MAG_DMB_ENTRY_CODE")
    mag_list_field    = _require_cfg("MAG_DMB_BATCH_LIST_MAGS")
    mag_url_field     = _require_cfg("MAG_ENTRY_URL_FASTA")
    reads1_field      = _conf(args, "reads1_field", "MAG_DMB_ENTRY_READS1", required=True)
    reads2_field      = _conf(args, "reads2_field", "MAG_DMB_ENTRY_READS2", required=True)

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
    _info(f"Found {len(entries)} sample entries for batch '{args.batch}'.")
    if not entries:
        _die(f"No sample entries found for batch '{args.batch}'.")

    # Fetch MAG records linked to this batch for the bins file
    mag_rec_ids = batch_record.get("fields", {}).get(mag_list_field, [])
    if not mag_rec_ids:
        _die(
            f"No MAG records linked in field {mag_list_field} of batch '{args.batch}'. "
            "Ensure MAG_DMB_BATCH_LIST_MAGS is populated in Airtable."
        )
    _info(f"Fetching {len(mag_rec_ids)} MAG record(s) from {mag_table}...")
    mag_records = []
    for rec_id in mag_rec_ids:
        if isinstance(rec_id, str) and rec_id.startswith("rec"):
            rec = client.fetch_record_by_id(mag_table, rec_id)
            if rec:
                mag_records.append(rec)
    if not mag_records:
        _die(f"Could not fetch any MAG records for batch '{args.batch}'.")

    bins_path = Path(args.bins_file)
    n_bins = write_bins_file(mag_records, bins_path, bins_field=mag_url_field)
    _info(f"Wrote {n_bins} MAG paths to {bins_path}")

    reads_path = Path(args.sample_file)
    n_reads = write_sample_file(
        entries,
        reads_path,
        sample_field=entry_code_field,
        reads1_field=reads1_field,
        reads2_field=reads2_field,
    )
    _info(f"Wrote {n_reads} read entries to {reads_path}")

    missing_reads = verify_input_files(entries, entry_code_field, [reads1_field, reads2_field])
    if missing_reads:
        for sample, path in missing_reads:
            print(f"  WARNING: [{sample}] reads file not found: {path}", file=sys.stderr)

    missing_bins = verify_input_files(mag_records, mag_url_field, [mag_url_field])
    if missing_bins:
        for _, path in missing_bins:
            print(f"  WARNING: MAG FASTA not found: {path}", file=sys.stderr)

    total_missing = len(missing_reads) + len(missing_bins)
    if total_missing:
        _die(f"{total_missing} input file(s) missing — fix paths in Airtable before launching drakkar.")
    return 0


def _run_quantifying_output(args: argparse.Namespace) -> int:
    """Parse profiling metadata from drakkar output, update Airtable, transfer files."""
    from ehio.airtable import AirtableClient
    from ehio.metadata import (
        collect_quantifying_metadata,
        build_entry_update,
        write_quantifying_output_tsv,
        QUANTIFYING_METRIC_KEYS,
    )
    from ehio.transfer import SFTPTransfer

    token       = _resolve_token(args)
    base_id     = _require_cfg("MAG_BASE")
    batch_table = _require_cfg("MAG_DMB_BATCH")
    entry_table = _require_cfg("MAG_DMB_ENTRY")

    batch_code_field  = _require_cfg("MAG_DMB_BATCH_CODE")
    entry_batch_field = _require_cfg("MAG_DMB_ENTRY_BATCH")
    entry_code_field  = _require_cfg("MAG_DMB_ENTRY_CODE")

    local_root = Path(args.local_dir).resolve()
    if not local_root.is_dir():
        _die(f"Local directory not found: {local_root}")

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

    field_map: dict[str, str] = {}
    for metric_key, config_key in QUANTIFYING_METRIC_KEYS.items():
        fld_id = str(cfg.get(config_key) or "").strip()
        if fld_id:
            field_map[metric_key] = fld_id

    all_metrics: dict[str, dict] = {}
    updates: list[dict] = []
    for entry in entries:
        sample = str(entry.get("fields", {}).get(entry_code_field, "")).strip()
        if not sample:
            continue
        metrics = collect_quantifying_metadata(sample, local_root)
        all_metrics[sample] = metrics
        payload = build_entry_update(entry["id"], metrics, field_map)
        if payload["fields"]:
            updates.append(payload)

    run_base = str(cfg.get("RUN_BASE") or "").strip()
    tsv_out: Path | None = None
    if run_base:
        tsv_out = Path(run_base) / args.batch / f"{args.batch}_output.tsv"
        write_quantifying_output_tsv(all_metrics, tsv_out)
        _info(f"Output summary written to {tsv_out}")

    if updates:
        _info(f"Updating {len(updates)} entry records in Airtable...")
        client.update_records(entry_table, updates)
        _info("Airtable update complete.")
    else:
        _info("No mapping metrics found to update.")

    final_dir = local_root / "profiling" / "final"
    if not final_dir.is_dir():
        _info(f"Final output directory not found ({final_dir}); skipping transfer.")
    else:
        host     = _conf(args, "host",     "SFTP_HOST",     required=True)
        user     = _conf(args, "user",     "SFTP_USER",     required=True)
        port     = int(_conf(args, "port", "SFTP_PORT") or 22)
        identity = _conf(args, "identity", "SFTP_IDENTITY") or None

        remote_base = _conf(args, "remote_dir", "SFTP_REMOTE_BASE", required=True)
        remote_dir  = f"{remote_base.rstrip('/')}/DMB/{args.batch}"

        import shutil as _shutil
        if tsv_out is not None and tsv_out.exists():
            _shutil.copy2(tsv_out, final_dir / tsv_out.name)

        _info(f"Transferring {final_dir} → {user}@{host}:{remote_dir} ...")
        with SFTPTransfer(host=host, username=user, port=port, key_path=identity or None) as xfer:
            if getattr(args, "rerun", False):
                xfer.remove_remote_dir(remote_dir)
                _info(f"Deleted remote directory {remote_dir} for rerun.")
            n = xfer.upload_dir(final_dir, remote_dir, verbose=getattr(args, "verbose", False))
        _info(f"Transferred {n} file(s) to {remote_dir}.")

        cleanup = str(cfg.get("CLEANUP_OUTPUT_DIR") or "true").strip().lower()
        if cleanup not in ("false", "0", "no"):
            _shutil.rmtree(local_root, ignore_errors=True)
            _info(f"Deleted output directory {local_root}.")

    batch_fields: dict = {}
    ehio_version_field    = str(cfg.get("MAG_DMB_BATCH_EHIO_VERSION")    or "").strip()
    drakkar_version_field = str(cfg.get("MAG_DMB_BATCH_DRAKKAR_VERSION") or "").strip()
    if ehio_version_field:
        batch_fields[ehio_version_field] = __version__
    if drakkar_version_field:
        batch_fields[drakkar_version_field] = _get_drakkar_version()

    done_status        = str(cfg.get("PROCESSING_DONE_STATUS") or "Done").strip()
    batch_status_field = _require_cfg("MAG_DMB_BATCH_STATUS")
    batch_fields[batch_status_field] = done_status

    client.update_records(
        batch_table,
        [{"id": batch_record["id"], "fields": batch_fields}],
    )
    _info(f"Batch '{args.batch}' status → '{done_status}'.")
    return 0


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
        g.add_argument("--rerun", action="store_true",
            help="Delete the remote archive directory before uploading (use when rerunning a batch).")

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
            "Input mode:  fetch batch + entries from MAG_BASE/MAG_DMB_* tables,\n"
            "             fetch linked MAG records (MAG_DMB_BATCH_LIST_MAGS → MAG_ENTRY),\n"
            "             and write a bins file (MAG FASTAs) and a reads sample file.\n"
            "Output mode: parse mapping metrics, update MAG_DMB_ENTRY, transfer files."
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
        help="Output MAG bins path file for drakkar (input mode). Default: bins.txt.")
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

    # ------------------------------------------------------------------
    # update
    # ------------------------------------------------------------------
    p_upd = sub.add_parser(
        "update",
        help="Update ehio to the latest version from GitHub.",
        description="Reinstalls ehio from the main branch on GitHub using pip.",
    )
    p_upd.add_argument(
        "--repo",
        default="https://github.com/earthhologenome/ehio.git",
        metavar="URL",
        help="Git repository URL to install from. Default: GitHub main branch.",
    )
    p_upd.set_defaults(func=cmd_update)

    # ------------------------------------------------------------------
    # stop
    # ------------------------------------------------------------------
    p_stop = sub.add_parser(
        "stop",
        help="Kill the screen session for a running batch.",
        description="Sends a quit signal to the screen session named after the batch.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p_stop.add_argument("--module", "-m", required=True,
        choices=["preprocessing", "binning", "quantifying"],
        help="Module whose batch table to update.")
    p_stop.add_argument("--batch", "-b", required=True, metavar="BATCH",
        help="Batch code (screen session name) to stop.")
    p_stop.add_argument("--airtable-token", metavar="TOKEN",
        help="Airtable personal access token. Overrides $AIRTABLE_TOKEN.")
    p_stop.set_defaults(func=cmd_stop)

    # ------------------------------------------------------------------
    # remove
    # ------------------------------------------------------------------
    p_rm = sub.add_parser(
        "remove",
        help="Delete the output directory for a batch (not the RUN directory).",
        description=(
            "Removes the working output directory (PPR/ASB/DMB)/{batch} for the given\n"
            "module. The RUN/{batch} directory (scripts and logs) is not touched."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p_rm.add_argument("--module", "-m", required=True,
        choices=["preprocessing", "binning", "quantifying"],
        help="Module whose output base to use.")
    p_rm.add_argument("--batch", "-b", required=True, metavar="BATCH",
        help="Batch code — the subdirectory to delete.")
    p_rm.set_defaults(func=cmd_remove)

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


_OUTPUT_BASE_CFG = {
    "preprocessing": "EHI_PPR_OUTPUT_BASE",
    "binning":       "EHI_ASB_OUTPUT_BASE",
    "quantifying":   "MAG_DMB_OUTPUT_BASE",
}


def cmd_stop(args: argparse.Namespace) -> int:
    import subprocess
    from ehio.airtable import AirtableClient

    base_cfg, table_cfg, code_cfg, status_cfg = _SET_STATUS_CFG[args.module]
    token            = _resolve_token(args)
    base_id          = _require_cfg(base_cfg)
    batch_table      = _require_cfg(table_cfg)
    batch_code_field = _require_cfg(code_cfg)
    status_field     = _require_cfg(status_cfg)
    stopped_status   = str(cfg.get("SCANNING_STOPPED_STATUS") or "Stopped").strip()

    client = AirtableClient(api_key=token, base_id=base_id)
    batch_record = client.fetch_batch_record(batch_table, batch_code_field, args.batch)
    if not batch_record:
        _die(f"Batch '{args.batch}' not found in {batch_table}.")
    client.update_records(
        batch_table,
        [{"id": batch_record["id"], "fields": {status_field: stopped_status}}],
    )
    _info(f"Batch '{args.batch}' status → '{stopped_status}'.")

    session = args.batch
    result = subprocess.run(
        ["screen", "-S", session, "-X", "quit"],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        _info(f"Screen session '{session}' terminated.")
    else:
        _info(f"No screen session named '{session}' found (already stopped or never started).")
    return 0


def cmd_remove(args: argparse.Namespace) -> int:
    import shutil

    output_base = _require_cfg(_OUTPUT_BASE_CFG[args.module])
    target = Path(output_base) / args.batch
    if not target.exists():
        _info(f"Output directory not found: {target}")
        return 0
    shutil.rmtree(target)
    _info(f"Deleted output directory: {target}")
    return 0


def cmd_config(args: argparse.Namespace) -> int:
    if args.view:
        return cfg.view_config()
    return cfg.edit_config()


def cmd_update(args: argparse.Namespace) -> int:
    import subprocess
    print(f"Current version: ehio {__version__}")
    print(f"Installing latest from {args.repo} ...")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--force-reinstall", f"git+{args.repo}"],
        check=False,
    )
    if result.returncode != 0:
        _die("Update failed. Check the output above for details.")
    print("Update complete. Run 'ehio --version' to confirm the new version.")
    return 0


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
