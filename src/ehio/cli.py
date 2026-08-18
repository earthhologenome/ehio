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
    """Return the Airtable token, after checking that Airtable accepts it."""
    from ehio.airtable import AirtableError, verify_token

    token = (getattr(args, "airtable_token", None) or "").strip()
    if not token:
        token = os.environ.get("AIRTABLE_TOKEN", "").strip()
    if not token:
        _die(
            "Airtable token not found. "
            "Provide --airtable-token or export AIRTABLE_TOKEN."
        )
    try:
        verify_token(token)
    except AirtableError as exc:
        _die(str(exc))
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
    from ehio.drakkar import write_sample_file, verify_input_files, verify_remote_urls

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

    if getattr(args, "no_url_check", False):
        _info("Skipping raw-read URL check (--no-url-check).")
    else:
        _info("Checking that raw-read URLs are downloadable...")
        unreachable = verify_remote_urls(entries, entry_code_field, [reads1_field, reads2_field])
        if unreachable:
            for sample, url, reason in unreachable:
                print(f"  WARNING: [{sample}] URL not downloadable: {url} ({reason})", file=sys.stderr)
            _die(
                f"{len(unreachable)} raw-read URL(s) not downloadable — fix them in "
                f"{entry_table} before launching drakkar."
            )
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
    from ehio.reference import upload_reference_index_status
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
        _timeout = getattr(args, "connect_timeout", 300.0)
        with SFTPTransfer(host=host, username=user, port=port, key_path=identity or None, timeout=_timeout) as xfer:
            if getattr(args, "rerun", False):
                xfer.remove_remote_dir(remote_dir)
                _info(f"Deleted remote directory {remote_dir} for rerun.")
            n_up, n_sk = xfer.upload_flat(
                files_to_transfer, remote_dir,
                verbose=getattr(args, "verbose", False),
            )
        _skip_msg = f", {n_sk} already present (skipped)" if n_sk else ""
        _info(f"Transferred {n_up} file(s) to {remote_dir}{_skip_msg}.")

    # Archive the Bowtie2 index drakkar built for a not-yet-indexed reference
    # genome and register it, so the next batch on this host is launched with -x.
    # Runs before the cleanup below, which is where the index would be deleted.
    keep_for_reference = False
    try:
        ref_status = upload_reference_index_status(
            batch_record, local_root, token,
            host=host, user=user, port=port, identity=identity or None,
            remote_base=remote_base,
            timeout=getattr(args, "connect_timeout", 300.0),
            verbose=getattr(args, "verbose", False),
        )
        keep_for_reference = ref_status == "ambiguous"
    except Exception as exc:  # noqa: BLE001 — the batch itself is already complete
        keep_for_reference = True
        print(
            f"{ERROR}Warning:{RESET} the reference index of batch '{args.batch}' "
            f"could not be uploaded: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

    # The index only exists inside the output directory, so deleting it below
    # would make the failure unrecoverable — keep it and let the user retry.
    if keep_for_reference:
        print(
            f"{ERROR}Warning:{RESET} keeping {local_root} so the reference index "
            f"is not lost. Retry with:\n"
            f"    ehio reference -b {args.batch} -l {local_root}",
            file=sys.stderr,
        )

    # Delete the output directory — only the RUN/{batch} directory is kept
    cleanup = str(cfg.get("CLEANUP_OUTPUT_DIR") or "true").strip().lower()
    if not keep_for_reference and cleanup not in ("false", "0", "no"):
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
    from ehio.drakkar import (
        write_sample_file,
        verify_input_files,
        normalise_assembly_type,
        check_assembly_type,
    )

    token       = _resolve_token(args)
    base_id     = _require_cfg("EHI_BASE")
    batch_table = _require_cfg("EHI_ASB_BATCH")
    entry_table = _require_cfg("EHI_ASB_ENTRY")

    batch_code_field     = _require_cfg("EHI_ASB_BATCH_CODE")
    batch_type_field     = str(cfg.get("EHI_ASB_BATCH_TYPE") or "").strip()
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

    # The assembly codes of the entries decide the grouping; the batch type says
    # what that grouping is meant to be.  Checking them against each other here
    # keeps a mismatch from surfacing as an empty drakkar run.
    if batch_type_field:
        assembly_type = normalise_assembly_type(
            batch_record.get("fields", {}).get(batch_type_field)
        )
        _info(f"Batch assembly type: {assembly_type or '(unset)'}")
        error, warning = check_assembly_type(
            entries, assembly_type,
            sample_field=ehi_number_field,
            assembly_field=assembly_code_field,
        )
        if warning:
            print(f"  WARNING: {warning}", file=sys.stderr)
        if error:
            _die(error)

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


_GZIP_COMPRESS_LEVEL = 6

# Assemblies have been on ERDA as ASB/{batch}/{assembly}_contigs.fasta.gz since
# before ehio; drakkar names them {assembly}.fna, so they are renamed on upload
# rather than breaking the links of every batch that predates this.
_ASSEMBLY_REMOTE_SUFFIX = "_contigs.fasta.gz"


def _assembly_remote_name(fasta: Path) -> str:
    """Return the ERDA filename for an assembly FASTA ({assembly}.fna)."""
    return f"{fasta.stem}{_ASSEMBLY_REMOTE_SUFFIX}"


def _find_assembly_fastas(local_root: Path) -> list[Path]:
    """Return the assembly FASTAs of a drakkar cataloging run.

    drakkar writes one per assembly as
    {local_root}/cataloging/megahit/{assembly}/{assembly}.fna — the renamed
    contigs, not megahit's own final.contigs.raw.fa, which keeps the .fa suffix.
    """
    megahit_dir = local_root / "cataloging" / "megahit"
    if not megahit_dir.is_dir():
        return []
    return sorted(p for p in megahit_dir.glob("*/*.fna") if p.is_file())


def _gzip_into(source: Path, handle) -> None:
    """Gzip `source` straight into an open remote file handle.

    Assemblies run to several GB, so they are compressed into the SFTP
    connection rather than to a temporary .gz on the local disk.  The gzip layer
    is built explicitly because Python 3.11 does not accept a compresslevel on
    a stream, which is what the cluster environment runs.
    """
    import gzip as _gzip
    import shutil as _sh

    with _gzip.GzipFile(
        filename="", mode="wb", fileobj=handle, compresslevel=_GZIP_COMPRESS_LEVEL
    ) as gz, source.open("rb") as fin:
        _sh.copyfileobj(fin, gz)


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

        # ASB/{batch} holds the assemblies and the batch-level summary tables.
        # The bin FASTAs sit in the per-assembly subdirectories of final/ and go
        # to MAG/{batch} compressed further down, so they are left out here
        # instead of being transferred a second time, uncompressed.
        summary_files = [p for p in sorted(final_dir.iterdir()) if p.is_file()]
        assembly_fastas = _find_assembly_fastas(local_root)
        if not assembly_fastas:
            print(
                f"  Warning: no assembly FASTA found under "
                f"{local_root / 'cataloging' / 'megahit'}; only summary files "
                f"will be transferred to {remote_dir}.",
                file=sys.stderr,
            )

        _verbose = getattr(args, "verbose", False)
        _info(
            f"Transferring {len(summary_files)} summary file(s) and "
            f"{len(assembly_fastas)} assembly FASTA(s) → {user}@{host}:{remote_dir} ..."
        )
        _timeout = getattr(args, "connect_timeout", 300.0)
        with SFTPTransfer(host=host, username=user, port=port, key_path=identity or None, timeout=_timeout) as xfer:
            if getattr(args, "rerun", False):
                xfer.remove_remote_dir(remote_dir)
                _info(f"Deleted remote directory {remote_dir} for rerun.")
            n_up, n_sk = xfer.upload(summary_files, final_dir, remote_dir, verbose=_verbose)
            for _fna in assembly_fastas:
                remote_fna = f"{remote_dir}/{_assembly_remote_name(_fna)}"
                if xfer.remote_exists(remote_fna):
                    n_sk += 1
                    if _verbose:
                        print(f"  SKIP {_fna} (already exists remotely)", file=sys.stderr)
                    continue
                size_mb = _fna.stat().st_size / (1024 * 1024)
                _info(
                    f"  Compressing and uploading {_fna.name} ({size_mb:.0f} MB) "
                    f"→ {_assembly_remote_name(_fna)} ..."
                )
                xfer.upload_stream(
                    remote_fna,
                    lambda handle, src=_fna: _gzip_into(src, handle),
                    verbose=_verbose,
                )
                n_up += 1
        _skip_msg = f", {n_sk} already present (skipped)" if n_sk else ""
        _info(f"Transferred {n_up} file(s) to {remote_dir}{_skip_msg}.")

        # --- Create MAG_ENTRY records and upload FASTA files ----------------
        bin_metadata_csv = final_dir / "all_bin_metadata.csv"
        bin_paths_txt    = final_dir / "all_bin_paths.txt"
        mag_base_id      = str(cfg.get("MAG_BASE") or "").strip()

        if not bin_metadata_csv.exists():
            _info(f"No bin metadata CSV found ({bin_metadata_csv}); skipping MAG creation.")
        elif not mag_base_id:
            _info("MAG_BASE not configured; skipping MAG creation.")
        else:
            mag_table          = _require_cfg("MAG_ENTRY")
            mag_client         = AirtableClient(api_key=token, base_id=mag_base_id)
            mag_name_fld       = str(cfg.get("MAG_ENTRY_NAME")       or "").strip()
            mag_assembly_fld   = str(cfg.get("MAG_ENTRY_ASSEMBLY")   or "").strip()
            mag_annotated_fld  = str(cfg.get("MAG_ENTRY_ANNOTATED")  or "").strip()
            mag_field_map: dict[str, str] = {}
            for _mk, _ck in BIN_METRIC_KEYS.items():
                _fid = str(cfg.get(_ck) or "").strip()
                if _fid:
                    mag_field_map[_mk] = _fid

            remote_mag_dir = f"{remote_base.rstrip('/')}/MAG/{args.batch}"
            _info(f"bin_metadata_csv: {bin_metadata_csv}")
            _info(f"bin_paths_txt:    {bin_paths_txt} (exists: {bin_paths_txt.exists()})")

            # Collect FASTA files listed in all_bin_paths.txt
            bin_files: list[Path] = []
            if bin_paths_txt.exists():
                raw_lines = [l.strip() for l in bin_paths_txt.read_text().splitlines() if l.strip()]
                _info(f"all_bin_paths.txt contains {len(raw_lines)} path(s).")
                for _line in raw_lines:
                    _p = local_root / _line
                    if _p.exists():
                        bin_files.append(_p)
                    else:
                        _info(f"  FASTA not found (skipped): {_p}")
                _info(f"{len(bin_files)} of {len(raw_lines)} FASTA file(s) resolved.")
            else:
                _info("all_bin_paths.txt not found; no FASTA files will be uploaded.")

            # Build and create MAG_ENTRY records
            bins_data = parse_bin_metadata_csv(bin_metadata_csv)
            _info(f"Parsed {len(bins_data)} bin(s) from {bin_metadata_csv.name}.")

            # Check which genomes already have a MAG_ENTRY record to avoid duplicates on resume
            existing_mag_names: set[str] = set()
            if mag_name_fld:
                all_genome_names = [str(r.get("genome", "")) for r in bins_data if r.get("genome")]
                if all_genome_names:
                    _info(f"Checking for existing MAG_ENTRY records ({len(all_genome_names)} genomes)...")
                    existing_mag_names = mag_client.fetch_existing_values(
                        mag_table, mag_name_fld, all_genome_names
                    )
                    if existing_mag_names:
                        _info(f"Found {len(existing_mag_names)} existing MAG_ENTRY records — skipping those.")

            records_to_create: list[dict] = []
            for bin_row in bins_data:
                genome = bin_row.get("genome", "")
                if not genome:
                    continue
                if genome in existing_mag_names:
                    continue
                genome_name   = genome.removesuffix(".fa").removesuffix(".fasta")
                assembly_code = genome_name.split("_bin_")[0] if "_bin_" in genome_name else genome_name
                rec_fields: dict = {}
                if mag_name_fld:
                    rec_fields[mag_name_fld] = genome
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
            else:
                _info("No new MAG_ENTRY records to create.")

            # Compress and upload FASTA files to MAG/{batch}/
            if bin_files:
                import gzip as _gzip
                _info(f"Uploading {len(bin_files)} compressed FASTA files to {remote_mag_dir} ...")
                n_mag_up = n_mag_sk = 0
                with SFTPTransfer(host=host, username=user, port=port, key_path=identity or None, timeout=_timeout) as xfer:
                    if getattr(args, "rerun", False):
                        xfer.remove_remote_dir(remote_mag_dir)
                        _info(f"Deleted remote MAG directory {remote_mag_dir} for rerun.")
                    xfer._ensure_remote_dir(remote_mag_dir)
                    for _fa in bin_files:
                        _gz = Path(str(_fa) + ".gz")
                        try:
                            with _fa.open("rb") as _fin, _gzip.open(_gz, "wb") as _fout:
                                _shutil.copyfileobj(_fin, _fout)
                            _up, _sk = xfer.upload_flat([_gz], remote_mag_dir,
                                                        verbose=getattr(args, "verbose", False))
                            n_mag_up += _up
                            n_mag_sk += _sk
                        finally:
                            _gz.unlink(missing_ok=True)
                _skip_msg = f", {n_mag_sk} already present (skipped)" if n_mag_sk else ""
                _info(f"Uploaded {n_mag_up} compressed FASTA files to {remote_mag_dir}{_skip_msg}.")

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
    from ehio.drakkar import write_bins_file, write_quality_file, write_sample_file, verify_input_files

    token       = _resolve_token(args)
    base_id     = _require_cfg("MAG_BASE")
    batch_table = _require_cfg("MAG_DMB_BATCH")
    mag_table   = _require_cfg("MAG_ENTRY")
    ppr_table   = _require_cfg("MAG_PPR")

    batch_code_field      = _require_cfg("MAG_DMB_BATCH_CODE")
    mag_list_field        = _require_cfg("MAG_DMB_BATCH_LIST_MAGS")
    ppr_list_field        = _require_cfg("MAG_DMB_BATCH_LIST_PPR")
    mag_name_field        = _require_cfg("MAG_ENTRY_NAME")
    mag_completeness_fld  = _require_cfg("MAG_ENTRY_CHECKM_COMPLETENESS")
    mag_contamination_fld = _require_cfg("MAG_ENTRY_CHECKM_CONTAMINATION")
    mag_url_field         = _require_cfg("MAG_ENTRY_URL_FASTA")
    ppr_ehi_field         = _require_cfg("MAG_PPR_EHI")
    reads1_field          = _require_cfg("MAG_PPR_READS1")
    reads2_field          = _require_cfg("MAG_PPR_READS2")

    _info(f"Looking up batch '{args.batch}'...")
    client = AirtableClient(api_key=token, base_id=base_id)
    batch_record = client.fetch_batch_record(batch_table, batch_code_field, args.batch)
    if batch_record is None:
        _die(f"Batch '{args.batch}' not found.")

    # Fetch MAG records from MAG_ENTRY
    mag_rec_ids = batch_record.get("fields", {}).get(mag_list_field, [])
    if not mag_rec_ids:
        _die(f"No MAG records linked in field {mag_list_field} of batch '{args.batch}'.")
    _info(f"Fetching {len(mag_rec_ids)} MAG record(s)...")
    mag_records = []
    for rec_id in mag_rec_ids:
        if isinstance(rec_id, str) and rec_id.startswith("rec"):
            rec = client.fetch_record_by_id(mag_table, rec_id)
            if rec:
                mag_records.append(rec)
    if not mag_records:
        _die(f"Could not fetch any MAG records for batch '{args.batch}'.")

    quality_path = Path(args.quality_file)
    n_quality = write_quality_file(
        mag_records, quality_path,
        name_field=mag_name_field,
        completeness_field=mag_completeness_fld,
        contamination_field=mag_contamination_fld,
    )
    _info(f"Wrote {n_quality} rows to {quality_path}")

    mags_path = Path(args.mags_file)
    n_mags = write_bins_file(mag_records, mags_path, bins_field=mag_url_field)
    _info(f"Wrote {n_mags} MAG URLs to {mags_path}")

    # Fetch PPR records from MAG_PPR
    ppr_rec_ids = batch_record.get("fields", {}).get(ppr_list_field, [])
    if not ppr_rec_ids:
        _die(f"No PPR records linked in field {ppr_list_field} of batch '{args.batch}'.")
    _info(f"Fetching {len(ppr_rec_ids)} PPR record(s)...")
    ppr_records = []
    for rec_id in ppr_rec_ids:
        if isinstance(rec_id, str) and rec_id.startswith("rec"):
            rec = client.fetch_record_by_id(ppr_table, rec_id)
            if rec:
                ppr_records.append(rec)
    if not ppr_records:
        _die(f"Could not fetch any PPR records for batch '{args.batch}'.")

    reads_path = Path(args.reads_file)
    n_reads = write_sample_file(
        ppr_records,
        reads_path,
        sample_field=ppr_ehi_field,
        reads1_field=reads1_field,
        reads2_field=reads2_field,
    )
    _info(f"Wrote {n_reads} read entries to {reads_path}")

    missing_reads = verify_input_files(ppr_records, ppr_ehi_field, [reads1_field, reads2_field])
    if missing_reads:
        for sample, path in missing_reads:
            print(f"  WARNING: [{sample}] reads file not found: {path}", file=sys.stderr)

    missing_mags = verify_input_files(mag_records, mag_url_field, [mag_url_field])
    if missing_mags:
        for _, path in missing_mags:
            print(f"  WARNING: MAG FASTA not found: {path}", file=sys.stderr)

    total_missing = len(missing_reads) + len(missing_mags)
    if total_missing:
        _die(f"{total_missing} input file(s) missing — fix paths in Airtable before launching drakkar.")
    return 0


def _run_quantifying_output(args: argparse.Namespace) -> int:
    """Parse profiling metadata from drakkar output, update Airtable, transfer files."""
    from ehio.airtable import AirtableClient
    from ehio.metadata import (
        write_quantifying_output_tsv,
        parse_profiling_genomes_tsv,
        parse_dereplicating_tsv,
    )
    from ehio.transfer import SFTPTransfer

    token       = _resolve_token(args)
    base_id     = _require_cfg("MAG_BASE")
    batch_table = _require_cfg("MAG_DMB_BATCH")
    entry_table = _require_cfg("MAG_DMB_ENTRY")
    ppr_table   = _require_cfg("MAG_PPR")

    batch_code_field  = _require_cfg("MAG_DMB_BATCH_CODE")
    ppr_list_field    = _require_cfg("MAG_DMB_BATCH_LIST_PPR")
    ppr_ehi_field     = _require_cfg("MAG_PPR_EHI")
    entry_batch_field = _require_cfg("MAG_DMB_ENTRY_BATCH")
    entry_ppr_field   = _require_cfg("MAG_DMB_ENTRY_PPR")
    entry_rate_field  = str(cfg.get("MAG_DMB_ENTRY_MAPPING_RATE") or "").strip()

    local_root = Path(args.local_dir).resolve()
    if not local_root.is_dir():
        _die(f"Local directory not found: {local_root}")

    _info(f"Looking up batch '{args.batch}' in Airtable...")
    client = AirtableClient(api_key=token, base_id=base_id)
    batch_record = client.fetch_batch_record(batch_table, batch_code_field, args.batch)
    if batch_record is None:
        _die(f"Batch '{args.batch}' not found.")

    # Fetch PPR records linked to this batch
    ppr_rec_ids = batch_record.get("fields", {}).get(ppr_list_field, [])
    if not ppr_rec_ids:
        _die(f"No PPR records linked in field {ppr_list_field} of batch '{args.batch}'.")
    _info(f"Fetching {len(ppr_rec_ids)} PPR record(s)...")
    ppr_records = []
    for rec_id in ppr_rec_ids:
        if isinstance(rec_id, str) and rec_id.startswith("rec"):
            rec = client.fetch_record_by_id(ppr_table, rec_id)
            if rec:
                ppr_records.append(rec)
    if not ppr_records:
        _die(f"Could not fetch any PPR records for batch '{args.batch}'.")

    # Parse per-sample mapping rates from drakkar output
    profiling_tsv = local_root / "profiling_genomes.tsv"
    per_sample = parse_profiling_genomes_tsv(profiling_tsv)
    if not per_sample:
        _info(f"profiling_genomes.tsv not found or empty at {profiling_tsv}; mapping rates will be empty.")

    # Create MAG_DMB_ENTRY records — one per PPR record
    batch_rec_id = batch_record["id"]
    records_to_create: list[dict] = []
    all_metrics: dict[str, dict] = {}
    for ppr_rec in ppr_records:
        ehi_raw = ppr_rec.get("fields", {}).get(ppr_ehi_field, "")
        if isinstance(ehi_raw, list):
            ehi_raw = ehi_raw[0] if ehi_raw else ""
        ehi = str(ehi_raw).strip()
        metrics = per_sample.get(ehi, {})
        all_metrics[ehi] = metrics
        rec_fields: dict = {
            entry_batch_field: [batch_rec_id],
            entry_ppr_field:   [ppr_rec["id"]],
        }
        if entry_rate_field:
            rate = metrics.get("mapping_rate")
            if rate is not None:
                rec_fields[entry_rate_field] = rate
        records_to_create.append(rec_fields)

    _existing_formula = f'FIND("{batch_rec_id}", ARRAYJOIN({{{entry_batch_field}}}))'
    _existing_entries = client._table(entry_table).all(formula=_existing_formula)
    if _existing_entries:
        _info(f"{len(_existing_entries)} MAG_DMB_ENTRY record(s) already exist for this batch — skipping creation.")
    elif records_to_create:
        _info(f"Creating {len(records_to_create)} MAG_DMB_ENTRY records...")
        client.create_records(entry_table, records_to_create)
        _info("MAG_DMB_ENTRY records created.")

    run_base = str(cfg.get("RUN_BASE") or "").strip()
    if run_base:
        tsv_out = Path(run_base) / args.batch / f"{args.batch}_output.tsv"
        write_quantifying_output_tsv(all_metrics, tsv_out)
        _info(f"Output summary written to {tsv_out}")

    # Genomes-type output: profiling_genomes/final/counts.tsv + bases.tsv
    # Pangenomes-type output path and files differ — to be wired when implemented.
    final_dir = local_root / "profiling_genomes" / "final"
    if not final_dir.is_dir():
        _info(f"Final output directory not found ({final_dir}); skipping transfer.")
    else:
        import gzip as _gzip
        import shutil as _shutil

        host     = _conf(args, "host",     "SFTP_HOST",     required=True)
        user     = _conf(args, "user",     "SFTP_USER",     required=True)
        port     = int(_conf(args, "port", "SFTP_PORT") or 22)
        identity = _conf(args, "identity", "SFTP_IDENTITY") or None

        remote_base = _conf(args, "remote_dir", "SFTP_REMOTE_BASE", required=True)
        remote_dir  = f"{remote_base.rstrip('/')}/DMB/{args.batch}"

        gz_files: list[Path] = []
        for src_name, dest_name in [
            ("counts.tsv", f"{args.batch}_counts.tsv.gz"),
            ("bases.tsv",  f"{args.batch}_bases.tsv.gz"),
        ]:
            src = final_dir / src_name
            if not src.exists():
                _info(f"  {src_name} not found in {final_dir} — skipping.")
                continue
            gz = final_dir / dest_name
            with src.open("rb") as _fin, _gzip.open(gz, "wb") as _fout:
                _shutil.copyfileobj(_fin, _fout)
            gz_files.append(gz)
            _info(f"  Compressed {src_name} → {dest_name}")

        if gz_files:
            _info(f"Transferring {len(gz_files)} file(s) to {user}@{host}:{remote_dir} ...")
            _timeout = getattr(args, "connect_timeout", 300.0)
            n_up = n_sk = 0
            try:
                with SFTPTransfer(host=host, username=user, port=port, key_path=identity or None, timeout=_timeout) as xfer:
                    if getattr(args, "rerun", False):
                        xfer.remove_remote_dir(remote_dir)
                        _info(f"Deleted remote directory {remote_dir} for rerun.")
                    n_up, n_sk = xfer.upload_flat(gz_files, remote_dir, verbose=getattr(args, "verbose", False))
            finally:
                for _gz in gz_files:
                    _gz.unlink(missing_ok=True)
            _skip_msg = f", {n_sk} already present (skipped)" if n_sk else ""
            _info(f"Transferred {n_up} file(s) to {remote_dir}{_skip_msg}.")

        cleanup = str(cfg.get("CLEANUP_OUTPUT_DIR") or "true").strip().lower()
        if cleanup not in ("false", "0", "no"):
            _shutil.rmtree(local_root, ignore_errors=True)
            _info(f"Deleted output directory {local_root}.")

    batch_fields: dict = {}
    ehio_version_field    = str(cfg.get("MAG_DMB_BATCH_EHIO_VERSION")    or "").strip()
    drakkar_version_field = str(cfg.get("MAG_DMB_BATCH_DRAKKAR_VERSION") or "").strip()
    derep_mags_field      = str(cfg.get("MAG_DMB_BATCH_DEREP_MAGS")      or "").strip()
    if ehio_version_field:
        batch_fields[ehio_version_field] = __version__
    if drakkar_version_field:
        batch_fields[drakkar_version_field] = _get_drakkar_version()
    if derep_mags_field:
        derep_tsv   = local_root / "dereplicating.tsv"
        derep_count = parse_dereplicating_tsv(derep_tsv)
        if derep_count is not None:
            batch_fields[derep_mags_field] = derep_count
            _info(f"Dereplicated MAGs: {derep_count}")
        else:
            _info(f"dereplicating.tsv not found or output_bin_number missing at {derep_tsv}.")

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
# annotating
# ---------------------------------------------------------------------------

def cmd_annotating(args: argparse.Namespace) -> int:
    if args.input:
        return _run_annotating_input(args)
    return _run_annotating_output(args)


def _run_annotating_input(args: argparse.Namespace) -> int:
    """Check which MAGs need functional annotation and write their paths to a file."""
    from ehio.airtable import AirtableClient

    token       = _resolve_token(args)
    base_id     = _require_cfg("MAG_BASE")
    batch_table = _require_cfg("MAG_DMB_BATCH")
    mag_table   = _require_cfg("MAG_ENTRY")

    batch_code_field = _require_cfg("MAG_DMB_BATCH_CODE")
    mag_list_field   = _require_cfg("MAG_DMB_BATCH_LIST_MAGS")
    mag_name_field   = _require_cfg("MAG_ENTRY_NAME")
    annotated_field  = str(cfg.get("MAG_ENTRY_ANNOTATED") or "").strip()

    force_reannotate = getattr(args, "rerun", False)

    ann_dir  = Path(args.annotation_dir).resolve()
    out_file = Path(args.annotation_file)

    _info(f"Looking up batch '{args.batch}' in Airtable...")
    client = AirtableClient(api_key=token, base_id=base_id)
    batch_record = client.fetch_batch_record(batch_table, batch_code_field, args.batch)
    if batch_record is None:
        _die(f"Batch '{args.batch}' not found.")

    # Read requested annotation type from batch record (kegg / genes / all)
    ann_type_field = str(cfg.get("MAG_DMB_BATCH_ANNOTATION_TYPE") or "").strip()
    requested_type = "all"
    if ann_type_field:
        raw = batch_record.get("fields", {}).get(ann_type_field)
        if raw:
            requested_type = str(raw).strip().lower()

    # Annotation hierarchy: kegg ⊂ genes ⊂ all.
    # "true" is treated as legacy equivalent of "all".
    _sufficient: dict[str, set[str]] = {
        "kegg":  {"kegg", "genes", "all", "true"},
        "genes": {"genes", "all", "true"},
        "all":   {"all", "true"},
    }
    sufficient_statuses = _sufficient.get(requested_type, {"all", "true"})

    # Build per-MAG annotation status from Airtable
    mag_rec_ids = batch_record.get("fields", {}).get(mag_list_field, [])
    if not mag_rec_ids:
        _die(f"No MAG records linked in field {mag_list_field} of batch '{args.batch}'.")
    _info(f"Fetching {len(mag_rec_ids)} MAG record(s) from Airtable...")

    mag_status: dict[str, str] = {}  # fa filename → annotated value
    for rec_id in mag_rec_ids:
        if not (isinstance(rec_id, str) and rec_id.startswith("rec")):
            continue
        rec = client.fetch_record_by_id(mag_table, rec_id)
        if not rec:
            continue
        fields = rec.get("fields", {})
        name = str(fields.get(mag_name_field, "") or "").strip()
        ann_val = str(fields.get(annotated_field) or "").strip().lower() if annotated_field else ""
        if name:
            mag_status[name] = ann_val

    # Scan the dereplicated genomes directory — it is the authoritative source
    # of which MAGs actually exist and need annotation.
    fa_files = sorted(ann_dir.glob("*.fa"))
    if not fa_files:
        _info(f"No .fa files found in {ann_dir}.")

    paths_to_annotate: list[str] = []          # full annotation (kegg/genes/function)
    paths_to_annotate_clusters: list[str] = []  # cluster-only upgrade (genes → all)
    n_skipped = 0
    for fa_file in fa_files:
        if force_reannotate:
            paths_to_annotate.append(str(fa_file))
            continue
        ann_val = mag_status.get(fa_file.name, "")
        if ann_val in sufficient_statuses:
            n_skipped += 1
            continue
        # MAG has "genes" status and "all" is requested: skip gene annotation,
        # run cluster annotation only.
        if requested_type == "all" and ann_val == "genes":
            paths_to_annotate_clusters.append(str(fa_file))
        else:
            paths_to_annotate.append(str(fa_file))

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("w", encoding="utf-8") as fh:
        for p in paths_to_annotate:
            fh.write(p + "\n")

    clusters_file = out_file.parent / (out_file.stem + "_clusters" + out_file.suffix)
    with clusters_file.open("w", encoding="utf-8") as fh:
        for p in paths_to_annotate_clusters:
            fh.write(p + "\n")

    msg_parts = [f"Wrote {len(paths_to_annotate)} MAG path(s) to {out_file}"]
    if paths_to_annotate_clusters:
        msg_parts.append(f"{len(paths_to_annotate_clusters)} cluster-only path(s) to {clusters_file}")
    if n_skipped:
        msg_parts.append(f"{n_skipped} already at '{requested_type}' level (skipped)")
    _info(", ".join(msg_parts) + ".")
    return 0


def _run_annotating_output(args: argparse.Namespace) -> int:
    """Parse annotation results, update MAG_ENTRY in Airtable, transfer files."""
    import gzip as _gzip
    import shutil as _shutil

    from ehio.airtable import AirtableClient
    from ehio.metadata import (
        parse_genome_taxonomy_tsv,
        parse_annotation_tsv,
        build_entry_update,
        ANNOTATING_TAXONOMY_KEYS,
        ANNOTATING_GTDB_KEYS,
        ANNOTATING_FUNC_KEYS,
    )
    from ehio.transfer import SFTPTransfer

    token       = _resolve_token(args)
    base_id     = _require_cfg("MAG_BASE")
    batch_table = _require_cfg("MAG_DMB_BATCH")
    mag_table   = _require_cfg("MAG_ENTRY")

    batch_code_field = _require_cfg("MAG_DMB_BATCH_CODE")
    mag_list_field   = _require_cfg("MAG_DMB_BATCH_LIST_MAGS")
    mag_name_field   = _require_cfg("MAG_ENTRY_NAME")

    local_root = Path(args.local_dir).resolve()
    if not local_root.is_dir():
        _die(f"Local directory not found: {local_root}")

    ann_dir = local_root / "annotating"
    if not ann_dir.is_dir():
        _die(f"Annotating output directory not found: {ann_dir}")

    _info(f"Looking up batch '{args.batch}' in Airtable...")
    client = AirtableClient(api_key=token, base_id=base_id)
    batch_record = client.fetch_batch_record(batch_table, batch_code_field, args.batch)
    if batch_record is None:
        _die(f"Batch '{args.batch}' not found.")

    # Read annotation type written to Airtable status (kegg / genes / all)
    ann_type_field = str(cfg.get("MAG_DMB_BATCH_ANNOTATION_TYPE") or "").strip()
    annotation_type_value = "all"
    if ann_type_field:
        raw = batch_record.get("fields", {}).get(ann_type_field)
        if raw:
            annotation_type_value = str(raw).strip().lower()

    # Fetch linked MAG records and key them by MAG_ENTRY_NAME
    mag_rec_ids = batch_record.get("fields", {}).get(mag_list_field, [])
    if not mag_rec_ids:
        _die(f"No MAG records linked in field {mag_list_field} of batch '{args.batch}'.")
    _info(f"Fetching {len(mag_rec_ids)} MAG record(s)...")
    mag_by_name: dict[str, dict] = {}
    for rec_id in mag_rec_ids:
        if isinstance(rec_id, str) and rec_id.startswith("rec"):
            rec = client.fetch_record_by_id(mag_table, rec_id)
            if rec:
                name = str(rec.get("fields", {}).get(mag_name_field, "") or "").strip()
                if name:
                    mag_by_name[name] = rec

    # Build field_map covering taxonomy, GTDB, and functional metrics
    all_metric_keys = {**ANNOTATING_TAXONOMY_KEYS, **ANNOTATING_GTDB_KEYS, **ANNOTATING_FUNC_KEYS}
    field_map: dict[str, str] = {}
    for metric_key, config_key in all_metric_keys.items():
        fld_id = str(cfg.get(config_key) or "").strip()
        if fld_id:
            field_map[metric_key] = fld_id

    # Parse genome_taxonomy.tsv
    taxonomy_tsv = ann_dir / "genome_taxonomy.tsv"
    taxonomy_data = parse_genome_taxonomy_tsv(taxonomy_tsv)
    if not taxonomy_data:
        _info(f"genome_taxonomy.tsv not found or empty at {taxonomy_tsv}.")

    # Parse per-genome annotation TSVs from annotating/final/
    final_dir = ann_dir / "final"
    annotation_data: dict[str, dict] = {}
    if final_dir.is_dir():
        for tsv_file in sorted(final_dir.glob("*.tsv")):
            mag_key = tsv_file.stem + ".fa"
            annotation_data[mag_key] = parse_annotation_tsv(tsv_file)

    # Build Airtable update payloads
    updates: list[dict] = []
    for genome_name, rec in mag_by_name.items():
        metrics: dict = {}
        if genome_name in taxonomy_data:
            metrics.update(taxonomy_data[genome_name])
        if genome_name in annotation_data:
            metrics.update(annotation_data[genome_name])
            metrics["annotated"] = annotation_type_value
        if not metrics:
            continue
        payload = build_entry_update(rec["id"], metrics, field_map)
        if payload["fields"]:
            updates.append(payload)

    if updates:
        _info(f"Updating {len(updates)} MAG_ENTRY records in Airtable...")
        client.update_records(mag_table, updates)
        _info("Airtable update complete.")
    else:
        _info("No annotation metrics found to update.")

    host        = _conf(args, "host",       "SFTP_HOST",        required=True)
    user        = _conf(args, "user",       "SFTP_USER",        required=True)
    port        = int(_conf(args, "port",   "SFTP_PORT") or 22)
    identity    = _conf(args, "identity",   "SFTP_IDENTITY") or None
    remote_base = _conf(args, "remote_dir", "SFTP_REMOTE_BASE", required=True)

    # Upload genome_taxonomy.tsv + tree files to DMB/{batch}/
    dmb_remote = f"{remote_base.rstrip('/')}/DMB/{args.batch}"
    dmb_files: list[Path] = []
    for fname in ("genome_taxonomy.tsv", "bacteria.tree", "archaea.tree", "gene_annotations.tsv.xz"):
        p = ann_dir / fname
        if p.exists():
            dmb_files.append(p)
        else:
            _info(f"  {fname} not found in {ann_dir} — skipping.")

    checkm2_report = local_root / "profiling_genomes" / "checkm2" / "quality_report.tsv"
    if checkm2_report.exists():
        dmb_files.append(checkm2_report)
    else:
        _info(f"  {checkm2_report} not found — skipping.")

    _timeout = getattr(args, "connect_timeout", 300.0)
    if dmb_files:
        _info(f"Uploading {len(dmb_files)} file(s) to {user}@{host}:{dmb_remote} ...")
        with SFTPTransfer(host=host, username=user, port=port, key_path=identity or None, timeout=_timeout) as xfer:
            n_up, n_sk = xfer.upload_flat(dmb_files, dmb_remote, verbose=getattr(args, "verbose", False))
        _skip_msg = f", {n_sk} already present (skipped)" if n_sk else ""
        _info(f"Uploaded {n_up} file(s) to {dmb_remote}{_skip_msg}.")

    # Gzip per-genome TSVs and upload to ANN/{batch}/
    ann_remote = f"{remote_base.rstrip('/')}/ANN/{args.batch}"
    gz_files: list[Path] = []
    if final_dir.is_dir():
        for tsv_file in sorted(final_dir.glob("*.tsv")):
            gz = Path(str(tsv_file) + ".gz")
            with tsv_file.open("rb") as _fin, _gzip.open(gz, "wb") as _fout:
                _shutil.copyfileobj(_fin, _fout)
            gz_files.append(gz)

    if gz_files:
        _info(f"Uploading {len(gz_files)} compressed annotation file(s) to {ann_remote} ...")
        n_ann_up = n_ann_sk = 0
        try:
            with SFTPTransfer(host=host, username=user, port=port, key_path=identity or None, timeout=_timeout) as xfer:
                if getattr(args, "rerun", False):
                    xfer.remove_remote_dir(ann_remote)
                    _info(f"Deleted remote directory {ann_remote} for rerun.")
                n_ann_up, n_ann_sk = xfer.upload_flat(gz_files, ann_remote, verbose=getattr(args, "verbose", False))
        finally:
            for _gz in gz_files:
                _gz.unlink(missing_ok=True)
        _skip_msg = f", {n_ann_sk} already present (skipped)" if n_ann_sk else ""
        _info(f"Uploaded {n_ann_up} compressed annotation file(s) to {ann_remote}{_skip_msg}.")

    done_status        = str(cfg.get("PROCESSING_DONE_STATUS") or "Done").strip()
    batch_status_field = str(cfg.get("MAG_DMB_BATCH_STATUS")   or "").strip()
    if batch_status_field:
        client.update_records(
            batch_table,
            [{"id": batch_record["id"], "fields": {batch_status_field: done_status}}],
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

    def _add_sftp_overrides(p: argparse.ArgumentParser, rerun: bool = True) -> None:
        g = p.add_argument_group("Output / transfer options")
        g.add_argument("--host",     metavar="HOST", help="SFTP host (overrides SFTP_HOST).")
        g.add_argument("--user", "-u", metavar="USER", help="SFTP username (overrides SFTP_USER).")
        g.add_argument("--port",     metavar="PORT", help="SFTP port (overrides SFTP_PORT).")
        g.add_argument("--identity", "-k", metavar="KEY", help="SSH private key path.")
        g.add_argument("--local-dir", "-l", default=os.getcwd(), metavar="DIR",
            help="Local drakkar output directory. Default: current directory.")
        g.add_argument("--remote-dir", "-r", metavar="DIR",
            help="Remote base directory for file transfer.")
        if rerun:
            g.add_argument("--rerun", action="store_true",
                help="Delete the remote archive directory before uploading (use when rerunning a batch).")
        g.add_argument("--connect-timeout", metavar="SECONDS", type=float, default=300.0,
            help="SFTP connection timeout in seconds (default: 300).")

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
    p_pre.add_argument("--no-url-check", action="store_true",
        help="Skip the download check on the raw-read URLs (input mode).")
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
    p_qnt.add_argument("--mags-file", default="mags.tsv", metavar="PATH",
        help="Output MAG URLs file for drakkar (input mode). Default: mags.tsv.")
    p_qnt.add_argument("--reads-file", default="reads.tsv", metavar="PATH",
        help="Output reads sample file for drakkar (input mode). Default: reads.tsv.")
    p_qnt.add_argument("--quality-file", default="quality.tsv", metavar="PATH",
        help="Output MAG quality file for drakkar (input mode). Default: quality.tsv.")
    _add_sftp_overrides(p_qnt)
    p_qnt.set_defaults(func=cmd_quantifying)

    # ------------------------------------------------------------------
    # annotating
    # ------------------------------------------------------------------
    p_ann = sub.add_parser(
        "annotating",
        help="Input/output for the genome annotation workflow.",
        description=(
            "Input mode:  write genome paths for all MAGs linked to the batch\n"
            "             into a file for drakkar functional annotation.\n"
            "Output mode: parse GTDB-Tk taxonomy and per-genome functional annotation\n"
            "             results, update MAG_ENTRY records in Airtable, upload\n"
            "             taxonomy/tree files to DMB/{batch} and compressed per-genome\n"
            "             TSVs to ANN/{batch} via SFTP."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_mode(p_ann)
    _add_batch(p_ann)
    _add_token(p_ann)
    _add_verbose(p_ann)
    p_ann.add_argument("--annotation-file", "-f", default="annotation.tsv", metavar="PATH",
        help="Output paths file for drakkar annotation (input mode). Default: annotation.tsv.")
    p_ann.add_argument("--annotation-dir", "-d", default=".", metavar="DIR",
        help="Directory containing the dereplicated genome FASTA files (input mode).")
    _add_sftp_overrides(p_ann)
    p_ann.set_defaults(func=cmd_annotating)

    # ------------------------------------------------------------------
    # reference
    # ------------------------------------------------------------------
    p_ref = sub.add_parser(
        "reference",
        help="Upload the reference genome index of a finished preprocessing batch.",
        description=(
            "Archive the Bowtie2 index drakkar built for a batch, upload it to\n"
            "{SFTP_REMOTE_BASE}/{SFTP_REMOTE_REFERENCE_DIR}/{genome_code}.tar.gz and\n"
            "flag the genome as indexed. This is the last step of\n"
            "'ehio preprocessing --output', repeated on its own for a batch whose\n"
            "drakkar run is already finished — nothing is sent through snakemake again."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_batch(p_ref)
    _add_token(p_ref)
    _add_verbose(p_ref)
    p_ref.add_argument("--force", action="store_true",
        help="Upload even when the genome is already flagged as indexed or the archive already exists.")
    _add_sftp_overrides(p_ref, rerun=False)
    p_ref.set_defaults(func=cmd_reference)

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
            "can also be used manually to correct a status.\n\n"
            "With --failures-dir, the newest drakkar_<run_id>_failures.tsv found\n"
            "in that directory is also attached to the batch's error files field,\n"
            "so the cause of the failure is visible from Airtable."
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
    p_ss.add_argument("--failures-dir", metavar="DIR",
        help="drakkar output directory. Attaches the newest drakkar failure\n"
             "report found there to the batch's error files field.")
    p_ss.add_argument("--failures-since", metavar="EPOCH",
        help="Ignore failure reports older than this Unix timestamp, so a\n"
             "report left by an earlier launch is not attached again.")
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
        help="Kill the screen session and the Slurm jobs of a running batch.",
        description=(
            "Stops a batch completely:\n"
            "  1. Quits the screen session named after the batch.\n"
            "  2. Cancels every queued or running Slurm job of the batch.\n"
            "  3. Sets the batch status to SCANNING_STOPPED_STATUS.\n\n"
            "The screen session goes first on purpose: while the drakkar\n"
            "workflow is alive it resubmits any job cancelled under it.\n"
            "Jobs are found by the directory they were submitted from and by\n"
            "the snakemake run ids logged in {RUN_BASE}/{batch}/{batch}.out,\n"
            "so orphaned jobs of an already-dead session are cancelled too."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p_stop.add_argument("--module", "-m", required=True,
        choices=["preprocessing", "binning", "quantifying"],
        help="Module whose batch table to update.")
    p_stop.add_argument("--batch", "-b", required=True, metavar="BATCH",
        help="Batch code (screen session name) to stop.")
    p_stop.add_argument("--keep-jobs", action="store_true",
        help="Leave the Slurm jobs of the batch running; only kill the\n"
             "screen session and update the status.")
    p_stop.add_argument("--airtable-token", metavar="TOKEN",
        help="Airtable personal access token. Overrides $AIRTABLE_TOKEN.")
    p_stop.set_defaults(func=cmd_stop)

    # ------------------------------------------------------------------
    # jobs
    # ------------------------------------------------------------------
    p_jobs = sub.add_parser(
        "jobs",
        help="List the queued and running Slurm jobs of a batch.",
        description=(
            "Shows every Slurm job of the current user that belongs to the\n"
            "batch, matched by the directory it was submitted from and by the\n"
            "snakemake run ids logged in {RUN_BASE}/{batch}/{batch}.out.\n\n"
            "Nothing is cancelled — use 'ehio stop' for that."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p_jobs.add_argument("--module", "-m", required=True,
        choices=["preprocessing", "binning", "quantifying"],
        help="Module whose output base the batch runs in.")
    p_jobs.add_argument("--batch", "-b", required=True, metavar="BATCH",
        help="Batch code to list the jobs of.")
    p_jobs.set_defaults(func=cmd_jobs)

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
    "preprocessing": ("EHI_BASE", "EHI_PPR_BATCH", "EHI_PPR_BATCH_CODE", "EHI_PPR_BATCH_STATUS", "EHI_PPR_BATCH_ERROR_FILES"),
    "binning":       ("EHI_BASE", "EHI_ASB_BATCH", "EHI_ASB_BATCH_CODE", "EHI_ASB_BATCH_STATUS", "EHI_ASB_BATCH_ERROR_FILES"),
    "quantifying":   ("MAG_BASE", "MAG_DMB_BATCH", "MAG_DMB_BATCH_CODE", "MAG_DMB_BATCH_STATUS", "MAG_DMB_BATCH_ERROR_FILES"),
}


def _upload_failure_report(
    client,
    batch_table: str,
    batch_record: dict,
    error_files_field: str,
    failures_dir: str,
    since: float | None = None,
) -> None:
    """Attach the newest drakkar failure report in failures_dir to the batch record.

    Never raises: a batch that failed must still end up flagged as failed in
    Airtable even when the report cannot be found or uploaded.
    """
    from ehio.airtable import AirtableError
    from ehio.drakkar import find_failure_report

    if not error_files_field:
        return
    try:
        report = find_failure_report(failures_dir, since=since)
    except OSError as exc:
        print(f"  Warning: could not look for a drakkar failure report in "
              f"{failures_dir}: {exc}", file=sys.stderr)
        return
    if report is None:
        print(f"  No drakkar failure report found in {failures_dir}.", file=sys.stderr)
        return

    # Filenames carry the drakkar run id, so an already-attached report means
    # this same failed run was reported before — do not attach it twice.
    attached = batch_record.get("fields", {}).get(error_files_field) or []
    if any(str(a.get("filename", "")) == report.name for a in attached if isinstance(a, dict)):
        _info(f"Failure report '{report.name}' is already attached.")
        return

    try:
        client.upload_attachment(batch_table, batch_record["id"], error_files_field, report)
    except AirtableError as exc:
        print(f"  Warning: could not attach {report.name}: {exc}", file=sys.stderr)
        return
    _info(f"Attached failure report '{report.name}'.")


def cmd_set_status(args: argparse.Namespace) -> int:
    from ehio.airtable import AirtableClient

    base_cfg, table_cfg, code_cfg, status_cfg, error_files_cfg = _SET_STATUS_CFG[args.module]

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

    if args.failures_dir:
        try:
            since = float(args.failures_since) if args.failures_since else None
        except ValueError:
            since = None
        _upload_failure_report(
            client,
            batch_table,
            batch_record,
            str(cfg.get(error_files_cfg) or "").strip(),
            args.failures_dir,
            since=since,
        )
    return 0


def cmd_reference(args: argparse.Namespace) -> int:
    """Upload the reference index of a finished batch, without rerunning drakkar.

    'ehio preprocessing --output' does this as its last step, but a batch whose
    upload was skipped or failed (before this existed, or because the transfer
    broke) has a finished output directory and no index on ERDA.  This picks up
    exactly that step, so nothing has to go through snakemake again.
    """
    from ehio.airtable import AirtableClient
    from ehio.reference import upload_reference_index_status

    token       = _resolve_token(args)
    base_id     = _require_cfg("EHI_BASE")
    batch_table = _require_cfg("EHI_PPR_BATCH")
    batch_code_field = _require_cfg("EHI_PPR_BATCH_CODE")

    local_root = Path(args.local_dir).resolve()
    if not local_root.is_dir():
        _die(f"Local directory not found: {local_root}")

    _info(f"Looking up batch '{args.batch}' in Airtable...")
    client = AirtableClient(api_key=token, base_id=base_id)
    batch_record = client.fetch_batch_record(batch_table, batch_code_field, args.batch)
    if batch_record is None:
        _die(f"Batch '{args.batch}' not found in {batch_table}.")

    host     = _conf(args, "host",     "SFTP_HOST",     required=True)
    user     = _conf(args, "user",     "SFTP_USER",     required=True)
    port     = int(_conf(args, "port", "SFTP_PORT") or 22)
    identity = _conf(args, "identity", "SFTP_IDENTITY") or None
    remote_base = _conf(args, "remote_dir", "SFTP_REMOTE_BASE", required=True)

    status = upload_reference_index_status(
        batch_record, local_root, token,
        host=host, user=user, port=port, identity=identity,
        remote_base=remote_base,
        timeout=getattr(args, "connect_timeout", 300.0),
        verbose=getattr(args, "verbose", False),
        force=args.force,
    )

    if status == "uploaded":
        return 0
    if status == "already-indexed":
        _info("Nothing to do. Pass --force to upload the index anyway.")
        return 0

    hints = {
        "not-flagged": (
            "The archive is on ERDA but the genome could not be flagged as indexed. "
            "Check EHI_GENOME_INDEX_TABLE / EHI_GENOME_INDEXED in the config, then "
            "rerun this command."
        ),
        "no-reference": f"Batch '{args.batch}' has no reference genome, so there is no index to upload.",
        "no-code": "The genome record has no code, so the archive cannot be named.",
        "no-index": (
            f"No complete Bowtie2 index under {local_root}. Point -l at the drakkar "
            "output directory of the batch (the one holding data/references), or at "
            "that references directory itself. If the output directory was already "
            "deleted, the index is gone and has to be rebuilt."
        ),
        "ambiguous": "Move the unrelated references out of the directory and rerun.",
    }
    print(f"{ERROR}Error:{RESET} {hints.get(status, status)}", file=sys.stderr)
    return 1


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


def _batch_dirs(module: str, batch: str) -> tuple[str, str, str]:
    """Return (output_dir, run_dir, out_file) of a batch.

    These are the paths ehio scanning writes the launch script and the
    drakkar output to, and the ones the Slurm jobs of the batch are
    submitted from.
    """
    output_dir = str(Path(_require_cfg(_OUTPUT_BASE_CFG[module])) / batch)
    run_base   = str(cfg.get("RUN_BASE") or "").strip()
    run_dir    = str(Path(run_base) / batch) if run_base else ""
    out_file   = str(Path(run_dir) / f"{batch}.out") if run_dir else ""
    return output_dir, run_dir, out_file


def cmd_jobs(args: argparse.Namespace) -> int:
    from ehio.slurm import SlurmUnavailable, find_batch_jobs

    output_dir, run_dir, out_file = _batch_dirs(args.module, args.batch)
    try:
        jobs = find_batch_jobs(output_dir, run_dir, out_file)
    except SlurmUnavailable as exc:
        _die(str(exc))
        return 1

    if not jobs:
        _info(f"No queued or running Slurm jobs found for batch '{args.batch}'.")
        return 0

    rows = [("JOBID", "STATE", "TIME", "PARTITION", "NAME")] + [
        (j.job_id, j.state, j.elapsed, j.partition, j.name) for j in jobs
    ]
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    for row in rows:
        print("  ".join(value.ljust(width) for value, width in zip(row, widths)).rstrip())

    states: dict[str, int] = {}
    for job in jobs:
        states[job.state] = states.get(job.state, 0) + 1
    summary = ", ".join(f"{count} {state.lower()}" for state, count in sorted(states.items()))
    _info(f"{len(jobs)} job(s) for batch '{args.batch}': {summary}.")
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    import subprocess
    from ehio.airtable import AirtableClient
    # Written before the screen session is killed and deleted by the launch
    # script on every start: it tells the exit trap of the dying script that
    # the batch was stopped on purpose, so it does not flag it as an error.
    from ehio.scanning import STOP_SENTINEL

    base_cfg, table_cfg, code_cfg, status_cfg, _ = _SET_STATUS_CFG[args.module]
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

    output_dir, run_dir, out_file = _batch_dirs(args.module, args.batch)

    # Tell the exit trap of the launch script that this is a deliberate stop,
    # before anything is killed — otherwise it overwrites the status below
    # with the processing error status on its way out.
    if run_dir and Path(run_dir).is_dir():
        try:
            (Path(run_dir) / STOP_SENTINEL).write_text("", encoding="utf-8")
        except OSError as exc:
            _info(f"Could not write the stop marker in {run_dir}: {exc}")

    # The screen session first: while the drakkar workflow is alive it
    # resubmits every job that is cancelled under it.
    session = args.batch
    result = subprocess.run(
        ["screen", "-S", session, "-X", "quit"],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        _info(f"Screen session '{session}' terminated.")
    else:
        _info(f"No screen session named '{session}' found (already stopped or never started).")

    if args.keep_jobs:
        _info("--keep-jobs: Slurm jobs of the batch left running.")
    else:
        _cancel_batch_jobs(args.batch, output_dir, run_dir, out_file)

    client.update_records(
        batch_table,
        [{"id": batch_record["id"], "fields": {status_field: stopped_status}}],
    )
    _info(f"Batch '{args.batch}' status → '{stopped_status}'.")
    return 0


def _cancel_batch_jobs(batch: str, output_dir: str, run_dir: str, out_file: str) -> None:
    """Cancel the Slurm jobs of a batch, reporting what happened."""
    from ehio.slurm import SlurmUnavailable, cancel_batch_jobs

    try:
        cancelled, remaining, errors = cancel_batch_jobs(output_dir, run_dir, out_file)
    except SlurmUnavailable as exc:
        _info(f"Slurm jobs not cancelled — {exc}")
        return

    for message in errors:
        _info(f"scancel reported: {message}")
    if cancelled:
        _info(f"Cancelled {len(cancelled)} Slurm job(s): {' '.join(cancelled)}")
    else:
        _info(f"No queued or running Slurm jobs found for batch '{batch}'.")
    if remaining:
        ids = " ".join(j.job_id for j in remaining)
        _info(
            f"WARNING: {len(remaining)} job(s) are still queued after cancelling: {ids}. "
            f"Check with 'ehio jobs -m ... -b {batch}' and cancel them manually."
        )


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
    from ehio.airtable import AirtableError

    parser = _build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 0
    try:
        return args.func(args)
    except AirtableError as exc:
        _die(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
