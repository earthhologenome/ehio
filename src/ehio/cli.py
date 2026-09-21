"""Command-line interface for ehio."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from ehio import __version__
from ehio import config as cfg
from ehio import mirror

ERROR = "\033[1;31m"
INFO  = "\033[1;34m"
RESET = "\033[0m"

_PRIMARY_BASE = {
    "PREPROCESSING": "EHI_BASE",
    "BINNING":       "EHI_BASE",
    "QUANTIFYING":   "MAG_BASE",
    "AMR":           "EHI_BASE",
}
_SECONDARY_BASE = {
    "BINNING": "MAG_BASE",
}
_BATCH_TABLE_KEY = {
    "PREPROCESSING": "EHI_PPR_BATCH",
    "BINNING":       "EHI_ASB_BATCH",
    "QUANTIFYING":   "MAG_DMB_BATCH",
    "AMR":           "EHI_AMR_BATCH",
}
_ENTRY_TABLE_KEY = {
    "PREPROCESSING": "EHI_PPR_ENTRY",
    "BINNING":       "EHI_ASB_ENTRY",
    "QUANTIFYING":   "MAG_DMB_ENTRY",
    # The AMR module runs on the assembly entries of the binning table.
    "AMR":           "EHI_ASB_ENTRY",
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


def _warn(msg: str) -> None:
    print(f"{ERROR}Warning:{RESET} {msg}", file=sys.stderr)


def _flag_cfg(key: str) -> bool:
    return str(cfg.get(key) or "").strip().lower() in ("true", "1", "yes")


def _core_token(args: argparse.Namespace) -> str:
    token = (getattr(args, "core_token", None) or "").strip()
    return token or os.environ.get("EHI_CORE_TOKEN", "").strip()


def _core(args: argparse.Namespace, *, holds: bool = False):
    """ehi-core for this command, as an ehio.core.CoreSession.

    The session is falsy — ehio runs on Airtable alone, as it did before the
    core — when EHI_CORE_URL is empty or when this machine has no pipeline
    token yet, which is said once.  With a token, a core that cannot be reached
    or refuses the token is reported and left out of this command, unless
    EHI_CORE_REQUIRED is on or `holds` says the command works on data only the
    core holds (the MAGs): then it stops the command instead of letting it run
    on part of its data.
    """
    from ehio.core import TOKEN_HINT, WAIT_MINUTES, CoreClient, CoreError, CoreSession, verify

    url = str(cfg.get("EHI_CORE_URL") or "").strip()
    if not url:
        return CoreSession()
    required = _flag_cfg("EHI_CORE_REQUIRED")
    token = _core_token(args)
    if not token:
        message = f"EHI_CORE_URL is set, but there is no ehi-core pipeline token. {TOKEN_HINT}"
        if required:
            _die(message)
        _warn(f"{message} Running on Airtable alone.")
        return CoreSession()
    try:
        wait_minutes = float(cfg.get("EHI_CORE_WAIT_MINUTES") or WAIT_MINUTES)
    except (TypeError, ValueError):
        _warn(f"EHI_CORE_WAIT_MINUTES is not a number; waiting up to {WAIT_MINUTES:.0f} min for ehi-core.")
        wait_minutes = WAIT_MINUTES
    client = CoreClient(url, token, wait_minutes=wait_minutes)
    try:
        verify(client, token)
    except CoreError as exc:
        if required or holds:
            _die(str(exc))
        _warn(f"{exc}\n  Running on Airtable alone for this command.")
        return CoreSession()
    return CoreSession(client, required=required)


def _dmb_reads(client, batch, code: str, *, need_reads: bool = True):
    """The preprocessed samples a DMB batch maps against its MAGs, and the
    keys they are read through.

    Airtable keeps them as a link from the batch out to its PPR table, one
    record fetched at a time; the core answers with the whole list at once.
    `need_reads` is for the input step, which writes the reads into a sample
    sheet; the output step only names the samples.
    """
    from ehio.batches import Fields

    if batch.from_core:
        if not batch.entries:
            _die(f"ehi-core holds no samples for batch '{code}'.")
        _info(f"Read {len(batch.entries)} sample(s) from ehi-core.")
        return batch.entries, batch.fields

    ppr_table      = _require_cfg("MAG_PPR")
    ppr_list_field = _require_cfg("MAG_DMB_BATCH_LIST_PPR")
    _reads = _require_cfg if need_reads else (lambda key: str(cfg.get(key) or "").strip())
    fields = Fields(
        sample=_require_cfg("MAG_PPR_EHI"),
        reads1=_reads("MAG_PPR_READS1"),
        reads2=_reads("MAG_PPR_READS2"),
    )
    rec_ids = batch.record.get("fields", {}).get(ppr_list_field, [])
    if not rec_ids:
        _die(f"No PPR records linked in field {ppr_list_field} of batch '{code}'.")
    _info(f"Fetching {len(rec_ids)} PPR record(s)...")
    records = []
    for rec_id in rec_ids:
        if isinstance(rec_id, str) and rec_id.startswith("rec"):
            record = client.fetch_record_by_id(ppr_table, rec_id)
            if record:
                records.append(record)
    if not records:
        _die(f"Could not fetch any PPR records for batch '{code}'.")
    return records, fields


def _create_airtable_mappings(client, batch_code, batch_record, ppr_records, all_metrics, ppr_ehi_field):
    """Create a DMB batch's MAG_DMB_ENTRY records, one per preprocessed sample.

    Returns (preprocessing code, DM code, mapping rate) per sample, which is
    what the core is told about them.
    """
    entry_table       = _require_cfg("MAG_DMB_ENTRY")
    entry_batch_field = _require_cfg("MAG_DMB_ENTRY_BATCH")
    entry_ppr_field   = _require_cfg("MAG_DMB_ENTRY_PPR")
    entry_rate_field  = str(cfg.get("MAG_DMB_ENTRY_MAPPING_RATE") or "").strip()
    batch_rec_id      = batch_record["id"]

    records_to_create: list[dict] = []
    for ppr_rec in ppr_records:
        metrics = all_metrics.get(_first_value(ppr_rec.get("fields", {}).get(ppr_ehi_field)), {})
        rec_fields: dict = {
            entry_batch_field: [batch_rec_id],
            entry_ppr_field:   [ppr_rec["id"]],
        }
        if entry_rate_field and metrics.get("mapping_rate") is not None:
            rec_fields[entry_rate_field] = metrics["mapping_rate"]
        records_to_create.append(rec_fields)

    existing = client._table(entry_table).all(
        formula=f'FIND("{batch_rec_id}", ARRAYJOIN({{{entry_batch_field}}}))'
    )
    dm_records = existing
    if existing:
        _info(f"{len(existing)} MAG_DMB_ENTRY record(s) already exist for this batch — skipping creation.")
    elif records_to_create:
        _info(f"Creating {len(records_to_create)} MAG_DMB_ENTRY records...")
        dm_records = client.create_records(entry_table, records_to_create)
        _info("MAG_DMB_ENTRY records created.")

    dm_code_field = str(cfg.get("MAG_DMB_ENTRY_CODE") or "").strip()
    dm_code_by_ppr = {
        _first_value(rec.get("fields", {}).get(entry_ppr_field)):
            _first_value(rec.get("fields", {}).get(dm_code_field)) if dm_code_field else None
        for rec in dm_records or []
    }
    return [
        (
            mirror.cell(ppr_rec.get("fields", {}), "MAG_PPR_CODE"),
            dm_code_by_ppr.get(ppr_rec["id"]),
            all_metrics.get(_first_value(ppr_rec.get("fields", {}).get(ppr_ehi_field)), {}).get("mapping_rate"),
        )
        for ppr_rec in ppr_records
    ]


def _reference_record(batch, entries: list[dict]) -> dict:
    """The record the host reference genome is resolved from.

    The genome table stays in Airtable, so a batch read from the core names its
    genome by code, which the resolver already accepts alongside the record id
    an Airtable link cell holds.
    """
    if not batch.from_core:
        return batch.record
    field = str(cfg.get("EHI_PPR_BATCH_REFERENCE") or "").strip()
    code = next(
        (str(e.get("reference_genome_code") or "").strip() for e in entries
         if str(e.get("reference_genome_code") or "").strip()),
        "",
    )
    return {"id": batch.code, "fields": {field: code}} if field and code else {"fields": {}}


def _open_batch(module: str, args: argparse.Namespace, core):
    """The batch a command was given, from Airtable or from ehi-core.

    Airtable holds today's batches, so it is looked in first and a batch it
    holds is read exactly as before. A batch it does not hold is read from the
    core, which is where the EHI is moving; emptying a module's Airtable keys
    in the config leaves the core as the only place looked at.
    """
    from ehio.batches import open_batch

    _info(f"Looking up batch '{args.batch}'...")
    try:
        return open_batch(module, args.batch, _resolve_token(args), core)
    except LookupError as exc:
        _die(str(exc))


def _dmb_mags(client, core, batch_record: dict | None, batch: str) -> list[dict]:
    """The MAGs a DMB batch works on, each shaped by ehio.mirror.

    Without the core they are the MAG records linked to the batch in Airtable.
    With it, the core is where they are read from, since it also holds the MAGs
    Airtable has no room for: the Airtable records are first copied in (only
    what the core lacks) and linked to the batch there.

    A batch Airtable does not hold (`batch_record` is None) has nothing to copy
    in, so its MAGs are read from the core and nothing else is touched.
    """
    if batch_record is None:
        mags = core.client.batch_mags(batch)
        if not mags:
            _die(f"ehi-core holds no MAGs for batch '{batch}'.")
        _info(f"Read {len(mags)} MAG(s) of batch '{batch}' from ehi-core.")
        return [mirror.mag_from_core(mag) for mag in mags]

    mag_table      = _require_cfg("MAG_ENTRY")
    mag_list_field = _require_cfg("MAG_DMB_BATCH_LIST_MAGS")
    rec_ids = [
        rec_id for rec_id in (batch_record.get("fields", {}).get(mag_list_field) or [])
        if isinstance(rec_id, str) and rec_id.startswith("rec")
    ]
    if not rec_ids and not core:
        _die(f"No MAG records linked in field {mag_list_field} of batch '{batch}'.")
    if rec_ids:
        _info(f"Fetching {len(rec_ids)} MAG record(s) from Airtable...")
    records = [rec for rec in (client.fetch_record_by_id(mag_table, rec_id) for rec_id in rec_ids) if rec]
    if not core:
        if not records:
            _die(f"Could not fetch any MAG records for batch '{batch}'.")
        return [mirror.mag_from_airtable(rec) for rec in records]

    core.write([mirror.batch("quantifying", batch, batch_record), *mirror.airtable_mags(records)],
               f"MAGs of batch '{batch}'")
    if records:
        core.client.link_batch_mags(batch, [rec["id"] for rec in records])
    mags = core.client.batch_mags(batch)
    if not mags:
        _die(
            f"Batch '{batch}' has no MAGs: none are linked in field {mag_list_field} of its "
            f"Airtable record, and none in ehi-core."
        )
    _info(f"Read {len(mags)} MAG(s) of batch '{batch}' from ehi-core.")
    return [mirror.mag_from_core(mag) for mag in mags]


# ---------------------------------------------------------------------------
# preprocessing
# ---------------------------------------------------------------------------

def _get_drakkar_version(output_dir: str | Path | None = None) -> str:
    """Return the drakkar version(s) a batch was processed with.

    drakkar stamps every run it starts with its own version, in the run
    metadata it leaves in the output directory
    ('logging/drakkar_<run id>.yaml', or the output root before drakkar 2.5.0),
    so reading it from there records the version that actually produced the
    results rather than whichever drakkar happens to be installed now.

    One batch can hold several runs — profiling and then annotating, or a batch
    resumed after a failure — and drakkar may have been updated between them.
    Every version that did part of the work is reported, oldest run first and
    each version once: '2.4.4/2.4.5'.

    Falls back to asking the installed drakkar when the output directory holds
    no run metadata, which is the case for a directory already cleaned up and
    for drakkar builds older than the metadata itself.
    """
    if output_dir is not None:
        from ehio.drakkar import drakkar_versions_used, format_drakkar_versions
        try:
            versions = drakkar_versions_used(output_dir)
        except OSError:
            versions = []
        if versions:
            return format_drakkar_versions(versions)

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
    """Fetch batch+entries and write a drakkar sample TSV."""
    from ehio.drakkar import write_sample_file, verify_input_files, verify_remote_urls

    core  = _core(args)
    batch = _open_batch("preprocessing", args, core)
    fields = batch.fields

    _info(f"Found {len(batch.entries)} entries for batch '{args.batch}'.")
    if not batch.entries:
        _die(f"No entries found for batch '{args.batch}'.")

    # The batch and its libraries may have been created in Airtable after the
    # core was loaded; the output step needs their rows there.
    if not batch.from_core:
        core.mirror(f"Batch '{args.batch}'", [
            mirror.batch("preprocessing", args.batch, batch.record),
            *mirror.preprocessings(args.batch, batch.entries),
        ])

    out_path = Path(args.sample_file)
    n = write_sample_file(
        batch.entries,
        out_path,
        sample_field=fields.sample,
        reads1_field=fields.reads1,
        reads2_field=fields.reads2,
    )
    _info(f"Wrote {n} samples to {out_path}")

    read_fields = [fields.reads1, fields.reads2]
    missing = verify_input_files(batch.entries, fields.sample, read_fields)
    if missing:
        for sample, path in missing:
            print(f"  WARNING: [{sample}] file not found: {path}", file=sys.stderr)
        _die(f"{len(missing)} input file(s) missing — fix the paths in {batch.source} "
             f"before launching drakkar.")

    if getattr(args, "no_url_check", False):
        _info("Skipping raw-read URL check (--no-url-check).")
    else:
        _info("Checking that raw-read URLs are downloadable...")
        unreachable = verify_remote_urls(batch.entries, fields.sample, read_fields)
        if unreachable:
            for sample, url, reason in unreachable:
                print(f"  WARNING: [{sample}] URL not downloadable: {url} ({reason})", file=sys.stderr)
            _die(
                f"{len(unreachable)} raw-read URL(s) not downloadable — fix them in "
                f"{batch.source} before launching drakkar."
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

    token = _resolve_token(args)

    local_root = Path(args.local_dir).resolve()
    if not local_root.is_dir():
        _die(f"Local directory not found: {local_root}")
    core = _core(args)

    # Read while the output directory is still there: the drakkar version(s)
    # that produced the results come from the run metadata inside it, which
    # the cleanup step further down may delete.
    drakkar_version = _get_drakkar_version(local_root)

    batch = _open_batch("preprocessing", args, core)
    entries = batch.entries
    if not entries:
        _die(f"No entries found for batch '{args.batch}'.")
    batch_record = batch.record
    entry_code_field = batch.fields.sample
    ehi_number_field = "hologenome_code" if batch.from_core else _require_cfg("EHI_PPR_ENTRY_EHI_NUMBER")
    client = None if batch.from_core else AirtableClient(
        api_key=token, base_id=_require_cfg("EHI_BASE")
    )
    entry_table = "" if batch.from_core else _require_cfg("EHI_PPR_ENTRY")

    # Build field_map: metric_key → field_id (resolved from config)
    field_map: dict[str, str] = {}
    if not batch.from_core:
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
        fields = entry.get("fields", entry)
        sample = str(fields.get(entry_code_field, "")).strip()
        ehi    = str(fields.get(ehi_number_field, "") or "").strip()
        if not sample:
            continue
        if ehi:
            code_to_ehi[sample] = ehi
        metrics = sample_stats.get(sample, {})
        if not metrics:
            print(f"  Warning: no stats found for sample '{sample}' in {stats_tsv}", file=sys.stderr)
        all_metrics[sample] = metrics
        if field_map:
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
    elif not batch.from_core:
        _info("No QC metrics found to update.")
    core.mirror(f"QC metrics of batch '{args.batch}'", (
        [*mirror.preprocessing_metrics(all_metrics)] if batch.from_core else [
            mirror.batch("preprocessing", args.batch, batch_record),
            *mirror.preprocessings(args.batch, entries, all_metrics),
        ]
    ))

    # Transfer preprocessed output files via SFTP
    ppr_dir = local_root / "preprocessing"
    # No preprocessing output means the workflow never ran (drakkar exits 0 on
    # some of its own error paths), so the batch must not be left in the
    # launched status with nothing transferred: failing here makes the launch
    # script set the error status.
    if not ppr_dir.is_dir():
        _die(f"Preprocessing output directory not found: {ppr_dir}. "
             f"The batch is not finished — check the drakkar log.")

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
            _reference_record(batch, entries), local_root, token,
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
        batch_fields[drakkar_version_field] = drakkar_version

    # Mark the batch as done
    done_status = str(cfg.get("PROCESSING_DONE_STATUS") or "Done").strip()
    if not batch.from_core:
        batch_fields[_require_cfg("EHI_PPR_BATCH_STATUS")] = done_status
        client.update_records(
            _require_cfg("EHI_PPR_BATCH"),
            [{"id": batch_record["id"], "fields": batch_fields}],
        )
    # Airtable builds the read and BAM URLs with formulas; the core stores them.
    core.mirror(f"Batch '{args.batch}'", [
        *mirror.preprocessing_files(args.batch, code_to_ehi, {f.name for f in files_to_transfer}),
        mirror.batch(
            "preprocessing", args.batch, None if batch.from_core else batch_record,
            status=done_status, ehio_version=__version__, drakkar_version=drakkar_version,
        ),
    ])
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
    from ehio.drakkar import (
        write_sample_file,
        verify_input_files,
        normalise_assembly_type,
        check_assembly_type,
    )

    core   = _core(args)
    batch  = _open_batch("binning", args, core)
    fields = batch.fields
    if not batch.from_core:
        fields.reads1 = _conf(args, "reads1_field", "EHI_ASB_ENTRY_READS1", required=True)
        fields.reads2 = _conf(args, "reads2_field", "EHI_ASB_ENTRY_READS2", required=True)

    _info(f"Found {len(batch.entries)} entries for batch '{args.batch}'.")
    if not batch.entries:
        _die(f"No entries found for batch '{args.batch}'.")

    if not batch.from_core:
        core.mirror(f"Batch '{args.batch}'", [
            mirror.batch("binning", args.batch, batch.record),
            *mirror.assemblies(args.batch, batch.entries),
        ])
        # Which samples each assembly is built from is a link of its own in the
        # core, because a coassembly has several; Airtable keeps it the other
        # way round, as the assembly code carried by each entry.
        _mirror_assembly_grouping(core, args.batch, batch)

    # The assembly codes of the entries decide the grouping; the batch type says
    # what that grouping is meant to be.  Checking them against each other here
    # keeps a mismatch from surfacing as an empty drakkar run.
    assembly_type = normalise_assembly_type(batch.value("EHI_ASB_BATCH_TYPE", "batch_type"))
    if assembly_type or batch.from_core:
        _info(f"Batch assembly type: {assembly_type or '(unset)'}")
        error, warning = check_assembly_type(
            batch.entries, assembly_type,
            sample_field=fields.sample,
            assembly_field=fields.assembly,
        )
        if warning:
            print(f"  WARNING: {warning}", file=sys.stderr)
        if error:
            _die(error)

    out_path = Path(args.sample_file)
    n = write_sample_file(
        batch.entries,
        out_path,
        sample_field=fields.sample,
        reads1_field=fields.reads1,
        reads2_field=fields.reads2,
        assembly_field=fields.assembly,
    )
    _info(f"Wrote {n} samples to {out_path}")

    missing = verify_input_files(batch.entries, fields.sample, [fields.reads1, fields.reads2])
    if missing:
        for sample, path in missing:
            print(f"  WARNING: [{sample}] file not found: {path}", file=sys.stderr)
        _die(f"{len(missing)} input file(s) missing — fix the paths in {batch.source} "
             f"before launching drakkar.")
    return 0


def _mirror_assembly_grouping(core, batch_code: str, batch) -> None:
    """Tell the core which preprocessed samples each assembly is built from.

    Airtable holds one entry per sample, carrying the code of the assembly it
    belongs to; the core links an assembly to every sample it was built from,
    so a coassembly keeps all of them instead of one.
    """
    if not core:
        return
    grouping: dict[str, list[str]] = {}
    for entry in batch.entries:
        fields = entry.get("fields", entry)
        assembly = str(fields.get(batch.fields.assembly) or "").strip()
        sample = mirror.cell(fields, "EHI_ASB_ENTRY_PREPROCESSING")
        if assembly and sample:
            grouping.setdefault(assembly, []).append(str(sample))
    if not grouping:
        return
    core.mirror_call(
        f"The samples each assembly of '{batch_code}' is built from",
        lambda client: client.link_assembly_samples(batch_code, grouping),
    )


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
    )
    from ehio.transfer import SFTPTransfer

    token = _resolve_token(args)

    local_root = Path(args.local_dir).resolve()
    if not local_root.is_dir():
        _die(f"Local directory not found: {local_root}")

    # Read while the output directory is still there: the drakkar version(s)
    # that produced the results come from the run metadata inside it, which
    # the cleanup step further down may delete.
    drakkar_version = _get_drakkar_version(local_root)

    # A batch whose cataloging left no output has nothing to report, and
    # marking it done would hide an unfinished run: drakkar exits 0 on some of
    # its own error paths, so the missing output is the only sign that the
    # workflow never ran.  Failing here makes the launch script set the error
    # status instead.
    final_dir = local_root / "cataloging" / "final"
    if not final_dir.is_dir():
        _die(f"Cataloging output directory not found: {final_dir}. "
             f"The batch is not finished — check the drakkar log.")
    # The MAGs of the batch are created in the core, so it has to be there.
    core = _core(args, holds=True)

    batch = _open_batch("binning", args, core)
    entries = batch.entries
    if not entries:
        _die(f"No entries found for batch '{args.batch}'.")
    batch_record = batch.record
    # A core entry is one sample of an assembly, so the assembly code is what
    # the metrics are keyed by there as well as here.
    entry_code_field    = batch.fields.code
    ehi_number_field    = batch.fields.sample
    assembly_code_field = batch.fields.assembly
    client = None if batch.from_core else AirtableClient(
        api_key=token, base_id=_require_cfg("EHI_BASE")
    )
    entry_table = "" if batch.from_core else _require_cfg("EHI_ASB_ENTRY")

    field_map: dict[str, str] = {}
    if not batch.from_core:
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
    metrics_by_entry: dict[str, dict] = {}
    # (assembly, preprocessing, metrics) of each sample: its mapping rate is
    # kept on the sample in the core, not on the assembly.
    sample_metrics: list[tuple[str, str | None, dict]] = []
    updates: list[dict] = []
    for entry in entries:
        fields = entry.get("fields", entry)
        entry_code = str(fields.get(entry_code_field, "") or "").strip()
        if not entry_code:
            continue
        # Use EHI number as the output-TSV key (one row per sample); fall back to entry code
        ehi_number = str(fields.get(ehi_number_field) or entry_code).strip() if ehi_number_field else entry_code
        # Metrics are keyed by assembly code in cataloging.tsv
        assembly_code = str(fields.get(assembly_code_field) or entry_code).strip() if assembly_code_field else entry_code
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
        metrics_by_entry[entry_code] = metrics
        preprocessing = (fields.get("preprocessing_code") if batch.from_core
                         else mirror.cell(fields, "EHI_ASB_ENTRY_PREPROCESSING"))
        sample_metrics.append((assembly_code, preprocessing, metrics))
        if field_map:
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
    elif not batch.from_core:
        _info("No assembly/binning metrics found to update.")
    core.mirror(f"Assembly metrics of batch '{args.batch}'", (
        mirror.assembly_metrics(metrics_by_entry) if batch.from_core else [
            mirror.batch("binning", args.batch, batch_record),
            *mirror.assemblies(args.batch, entries, metrics_by_entry),
        ]
    ))
    if not batch.from_core:
        # A batch launched before the core was told its grouping gets it now,
        # so that each sample has a row for its mapping rate to land on.
        _mirror_assembly_grouping(core, args.batch, batch)
    core.mirror(f"Sample mapping rates of batch '{args.batch}'",
                mirror.assembly_samples(sample_metrics))

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

    # --- Create the MAG records and upload FASTA files -----------------
    # With the core in use, new MAGs are created there and only there.
    # Airtable's MAG table is full, and two databases each numbering new MAGs
    # would hand the same EHM code to two different genomes.
    bin_metadata_csv = final_dir / "all_bin_metadata.csv"
    bin_paths_txt    = final_dir / "all_bin_paths.txt"
    mag_base_id      = str(cfg.get("MAG_BASE") or "").strip()

    if not bin_metadata_csv.exists():
        _info(f"No bin metadata CSV found ({bin_metadata_csv}); skipping MAG creation.")
    elif not (core or mag_base_id):
        _info("MAG_BASE not configured; skipping MAG creation.")
    else:
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

        bins_data = parse_bin_metadata_csv(bin_metadata_csv)
        _info(f"Parsed {len(bins_data)} bin(s) from {bin_metadata_csv.name}.")
        if not core:
            _create_airtable_mags(token, mag_base_id, bins_data)

        # Compress and upload FASTA files to MAG/{batch}/
        uploaded_mags: set[str] = set()
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
                        uploaded_mags.add(_gz.name)
                    finally:
                        _gz.unlink(missing_ok=True)
            _skip_msg = f", {n_mag_sk} already present (skipped)" if n_mag_sk else ""
            _info(f"Uploaded {n_mag_up} compressed FASTA files to {remote_mag_dir}{_skip_msg}.")

        # After the upload, so each new MAG's FASTA URL points at a file that
        # is there.  The core is their only home, so failing here fails the batch.
        if core:
            _info(f"Recording {len(bins_data)} MAG(s) in ehi-core...")
            results = core.write(mirror.new_mags(args.batch, bins_data, uploaded_mags),
                                 f"new MAGs of batch '{args.batch}'")
            created = sum(1 for r in results if r["action"] == "created")
            _info(f"{created} new MAG(s) recorded in ehi-core, "
                  f"{len(results) - created} already there.")

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
        batch_fields[drakkar_version_field] = drakkar_version

    done_status = str(cfg.get("PROCESSING_DONE_STATUS") or "Done").strip()
    if not batch.from_core:
        batch_fields[_require_cfg("EHI_ASB_BATCH_STATUS")] = done_status
        client.update_records(
            _require_cfg("EHI_ASB_BATCH"),
            [{"id": batch_record["id"], "fields": batch_fields}],
        )
    core.mirror(f"Batch '{args.batch}'", [
        *mirror.assembly_files(args.batch, {fna.stem: _assembly_remote_name(fna) for fna in assembly_fastas}),
        mirror.batch(
            "binning", args.batch, None if batch.from_core else batch_record,
            status=done_status, ehio_version=__version__, drakkar_version=drakkar_version,
        ),
    ])
    _info(f"Batch '{args.batch}' status → '{done_status}'.")
    return 0


def _create_airtable_mags(token: str, mag_base_id: str, bins_data: list[dict]) -> None:
    """Create the MAG_ENTRY records of a batch's bins in Airtable, as ehio did
    before the core; used only when the core is not in use."""
    from ehio.airtable import AirtableClient
    from ehio.metadata import BIN_METRIC_KEYS

    mag_table          = _require_cfg("MAG_ENTRY")
    mag_client         = AirtableClient(api_key=token, base_id=mag_base_id)
    mag_name_fld       = str(cfg.get("MAG_ENTRY_NAME")       or "").strip()
    mag_assembly_fld   = str(cfg.get("MAG_ENTRY_ASSEMBLY")   or "").strip()
    mag_field_map: dict[str, str] = {}
    for _mk, _ck in BIN_METRIC_KEYS.items():
        _fid = str(cfg.get(_ck) or "").strip()
        if _fid:
            mag_field_map[_mk] = _fid

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

    token = _resolve_token(args)
    # With the core in use the batch's MAGs are read from it.
    core  = _core(args, holds=True)
    batch = _open_batch("quantifying", args, core)

    client = None
    if not batch.from_core:
        # The fields a MAG is read through when it comes from Airtable.
        for key in ("MAG_ENTRY", "MAG_DMB_BATCH_LIST_MAGS", "MAG_ENTRY_NAME", "MAG_ENTRY_URL_FASTA",
                    "MAG_ENTRY_CHECKM_COMPLETENESS", "MAG_ENTRY_CHECKM_CONTAMINATION"):
            _require_cfg(key)
        client = AirtableClient(api_key=token, base_id=_require_cfg("MAG_BASE"))

    mags = _dmb_mags(client, core, batch.record if not batch.from_core else None, args.batch)

    quality_path = Path(args.quality_file)
    n_quality = write_quality_file(
        mags, quality_path,
        name_field="name",
        completeness_field="completeness",
        contamination_field="contamination",
    )
    _info(f"Wrote {n_quality} rows to {quality_path}")

    mags_path = Path(args.mags_file)
    n_mags = write_bins_file(mags, mags_path, bins_field="fasta_url")
    _info(f"Wrote {n_mags} MAG URLs to {mags_path}")

    ppr_records, fields = _dmb_reads(client, batch, args.batch)

    reads_path = Path(args.reads_file)
    n_reads = write_sample_file(
        ppr_records,
        reads_path,
        sample_field=fields.sample,
        reads1_field=fields.reads1,
        reads2_field=fields.reads2,
    )
    _info(f"Wrote {n_reads} read entries to {reads_path}")

    missing_reads = verify_input_files(ppr_records, fields.sample, [fields.reads1, fields.reads2])
    if missing_reads:
        for sample, path in missing_reads:
            print(f"  WARNING: [{sample}] reads file not found: {path}", file=sys.stderr)

    missing_mags = verify_input_files(mags, "fasta_url", ["fasta_url"])
    if missing_mags:
        for _, path in missing_mags:
            print(f"  WARNING: MAG FASTA not found: {path}", file=sys.stderr)

    total_missing = len(missing_reads) + len(missing_mags)
    if total_missing:
        _die(f"{total_missing} input file(s) missing — fix the paths in {batch.source} "
             f"before launching drakkar.")
    return 0


def _run_quantifying_output(args: argparse.Namespace) -> int:
    """Parse profiling metadata from drakkar output, update Airtable, transfer files."""
    from ehio.airtable import AirtableClient
    from ehio.metadata import (
        write_quantifying_output_tsv,
        parse_counts_genomes,
        parse_profiling_genomes_tsv,
        parse_dereplicating_tsv,
    )
    from ehio.transfer import SFTPTransfer

    token = _resolve_token(args)

    local_root = Path(args.local_dir).resolve()
    if not local_root.is_dir():
        _die(f"Local directory not found: {local_root}")
    core = _core(args)

    # Read while the output directory is still there: the drakkar version(s)
    # that produced the results come from the run metadata inside it, which
    # the cleanup step further down may delete.
    drakkar_version = _get_drakkar_version(local_root)
    # Likewise the genome catalogue: the counts table has one row per MAG that
    # survived dereplication, which Airtable only ever counted.
    kept_mags = parse_counts_genomes(local_root / "profiling_genomes" / "final" / "counts.tsv")

    batch = _open_batch("quantifying", args, core)
    batch_record = batch.record
    client = None if batch.from_core else AirtableClient(
        api_key=token, base_id=_require_cfg("MAG_BASE")
    )
    ppr_records, read_fields = _dmb_reads(client, batch, args.batch, need_reads=False)
    ppr_ehi_field = read_fields.sample

    # Parse per-sample mapping rates from drakkar output
    profiling_tsv = local_root / "profiling_genomes.tsv"
    per_sample = parse_profiling_genomes_tsv(profiling_tsv)
    if not per_sample:
        _info(f"profiling_genomes.tsv not found or empty at {profiling_tsv}; mapping rates will be empty.")

    all_metrics: dict[str, dict] = {
        _first_value((rec.get("fields", rec)).get(ppr_ehi_field)):
            per_sample.get(_first_value((rec.get("fields", rec)).get(ppr_ehi_field)), {})
        for rec in ppr_records
    }

    if batch.from_core:
        # A core batch brings the samples queued in it, not mappings: the core
        # numbers those itself, so no DM code is sent and only the rate each
        # run measured is left to write.
        mappings = [
            (entry.get("preprocessing_code"), None,
             all_metrics.get(str(entry.get(ppr_ehi_field) or ""), {}).get("mapping_rate"))
            for entry in ppr_records
        ]
    else:
        mappings = _create_airtable_mappings(
            client, args.batch, batch_record, ppr_records, all_metrics, ppr_ehi_field
        )

    if core:
        core.mirror(f"Mappings of batch '{args.batch}'", [
            *([] if batch.from_core else [mirror.batch("quantifying", args.batch, batch_record)]),
            *mirror.mappings(args.batch, mappings),
        ])
        if kept_mags:
            core.mirror_call(
                f"The dereplicated MAGs of batch '{args.batch}'",
                lambda c: _record_representatives(c, args.batch, kept_mags),
            )

    run_base = str(cfg.get("RUN_BASE") or "").strip()
    if run_base:
        tsv_out = Path(run_base) / args.batch / f"{args.batch}_output.tsv"
        write_quantifying_output_tsv(all_metrics, tsv_out)
        _info(f"Output summary written to {tsv_out}")

    # Genomes-type output: profiling_genomes/final/counts.tsv + bases.tsv + mags.tsv
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
            ("mags.tsv",   f"{args.batch}_mag_info.tsv.gz"),
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
        batch_fields[drakkar_version_field] = drakkar_version
    if derep_mags_field:
        derep_tsv   = local_root / "dereplicating.tsv"
        derep_count = parse_dereplicating_tsv(derep_tsv)
        if derep_count is not None:
            batch_fields[derep_mags_field] = derep_count
            _info(f"Dereplicated MAGs: {derep_count}")
        else:
            _info(f"dereplicating.tsv not found or output_bin_number missing at {derep_tsv}.")

    done_status = str(cfg.get("PROCESSING_DONE_STATUS") or "Done").strip()
    if not batch.from_core:
        batch_fields[_require_cfg("MAG_DMB_BATCH_STATUS")] = done_status
        client.update_records(
            _require_cfg("MAG_DMB_BATCH"),
            [{"id": batch_record["id"], "fields": batch_fields}],
        )
    core.mirror(f"Batch '{args.batch}'", [mirror.batch(
        "quantifying", args.batch, None if batch.from_core else batch_record,
        status=done_status, ehio_version=__version__, drakkar_version=drakkar_version,
    )])
    _info(f"Batch '{args.batch}' status → '{done_status}'.")
    return 0


def _record_representatives(client, batch: str, kept_ids: list[str]) -> None:
    """Mark in the core which of a batch's MAGs dereplication kept, from the
    MAG ids of its counts table."""
    from ehio.metadata import drakkar_mag_id

    code_by_id = {
        drakkar_mag_id(mag["name"]): mag["code"]
        for mag in client.batch_mags(batch) if mag.get("name")
    }
    kept = [code_by_id[mag_id] for mag_id in kept_ids if mag_id in code_by_id]
    unknown = [mag_id for mag_id in kept_ids if mag_id not in code_by_id]
    if unknown:
        _warn(
            f"{len(unknown)} genome(s) of the counts table of '{batch}' are not among its "
            f"MAGs in ehi-core ({', '.join(unknown[:5])}{', ...' if len(unknown) > 5 else ''})."
        )
    if kept:
        result = client.link_batch_mags(batch, [], representatives=kept)
        _info(f"ehi-core: {result['representative_count']} of {result['mag_count']} MAG(s) "
              f"of '{batch}' kept by dereplication.")


# ---------------------------------------------------------------------------
# annotating
# ---------------------------------------------------------------------------

def _merge_drakkar_versions(recorded: str, new: str) -> str:
    """Append the versions of a new run to those already on a batch record.

    Both sides are the slash-separated list 'ehio' writes to the drakkar
    version field ('2.4.4/2.5.0'), oldest run first.  A version already on the
    record is not repeated, so re-annotating twice with the same drakkar leaves
    the field as it was.
    """
    from ehio.drakkar import format_drakkar_versions

    merged: list[str] = []
    for value in (recorded, new):
        for part in str(value or "").split("/"):
            part = part.strip()
            if part and part not in merged:
                merged.append(part)
    return format_drakkar_versions(merged)


def cmd_annotating(args: argparse.Namespace) -> int:
    if getattr(args, "stage", False):
        return _run_annotating_stage(args)
    if args.input:
        return _run_annotating_input(args)
    return _run_annotating_output(args)


# Suffixes a MAG FASTA URL may carry.  The staged copy is always written as
# plain '.fa', which is what 'drakkar annotating -b' reads and what the '*.fa'
# glob of 'ehio annotating --input' finds.
_MAG_FASTA_SUFFIXES = (".fa.gz", ".fna.gz", ".fasta.gz", ".fa", ".fna", ".fasta")


def _decompress_gz(src: Path, dest: Path) -> None:
    """Gunzip src into dest, leaving nothing behind if it fails."""
    import gzip as _gzip
    import shutil as _shutil

    part = dest.with_name(dest.name + ".part")
    try:
        with _gzip.open(src, "rb") as _fin, part.open("wb") as _fout:
            _shutil.copyfileobj(_fin, _fout, length=1024 * 1024)
    except BaseException:
        part.unlink(missing_ok=True)
        raise
    part.replace(dest)


def _run_annotating_stage(args: argparse.Namespace) -> int:
    """Rebuild the dereplicated genome directory of a finished DMB batch.

    A batch whose results are already on ERDA has nothing left on the cluster,
    so the genomes a re-annotation runs over have to be put back on disk first.
    Airtable records how many MAGs came out of dereplication but not which
    ones, so the catalogue is read from the batch's counts table on ERDA — one
    row per dereplicated genome — and each of those genomes is downloaded from
    the FASTA URL on its own MAG record.

    Every genome is staged as '{mag id}.fa', the name drakkar derives from it
    everywhere else, so the directory is indistinguishable from the one a fresh
    profiling run leaves behind.
    """
    import tempfile

    from ehio.airtable import AirtableClient
    from ehio.metadata import drakkar_mag_id, parse_counts_genomes
    from ehio.transfer import SFTPTransfer
    from ehio.urls import DownloadError, download_url, filename_from_url, is_remote_url

    token = _resolve_token(args)
    _MAG_KEYS = ("MAG_ENTRY", "MAG_DMB_BATCH_LIST_MAGS", "MAG_ENTRY_NAME", "MAG_ENTRY_URL_FASTA")

    derep_dir = Path(args.annotation_dir).resolve()
    derep_dir.mkdir(parents=True, exist_ok=True)
    core = _core(args, holds=True)

    batch = _open_batch("quantifying", args, core)
    batch_record = None if batch.from_core else batch.record
    client = None
    if not batch.from_core:
        for key in _MAG_KEYS:
            _require_cfg(key)
        client = AirtableClient(api_key=token, base_id=_require_cfg("MAG_BASE"))

    # Keyed by drakkar MAG id, so a catalogue written with the '.fa' suffix and
    # one written without it both find their record.
    mag_by_id: dict[str, dict] = {
        drakkar_mag_id(mag["name"]): mag
        for mag in _dmb_mags(client, core, batch_record, args.batch) if mag["name"]
    }
    if not mag_by_id:
        _die(f"Could not fetch any MAG records for batch '{args.batch}'.")

    # The dereplicated catalogue.  A list given on the command line wins, so a
    # batch whose counts table is missing or oddly shaped can still be staged.
    catalogue: list[str] = []
    if getattr(args, "genomes_file", None):
        genomes_path = Path(args.genomes_file).expanduser()
        if not genomes_path.is_file():
            _die(f"Genome list not found: {genomes_path}")
        catalogue = [
            drakkar_mag_id(line) for line in genomes_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        _info(f"Read {len(catalogue)} genome(s) from {genomes_path}.")
    else:
        host        = _conf(args, "host",       "SFTP_HOST",        required=True)
        user        = _conf(args, "user",       "SFTP_USER",        required=True)
        port        = int(_conf(args, "port",   "SFTP_PORT") or 22)
        identity    = _conf(args, "identity",   "SFTP_IDENTITY") or None
        remote_base = _conf(args, "remote_dir", "SFTP_REMOTE_BASE", required=True)
        counts_remote = f"{remote_base.rstrip('/')}/DMB/{args.batch}/{args.batch}_counts.tsv.gz"
        _timeout = getattr(args, "connect_timeout", 300.0)

        with tempfile.TemporaryDirectory(prefix="ehio-stage-") as tmp:
            counts_local = Path(tmp) / f"{args.batch}_counts.tsv.gz"
            _info(f"Downloading the genome catalogue from {counts_remote} ...")
            try:
                with SFTPTransfer(host=host, username=user, port=port,
                                  key_path=identity or None, timeout=_timeout) as xfer:
                    xfer.download(counts_remote, counts_local, verbose=getattr(args, "verbose", False))
            except FileNotFoundError:
                _die(
                    f"No counts table at {counts_remote}. It is what says which of the "
                    f"batch's MAGs survived dereplication, and Airtable does not keep "
                    f"that list. Pass the genome names with --genomes-file instead."
                )
            catalogue = parse_counts_genomes(counts_local)

        if not catalogue:
            _die(
                f"The counts table of '{args.batch}' holds no genome names. Check "
                f"{counts_remote}, or pass the genome names with --genomes-file."
            )
        _info(f"Catalogue: {len(catalogue)} dereplicated genome(s).")

    # Every problem is reported at once, so a batch is not staged one failure
    # per run.
    problems: list[str] = []
    timeout = float(getattr(args, "download_timeout", 600.0))
    redownload = getattr(args, "redownload", False)
    n_staged = n_present = 0

    for mag_id in catalogue:
        mag = mag_by_id.get(mag_id)
        if mag is None:
            problems.append(f"{mag_id}: in the catalogue of '{args.batch}' but not among the batch's MAGs")
            continue
        source = str(mag.get("fasta_url") or "").strip()
        if not source:
            problems.append(f"{mag_id}: its MAG record has no FASTA URL")
            continue

        destination = derep_dir / f"{mag_id}.fa"
        if destination.exists() and destination.stat().st_size > 0 and not redownload:
            n_present += 1
            continue

        if is_remote_url(source):
            name = filename_from_url(source, "")
            if not name.lower().endswith(_MAG_FASTA_SUFFIXES):
                problems.append(
                    f"{mag_id}: {source} is not a FASTA "
                    f"(expected one of {', '.join(_MAG_FASTA_SUFFIXES)})"
                )
                continue
            _info(f"  {mag_id}: downloading {source} ...")
            # Fetched next to the staged copy rather than into a temporary
            # directory: the two are then always on the same filesystem, and
            # renaming a multi-hundred-MB genome into place cannot fail on a
            # cross-device link.
            fetched = derep_dir / f"{mag_id}.download"
            try:
                download_url(source, fetched, timeout=timeout, overwrite=True)
                if name.lower().endswith(".gz"):
                    _decompress_gz(fetched, destination)
                else:
                    fetched.replace(destination)
            except DownloadError as exc:
                problems.append(f"{mag_id}: {source} could not be downloaded ({exc})")
                continue
            except OSError as exc:
                problems.append(f"{mag_id}: {source} could not be unpacked ({exc})")
                continue
            finally:
                fetched.unlink(missing_ok=True)
        else:
            local = Path(source).expanduser()
            if not local.is_file():
                problems.append(f"{mag_id}: FASTA not found at {local}")
                continue
            try:
                if local.name.lower().endswith(".gz"):
                    _decompress_gz(local, destination)
                else:
                    import shutil as _shutil
                    _shutil.copyfile(local, destination)
            except OSError as exc:
                problems.append(f"{mag_id}: {local} could not be copied ({exc})")
                continue
        n_staged += 1

    if problems:
        detail = "\n  ".join(problems)
        _die(
            f"{len(problems)} of the {len(catalogue)} dereplicated genome(s) of "
            f"'{args.batch}' could not be staged:\n  {detail}"
        )

    _present_msg = f", {n_present} already present" if n_present else ""
    _info(f"Staged {n_staged} genome(s) in {derep_dir}{_present_msg}.")
    return 0


def _run_annotating_input(args: argparse.Namespace) -> int:
    """Check which MAGs need functional annotation and write their paths to a file."""
    from ehio.airtable import AirtableClient

    token = _resolve_token(args)
    _MAG_KEYS = ("MAG_ENTRY", "MAG_DMB_BATCH_LIST_MAGS", "MAG_ENTRY_NAME")

    force_reannotate = getattr(args, "rerun", False)

    ann_dir  = Path(args.annotation_dir).resolve()
    out_file = Path(args.annotation_file)
    core = _core(args, holds=True)

    batch = _open_batch("quantifying", args, core)
    batch_record = None if batch.from_core else batch.record
    client = None
    if not batch.from_core:
        for key in _MAG_KEYS:
            _require_cfg(key)
        client = AirtableClient(api_key=token, base_id=_require_cfg("MAG_BASE"))

    # Read requested annotation type from the batch record (kegg / genes / all)
    requested_type = str(
        batch.value("MAG_DMB_BATCH_ANNOTATION_TYPE", "annotation_type") or "all"
    ).strip().lower() or "all"

    # Annotation hierarchy: kegg ⊂ genes ⊂ all.
    # "true" is treated as legacy equivalent of "all".
    _sufficient: dict[str, set[str]] = {
        "kegg":  {"kegg", "genes", "all", "true"},
        "genes": {"genes", "all", "true"},
        "all":   {"all", "true"},
    }
    sufficient_statuses = _sufficient.get(requested_type, {"all", "true"})

    # How far each MAG is annotated already: fa filename → annotated value
    mag_status: dict[str, str] = {
        mag["name"]: mag["annotation_level"]
        for mag in _dmb_mags(client, core, batch_record, args.batch) if mag["name"]
    }

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
    import os as _os
    import shutil as _shutil

    from ehio.airtable import AirtableClient
    from ehio.metadata import (
        parse_genome_taxonomy_tsv,
        parse_annotation_tsv,
        build_entry_update,
        drakkar_mag_id,
        find_gene_tables,
        ANNOTATING_TAXONOMY_KEYS,
        ANNOTATING_GTDB_KEYS,
        ANNOTATING_FUNC_KEYS,
    )
    from ehio.transfer import SFTPTransfer

    token = _resolve_token(args)
    _MAG_KEYS = ("MAG_ENTRY", "MAG_DMB_BATCH_LIST_MAGS", "MAG_ENTRY_NAME")

    local_root = Path(args.local_dir).resolve()
    if not local_root.is_dir():
        _die(f"Local directory not found: {local_root}")

    ann_dir = local_root / "annotating"
    if not ann_dir.is_dir():
        _die(f"Annotating output directory not found: {ann_dir}")
    # The MAGs live in the core, so their annotation has to reach it.
    core = _core(args, holds=True)

    batch = _open_batch("quantifying", args, core)
    batch_record = None if batch.from_core else batch.record
    client = None
    if not batch.from_core:
        for key in _MAG_KEYS:
            _require_cfg(key)
        client = AirtableClient(api_key=token, base_id=_require_cfg("MAG_BASE"))

    # Read the annotation type the batch asks for (kegg / genes / all)
    annotation_type_value = str(
        batch.value("MAG_DMB_BATCH_ANNOTATION_TYPE", "annotation_type") or "all"
    ).strip().lower() or "all"

    # The batch's MAGs, keyed by MAG_ENTRY_NAME
    mag_by_name: dict[str, dict] = {
        mag["name"]: mag
        for mag in _dmb_mags(client, core, batch_record, args.batch) if mag["name"]
    }

    # Build field_map covering taxonomy, GTDB, and functional metrics
    all_metric_keys = {**ANNOTATING_TAXONOMY_KEYS, **ANNOTATING_GTDB_KEYS, **ANNOTATING_FUNC_KEYS}
    field_map: dict[str, str] = {}
    for metric_key, config_key in all_metric_keys.items():
        fld_id = str(cfg.get(config_key) or "").strip()
        if fld_id:
            field_map[metric_key] = fld_id

    # A re-annotation is the functional half of the batch only: a genome's
    # classification is fixed when it is binned, and dereplicating it neither
    # changes it nor produces a better one.  The taxonomy already on the MAG
    # records is therefore left alone — including when the output directory
    # happens to hold a genome_taxonomy.tsv from some earlier run, which would
    # otherwise be parsed and written back over it.
    reannotate = getattr(args, "reannotate", False)

    # Parse genome_taxonomy.tsv
    taxonomy_tsv = ann_dir / "genome_taxonomy.tsv"
    if reannotate:
        taxonomy_data: dict = {}
        _info("Re-annotation: taxonomy is left as it is on the MAG records.")
    else:
        taxonomy_data = parse_genome_taxonomy_tsv(taxonomy_tsv)
        if not taxonomy_data:
            _info(f"genome_taxonomy.tsv not found or empty at {taxonomy_tsv}.")

    # Parse the per-genome gene tables from annotating/final/.  drakkar names
    # them after the genome FASTA with its suffix stripped and '_genes'
    # appended — EHA00123_bin_1.fa gives EHA00123_bin_1_genes.tsv — and writes
    # a '_clusters.tsv' beside each of them holding a different table
    # altogether, which is why only the gene tables are read here.
    final_dir = ann_dir / "final"
    annotation_data = {
        mag_id: parse_annotation_tsv(path)
        for mag_id, path in find_gene_tables(final_dir).items()
    }
    if final_dir.is_dir() and not annotation_data:
        _info(f"No per-genome gene table (*_genes.tsv) found in {final_dir}.")

    # Build the update payloads: Airtable for the MAGs it holds, the core for all
    updates: list[dict] = []
    core_units: list = []
    n_annotated = 0
    for genome_name, mag in mag_by_name.items():
        metrics: dict = {}
        if genome_name in taxonomy_data:
            metrics.update(taxonomy_data[genome_name])
        # The MAG is named by its FASTA file in Airtable and by the stem of
        # that name in the drakkar output, so the two are matched on the id.
        mag_id = drakkar_mag_id(genome_name)
        if mag_id in annotation_data:
            metrics.update(annotation_data[mag_id])
            metrics["annotated"] = annotation_type_value
            n_annotated += 1
        if not metrics:
            continue
        if mag["airtable_id"]:
            payload = build_entry_update(mag["airtable_id"], metrics, field_map)
            if payload["fields"]:
                updates.append(payload)
        if core:
            core_units.append(mirror.annotated_mag(mag, metrics))

    if annotation_data:
        _info(f"Gene metrics matched for {n_annotated} of {len(mag_by_name)} MAG(s) "
              f"from {len(annotation_data)} gene table(s).")

    if updates:
        _info(f"Updating {len(updates)} MAG_ENTRY records in Airtable...")
        client.update_records(_require_cfg("MAG_ENTRY"), updates)
        _info("Airtable update complete.")
    elif not core_units:
        _info("No annotation metrics found to update.")
    if core_units:
        _info(f"Updating {len(core_units)} MAG(s) in ehi-core...")
        core.write(core_units, f"MAG annotations of batch '{args.batch}'")

    host        = _conf(args, "host",       "SFTP_HOST",        required=True)
    user        = _conf(args, "user",       "SFTP_USER",        required=True)
    port        = int(_conf(args, "port",   "SFTP_PORT") or 22)
    identity    = _conf(args, "identity",   "SFTP_IDENTITY") or None
    remote_base = _conf(args, "remote_dir", "SFTP_REMOTE_BASE", required=True)

    # Upload genome taxonomy + tree files to DMB/{batch}/
    dmb_remote = f"{remote_base.rstrip('/')}/DMB/{args.batch}"
    dmb_files: list[Path] = []
    dmb_tmp: list[Path] = []

    # genome_taxonomy.tsv is compressed and renamed to {batch}_genome_taxonomy.tsv.gz
    if reannotate:
        _info("  Re-annotation: the taxonomy table and trees on ERDA are left as they are.")
    elif taxonomy_tsv.exists():
        tax_gz = ann_dir / f"{args.batch}_genome_taxonomy.tsv.gz"
        with taxonomy_tsv.open("rb") as _fin, _gzip.open(tax_gz, "wb") as _fout:
            _shutil.copyfileobj(_fin, _fout)
        dmb_files.append(tax_gz)
        dmb_tmp.append(tax_gz)
        _info(f"  Compressed {taxonomy_tsv.name} → {tax_gz.name}")
    else:
        _info(f"  genome_taxonomy.tsv not found in {ann_dir} — skipping.")

    # The batch-level annotation tables are already compressed; upload them
    # under batch-prefixed names.  cluster_annotations.tsv.xz holds the dbCAN
    # gene clusters, antiSMASH regions, geNomad mobile elements and defense
    # systems, and only exists for a batch whose annotation type covers them.
    for ann_name in ("gene_annotations.tsv.xz", "cluster_annotations.tsv.xz"):
        ann_file = ann_dir / ann_name
        if not ann_file.exists():
            _info(f"  {ann_name} not found in {ann_dir} — skipping.")
            continue
        ann_alias = ann_dir / f"{args.batch}_{ann_name}"
        ann_alias.unlink(missing_ok=True)
        try:
            _os.link(ann_file, ann_alias)
        except OSError:
            _shutil.copy2(ann_file, ann_alias)
        dmb_files.append(ann_alias)
        dmb_tmp.append(ann_alias)
        _info(f"  Renamed {ann_file.name} → {ann_alias.name}")

    # The trees are GTDB-Tk products, so they belong to the taxonomy run that a
    # re-annotation does not repeat.
    for fname in () if reannotate else ("bacteria.tree", "archaea.tree"):
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
        try:
            with SFTPTransfer(host=host, username=user, port=port, key_path=identity or None, timeout=_timeout) as xfer:
                n_up, n_sk = xfer.upload_flat(dmb_files, dmb_remote, verbose=getattr(args, "verbose", False))
        finally:
            for _tmp in dmb_tmp:
                _tmp.unlink(missing_ok=True)
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

    done_status           = str(cfg.get("PROCESSING_DONE_STATUS") or "Done").strip()
    batch_status_field    = str(cfg.get("MAG_DMB_BATCH_STATUS")   or "").strip()
    drakkar_version_field = str(cfg.get("MAG_DMB_BATCH_DRAKKAR_VERSION") or "").strip()

    batch_fields: dict = {}
    if batch_status_field:
        batch_fields[batch_status_field] = done_status
    # This is the last step of a DMB batch, and the annotating runs come after
    # 'ehio quantifying --output' has already written the version of the
    # profiling run.  Rewriting it here covers every run of the batch, which is
    # what makes an upgrade between profiling and annotating visible.
    if drakkar_version_field:
        version = _get_drakkar_version(local_root)
        # A re-annotation runs in a fresh output directory that holds the
        # annotation runs and nothing else, so the version read from it does
        # not know about the drakkar that dereplicated and profiled the batch
        # however long ago.  That version only survives in Airtable, so it is
        # kept and the new one appended to it rather than overwriting it.
        if getattr(args, "reannotate", False):
            # The version that dereplicated and profiled the batch, however
            # long ago, only survives on the batch record, so it is kept and
            # the new one appended rather than overwriting it.
            version = _merge_drakkar_versions(
                _first_value(batch.value("MAG_DMB_BATCH_DRAKKAR_VERSION", "drakkar_version")),
                version,
            )
        batch_fields[drakkar_version_field] = version

    if batch_fields and not batch.from_core:
        client.update_records(
            _require_cfg("MAG_DMB_BATCH"),
            [{"id": batch_record["id"], "fields": batch_fields}],
        )
        if batch_status_field:
            _info(f"Batch '{args.batch}' status → '{done_status}'.")
    if core:
        core_batch = {"status": done_status}
        if drakkar_version_field:
            core_batch["drakkar_version"] = batch_fields[drakkar_version_field]
        core.mirror(f"Batch '{args.batch}'",
                    [mirror.batch("quantifying", args.batch, batch_record, **core_batch)])

    return 0


# ---------------------------------------------------------------------------
# amr
# ---------------------------------------------------------------------------

def cmd_amr(args: argparse.Namespace) -> int:
    if args.input:
        return _run_amr_input(args)
    return _run_amr_output(args)


# Suffixes 'drakkar amr' accepts for an assembly, checked here so a badly named
# file is reported before the workflow starts instead of inside it.
_AMR_FASTA_SUFFIXES = (".fa.gz", ".fna.gz", ".fasta.gz", ".fa", ".fna", ".fasta")


def _amr_fasta_suffix(name: str) -> str:
    """Return the FASTA suffix of `name` that drakkar accepts, or ''.

    Longest first, so 'EHA00405.fna.gz' resolves to '.fna.gz' and not '.gz'.
    """
    lowered = name.lower()
    return next((s for s in _AMR_FASTA_SUFFIXES if lowered.endswith(s)), "")


def _first_value(value: object) -> str:
    """Return an Airtable cell as a stripped string, taking [0] of a list."""
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value or "").strip()


def _linked_assembly_ids(batch_record: dict, field_id: str, batch: str) -> list[str]:
    """Return the assembly record ids linked to an AMR batch record.

    A field that is not a linked-record field comes back as a string rather than
    a list, which would be iterated character by character and leave the batch
    looking empty.  That is a config mistake, so it is reported as one.
    """
    raw = batch_record.get("fields", {}).get(field_id)
    if raw is None or raw == []:
        _die(f"No assembly records linked in field {field_id} of batch '{batch}'.")
    if not isinstance(raw, list):
        _die(
            f"Field {field_id} of batch '{batch}' holds {raw!r}, not a list of linked "
            f"records. EHI_AMR_BATCH_LIST_ASSEMBLIES must be the linked-record field "
            f"pointing at the assembly table, not the batch code or another text field "
            f"(ehio config --edit)."
        )
    ids = [value for value in raw if isinstance(value, str) and value.startswith("rec")]
    if not ids:
        _die(
            f"Field {field_id} of batch '{batch}' holds no assembly record ids ({raw!r}). "
            f"Check EHI_AMR_BATCH_LIST_ASSEMBLIES in the config (ehio config --edit)."
        )
    return ids


def _run_amr_input(args: argparse.Namespace) -> int:
    """Fetch the assemblies of an AMR batch and write a drakkar amr manifest.

    'drakkar amr' inspects and hashes every assembly before the run and has no
    downloader of its own, so the ERDA URLs held in Airtable are fetched here
    and the manifest points at the local copies.
    """
    from ehio.drakkar import write_amr_manifest
    from ehio.urls import DownloadError, download_url, filename_from_url, is_remote_url

    core  = _core(args)
    batch = _open_batch("amr", args, core)
    assembly_code_field = batch.fields.code
    assembly_url_field  = batch.fields.url

    manifest_path  = Path(args.manifest_file)
    assemblies_dir = Path(args.assemblies_dir).resolve()

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    problems: list[str] = []
    records: list[dict] = []
    timeout = float(getattr(args, "download_timeout", 600.0))
    redownload = getattr(args, "redownload", False)

    for record in _amr_assemblies(args, batch, problems):
        records.append(record)
        fields   = record.get("fields", record)
        code     = _first_value(fields.get(assembly_code_field))
        source   = _first_value(fields.get(assembly_url_field))
        if not code:
            problems.append(
                f"{record.get('id', '?')}: assembly record has no code "
                f"in field {assembly_code_field}"
            )
            continue
        if code in seen:
            _info(f"  {code}: linked to the batch more than once — kept once.")
            continue
        if not source:
            problems.append(f"{code}: no assembly file in field {assembly_url_field}")
            continue

        if is_remote_url(source):
            suffix = _amr_fasta_suffix(filename_from_url(source, ""))
            if not suffix:
                problems.append(
                    f"{code}: {source} is not a FASTA drakkar accepts "
                    f"(expected one of {', '.join(_AMR_FASTA_SUFFIXES)})"
                )
                continue
            # Named after the assembly, not after the URL: two assemblies whose
            # URLs happen to share a basename would otherwise overwrite each
            # other in the staging directory.
            destination = assemblies_dir / f"{code}{suffix}"
            if destination.exists() and destination.stat().st_size > 0 and not redownload:
                _info(f"  {code}: {destination.name} already downloaded.")
            else:
                _info(f"  {code}: downloading {source} → {destination} ...")
            try:
                path = download_url(source, destination, timeout=timeout, overwrite=redownload)
            except DownloadError as exc:
                problems.append(f"{code}: {source} could not be downloaded ({exc})")
                continue
        else:
            path = Path(source).expanduser()
            if not path.is_file():
                problems.append(f"{code}: assembly file not found at {path}")
                continue
            if not _amr_fasta_suffix(path.name):
                problems.append(
                    f"{code}: {path.name} is not a FASTA drakkar accepts "
                    f"(expected one of {', '.join(_AMR_FASTA_SUFFIXES)})"
                )
                continue

        seen.add(code)
        rows.append({"assembly_id": code, "assembly_path": str(path)})

    if problems:
        detail = "\n  ".join(problems)
        _die(
            f"{len(problems)} assembly/assemblies of batch '{args.batch}' cannot be "
            f"processed:\n  {detail}"
        )
    if not rows:
        _die(f"No usable assemblies found for batch '{args.batch}'.")

    n = write_amr_manifest(rows, manifest_path)
    _info(f"Wrote {n} assembly/assemblies to {manifest_path}.")

    # The batch, and which assemblies it runs over, in the core too.
    if not batch.from_core:
        core.mirror(f"Batch '{args.batch}'", [
            mirror.batch("amr", args.batch, batch.record),
            *mirror.amr_assemblies(args.batch, records),
        ])
    return 0


def _amr_assemblies(args: argparse.Namespace, batch, problems: list[str]):
    """The assembly records an AMR batch runs over.

    Airtable links the batch out to its assembly entries, one record fetched at
    a time; the core answers with the whole list at once.
    """
    if batch.from_core:
        if not batch.entries:
            _die(f"ehi-core holds no assemblies for batch '{args.batch}'.")
        _info(f"Read {len(batch.entries)} assembly/assemblies of '{args.batch}' from ehi-core.")
        return batch.entries

    from ehio.airtable import AirtableClient

    entry_table = _require_cfg("EHI_ASB_ENTRY")
    rec_ids = _linked_assembly_ids(
        batch.record, _require_cfg("EHI_AMR_BATCH_LIST_ASSEMBLIES"), args.batch
    )
    _info(f"Fetching {len(rec_ids)} assembly record(s) from Airtable...")
    client = AirtableClient(api_key=_resolve_token(args), base_id=_require_cfg("EHI_BASE"))
    records = []
    for rec_id in rec_ids:
        record = client.fetch_record_by_id(entry_table, rec_id)
        if record:
            records.append(record)
        else:
            problems.append(f"{rec_id}: assembly record not found in {entry_table}")
    return records


# Content types used when a result file is attached to the batch record.
_AMR_CONTENT_TYPES = {
    ".xz":   "application/x-xz",
    ".gz":   "application/gzip",
    ".tsv":  "text/tab-separated-values",
    ".yaml": "text/yaml",
}


def _content_type_of(path: Path) -> str:
    return _AMR_CONTENT_TYPES.get(path.suffix.lower(), "application/octet-stream")


# Gene calls 'drakkar amr' makes with prodigal before AMRFinderPlus, one pair per
# assembly: {assembly}.faa (proteins) and {assembly}.ffn (nucleotides).
_AMR_GENE_CALL_SUFFIXES = (".faa", ".ffn")


def _find_amr_gene_calls(amr_dir: Path) -> list[Path]:
    """Return the prodigal .faa and .ffn files of a drakkar amr run.

    The .gff and .amrfinder.gff files in the same folder are AMRFinderPlus
    intermediates and are left out.
    """
    prodigal_dir = amr_dir / "raw" / "prodigal"
    if not prodigal_dir.is_dir():
        return []
    return sorted(
        p for p in prodigal_dir.iterdir()
        if p.suffix in _AMR_GENE_CALL_SUFFIXES and p.is_file()
    )


def _run_amr_output(args: argparse.Namespace) -> int:
    """Parse amr_qc.tsv, update EHI_ASB_ENTRY, transfer and attach the AMR tables."""
    import gzip as _gzip
    import os as _os
    import shutil as _shutil

    from ehio.airtable import AirtableError, attachment_encoded_size, ATTACHMENT_MAX_BYTES, AirtableClient
    from ehio.metadata import (
        AMR_METRIC_KEYS,
        AMR_OUTPUT_FILES,
        build_entry_update,
        parse_amr_qc_tsv,
        write_amr_output_tsv,
    )
    from ehio.transfer import SFTPTransfer

    token = _resolve_token(args)

    local_root = Path(args.local_dir).resolve()
    if not local_root.is_dir():
        _die(f"Local directory not found: {local_root}")

    # Read while the output directory is still there: the drakkar version(s)
    # that produced the results come from the run metadata inside it, which
    # the cleanup step further down may delete.
    drakkar_version = _get_drakkar_version(local_root)

    # A batch whose AMR run left no summary has nothing to report, and marking
    # it done would hide an unfinished run: drakkar exits 0 on some of its own
    # error paths, so the missing output is the only sign that nothing ran.
    amr_dir = local_root / "amr"
    qc_tsv  = amr_dir / "amr_qc.tsv"
    if not qc_tsv.is_file():
        _die(f"AMR summary not found: {qc_tsv}. "
             f"The batch is not finished — check the drakkar log.")

    assembly_stats = parse_amr_qc_tsv(qc_tsv)
    if not assembly_stats:
        _die(f"No assemblies found in {qc_tsv}.")
    core = _core(args)

    batch = _open_batch("amr", args, core)
    batch_record = batch.record
    batch_record_id = None if batch.from_core else batch_record["id"]
    assembly_code_field = batch.fields.code
    client = None if batch.from_core else AirtableClient(
        api_key=token, base_id=_require_cfg("EHI_BASE")
    )
    entry_table = "" if batch.from_core else _require_cfg("EHI_ASB_ENTRY")

    field_map: dict[str, str] = {}
    if not batch.from_core:
        for metric_key, config_key in AMR_METRIC_KEYS.items():
            fld_id = str(cfg.get(config_key) or "").strip()
            if fld_id:
                field_map[metric_key] = fld_id

    all_metrics: dict[str, dict] = {}
    updates: list[dict] = []
    records: list[dict] = list(_amr_assemblies(args, batch, []))
    for record in records:
        code = _first_value(record.get("fields", record).get(assembly_code_field))
        if not code:
            continue
        metrics = assembly_stats.get(code)
        if metrics is None:
            print(f"  Warning: no AMR stats found for assembly '{code}' in {qc_tsv}",
                  file=sys.stderr)
            continue
        all_metrics[code] = metrics
        if field_map:
            payload = build_entry_update(record["id"], metrics, field_map)
            if payload["fields"]:
                updates.append(payload)

    run_base = str(cfg.get("RUN_BASE") or "").strip()
    if run_base and all_metrics:
        tsv_out = Path(run_base) / args.batch / f"{args.batch}_output.tsv"
        write_amr_output_tsv(all_metrics, tsv_out)
        _info(f"Output summary written to {tsv_out}")

    if updates:
        _info(f"Updating {len(updates)} assembly record(s) in Airtable...")
        client.update_records(entry_table, updates)
        _info("Airtable update complete.")
    elif not batch.from_core:
        _info("No AMR metrics found to update.")
    core.mirror(f"AMR metrics of batch '{args.batch}'", (
        mirror.amr_metrics(args.batch, all_metrics, all_metrics) if batch.from_core else [
            mirror.batch("amr", args.batch, batch_record),
            *mirror.amr_assemblies(args.batch, records, all_metrics),
        ]
    ))

    # Build the batch-prefixed copies that go to ERDA and to the attachment
    # fields.  The .tsv.xz tables drakkar writes are already compressed, so they
    # are only renamed; the two plain summaries are gzipped on the way out.
    host        = _conf(args, "host",       "SFTP_HOST",        required=True)
    user        = _conf(args, "user",       "SFTP_USER",        required=True)
    port        = int(_conf(args, "port",   "SFTP_PORT") or 22)
    identity    = _conf(args, "identity",   "SFTP_IDENTITY") or None
    remote_base = _conf(args, "remote_dir", "SFTP_REMOTE_BASE", required=True)
    remote_dir  = f"{remote_base.rstrip('/')}/AMR/{args.batch}"
    rerun       = getattr(args, "rerun", False)

    def _alias(source: Path, name: str) -> Path:
        """Return a batch-prefixed hard link (or copy) of source."""
        alias = source.with_name(name)
        alias.unlink(missing_ok=True)
        try:
            _os.link(source, alias)
        except OSError:
            _shutil.copy2(source, alias)
        return alias

    upload_files: list[Path] = []
    temporary: list[Path] = []
    attachments: list[tuple[str, Path]] = []  # (field id, file)

    for file_name, config_key in AMR_OUTPUT_FILES.items():
        source = amr_dir / file_name
        if not source.exists():
            _info(f"  {file_name} not found in {amr_dir} — skipping.")
            continue
        alias = _alias(source, f"{args.batch}_{file_name}")
        upload_files.append(alias)
        temporary.append(alias)
        field_id = str(cfg.get(config_key) or "").strip()
        if field_id:
            attachments.append((field_id, alias))

    for file_name in ("amr_qc.tsv", "assembly_summary.tsv"):
        source = amr_dir / file_name
        if not source.exists():
            _info(f"  {file_name} not found in {amr_dir} — skipping.")
            continue
        gz = amr_dir / f"{args.batch}_{file_name}.gz"
        with source.open("rb") as _fin, _gzip.open(gz, "wb") as _fout:
            _shutil.copyfileobj(_fin, _fout)
        upload_files.append(gz)
        temporary.append(gz)

    provenance = amr_dir / "manifest.yaml"
    if provenance.exists():
        alias = _alias(provenance, f"{args.batch}_amr_manifest.yaml")
        upload_files.append(alias)
        temporary.append(alias)
        manifest_field = str(cfg.get("EHI_AMR_BATCH_FILE_MANIFEST") or "").strip()
        if manifest_field:
            attachments.append((manifest_field, alias))
    else:
        _info(f"  manifest.yaml not found in {amr_dir} — skipping.")

    # The prodigal gene calls go to their own subfolder, named after the
    # assembly and gzipped into the connection: the .ffn is about as large as
    # the assembly, so no temporary .gz is written to the local disk.
    gene_calls = _find_amr_gene_calls(amr_dir)
    gene_dir   = f"{remote_dir}/genes"
    if not gene_calls:
        _info(f"  No prodigal gene calls found in {amr_dir / 'raw' / 'prodigal'} — skipping.")

    try:
        if upload_files or gene_calls:
            _verbose = getattr(args, "verbose", False)
            _timeout = getattr(args, "connect_timeout", 300.0)
            with SFTPTransfer(host=host, username=user, port=port,
                              key_path=identity or None, timeout=_timeout) as xfer:
                if rerun:
                    xfer.remove_remote_dir(remote_dir)
                    _info(f"Deleted remote directory {remote_dir} for rerun.")
                if upload_files:
                    _info(f"Transferring {len(upload_files)} file(s) to {user}@{host}:{remote_dir} ...")
                    n_up, n_sk = xfer.upload_flat(upload_files, remote_dir, verbose=_verbose)
                    _skip_msg = f", {n_sk} already present (skipped)" if n_sk else ""
                    _info(f"Transferred {n_up} file(s) to {remote_dir}{_skip_msg}.")
                if gene_calls:
                    total_mb = sum(p.stat().st_size for p in gene_calls) / (1024 * 1024)
                    _info(
                        f"Transferring {len(gene_calls)} gene call file(s) ({total_mb:.0f} MB) "
                        f"to {user}@{host}:{gene_dir} ..."
                    )
                    n_up = n_sk = 0
                    for source in gene_calls:
                        remote_path = f"{gene_dir}/{source.name}.gz"
                        if xfer.remote_exists(remote_path):
                            n_sk += 1
                            if _verbose:
                                print(f"  SKIP {source} (already exists remotely)", file=sys.stderr)
                            continue
                        size_mb = source.stat().st_size / (1024 * 1024)
                        _info(f"  Compressing and uploading {source.name} ({size_mb:.0f} MB) ...")
                        xfer.upload_stream(
                            remote_path,
                            lambda handle, src=source: _gzip_into(src, handle),
                            verbose=_verbose,
                        )
                        n_up += 1
                    _skip_msg = f", {n_sk} already present (skipped)" if n_sk else ""
                    _info(f"Transferred {n_up} gene call file(s) to {gene_dir}{_skip_msg}.")

        # uploadAttachment appends to whatever the field already holds, so a
        # rerun clears the fields first instead of stacking a second copy of
        # every table on the record.
        if attachments and rerun and not batch.from_core:
            client.update_records(
                _require_cfg("EHI_AMR_BATCH"),
                [{"id": batch_record_id, "fields": {fld: [] for fld, _ in attachments}}],
            )
            _info("Cleared the AMR attachment fields for rerun.")
            batch_record = client.fetch_batch_record(
                _require_cfg("EHI_AMR_BATCH"), _require_cfg("EHI_AMR_BATCH_CODE"), args.batch
            )

        # An attachment needs an Airtable record to hang on; the core keeps the
        # ERDA URLs of the same files instead.
        batch_fields_now = batch_record.get("fields", {}) if batch_record else {}
        if batch.from_core:
            attachments = []
        for field_id, path in attachments:
            attached = batch_fields_now.get(field_id) or []
            if any(str(a.get("filename", "")) == path.name
                   for a in attached if isinstance(a, dict)):
                _info(f"  {path.name} is already attached — skipping.")
                continue
            encoded = attachment_encoded_size(path.stat().st_size)
            if encoded > ATTACHMENT_MAX_BYTES:
                _info(
                    f"  {path.name} is {path.stat().st_size / (1024 * 1024):.1f} MB, over the "
                    f"{ATTACHMENT_MAX_BYTES // (1024 * 1024)} MB Airtable attachment limit — "
                    f"left on ERDA only ({remote_dir}/{path.name})."
                )
                continue
            try:
                client.upload_attachment(
                    _require_cfg("EHI_AMR_BATCH"), batch_record_id, field_id, path,
                    content_type=_content_type_of(path),
                )
            except AirtableError as exc:
                print(f"  Warning: could not attach {path.name}: {exc}", file=sys.stderr)
                continue
            _info(f"  Attached {path.name}.")
    finally:
        for tmp in temporary:
            tmp.unlink(missing_ok=True)

    batch_fields: dict = {}
    ehio_version_field    = str(cfg.get("EHI_AMR_BATCH_EHIO_VERSION")    or "").strip()
    drakkar_version_field = str(cfg.get("EHI_AMR_BATCH_DRAKKAR_VERSION") or "").strip()
    if ehio_version_field:
        batch_fields[ehio_version_field] = __version__
    if drakkar_version_field:
        batch_fields[drakkar_version_field] = drakkar_version

    done_status = str(cfg.get("PROCESSING_DONE_STATUS") or "Done").strip()
    if not batch.from_core:
        batch_fields[_require_cfg("EHI_AMR_BATCH_STATUS")] = done_status
        client.update_records(
            _require_cfg("EHI_AMR_BATCH"), [{"id": batch_record_id, "fields": batch_fields}]
        )
    # The attachments are copies of ERDA files; the core keeps their URLs.
    uploaded = {path.name for path in upload_files}
    tables = {
        column: mirror.erda_url("AMR", args.batch, f"{args.batch}_{name}")
        for column, name in (("hits_url", "amr_hits.tsv.xz"), ("loci_url", "amr_loci.tsv.xz"))
        if f"{args.batch}_{name}" in uploaded
    }
    gene_call_files = {f"{p.name}.gz" for p in gene_calls}
    core.mirror(f"Batch '{args.batch}'", [
        *(mirror.amr_metrics(args.batch, all_metrics, gene_calls=gene_call_files)
          if batch.from_core
          else mirror.amr_assemblies(args.batch, records, gene_calls=gene_call_files)),
        mirror.batch(
            "amr", args.batch, None if batch.from_core else batch_record,
            status=done_status, ehio_version=__version__, drakkar_version=drakkar_version, **tables,
        ),
    ])
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

    def _add_mode(p: argparse.ArgumentParser) -> argparse._MutuallyExclusiveGroup:
        mode = p.add_mutually_exclusive_group(required=True)
        mode.add_argument("--input", action="store_true",
            help="Input mode: fetch records from Airtable and write drakkar input files.")
        mode.add_argument("--output", action="store_true",
            help="Output mode: collect metadata, update Airtable, transfer files.")
        return mode

    def _add_batch(p: argparse.ArgumentParser) -> None:
        p.add_argument("--batch", "-b", required=True, metavar="BATCH",
            help="Batch code used to look up the batch record in Airtable.")

    def _add_core_token(p: argparse.ArgumentParser) -> None:
        p.add_argument("--core-token", metavar="TOKEN",
            help="ehi-core pipeline token. Overrides $EHI_CORE_TOKEN.")

    def _add_token(p: argparse.ArgumentParser, core: bool = True) -> None:
        p.add_argument("--airtable-token", metavar="TOKEN",
            help="Airtable personal access token. Overrides $AIRTABLE_TOKEN.")
        if core:
            _add_core_token(p)

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
            "Stage mode:  rebuild the dereplicated genome directory of a finished\n"
            "             batch — read the catalogue from its counts table on ERDA\n"
            "             and download each genome from its MAG record — so an old\n"
            "             batch can be annotated again without being profiled again.\n"
            "Input mode:  write genome paths for all MAGs linked to the batch\n"
            "             into a file for drakkar functional annotation.\n"
            "Output mode: parse GTDB-Tk taxonomy and per-genome functional annotation\n"
            "             results, update MAG_ENTRY records in Airtable, upload\n"
            "             taxonomy/tree files to DMB/{batch} and compressed per-genome\n"
            "             TSVs to ANN/{batch} via SFTP."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _ann_mode = _add_mode(p_ann)
    _ann_mode.add_argument("--stage", action="store_true",
        help="Stage mode: download the dereplicated genomes of a finished batch into --annotation-dir.")
    _add_batch(p_ann)
    _add_token(p_ann)
    _add_verbose(p_ann)
    p_ann.add_argument("--annotation-file", "-f", default="annotation.tsv", metavar="PATH",
        help="Output paths file for drakkar annotation (input mode). Default: annotation.tsv.")
    p_ann.add_argument("--annotation-dir", "-d", default=".", metavar="DIR",
        help="Directory holding the dereplicated genome FASTA files (input and stage modes).")
    p_ann.add_argument("--genomes-file", metavar="PATH",
        help="Stage mode: read the dereplicated genome names from this file (one per line) "
             "instead of from the batch's counts table on ERDA.")
    p_ann.add_argument("--redownload", action="store_true",
        help="Stage mode: fetch every genome again, even one already in --annotation-dir.")
    p_ann.add_argument("--reannotate", action="store_true",
        help="Output mode: this is a re-annotation of an already-processed batch. Taxonomy "
             "is left as it is on the MAG records and on ERDA, and the drakkar version "
             "already on the batch record is kept with the new one appended.")
    p_ann.add_argument("--download-timeout", metavar="SECONDS", type=float, default=600.0,
        help="Stage mode: per-genome download timeout in seconds (default: 600).")
    _add_sftp_overrides(p_ann)
    p_ann.set_defaults(func=cmd_annotating)

    # ------------------------------------------------------------------
    # amr
    # ------------------------------------------------------------------
    p_amr = sub.add_parser(
        "amr",
        help="Input/output for the antimicrobial resistance workflow.",
        description=(
            "Input mode:  fetch the assemblies linked to an AMR batch in\n"
            "             EHI_BASE/EHI_AMR_BATCH, download their FASTAs from the\n"
            "             URLs held in EHI_ASB_ENTRY, and write a drakkar amr\n"
            "             assembly manifest pointing at the local copies.\n"
            "Output mode: parse amr/amr_qc.tsv, write the per-assembly AMR stats\n"
            "             back to EHI_ASB_ENTRY, upload the aggregate tables to\n"
            "             AMR/{batch} via SFTP and attach them to the batch record,\n"
            "             and upload the prodigal gene calls to AMR/{batch}/genes."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_mode(p_amr)
    _add_batch(p_amr)
    _add_token(p_amr)
    _add_verbose(p_amr)
    p_amr.add_argument("--manifest-file", "-f", default="assemblies.tsv", metavar="PATH",
        help="Output assembly manifest for drakkar amr (input mode). Default: assemblies.tsv.")
    p_amr.add_argument("--assemblies-dir", "-d", default="assemblies", metavar="DIR",
        help="Directory the assembly FASTAs are downloaded into (input mode).\n"
             "Default: assemblies.")
    p_amr.add_argument("--redownload", action="store_true",
        help="Download every assembly again, even when it is already in the\n"
             "assemblies directory (input mode).")
    p_amr.add_argument("--download-timeout", metavar="SECONDS", type=float, default=600.0,
        help="Timeout for a single assembly download in seconds (default: 600).")
    _add_sftp_overrides(p_amr)
    p_amr.set_defaults(func=cmd_amr)

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
    _add_token(p_ref, core=False)
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
        help="Scan the batch tables for pending batches and launch them in screen sessions.",
        description=(
            "Queries each batch table in Airtable and in ehi-core for batches whose\n"
            "status matches SCANNING_TRIGGER_STATUS, then for each pending batch:\n"
            "  1. Creates a screen session named after the batch.\n"
            "  2. Runs: ehio <module> --input -b BATCH && drakkar <cmd> ...\n"
            "  3. Sets the status to SCANNING_LAUNCHED_STATUS in both databases.\n\n"
            "A batch both databases hold is launched once, as ehi-core has it.\n"
            "Already-running screen sessions are skipped automatically."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p_scan.add_argument("--module", "-m",
        choices=["preprocessing", "binning", "quantifying", "amr"],
        metavar="MODULE",
        help="Scan only this module. Default: scan all four.")
    p_scan.add_argument("--airtable-token", metavar="TOKEN",
        help="Airtable personal access token. Overrides $AIRTABLE_TOKEN.")
    _add_core_token(p_scan)
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
            "With --failures-dir, the newest drakkar failure table found in that\n"
            "directory (logging/drakkar_<run_id>.failures.tsv, or\n"
            "drakkar_<run_id>_failures.tsv before drakkar 2.5.0) is also attached\n"
            "to the batch's error files field, so the cause of the failure is\n"
            "visible from Airtable."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p_ss.add_argument("--module", "-m", required=True,
        choices=["preprocessing", "binning", "quantifying", "amr"],
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
    _add_core_token(p_ss)
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
        choices=["preprocessing", "binning", "quantifying", "amr"],
        help="Module whose batch table to update.")
    p_stop.add_argument("--batch", "-b", required=True, metavar="BATCH",
        help="Batch code (screen session name) to stop.")
    p_stop.add_argument("--keep-jobs", action="store_true",
        help="Leave the Slurm jobs of the batch running; only kill the\n"
             "screen session and update the status.")
    p_stop.add_argument("--airtable-token", metavar="TOKEN",
        help="Airtable personal access token. Overrides $AIRTABLE_TOKEN.")
    _add_core_token(p_stop)
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
        choices=["preprocessing", "binning", "quantifying", "amr"],
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
            "Removes the working output directory (PPR/ASB/DMB/AMR)/{batch} for the given\n"
            "module. The RUN/{batch} directory (scripts and logs) is not touched."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p_rm.add_argument("--module", "-m", required=True,
        choices=["preprocessing", "binning", "quantifying", "amr"],
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
    "amr":           ("EHI_BASE", "EHI_AMR_BATCH", "EHI_AMR_BATCH_CODE", "EHI_AMR_BATCH_STATUS", "EHI_AMR_BATCH_ERROR_FILES"),
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
    core = _core(args)
    # A batch created in ehi-core has no Airtable record, and the exit trap of
    # a failed run has nothing else to report its error to.
    if not batch_record:
        if not core:
            _die(f"Batch '{args.batch}' not found in {batch_table}.")
        _info(f"Batch '{args.batch}' is not in {batch_table} — ehi-core alone holds it.")
    else:
        client.update_records(
            batch_table,
            [{"id": batch_record["id"], "fields": {status_field: args.status}}],
        )
    units = [mirror.batch(args.module, args.batch, batch_record, status=args.status)]
    # Mirrored only while Airtable holds the batch too: for one the core alone
    # holds, a status that did not reach the core was not set at all.
    results = (core.mirror(f"Status of batch '{args.batch}'", units) if batch_record
               else core.write(units, f"Status of batch '{args.batch}'"))
    if not batch_record and any(r.get("action") == "created" for r in results):
        _warn(f"ehi-core held no batch '{args.batch}' either; it was created with this status.")
    _info(f"Batch '{args.batch}' status → '{args.status}'.")

    if args.failures_dir:
        if not batch_record:
            _info("The failure report needs an Airtable record to attach to — not uploaded.")
            return 0
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
    core = _core(args)

    where = "Airtable and ehi-core" if core else "Airtable"
    print(f"Scanning {where} for pending batches...")
    total = run_scan(
        token=token,
        modules=modules,
        dry_run=args.dry_run,
        verbose=args.verbose,
        core=core,
        core_token=_core_token(args) if core else "",
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
    "amr":           "EHI_AMR_OUTPUT_BASE",
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
    core = _core(args)
    # A batch created in ehi-core has no Airtable record, and stopping it still
    # has to kill its session and jobs and say so somewhere.
    if not batch_record:
        if not core:
            _die(f"Batch '{args.batch}' not found in {batch_table}.")
        _info(f"Batch '{args.batch}' is not in {batch_table} — ehi-core alone holds it.")

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

    if batch_record:
        client.update_records(
            batch_table,
            [{"id": batch_record["id"], "fields": {status_field: stopped_status}}],
        )
    core.mirror(f"Status of batch '{args.batch}'", [
        mirror.batch(args.module, args.batch, batch_record, status=stopped_status),
    ])
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
    from ehio.core import CoreError

    parser = _build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 0
    try:
        return args.func(args)
    except (AirtableError, CoreError) as exc:
        _die(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
