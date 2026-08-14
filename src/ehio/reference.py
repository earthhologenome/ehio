"""Reference genome index archiving and registration.

A preprocessing batch whose genome record has no indexed tarball is launched
with drakkar's -r flag, and drakkar runs bowtie2-build on the raw FASTA before
it can map anything.  That index is worth far more than the batch that paid for
it: uploaded to ERDA and recorded in Airtable, every later batch on the same
host is launched with -x and skips bowtie2-build entirely.

This module closes that loop.  After a preprocessing batch finishes, it packs
the index drakkar just built, streams it to {SFTP_REMOTE_BASE}/REF/{code}.tar.gz
and flags the genome record as indexed.
"""

from __future__ import annotations

import sys
import tarfile
from pathlib import Path
from typing import Any, Callable

from ehio import config as cfg
from ehio.airtable import AirtableClient, AirtableError

_ERROR = "\033[1;31m"
_INFO  = "\033[1;34m"
_RESET = "\033[0m"

# The two Bowtie2 index flavours: small (.bt2) and large (.bt2l).  A reference
# carries one complete set or the other, never a mix.
BT2_SUFFIXES  = (".1.bt2",  ".2.bt2",  ".3.bt2",  ".4.bt2",  ".rev.1.bt2",  ".rev.2.bt2")
BT2L_SUFFIXES = (".1.bt2l", ".2.bt2l", ".3.bt2l", ".4.bt2l", ".rev.1.bt2l", ".rev.2.bt2l")

# gzip level 6 rather than tarfile's default 9: the Bowtie2 files barely
# compress, so level 9 costs a lot of CPU on a multi-GB archive for very little.
_COMPRESS_LEVEL = 6

_DEFAULT_REMOTE_DIR = "REF"
_DEFAULT_CODE_FIELD = "Code"
_DEFAULT_INDEXED_VALUE = "YES"


def _info(msg: str) -> None:
    print(f"{_INFO}Info:{_RESET} {msg}", file=sys.stderr)


def _warn(msg: str) -> None:
    print(f"{_ERROR}Warning:{_RESET} {msg}", file=sys.stderr)


def _noop(_msg: str) -> None:
    pass


# ---------------------------------------------------------------------------
# Genome record lookup
# ---------------------------------------------------------------------------

def resolve_genome_record(
    batch_record: dict[str, Any],
    token: str,
    dbg: Callable[[str], None] = _noop,
) -> dict[str, Any] | None:
    """Return the EHI_GENOME record linked to a preprocessing batch, or None.

    EHI_PPR_BATCH_REFERENCE is either a linked-record field (a recXXX id, looked
    up directly) or a text/formula field holding the genome code (looked up by
    EHI_GENOME_CODE).  None means no reference is configured for this batch, or
    the genome record could not be found — both are non-fatal for the caller.
    """
    batch_ref_field = str(cfg.get("EHI_PPR_BATCH_REFERENCE") or "").strip()
    ehi_base_id     = str(cfg.get("EHI_BASE")                or "").strip()
    genome_table    = str(cfg.get("EHI_GENOME")              or "").strip()

    if not batch_ref_field:
        dbg("EHI_PPR_BATCH_REFERENCE not configured — no reference.")
        return None

    ref_value = batch_record.get("fields", {}).get(batch_ref_field)
    dbg(f"EHI_PPR_BATCH_REFERENCE field ({batch_ref_field}) raw value: {ref_value!r}")

    if isinstance(ref_value, list):
        ref_value = ref_value[0] if ref_value else None
    if not ref_value:
        dbg("Reference field is empty — no reference.")
        return None

    ref_rec_id = str(ref_value).strip()
    dbg(f"Resolved reference value: {ref_rec_id!r}")

    if not (ehi_base_id and genome_table):
        _warn("EHI_BASE or EHI_GENOME not configured.")
        return None

    genome_client = AirtableClient(api_key=token, base_id=ehi_base_id)

    if ref_rec_id.startswith("rec"):
        # Linked-record field — fetch the genome record directly by its record ID.
        dbg(f"Looking up genome record by ID: {ref_rec_id}")
        genome_rec = genome_client.fetch_record_by_id(genome_table, ref_rec_id)
    else:
        # Text/formula field containing the genome code (e.g. "G0001").
        genome_code_fld = str(cfg.get("EHI_GENOME_CODE") or "").strip()
        dbg(f"Looking up genome record by code: {ref_rec_id!r} in field {genome_code_fld}")
        if not genome_code_fld:
            _warn("EHI_GENOME_CODE not configured.")
            return None
        records = genome_client.fetch_records_by_value(
            genome_table, genome_code_fld, ref_rec_id
        )
        genome_rec = records[0] if records else None

    if not genome_rec:
        _warn(
            f"genome record {ref_rec_id!r} not found in EHI_GENOME ({genome_table})."
        )
        return None

    return genome_rec


def genome_code(genome_record: dict[str, Any]) -> str:
    """Return the genome code (e.g. 'G0001') of a genome record, or ''."""
    code_field = str(cfg.get("EHI_GENOME_CODE") or "").strip()
    if not code_field:
        return ""
    value = genome_record.get("fields", {}).get(code_field, "")
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value or "").strip()


def indexed_url(genome_record: dict[str, Any]) -> str:
    """Return the indexed-tarball URL recorded on a genome record, or ''."""
    field = str(cfg.get("EHI_GENOME_URL_INDEXED") or "").strip()
    if not field:
        return ""
    return str(genome_record.get("fields", {}).get(field, "") or "").strip()


# ---------------------------------------------------------------------------
# Index discovery and archiving
# ---------------------------------------------------------------------------

def find_index_sets(references_dir: Path) -> list[tuple[Path, list[Path]]]:
    """Return every (fasta, index_files) pair found in a drakkar references dir.

    drakkar writes {output_dir}/data/references/{reference}.fna plus the six
    Bowtie2 files next to it; with -r/-x the basename is always 'reference'.
    Only complete index sets are returned — a half-built index is not one.
    """
    if not references_dir.is_dir():
        return []

    found: list[tuple[Path, list[Path]]] = []
    for fasta in sorted(references_dir.glob("*.fna")):
        stem = fasta.name[: -len(".fna")]
        for suffixes in (BT2_SUFFIXES, BT2L_SUFFIXES):
            files = [references_dir / f"{stem}{suffix}" for suffix in suffixes]
            if all(f.is_file() for f in files):
                found.append((fasta, files))
                break
    return found


def write_index_archive(
    fileobj: Any,
    fasta: Path,
    index_files: list[Path],
    code: str,
) -> None:
    """Stream a tar.gz of a reference FASTA and its Bowtie2 index into fileobj.

    Members are renamed to the genome code ({code}.fna, {code}.1.bt2, ...), the
    layout drakkar's extract_reference_index.py expects when the archive comes
    back as -x.  Stream mode ('w|gz') is used so the archive never has to exist
    on disk — multi-GB indexes go straight over the wire.
    """
    stem = fasta.name[: -len(".fna")]
    with tarfile.open(fileobj=fileobj, mode="w|gz", compresslevel=_COMPRESS_LEVEL) as tar:
        tar.add(str(fasta), arcname=f"{code}.fna")
        for path in index_files:
            tar.add(str(path), arcname=f"{code}{path.name[len(stem):]}")


# ---------------------------------------------------------------------------
# Airtable registration
# ---------------------------------------------------------------------------

def flag_genome_indexed(code: str, token: str) -> bool:
    """Mark a genome as indexed in the reference genome table.

    The flag lives in the master reference genome table (EHI_GENOME_INDEX_BASE /
    EHI_GENOME_INDEX_TABLE), which is a different table from the one the batch
    reference is read from, so the record is matched on the genome code.
    Returns True when the record carries the flag afterwards.
    """
    base_id    = str(cfg.get("EHI_GENOME_INDEX_BASE")     or "").strip()
    table      = str(cfg.get("EHI_GENOME_INDEX_TABLE")    or "").strip()
    field      = str(cfg.get("EHI_GENOME_INDEXED")        or "").strip()
    code_field = str(cfg.get("EHI_GENOME_INDEX_CODE")     or _DEFAULT_CODE_FIELD).strip()
    value      = str(cfg.get("EHI_GENOME_INDEXED_VALUE")  or _DEFAULT_INDEXED_VALUE).strip()

    if not (base_id and table and field):
        _warn(
            "EHI_GENOME_INDEX_BASE, EHI_GENOME_INDEX_TABLE or EHI_GENOME_INDEXED is "
            f"not configured — genome {code} was not flagged as indexed."
        )
        return False

    client  = AirtableClient(api_key=token, base_id=base_id)
    records = client.fetch_records_by_value(table, code_field, code)

    if not records:
        _warn(
            f"genome {code} not found in the reference genome table ({table}) via "
            f"field {code_field} — it was not flagged as indexed."
        )
        return False
    if len(records) > 1:
        _warn(
            f"genome code {code} matches {len(records)} records in {table} — "
            "not flagging any of them as indexed."
        )
        return False

    record = records[0]
    if str(record.get("fields", {}).get(field, "") or "").strip() == value:
        _info(f"Genome {code} is already flagged as indexed.")
        return True

    client.update_records(table, [{"id": record["id"], "fields": {field: value}}])
    _info(f"Genome {code} flagged as indexed ({field} → '{value}').")
    return True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def upload_reference_index(
    batch_record: dict[str, Any],
    local_root: Path,
    token: str,
    *,
    host: str,
    user: str,
    remote_base: str,
    port: int = 22,
    identity: str | None = None,
    timeout: float = 300.0,
    verbose: bool = False,
) -> bool:
    """Upload the Bowtie2 index a preprocessing batch built, and register it.

    Does nothing when the batch ran against an already-indexed reference, when
    it had no reference at all, or when no complete index is present under
    local_root — all normal situations.  Must be called before the output
    directory is cleaned up, since that is where the index lives.

    Returns True when the genome ends up indexed on ERDA and flagged in Airtable.
    """
    from ehio.transfer import SFTPTransfer

    genome_record = resolve_genome_record(
        batch_record, token, dbg=(lambda m: _info(f"[ref] {m}")) if verbose else _noop
    )
    if genome_record is None:
        return False

    code = genome_code(genome_record)
    if not code:
        _warn(
            "the genome record of this batch has no code (EHI_GENOME_CODE) — "
            "the reference index cannot be named and was not uploaded."
        )
        return False

    if indexed_url(genome_record):
        _info(f"Genome {code} is already indexed on ERDA; nothing to upload.")
        return False

    references_dir = Path(local_root) / "data" / "references"
    index_sets = find_index_sets(references_dir)
    if not index_sets:
        _info(f"No Bowtie2 index found in {references_dir}; nothing to upload.")
        return False
    if len(index_sets) > 1:
        names = ", ".join(fasta.name for fasta, _ in index_sets)
        _warn(
            f"{references_dir} holds more than one reference index ({names}) — "
            f"cannot tell which one belongs to genome {code}; nothing uploaded."
        )
        return False

    fasta, index_files = index_sets[0]
    remote_dir  = f"{remote_base.rstrip('/')}/{str(cfg.get('SFTP_REMOTE_REFERENCE_DIR') or _DEFAULT_REMOTE_DIR).strip('/')}"
    remote_path = f"{remote_dir}/{code}.tar.gz"

    with SFTPTransfer(host=host, username=user, port=port, key_path=identity or None, timeout=timeout) as xfer:
        if xfer.remote_exists(remote_path):
            _info(f"{remote_path} already exists; skipping upload.")
        else:
            size_gb = (fasta.stat().st_size + sum(f.stat().st_size for f in index_files)) / (1024 ** 3)
            _info(
                f"Archiving the reference index of genome {code} ({size_gb:.1f} GB) "
                f"and uploading it to {user}@{host}:{remote_path} ..."
            )
            xfer.upload_stream(
                remote_path,
                lambda handle: write_index_archive(handle, fasta, index_files, code),
            )
            _info(f"Reference index of genome {code} uploaded to {remote_path}.")

    try:
        return flag_genome_indexed(code, token)
    except AirtableError as exc:
        _warn(
            f"the reference index of genome {code} is on ERDA at {remote_path}, but "
            f"the genome record could not be flagged as indexed: {exc}"
        )
        return False
