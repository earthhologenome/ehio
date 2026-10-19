"""Reference genome index archiving and registration in ehi-core."""

from __future__ import annotations

import gzip
import sys
import tarfile
from pathlib import Path
from typing import Any

from ehio import config as cfg
from ehio.core import CoreError

_ERROR = "\033[1;31m"
_INFO  = "\033[1;34m"
_RESET = "\033[0m"

BT2_SUFFIXES  = (".1.bt2",  ".2.bt2",  ".3.bt2",  ".4.bt2",  ".rev.1.bt2",  ".rev.2.bt2")
BT2L_SUFFIXES = (".1.bt2l", ".2.bt2l", ".3.bt2l", ".4.bt2l", ".rev.1.bt2l", ".rev.2.bt2l")

_COMPRESS_LEVEL = 6
_DEFAULT_REMOTE_DIR = "GEN"


def _info(msg: str) -> None:
    print(f"{_INFO}Info:{_RESET} {msg}", file=sys.stderr)


def _warn(msg: str) -> None:
    print(f"{_ERROR}Warning:{_RESET} {msg}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Index discovery and archiving
# ---------------------------------------------------------------------------

def locate_references_dir(local_root: Path) -> Path:
    """Return the directory that holds the reference index under local_root.

    Normally the drakkar output directory, whose index sits in
    data/references; local_root may also be that references directory itself,
    which is what 'ehio reference' is pointed at when the surrounding output
    directory is already gone.
    """
    local_root = Path(local_root)
    nested = local_root / "data" / "references"
    if nested.is_dir():
        return nested
    return local_root


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
    back as -x.  Stream mode ('w|') is used so the archive never has to exist on
    disk — multi-GB indexes go straight over the wire.

    The gzip layer is built here rather than left to tarfile's 'w|gz': only
    Python 3.12 and later accept a compresslevel alongside a stream mode, and
    passing one on 3.11 raises TypeError from TarFile.__init__.
    """
    stem = fasta.name[: -len(".fna")]
    with gzip.GzipFile(
        filename="", mode="wb", fileobj=fileobj, compresslevel=_COMPRESS_LEVEL
    ) as gz:
        with tarfile.open(fileobj=gz, mode="w|") as tar:
            tar.add(str(fasta), arcname=f"{code}.fna")
            for path in index_files:
                tar.add(str(path), arcname=f"{code}{path.name[len(stem):]}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def upload_reference_index_status(
    code: str,
    local_root: Path,
    core,
    *,
    host: str,
    user: str,
    remote_base: str,
    port: int = 22,
    identity: str | None = None,
    timeout: float = 300.0,
    verbose: bool = False,
    force: bool = False,
) -> str:
    """Upload a Bowtie2 index to ERDA, then register its public URL in ehi-core.

    Returns 'uploaded', 'not-flagged', 'already-indexed', 'no-reference',
    'no-index' or 'ambiguous'.
    """
    from ehio.transfer import SFTPTransfer

    if not code:
        return "no-reference"
    try:
        genome = core.client.reference_genome(code)
    except CoreError as exc:
        if exc.status == 404:
            raise CoreError(f"No reference genome {code} in ehi-core.", status=404,
                            detail=exc.detail) from exc
        raise
    if genome.get("url_indexed") and not force:
        _info(f"Genome {code} is already indexed on ERDA; nothing to upload.")
        return "already-indexed"

    references_dir = locate_references_dir(local_root)
    index_sets = find_index_sets(references_dir)
    if not index_sets:
        _info(f"No Bowtie2 index found in {references_dir}; nothing to upload.")
        return "no-index"
    if len(index_sets) > 1:
        names = ", ".join(fasta.name for fasta, _ in index_sets)
        _warn(f"{references_dir} holds more than one reference index ({names}) — "
              f"cannot tell which one belongs to genome {code}; nothing uploaded.")
        return "ambiguous"

    share_base = str(cfg.get("ERDA_SHARE_BASE") or "").strip().rstrip("/")
    if not share_base:
        raise ValueError("ERDA_SHARE_BASE is not configured; the index URL cannot be registered.")
    reference_dir = str(cfg.get("SFTP_REMOTE_REFERENCE_DIR") or _DEFAULT_REMOTE_DIR).strip("/")
    url = f"{share_base}/{reference_dir}/{code}.tar.gz"
    remote_path = f"{remote_base.rstrip('/')}/{reference_dir}/{code}.tar.gz"
    fasta, index_files = index_sets[0]
    with SFTPTransfer(host=host, username=user, port=port, key_path=identity or None, timeout=timeout) as xfer:
        if xfer.remote_exists(remote_path) and not force:
            _info(f"{remote_path} already exists; skipping upload.")
        else:
            size_gb = (fasta.stat().st_size + sum(f.stat().st_size for f in index_files)) / (1024 ** 3)
            _info(f"Archiving the reference index of genome {code} ({size_gb:.1f} GB) "
                  f"and uploading it to {user}@{host}:{remote_path} ...")
            xfer.upload_stream(remote_path,
                               lambda handle: write_index_archive(handle, fasta, index_files, code),
                               verbose=verbose)
            _info(f"Reference index of genome {code} uploaded to {remote_path}.")

    if not genome.get("url_indexed"):
        try:
            core.client.set_genome_indexed(code, url)
        except CoreError as exc:
            _warn(f"the reference index of genome {code} is on ERDA at {remote_path}, but "
                  f"the genome URL could not be registered in ehi-core: {exc}")
            return "not-flagged"
    return "uploaded"


def upload_reference_index(*args: Any, **kwargs: Any) -> bool:
    """upload_reference_index_status, reduced to 'is the genome indexed now?'."""
    return upload_reference_index_status(*args, **kwargs) == "uploaded"
