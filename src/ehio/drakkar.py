"""Generate drakkar-compatible input files from Airtable records."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def write_sample_file(
    records: list[dict[str, Any]],
    path: Path,
    sample_field: str,
    reads1_field: str,
    reads2_field: str,
    assembly_field: str | None = None,
) -> int:
    """Write a drakkar sample info TSV (used by preprocessing and binning).

    Columns: sample[, assembly], rawreads1, rawreads2
    When assembly_field is provided an 'assembly' column is written; drakkar
    groups rows with the same assembly value into a co-assembly automatically.
    Returns the number of rows written.
    """
    columns = ["sample", "assembly", "rawreads1", "rawreads2"] if assembly_field else ["sample", "rawreads1", "rawreads2"]

    def _str(value: object) -> str:
        if isinstance(value, list):
            value = value[0] if value else ""
        return str(value).strip()

    rows = []
    for rec in records:
        fields = rec.get("fields", rec)
        sample    = _str(fields.get(sample_field,  ""))
        rawreads1 = _str(fields.get(reads1_field, ""))
        rawreads2 = _str(fields.get(reads2_field, ""))
        if not sample or not rawreads1:
            continue
        row: dict[str, str] = {"sample": sample, "rawreads1": rawreads1, "rawreads2": rawreads2}
        if assembly_field:
            row["assembly"] = _str(fields.get(assembly_field, sample))
        rows.append(row)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def verify_input_files(
    records: list[dict[str, Any]],
    sample_field: str,
    path_fields: list[str],
) -> list[tuple[str, str]]:
    """Check that all local file paths referenced in records actually exist.

    Returns a list of (sample, path) pairs for every path that is missing.
    Paths that look like URLs (http/https/ftp/sftp) are skipped — they cannot
    be verified without a network round-trip.
    """
    missing: list[tuple[str, str]] = []
    for rec in records:
        fields = rec.get("fields", rec)
        sample = str(fields.get(sample_field, "")).strip()
        for fld in path_fields:
            raw = fields.get(fld, "")
            if isinstance(raw, list):
                raw = raw[0] if raw else ""
            path_str = str(raw).strip()
            if not path_str:
                continue
            if path_str.startswith(("http://", "https://", "ftp://", "sftp://")):
                continue  # remote URL — cannot check locally
            if not Path(path_str).exists():
                missing.append((sample, path_str))
    return missing


def verify_remote_urls(
    records: list[dict[str, Any]],
    sample_field: str,
    url_fields: list[str],
    timeout: float = 20.0,
) -> list[tuple[str, str, str]]:
    """Check that every remote URL referenced in records can be downloaded.

    Only headers (or the first byte) are fetched, and duplicate URLs are
    checked once.  Local paths are ignored — verify_input_files covers those.
    Returns a list of (sample, url, reason) triples for the unreachable URLs.
    """
    from ehio.urls import check_urls, is_remote_url

    owners: list[tuple[str, str]] = []  # (sample, url)
    for rec in records:
        fields = rec.get("fields", rec)
        sample = str(fields.get(sample_field, "")).strip()
        for fld in url_fields:
            raw = fields.get(fld, "")
            if isinstance(raw, list):
                raw = raw[0] if raw else ""
            url = str(raw).strip()
            if url and is_remote_url(url):
                owners.append((sample, url))

    failures = check_urls([url for _, url in owners], timeout=timeout)
    return [(sample, url, failures[url]) for sample, url in owners if url in failures]


FAILURE_REPORT_GLOB = "drakkar_*_failures.tsv"


def find_failure_report(output_dir: str | Path, since: float | None = None) -> Path | None:
    """Return the most recent drakkar failure report in output_dir, or None.

    drakkar writes 'drakkar_{run_id}_failures.tsv' into the root of its output
    directory when a workflow stops after failures.  A batch can run drakkar
    several times (profiling, then annotating), each with its own run id, so
    the newest report is the one describing the failure that just happened.

    `since` is a Unix timestamp: reports older than it are ignored, which keeps
    the report of an earlier, already-reported launch of the same batch from
    being mistaken for the current one.
    """
    directory = Path(output_dir)
    reports = [p for p in directory.glob(FAILURE_REPORT_GLOB) if p.is_file()]
    if since is not None:
        reports = [p for p in reports if p.stat().st_mtime >= since]
    if not reports:
        return None
    return max(reports, key=lambda p: p.stat().st_mtime)


def write_quality_file(
    records: list[dict[str, Any]],
    path: Path,
    name_field: str,
    completeness_field: str,
    contamination_field: str,
) -> int:
    """Write a MAG quality TSV for drakkar profiling.

    Columns: genome, completeness, contamination
    Returns the number of rows written.
    """
    def _val(v: object) -> object:
        if isinstance(v, list):
            v = v[0] if v else ""
        return v if v is not None else ""

    rows = []
    for rec in records:
        fields = rec.get("fields", rec)
        genome = str(_val(fields.get(name_field, ""))).strip()
        if not genome:
            continue
        rows.append({
            "genome":        genome,
            "completeness":  _val(fields.get(completeness_field, "")),
            "contamination": _val(fields.get(contamination_field, "")),
        })

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["genome", "completeness", "contamination"], delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def write_bins_file(
    records: list[dict[str, Any]],
    path: Path,
    bins_field: str,
) -> int:
    """Write a bins path file for drakkar profiling/quantifying.

    One bin path per line. Returns the number of paths written.
    """
    paths = []
    for rec in records:
        fields = rec.get("fields", rec)
        bin_path = str(fields.get(bins_field, "")).strip()
        if bin_path:
            paths.append(bin_path)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for p in paths:
            fh.write(p + "\n")
    return len(paths)
