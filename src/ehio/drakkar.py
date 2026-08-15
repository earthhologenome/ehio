"""Generate drakkar-compatible input files from Airtable records."""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any

# Assembly types declared in the batch record (EHI_ASB_BATCH_TYPE).
# The grouping itself always comes from the assembly codes of the entries; the
# type states the intent, which decides whether drakkar is called with
# --multicoverage and lets the codes be checked before the run starts.
ASSEMBLY_TYPES = ("individual", "coassembly", "multicoverage")


def normalise_assembly_type(value: object) -> str:
    """Return the canonical assembly type of an Airtable batch-type value.

    Punctuation and case are ignored, so 'Coassembly', 'co-assembly' and
    'Co assembly' all mean the same thing.  An empty or unrecognised value
    returns '', which leaves the run unchanged.
    """
    if isinstance(value, list):
        value = value[0] if value else ""
    text = re.sub(r"[^a-z]", "", str(value or "").lower())
    return text if text in ASSEMBLY_TYPES else ""


def group_samples_by_assembly(
    records: list[dict[str, Any]],
    sample_field: str,
    assembly_field: str,
) -> dict[str, list[str]]:
    """Return {assembly code: [sample, ...]} for a set of binning entries.

    One entry is one sample; entries sharing an assembly code are co-assembled
    by drakkar.  Entries without a sample or an assembly code are skipped, as
    they are by write_sample_file.
    """
    groups: dict[str, list[str]] = {}
    for rec in records:
        fields = rec.get("fields", rec)
        sample = _first(fields.get(sample_field, ""))
        assembly = _first(fields.get(assembly_field, "")) or sample
        if not sample or not assembly:
            continue
        groups.setdefault(assembly, []).append(sample)
    return groups


def check_assembly_type(
    records: list[dict[str, Any]],
    assembly_type: str,
    sample_field: str,
    assembly_field: str,
) -> tuple[str, str]:
    """Check the entry assembly codes against the declared assembly type.

    Returns an (error, warning) pair of messages, either of which may be empty.

    A multicoverage batch is the only fatal mismatch: drakkar refuses
    --multicoverage on co-assembled samples and exits without running anything,
    which would leave the batch looking successful with no output at all.  A
    mislabelled individual or co-assembly batch still runs exactly as its codes
    say, so it is only reported.
    """
    groups = group_samples_by_assembly(records, sample_field, assembly_field)
    coassembled = {code: samples for code, samples in groups.items() if len(samples) > 1}

    if assembly_type == "multicoverage" and coassembled:
        detail = "; ".join(f"{code}: {', '.join(samples)}" for code, samples in sorted(coassembled.items()))
        return (
            f"Batch type is 'Multicoverage' but {len(coassembled)} assembly code(s) are "
            f"shared by several entries ({detail}). Multicoverage assemblies must be "
            "individual — give every entry its own assembly code, or set the batch "
            "type to 'Coassembly'.",
            "",
        )
    if assembly_type == "individual" and coassembled:
        return "", (
            f"Batch type is 'Individual' but {len(coassembled)} assembly code(s) are shared "
            f"by several entries ({', '.join(sorted(coassembled))}); these will be co-assembled."
        )
    if assembly_type == "coassembly" and not coassembled:
        return "", (
            "Batch type is 'Coassembly' but every entry has its own assembly code; "
            "all assemblies will be individual."
        )
    return "", ""


def _first(value: object) -> str:
    """Return an Airtable cell as a stripped string, taking [0] of a list."""
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value).strip()


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

    rows = []
    for rec in records:
        fields = rec.get("fields", rec)
        sample    = _first(fields.get(sample_field, ""))
        rawreads1 = _first(fields.get(reads1_field, ""))
        rawreads2 = _first(fields.get(reads2_field, ""))
        if not sample or not rawreads1:
            continue
        row: dict[str, str] = {"sample": sample, "rawreads1": rawreads1, "rawreads2": rawreads2}
        if assembly_field:
            row["assembly"] = _first(fields.get(assembly_field, sample))
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
