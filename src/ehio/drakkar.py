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
    reference: str | None = None,
) -> int:
    """Write a drakkar sample info TSV (used by preprocessing and binning).

    Columns: sample, rawreads1, rawreads2[, reference]
    `reference` is a batch-level string applied to every row.
    Returns the number of rows written.
    """
    columns = ["sample", "rawreads1", "rawreads2"]
    if reference:
        columns.append("reference")

    def _str(value: object) -> str:
        """Return a plain string from a field value that may be a list."""
        if isinstance(value, list):
            value = value[0] if value else ""
        return str(value).strip()

    rows = []
    for rec in records:
        fields = rec.get("fields", rec)
        sample  = _str(fields.get(sample_field,  ""))
        rawreads1 = _str(fields.get(reads1_field, ""))
        rawreads2 = _str(fields.get(reads2_field, ""))
        if not sample or not rawreads1:
            continue
        row: dict[str, str] = {"sample": sample, "rawreads1": rawreads1, "rawreads2": rawreads2}
        if reference:
            row["reference"] = reference
        rows.append(row)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, delimiter="\t")
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
