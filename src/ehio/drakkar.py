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

    Columns: sample, reads1, reads2[, reference]
    `reference` is a batch-level string applied to every row.
    Returns the number of rows written.
    """
    columns = ["sample", "reads1", "reads2"]
    if reference:
        columns.append("reference")

    rows = []
    for rec in records:
        fields = rec.get("fields", rec)
        sample = str(fields.get(sample_field, "")).strip()
        reads1 = str(fields.get(reads1_field, "")).strip()
        reads2 = str(fields.get(reads2_field, "")).strip()
        if not sample or not reads1:
            continue
        row: dict[str, str] = {"sample": sample, "reads1": reads1, "reads2": reads2}
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
