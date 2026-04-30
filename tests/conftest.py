"""Shared fixtures for the ehio test suite."""

from __future__ import annotations

import json
import pytest
from pathlib import Path


# ---------------------------------------------------------------------------
# Minimal fastp JSON (mirrors the structure produced by fastp)
# ---------------------------------------------------------------------------

FASTP_JSON: dict = {
    "summary": {
        "before_filtering": {
            "total_reads": 10_000_000,
            "total_bases": 1_500_000_000,
        },
        "after_filtering": {
            "total_reads": 9_200_000,
            "total_bases": 1_380_000_000,
        },
    },
    "adapter_cutting": {
        "adapter_trimmed_reads": 4_100_000,
        "adapter_trimmed_bases": 82_000_000,
    },
}


# ---------------------------------------------------------------------------
# Minimal singlem microbial_fraction TSV
# ---------------------------------------------------------------------------

SINGLEM_SMF_TSV = (
    "sample\tbacterial_archaeal_bases\tmetagenome_size\tread_fraction\twarning\n"
    "EHI00001\t1_200_000_000\t1_380_000_000\t0.869565\t\n"
)


# ---------------------------------------------------------------------------
# Minimal nonpareil summary TSV (optional / may not exist)
# ---------------------------------------------------------------------------

NONPAREIL_TSV = (
    "sample\tC\tLR\tmodelR\tLR*\tdiversity\n"
    "EHI00001\t0.92\t14.5\t16.2\t18.1\t21.3\n"
)


# ---------------------------------------------------------------------------
# Mock Airtable records (field IDs as keys, as returned by use_field_ids=True)
# ---------------------------------------------------------------------------

BATCH_RECORD = {
    "id": "recBATCH000001",
    "fields": {
        "fldeNHpDmJDinU1Uc": "PPR001",           # EHI_PPR_BATCH_CODE
        "fldhFIPsoslCbCyfo": "Ready",              # EHI_PPR_BATCH_STATUS
        "fldPte4nQtlSfen8B": "https://example.com/ref.fna",  # EHI_PPR_BATCH_REFERENCE
    },
}

ENTRY_RECORDS = [
    {
        "id": "recENTRY000001",
        "fields": {
            "fldNGN3g6Lvqo4ySR": "EHI00001",                          # EHI_PPR_ENTRY_CODE
            "fld2lF4Tj0MQ82HIg": ["recBATCH000001"],                   # EHI_PPR_ENTRY_BATCH
            "fldSsNtgHYgxRaYxN": "https://example.com/EHI00001_1.fq.gz",  # reads1
            "fldMmJiQLVVoJOAjF": "https://example.com/EHI00001_2.fq.gz",  # reads2
        },
    },
    {
        "id": "recENTRY000002",
        "fields": {
            "fldNGN3g6Lvqo4ySR": "EHI00002",
            "fld2lF4Tj0MQ82HIg": ["recBATCH000001"],
            "fldSsNtgHYgxRaYxN": "https://example.com/EHI00002_1.fq.gz",
            "fldMmJiQLVVoJOAjF": "https://example.com/EHI00002_2.fq.gz",
        },
    },
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fastp_json(tmp_path: Path) -> Path:
    """Write a minimal fastp JSON and return its path."""
    p = tmp_path / "EHI00001.json"
    p.write_text(json.dumps(FASTP_JSON))
    return p


@pytest.fixture
def drakkar_output(tmp_path: Path) -> Path:
    """Build a minimal drakkar preprocessing output tree for two samples."""
    for sample in ("EHI00001", "EHI00002"):
        fastp_dir   = tmp_path / "preprocessing" / "fastp"
        final_dir   = tmp_path / "preprocessing" / "final"
        singlem_dir = tmp_path / "preprocessing" / "singlem"
        nonpareil_dir = tmp_path / "preprocessing" / "nonpareil"

        fastp_dir.mkdir(parents=True, exist_ok=True)
        final_dir.mkdir(parents=True, exist_ok=True)
        singlem_dir.mkdir(parents=True, exist_ok=True)
        nonpareil_dir.mkdir(parents=True, exist_ok=True)

        (fastp_dir / f"{sample}.json").write_text(json.dumps(FASTP_JSON))
        (final_dir / f"{sample}.metareads").write_text("8500000\n")
        (final_dir / f"{sample}.metabases").write_text("1275000000\n")
        (singlem_dir / f"{sample}_smf.tsv").write_text(SINGLEM_SMF_TSV)
        (nonpareil_dir / f"{sample}_nonpareil.tsv").write_text(NONPAREIL_TSV)

    return tmp_path
