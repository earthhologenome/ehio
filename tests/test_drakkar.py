"""Tests for ehio.drakkar — drakkar input file writers."""

from __future__ import annotations

import csv
import pytest
from pathlib import Path

from ehio.drakkar import write_sample_file, write_bins_file
from tests.conftest import ENTRY_RECORDS


# ---------------------------------------------------------------------------
# write_sample_file
# ---------------------------------------------------------------------------

class TestWriteSampleFile:
    SAMPLE_FIELD = "fldNGN3g6Lvqo4ySR"  # EHI_PPR_ENTRY_CODE
    READS1_FIELD = "fldSsNtgHYgxRaYxN"  # EHI_PPR_ENTRY_RAW_FILE_FORWARD
    READS2_FIELD = "fldMmJiQLVVoJOAjF"  # EHI_PPR_ENTRY_RAW_FILE_REVERSE

    def _read_tsv(self, path: Path) -> list[dict]:
        with path.open(newline="") as fh:
            return list(csv.DictReader(fh, delimiter="\t"))

    def test_basic_output_without_reference(self, tmp_path: Path):
        out = tmp_path / "samples.tsv"
        n = write_sample_file(
            ENTRY_RECORDS, out,
            sample_field=self.SAMPLE_FIELD,
            reads1_field=self.READS1_FIELD,
            reads2_field=self.READS2_FIELD,
        )
        assert n == 2
        rows = self._read_tsv(out)
        assert rows[0]["sample"] == "EHI00001"
        assert rows[0]["rawreads1"] == "https://example.com/EHI00001_1.fq.gz"
        assert rows[0]["rawreads2"] == "https://example.com/EHI00001_2.fq.gz"
        assert "reference" not in rows[0]

    def test_columns_with_reference(self, tmp_path: Path):
        out = tmp_path / "samples_ref.tsv"
        n = write_sample_file(
            ENTRY_RECORDS, out,
            sample_field=self.SAMPLE_FIELD,
            reads1_field=self.READS1_FIELD,
            reads2_field=self.READS2_FIELD,
            reference="https://example.com/ref.fna",
        )
        assert n == 2
        rows = self._read_tsv(out)
        assert rows[0]["reference"] == "https://example.com/ref.fna"
        assert rows[1]["reference"] == "https://example.com/ref.fna"

    def test_reference_applied_to_all_rows(self, tmp_path: Path):
        """Every row should get the same batch-level reference string."""
        out = tmp_path / "samples_ref.tsv"
        write_sample_file(
            ENTRY_RECORDS, out,
            sample_field=self.SAMPLE_FIELD,
            reads1_field=self.READS1_FIELD,
            reads2_field=self.READS2_FIELD,
            reference="/data/host/reference.fna",
        )
        rows = self._read_tsv(out)
        refs = {r["reference"] for r in rows}
        assert refs == {"/data/host/reference.fna"}

    def test_row_without_sample_id_is_skipped(self, tmp_path: Path):
        """Records missing the sample field should be silently dropped."""
        records = [
            {"id": "recX", "fields": {
                self.SAMPLE_FIELD: "",
                self.READS1_FIELD: "https://example.com/x_1.fq.gz",
                self.READS2_FIELD: "https://example.com/x_2.fq.gz",
            }},
            *ENTRY_RECORDS,
        ]
        out = tmp_path / "samples.tsv"
        n = write_sample_file(records, out,
            sample_field=self.SAMPLE_FIELD,
            reads1_field=self.READS1_FIELD,
            reads2_field=self.READS2_FIELD,
        )
        assert n == 2  # only the two valid records

    def test_row_without_reads1_is_skipped(self, tmp_path: Path):
        records = [
            {"id": "recY", "fields": {
                self.SAMPLE_FIELD: "EHI00003",
                self.READS1_FIELD: "",
                self.READS2_FIELD: "https://example.com/y_2.fq.gz",
            }},
        ]
        out = tmp_path / "samples.tsv"
        n = write_sample_file(records, out,
            sample_field=self.SAMPLE_FIELD,
            reads1_field=self.READS1_FIELD,
            reads2_field=self.READS2_FIELD,
        )
        assert n == 0

    def test_creates_parent_directories(self, tmp_path: Path):
        out = tmp_path / "deep" / "nested" / "samples.tsv"
        write_sample_file(
            ENTRY_RECORDS, out,
            sample_field=self.SAMPLE_FIELD,
            reads1_field=self.READS1_FIELD,
            reads2_field=self.READS2_FIELD,
        )
        assert out.exists()

    def test_empty_records_writes_header_only(self, tmp_path: Path):
        out = tmp_path / "empty.tsv"
        n = write_sample_file([], out,
            sample_field=self.SAMPLE_FIELD,
            reads1_field=self.READS1_FIELD,
            reads2_field=self.READS2_FIELD,
        )
        assert n == 0
        lines = out.read_text().splitlines()
        assert lines[0] == "sample\trawreads1\trawreads2"

    def test_list_field_values_are_unwrapped(self, tmp_path: Path):
        """Airtable URL fields may be returned as a single-element list; extract the string."""
        records = [{"id": "recZ", "fields": {
            self.SAMPLE_FIELD: "EHI00099",
            self.READS1_FIELD: ["https://example.com/EHI00099_1.fq.gz"],
            self.READS2_FIELD: ["https://example.com/EHI00099_2.fq.gz"],
        }}]
        out = tmp_path / "samples.tsv"
        write_sample_file(records, out,
            sample_field=self.SAMPLE_FIELD,
            reads1_field=self.READS1_FIELD,
            reads2_field=self.READS2_FIELD,
        )
        rows = self._read_tsv(out)
        assert rows[0]["rawreads1"] == "https://example.com/EHI00099_1.fq.gz"
        assert rows[0]["rawreads2"] == "https://example.com/EHI00099_2.fq.gz"

    def test_tab_delimiter(self, tmp_path: Path):
        out = tmp_path / "samples.tsv"
        write_sample_file(
            ENTRY_RECORDS, out,
            sample_field=self.SAMPLE_FIELD,
            reads1_field=self.READS1_FIELD,
            reads2_field=self.READS2_FIELD,
        )
        first_line = out.read_text().splitlines()[0]
        assert "\t" in first_line
        assert "," not in first_line


# ---------------------------------------------------------------------------
# write_bins_file
# ---------------------------------------------------------------------------

class TestWriteBinsFile:
    BINS_FIELD = "fldBINS"

    _RECORDS = [
        {"id": "rec1", "fields": {"fldBINS": "/data/bins/MAG001.fa"}},
        {"id": "rec2", "fields": {"fldBINS": "/data/bins/MAG002.fa"}},
        {"id": "rec3", "fields": {"fldBINS": ""}},  # empty — should be skipped
    ]

    def test_writes_one_path_per_line(self, tmp_path: Path):
        out = tmp_path / "bins.txt"
        n = write_bins_file(self._RECORDS, out, bins_field=self.BINS_FIELD)
        assert n == 2
        lines = out.read_text().splitlines()
        assert lines == ["/data/bins/MAG001.fa", "/data/bins/MAG002.fa"]

    def test_empty_path_skipped(self, tmp_path: Path):
        out = tmp_path / "bins.txt"
        n = write_bins_file(self._RECORDS, out, bins_field=self.BINS_FIELD)
        assert n == 2

    def test_empty_records(self, tmp_path: Path):
        out = tmp_path / "bins.txt"
        n = write_bins_file([], out, bins_field=self.BINS_FIELD)
        assert n == 0
        assert out.read_text() == ""

    def test_creates_parent_directories(self, tmp_path: Path):
        out = tmp_path / "nested" / "dir" / "bins.txt"
        write_bins_file(self._RECORDS, out, bins_field=self.BINS_FIELD)
        assert out.exists()
