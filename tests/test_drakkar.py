"""Tests for ehio.drakkar — drakkar input file writers."""

from __future__ import annotations

import csv
import os
import pytest
from pathlib import Path

from ehio.drakkar import (
    check_assembly_type,
    drakkar_versions_used,
    find_failure_report,
    find_run_metadata,
    format_drakkar_versions,
    group_samples_by_assembly,
    read_drakkar_version,
    normalise_assembly_type,
    verify_input_files,
    write_bins_file,
    write_sample_file,
)
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

    def test_no_reference_column(self, tmp_path: Path):
        """Reference genome is passed as a drakkar CLI flag, not a TSV column."""
        out = tmp_path / "samples.tsv"
        write_sample_file(
            ENTRY_RECORDS, out,
            sample_field=self.SAMPLE_FIELD,
            reads1_field=self.READS1_FIELD,
            reads2_field=self.READS2_FIELD,
        )
        rows = self._read_tsv(out)
        assert "reference" not in rows[0]

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

    def test_assembly_column_written_when_field_provided(self, tmp_path: Path):
        ASSEMBLY_FIELD = "fldASSEMBLY"
        records = [
            {"id": "rec1", "fields": {
                self.SAMPLE_FIELD: "EHI00001",
                self.READS1_FIELD: "https://example.com/1.fq.gz",
                self.READS2_FIELD: "https://example.com/2.fq.gz",
                ASSEMBLY_FIELD: "CA001",
            }},
            {"id": "rec2", "fields": {
                self.SAMPLE_FIELD: "EHI00002",
                self.READS1_FIELD: "https://example.com/3.fq.gz",
                self.READS2_FIELD: "https://example.com/4.fq.gz",
                ASSEMBLY_FIELD: "CA001",
            }},
        ]
        out = tmp_path / "samples.tsv"
        n = write_sample_file(records, out,
            sample_field=self.SAMPLE_FIELD,
            reads1_field=self.READS1_FIELD,
            reads2_field=self.READS2_FIELD,
            assembly_field=ASSEMBLY_FIELD,
        )
        assert n == 2
        rows = self._read_tsv(out)
        assert rows[0]["assembly"] == "CA001"
        assert rows[1]["assembly"] == "CA001"
        # column order: sample, assembly, rawreads1, rawreads2
        assert list(rows[0].keys()) == ["sample", "assembly", "rawreads1", "rawreads2"]

    def test_no_assembly_column_without_field(self, tmp_path: Path):
        out = tmp_path / "samples.tsv"
        write_sample_file(
            ENTRY_RECORDS, out,
            sample_field=self.SAMPLE_FIELD,
            reads1_field=self.READS1_FIELD,
            reads2_field=self.READS2_FIELD,
        )
        rows = self._read_tsv(out)
        assert "assembly" not in rows[0]

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
# assembly type (EHI_ASB_BATCH_TYPE)
# ---------------------------------------------------------------------------

SAMPLE_FIELD   = "fldEHINUMBER"
ASSEMBLY_FIELD = "fldASSEMBLY"


def _entries(*pairs: tuple[str, str]) -> list[dict]:
    """Build binning entry records from (sample, assembly code) pairs."""
    return [
        {"id": f"rec{i}", "fields": {SAMPLE_FIELD: sample, ASSEMBLY_FIELD: assembly}}
        for i, (sample, assembly) in enumerate(pairs)
    ]


INDIVIDUAL_ENTRIES = _entries(("EHI00001", "EHA00001"), ("EHI00002", "EHA00002"))
COASSEMBLY_ENTRIES = _entries(("EHI00001", "EHA00001"), ("EHI00002", "EHA00001"))


class TestNormaliseAssemblyType:
    @pytest.mark.parametrize("value,expected", [
        ("Individual",    "individual"),
        ("Coassembly",    "coassembly"),
        ("Co-assembly",   "coassembly"),
        ("co assembly",   "coassembly"),
        ("Multicoverage", "multicoverage"),
        ("multi-coverage", "multicoverage"),
        (["Multicoverage"], "multicoverage"),
    ])
    def test_recognised_values(self, value, expected):
        assert normalise_assembly_type(value) == expected

    @pytest.mark.parametrize("value", ["", None, [], "Something else"])
    def test_unset_or_unknown_returns_empty(self, value):
        assert normalise_assembly_type(value) == ""


class TestGroupSamplesByAssembly:
    def test_individual_codes(self):
        groups = group_samples_by_assembly(INDIVIDUAL_ENTRIES, SAMPLE_FIELD, ASSEMBLY_FIELD)
        assert groups == {"EHA00001": ["EHI00001"], "EHA00002": ["EHI00002"]}

    def test_shared_code_groups_samples(self):
        groups = group_samples_by_assembly(COASSEMBLY_ENTRIES, SAMPLE_FIELD, ASSEMBLY_FIELD)
        assert groups == {"EHA00001": ["EHI00001", "EHI00002"]}

    def test_missing_assembly_code_falls_back_to_sample(self):
        records = [{"id": "rec0", "fields": {SAMPLE_FIELD: "EHI00001"}}]
        groups = group_samples_by_assembly(records, SAMPLE_FIELD, ASSEMBLY_FIELD)
        assert groups == {"EHI00001": ["EHI00001"]}

    def test_entry_without_sample_is_skipped(self):
        records = [{"id": "rec0", "fields": {ASSEMBLY_FIELD: "EHA00001"}}]
        assert group_samples_by_assembly(records, SAMPLE_FIELD, ASSEMBLY_FIELD) == {}


class TestCheckAssemblyType:
    def _check(self, records, assembly_type):
        return check_assembly_type(records, assembly_type, SAMPLE_FIELD, ASSEMBLY_FIELD)

    def test_multicoverage_on_individual_codes_is_accepted(self):
        assert self._check(INDIVIDUAL_ENTRIES, "multicoverage") == ("", "")

    def test_multicoverage_on_coassembly_is_an_error(self):
        error, warning = self._check(COASSEMBLY_ENTRIES, "multicoverage")
        assert "Multicoverage" in error
        assert "EHA00001" in error
        assert "EHI00001" in error and "EHI00002" in error
        assert warning == ""

    def test_individual_and_coassembly_are_accepted(self):
        assert self._check(INDIVIDUAL_ENTRIES, "individual") == ("", "")
        assert self._check(COASSEMBLY_ENTRIES, "coassembly") == ("", "")

    def test_individual_with_shared_codes_only_warns(self):
        error, warning = self._check(COASSEMBLY_ENTRIES, "individual")
        assert error == ""
        assert "co-assembled" in warning

    def test_coassembly_without_shared_codes_only_warns(self):
        error, warning = self._check(INDIVIDUAL_ENTRIES, "coassembly")
        assert error == ""
        assert "individual" in warning

    def test_unset_type_never_complains(self):
        assert self._check(COASSEMBLY_ENTRIES, "") == ("", "")
        assert self._check(INDIVIDUAL_ENTRIES, "") == ("", "")


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


# ---------------------------------------------------------------------------
# verify_input_files
# ---------------------------------------------------------------------------

class TestVerifyInputFiles:
    SAMPLE_FIELD = "fldNGN3g6Lvqo4ySR"
    READS1_FIELD = "fldSsNtgHYgxRaYxN"
    READS2_FIELD = "fldMmJiQLVVoJOAjF"

    def _records(self, r1: str, r2: str) -> list[dict]:
        return [{"id": "recX", "fields": {
            self.SAMPLE_FIELD: "EHI00001",
            self.READS1_FIELD: r1,
            self.READS2_FIELD: r2,
        }}]

    def test_existing_files_return_empty(self, tmp_path: Path):
        r1 = tmp_path / "EHI00001_1.fq.gz"
        r2 = tmp_path / "EHI00001_2.fq.gz"
        r1.write_text("x")
        r2.write_text("x")
        missing = verify_input_files(
            self._records(str(r1), str(r2)),
            self.SAMPLE_FIELD, [self.READS1_FIELD, self.READS2_FIELD],
        )
        assert missing == []

    def test_missing_local_file_reported(self, tmp_path: Path):
        r1 = str(tmp_path / "missing_1.fq.gz")
        r2 = str(tmp_path / "missing_2.fq.gz")
        missing = verify_input_files(
            self._records(r1, r2),
            self.SAMPLE_FIELD, [self.READS1_FIELD, self.READS2_FIELD],
        )
        assert len(missing) == 2
        paths = [p for _, p in missing]
        assert r1 in paths
        assert r2 in paths

    def test_url_paths_are_skipped(self, tmp_path: Path):
        records = self._records(
            "https://example.com/EHI00001_1.fq.gz",
            "https://example.com/EHI00001_2.fq.gz",
        )
        missing = verify_input_files(records, self.SAMPLE_FIELD,
                                     [self.READS1_FIELD, self.READS2_FIELD])
        assert missing == []

    def test_empty_path_is_skipped(self, tmp_path: Path):
        records = self._records("", "")
        missing = verify_input_files(records, self.SAMPLE_FIELD,
                                     [self.READS1_FIELD, self.READS2_FIELD])
        assert missing == []

    def test_one_missing_one_present(self, tmp_path: Path):
        r1 = tmp_path / "present_1.fq.gz"
        r1.write_text("x")
        r2 = str(tmp_path / "absent_2.fq.gz")
        missing = verify_input_files(
            self._records(str(r1), r2),
            self.SAMPLE_FIELD, [self.READS1_FIELD, self.READS2_FIELD],
        )
        assert len(missing) == 1
        assert missing[0][1] == r2


# ---------------------------------------------------------------------------
# find_failure_report
# ---------------------------------------------------------------------------

class TestFindFailureReport:
    """drakkar 2.5.0 writes logging/drakkar_<run id>.failures.tsv; before it,
    the table sat in the output root as drakkar_<run id>_failures.tsv."""

    def _report(self, directory: Path, run_id: str, mtime: float | None = None,
                legacy: bool = False) -> Path:
        if legacy:
            path = directory / f"drakkar_{run_id}_failures.tsv"
        else:
            path = directory / "logging" / f"drakkar_{run_id}.failures.tsv"
            path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("rule\tsample\n")
        if mtime is not None:
            os.utime(path, (mtime, mtime))
        return path

    def test_none_when_directory_is_empty(self, tmp_path: Path):
        assert find_failure_report(tmp_path) is None

    def test_none_when_directory_does_not_exist(self, tmp_path: Path):
        assert find_failure_report(tmp_path / "absent") is None

    def test_finds_single_report(self, tmp_path: Path):
        report = self._report(tmp_path, "20260814-101500")
        assert find_failure_report(tmp_path) == report

    def test_finds_report_of_the_legacy_layout(self, tmp_path: Path):
        report = self._report(tmp_path, "20260814-101500", legacy=True)
        assert find_failure_report(tmp_path) == report

    def test_other_drakkar_files_are_ignored(self, tmp_path: Path):
        (tmp_path / "logging").mkdir()
        (tmp_path / "logging" / "drakkar_20260814-101500.yaml").write_text("x")
        (tmp_path / "logging" / "drakkar_20260814-101500.snakemake.log").write_text("x")
        (tmp_path / "drakkar_20260814-101500.yaml").write_text("x")
        (tmp_path / "drakkar_20260814-101500_resources.yaml").write_text("x")
        assert find_failure_report(tmp_path) is None

    def test_returns_newest_of_several_reports(self, tmp_path: Path):
        old = self._report(tmp_path, "20260814-101500", mtime=1_000_000)
        new = self._report(tmp_path, "20260814-113000", mtime=2_000_000)
        assert find_failure_report(tmp_path) == new
        assert old.exists()

    def test_newest_report_wins_across_both_layouts(self, tmp_path: Path):
        self._report(tmp_path, "20260814-101500", mtime=1_000_000, legacy=True)
        resumed = self._report(tmp_path, "20260901-113000", mtime=2_000_000)
        assert find_failure_report(tmp_path) == resumed

    def test_since_filters_out_older_reports(self, tmp_path: Path):
        self._report(tmp_path, "20260814-101500", mtime=1_000_000)
        assert find_failure_report(tmp_path, since=1_500_000) is None

    def test_since_keeps_reports_of_the_current_run(self, tmp_path: Path):
        self._report(tmp_path, "20260814-101500", mtime=1_000_000)
        fresh = self._report(tmp_path, "20260814-113000", mtime=2_000_000)
        assert find_failure_report(tmp_path, since=1_500_000) == fresh


# ---------------------------------------------------------------------------
# run metadata / drakkar version
# ---------------------------------------------------------------------------

def _write_metadata(output_dir: Path, run_id: str, version: str,
                    status: str = "success", legacy: bool = False) -> Path:
    directory = output_dir if legacy else output_dir / "logging"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"drakkar_{run_id}.yaml"
    path.write_text(
        f"run_id: '{run_id}'\n"
        f"drakkar_version: {version}\n"
        "command: preprocessing\n"
        f"status: {status}\n"
    )
    return path


class TestFindRunMetadata:
    def test_empty_directory(self, tmp_path: Path):
        assert find_run_metadata(tmp_path) == []

    def test_finds_metadata_in_the_logging_directory(self, tmp_path: Path):
        path = _write_metadata(tmp_path, "20260901-101500", "2.5.0")
        assert find_run_metadata(tmp_path) == [path]

    def test_finds_metadata_of_the_legacy_layout(self, tmp_path: Path):
        path = _write_metadata(tmp_path, "20260814-101500", "2.4.4", legacy=True)
        assert find_run_metadata(tmp_path) == [path]

    def test_runs_come_back_oldest_first(self, tmp_path: Path):
        first  = _write_metadata(tmp_path, "20260814-101500", "2.4.4", legacy=True)
        second = _write_metadata(tmp_path, "20260901-113000", "2.5.0")
        assert find_run_metadata(tmp_path) == [first, second]

    def test_benchmark_rollup_is_not_run_metadata(self, tmp_path: Path):
        (tmp_path / "drakkar_20260814-101500_resources.yaml").write_text("x")
        (tmp_path / "logging").mkdir()
        (tmp_path / "logging" / "benchmark").mkdir()
        (tmp_path / "logging" / "benchmark" / "drakkar_20260901-101500.resources.yaml").write_text("x")
        assert find_run_metadata(tmp_path) == []

    def test_a_run_written_in_both_layouts_is_read_once(self, tmp_path: Path):
        _write_metadata(tmp_path, "20260901-101500", "2.4.4", legacy=True)
        current = _write_metadata(tmp_path, "20260901-101500", "2.5.0")
        assert find_run_metadata(tmp_path) == [current]


class TestReadDrakkarVersion:
    def test_reads_the_version(self, tmp_path: Path):
        path = _write_metadata(tmp_path, "20260901-101500", "2.5.0")
        assert read_drakkar_version(path) == "2.5.0"

    def test_reads_a_quoted_version(self, tmp_path: Path):
        path = tmp_path / "drakkar_20260901-101500.yaml"
        path.write_text("drakkar_version: '2.4.5'\nstatus: success\n")
        assert read_drakkar_version(path) == "2.4.5"

    def test_reads_a_truncated_file(self, tmp_path: Path):
        """A run killed mid-write leaves yaml it cannot parse as a whole."""
        path = tmp_path / "drakkar_20260901-101500.yaml"
        path.write_text("run_id: '20260901-101500'\ndrakkar_version: 2.5.0\narguments: {unclosed\n")
        assert read_drakkar_version(path) == "2.5.0"

    def test_missing_file(self, tmp_path: Path):
        assert read_drakkar_version(tmp_path / "absent.yaml") == ""

    def test_file_without_a_version(self, tmp_path: Path):
        path = tmp_path / "drakkar_20260901-101500.yaml"
        path.write_text("status: success\n")
        assert read_drakkar_version(path) == ""


class TestDrakkarVersionsUsed:
    def test_no_metadata(self, tmp_path: Path):
        assert drakkar_versions_used(tmp_path) == []

    def test_single_version(self, tmp_path: Path):
        _write_metadata(tmp_path, "20260901-101500", "2.5.0")
        assert drakkar_versions_used(tmp_path) == ["2.5.0"]

    def test_repeated_runs_of_one_version_are_reported_once(self, tmp_path: Path):
        _write_metadata(tmp_path, "20260901-101500", "2.5.0")
        _write_metadata(tmp_path, "20260901-131500", "2.5.0")
        assert drakkar_versions_used(tmp_path) == ["2.5.0"]

    def test_a_batch_spanning_an_upgrade_reports_both(self, tmp_path: Path):
        _write_metadata(tmp_path, "20260830-101500", "2.4.4", legacy=True)
        _write_metadata(tmp_path, "20260831-113000", "2.4.5", legacy=True)
        assert drakkar_versions_used(tmp_path) == ["2.4.4", "2.4.5"]

    def test_versions_are_ordered_by_run(self, tmp_path: Path):
        _write_metadata(tmp_path, "20260901-131500", "2.5.0")
        _write_metadata(tmp_path, "20260830-101500", "2.4.4", legacy=True)
        assert drakkar_versions_used(tmp_path) == ["2.4.4", "2.5.0"]

    def test_a_failed_run_still_counts(self, tmp_path: Path):
        """A failed run did part of the work the resumed run builds on."""
        _write_metadata(tmp_path, "20260830-101500", "2.4.4", status="failed", legacy=True)
        _write_metadata(tmp_path, "20260901-113000", "2.5.0")
        assert drakkar_versions_used(tmp_path) == ["2.4.4", "2.5.0"]


class TestFormatDrakkarVersions:
    def test_single_version(self):
        assert format_drakkar_versions(["2.5.0"]) == "2.5.0"

    def test_several_versions(self):
        assert format_drakkar_versions(["2.4.4", "2.4.5"]) == "2.4.4/2.4.5"

    def test_no_versions(self):
        assert format_drakkar_versions([]) == ""


class TestReadOnlyRunsAreNotCounted:
    """Older drakkar wrote run metadata for its read-only commands too, in
    whatever directory they were run from."""

    def _metadata(self, output_dir: Path, run_id: str, version: str, command: str) -> Path:
        path = output_dir / f"drakkar_{run_id}.yaml"
        path.write_text(f"drakkar_version: {version}\ncommand: {command}\nstatus: success\n")
        return path

    def test_a_config_run_does_not_count_as_a_version(self, tmp_path: Path):
        self._metadata(tmp_path, "20260830-090000", "2.3.0", "config")
        _write_metadata(tmp_path, "20260901-101500", "2.5.0")
        assert drakkar_versions_used(tmp_path) == ["2.5.0"]

    def test_every_workflow_command_counts(self, tmp_path: Path):
        self._metadata(tmp_path, "20260901-090000", "2.5.0", "profiling")
        self._metadata(tmp_path, "20260901-110000", "2.5.1", "annotating")
        assert drakkar_versions_used(tmp_path) == ["2.5.0", "2.5.1"]

    def test_metadata_without_a_command_still_counts(self, tmp_path: Path):
        path = tmp_path / "drakkar_20260901-101500.yaml"
        path.write_text("drakkar_version: 2.4.0\nstatus: success\n")
        assert drakkar_versions_used(tmp_path) == ["2.4.0"]
