"""Tests for ehio.drakkar — drakkar input file writers."""

from __future__ import annotations

import pytest
from pathlib import Path

from ehio.drakkar import (
    check_assembly_type,
    drakkar_versions_used,
    find_run_metadata,
    format_drakkar_versions,
    group_samples_by_assembly,
    read_drakkar_version,
    DMB_TASKS,
    normalise_assembly_type,
    normalise_dmb_tasks,
    normalise_profiling_type,
    verify_input_files,
    write_bins_file,
    write_sample_file,
)


# ---------------------------------------------------------------------------
# assembly type (EHI_ASB_BATCH_TYPE)
# ---------------------------------------------------------------------------

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


class TestNormaliseProfilingType:
    """ehi-core's batch types are 'genome' and 'pangenome'; drakkar's workflow
    only knows 'genomes' and 'pangenomes', and runs nothing given 'genome'."""

    @pytest.mark.parametrize("value,expected", [
        ("genome",              "genomes"),
        ("Genome",              "genomes"),
        ("genomes",             "genomes"),
        ("pangenome",           "pangenomes"),
        ("Pangenomes",          "pangenomes"),
        ("genome, pangenome",   "genomes,pangenomes"),
        (["genome"],            "genomes"),
        ("genome,genomes",      "genomes"),
    ])
    def test_recognised_values(self, value, expected):
        assert normalise_profiling_type(value) == expected

    @pytest.mark.parametrize("value", ["", None, [], "Something else"])
    def test_unset_or_unknown_leaves_the_drakkar_default(self, value):
        assert normalise_profiling_type(value) == ""






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


class TestNormaliseDmbTasks:
    """ehi-core's Tasks: what a DMB batch does, in the order the steps run."""

    @pytest.mark.parametrize("value,expected", [
        (["Profile"],                         ("profile",)),
        (["Function", "Taxonomy"],            ("taxonomy", "function")),
        ("Taxonomy, function",                ("taxonomy", "function")),
        (["Profiling", "Functional"],         ("profile", "function")),
        (["Dereplicate", "Profile", "Taxonomy", "Function"], DMB_TASKS),
        (["Taxonomy", "Dereplication"],       ("dereplicate", "taxonomy")),
        ("derep, taxonomy",                   ("dereplicate", "taxonomy")),
    ])
    def test_recognised_values(self, value, expected):
        assert normalise_dmb_tasks(value) == expected

    @pytest.mark.parametrize("value", [None, "", [], ["Something else"]])
    def test_a_batch_naming_no_task_does_them_all(self, value):
        # Every DMB batch did all four before Tasks existed.
        assert normalise_dmb_tasks(value) == DMB_TASKS


# File writers also receive plain entry rows from a batch held only in ehi-core.
CORE_ENTRIES = [
    {"code": "PR00001", "raw_forward_url": "https://erda/raw_1.fq.gz",
     "raw_reverse_url": "https://erda/raw_2.fq.gz"},
    {"code": "PR00002", "raw_forward_url": "https://erda/raw_3.fq.gz",
     "raw_reverse_url": "https://erda/raw_4.fq.gz"},
]


class TestCoreSampleFile:
    def test_writes_every_entry_with_the_drakkar_columns(self, tmp_path: Path):
        output = tmp_path / "nested" / "samples.tsv"
        assert write_sample_file(CORE_ENTRIES, output, "code", "raw_forward_url", "raw_reverse_url") == 2
        assert output.read_text().splitlines() == [
            "sample\trawreads1\trawreads2",
            "PR00001\thttps://erda/raw_1.fq.gz\thttps://erda/raw_2.fq.gz",
            "PR00002\thttps://erda/raw_3.fq.gz\thttps://erda/raw_4.fq.gz",
        ]

    def test_empty_entries_write_only_the_header(self, tmp_path: Path):
        output = tmp_path / "empty.tsv"
        assert write_sample_file([], output, "code", "raw_forward_url", "raw_reverse_url") == 0
        assert output.read_text() == "sample\trawreads1\trawreads2\n"

    def test_entry_without_a_code_or_forward_reads_is_skipped(self, tmp_path: Path):
        entries = [{"code": "", "raw_forward_url": "https://erda/x.fq.gz"},
                   {"code": "PR00003", "raw_forward_url": ""}, CORE_ENTRIES[0]]
        output = tmp_path / "samples.tsv"
        assert write_sample_file(entries, output, "code", "raw_forward_url", "raw_reverse_url") == 1
        assert len(output.read_text().splitlines()) == 2

    def test_binning_entries_include_the_assembly_column(self, tmp_path: Path):
        entries = [
            {"hologenome_code": "EHI00001", "assembly_code": "EHA00001",
             "forward_url": "https://erda/1.fq.gz", "reverse_url": "https://erda/2.fq.gz"},
            {"hologenome_code": "EHI00002", "assembly_code": "EHA00001",
             "forward_url": "https://erda/3.fq.gz", "reverse_url": "https://erda/4.fq.gz"},
        ]
        output = tmp_path / "binning.tsv"
        assert write_sample_file(entries, output, "hologenome_code", "forward_url",
                                 "reverse_url", "assembly_code") == 2
        assert output.read_text().splitlines()[0] == "sample\tassembly\trawreads1\trawreads2"
        assert [line.split("\t")[1] for line in output.read_text().splitlines()[1:]] == [
            "EHA00001", "EHA00001",
        ]


class TestCoreAssemblyGrouping:
    INDIVIDUAL = [{"hologenome_code": "EHI00001", "assembly_code": "EHA00001"},
                  {"hologenome_code": "EHI00002", "assembly_code": "EHA00002"}]
    COASSEMBLY = [{"hologenome_code": "EHI00001", "assembly_code": "EHA00001"},
                  {"hologenome_code": "EHI00002", "assembly_code": "EHA00001"}]

    def _groups(self, entries):
        return group_samples_by_assembly(entries, "hologenome_code", "assembly_code")

    def _check(self, entries, kind):
        return check_assembly_type(entries, kind, "hologenome_code", "assembly_code")

    def test_each_sample_can_have_its_own_assembly(self):
        assert self._groups(self.INDIVIDUAL) == {
            "EHA00001": ["EHI00001"], "EHA00002": ["EHI00002"]}

    def test_samples_can_share_a_coassembly(self):
        assert self._groups(self.COASSEMBLY) == {"EHA00001": ["EHI00001", "EHI00002"]}

    def test_missing_assembly_falls_back_to_sample_and_missing_sample_is_skipped(self):
        assert self._groups([{"hologenome_code": "EHI00001"}, {"assembly_code": "EHA00002"}]) == {
            "EHI00001": ["EHI00001"]}

    def test_multicoverage_rejects_a_shared_assembly(self):
        error, warning = self._check(self.COASSEMBLY, "multicoverage")
        assert "Multicoverage" in error and "EHA00001" in error
        assert warning == ""

    def test_individual_and_coassembly_mismatch_only_warn(self):
        assert self._check(self.COASSEMBLY, "individual")[0] == ""
        assert "co-assembled" in self._check(self.COASSEMBLY, "individual")[1]
        assert "individual" in self._check(self.INDIVIDUAL, "coassembly")[1]


class TestCoreBinsFile:
    def test_writes_available_mag_paths(self, tmp_path: Path):
        output = tmp_path / "nested" / "bins.txt"
        mags = [{"fasta_url": "/data/MAG001.fa"}, {"fasta_url": ""},
                {"fasta_url": "/data/MAG002.fa"}]
        assert write_bins_file(mags, output, bins_field="fasta_url") == 2
        assert output.read_text().splitlines() == ["/data/MAG001.fa", "/data/MAG002.fa"]

    def test_empty_mags_write_an_empty_file(self, tmp_path: Path):
        output = tmp_path / "bins.txt"
        assert write_bins_file([], output, bins_field="fasta_url") == 0
        assert output.read_text() == ""


class TestCoreInputVerification:
    def _verify(self, entries):
        return verify_input_files(entries, "code", ["raw_forward_url", "raw_reverse_url"])

    def test_existing_local_files_are_accepted(self, tmp_path: Path):
        forward, reverse = tmp_path / "1.fq.gz", tmp_path / "2.fq.gz"
        forward.write_text("x")
        reverse.write_text("x")
        assert self._verify([{"code": "PR00001", "raw_forward_url": str(forward),
                              "raw_reverse_url": str(reverse)}]) == []

    def test_missing_local_files_are_reported(self, tmp_path: Path):
        forward, reverse = tmp_path / "missing_1.fq.gz", tmp_path / "missing_2.fq.gz"
        missing = self._verify([{"code": "PR00001", "raw_forward_url": str(forward),
                                 "raw_reverse_url": str(reverse)}])
        assert {path for _, path in missing} == {str(forward), str(reverse)}

    def test_remote_urls_and_empty_paths_are_not_checked_locally(self):
        assert self._verify(CORE_ENTRIES + [{"code": "PR00003", "raw_forward_url": ""}]) == []
