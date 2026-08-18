"""Tests for ehio.slurm — Slurm job discovery and cancellation."""

from __future__ import annotations

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from ehio.slurm import (
    SlurmJob,
    SlurmUnavailable,
    cancel_batch_jobs,
    cancel_jobs,
    find_batch_jobs,
    queued_jobs,
    read_run_ids,
    slurm_available,
)

OUT_DIR = "/projects/ehi/data/PPR/PPR001"
RUN_DIR = "/projects/ehi/data/RUN/PPR001"

UUID_A = "3f2b1c8e-4d5a-4e6f-8a9b-0c1d2e3f4a5b"
UUID_B = "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d"


def _squeue(stdout: str, returncode: int = 0) -> MagicMock:
    return MagicMock(returncode=returncode, stdout=stdout, stderr="")


# ---------------------------------------------------------------------------
# read_run_ids
# ---------------------------------------------------------------------------

class TestReadRunIds:
    def test_reads_every_run_id(self, tmp_path: Path):
        out = tmp_path / "PPR001.out"
        out.write_text(
            f"Building DAG of jobs...\nSLURM run ID: {UUID_A}\n"
            f"some other output\nSLURM run ID: {UUID_B}\n"
        )
        assert read_run_ids(out) == {UUID_A, UUID_B}

    def test_ids_are_lowercased(self, tmp_path: Path):
        out = tmp_path / "PPR001.out"
        out.write_text(f"SLURM run ID: {UUID_A.upper()}\n")
        assert read_run_ids(out) == {UUID_A}

    def test_repeated_id_collapses(self, tmp_path: Path):
        out = tmp_path / "PPR001.out"
        out.write_text(f"SLURM run ID: {UUID_A}\nSLURM run ID: {UUID_A}\n")
        assert read_run_ids(out) == {UUID_A}

    def test_missing_file_is_empty(self, tmp_path: Path):
        assert read_run_ids(tmp_path / "nope.out") == set()

    def test_no_run_id_in_file(self, tmp_path: Path):
        out = tmp_path / "PPR001.out"
        out.write_text("nothing to see here\n")
        assert read_run_ids(out) == set()


# ---------------------------------------------------------------------------
# queued_jobs
# ---------------------------------------------------------------------------

class TestQueuedJobs:
    def test_parses_squeue_lines(self):
        line = f"12345|RUNNING|{UUID_A}|cpuqueue|1:02:03|{OUT_DIR}"
        with patch("ehio.slurm.slurm_available", return_value=True), \
             patch("ehio.slurm.subprocess.run", return_value=_squeue(line + "\n")):
            jobs = queued_jobs()
        assert jobs == [SlurmJob("12345", "RUNNING", UUID_A, "cpuqueue", "1:02:03", OUT_DIR)]

    def test_skips_blank_and_short_lines(self):
        stdout = f"\n12345|RUNNING|{UUID_A}|cpuqueue|0:10|{OUT_DIR}\nbroken|line\n"
        with patch("ehio.slurm.slurm_available", return_value=True), \
             patch("ehio.slurm.subprocess.run", return_value=_squeue(stdout)):
            assert [j.job_id for j in queued_jobs()] == ["12345"]

    def test_state_is_uppercased(self):
        line = f"12345|pending|{UUID_A}|cpuqueue|0:00|{OUT_DIR}"
        with patch("ehio.slurm.slurm_available", return_value=True), \
             patch("ehio.slurm.subprocess.run", return_value=_squeue(line)):
            assert queued_jobs()[0].state == "PENDING"

    def test_raises_when_slurm_missing(self):
        with patch("ehio.slurm.slurm_available", return_value=False):
            with pytest.raises(SlurmUnavailable):
                queued_jobs()

    def test_raises_when_squeue_fails(self):
        with patch("ehio.slurm.slurm_available", return_value=True), \
             patch("ehio.slurm.subprocess.run", return_value=_squeue("", returncode=1)):
            with pytest.raises(SlurmUnavailable):
                queued_jobs()


# ---------------------------------------------------------------------------
# find_batch_jobs
# ---------------------------------------------------------------------------

class TestFindBatchJobs:
    def _find(self, jobs: list[SlurmJob], out_file: str = "") -> list[str]:
        with patch("ehio.slurm.queued_jobs", return_value=jobs):
            return [j.job_id for j in find_batch_jobs(OUT_DIR, RUN_DIR, out_file)]

    def _job(self, job_id: str, workdir: str, name: str = "somename") -> SlurmJob:
        return SlurmJob(job_id, "RUNNING", name, "cpuqueue", "0:10", workdir)

    def test_matches_output_dir(self):
        assert self._find([self._job("1", OUT_DIR)]) == ["1"]

    def test_matches_subdirectory_of_output_dir(self):
        assert self._find([self._job("1", f"{OUT_DIR}/preprocessing")]) == ["1"]

    def test_matches_run_dir(self):
        assert self._find([self._job("1", RUN_DIR)]) == ["1"]

    def test_trailing_slash_still_matches(self):
        assert self._find([self._job("1", OUT_DIR + "/")]) == ["1"]

    def test_ignores_other_batches(self):
        other = "/projects/ehi/data/PPR/PPR002"
        assert self._find([self._job("1", other)]) == []

    def test_sibling_with_shared_prefix_is_not_matched(self):
        # PPR0010 must not be taken for a subdirectory of PPR001
        assert self._find([self._job("1", OUT_DIR + "0")]) == []

    def test_matches_by_run_id_job_name(self, tmp_path: Path):
        out = tmp_path / "PPR001.out"
        out.write_text(f"SLURM run ID: {UUID_A}\n")
        job = self._job("1", "/somewhere/else", name=UUID_A)
        assert self._find([job], out_file=str(out)) == ["1"]

    def test_unrelated_name_without_matching_dir(self, tmp_path: Path):
        out = tmp_path / "PPR001.out"
        out.write_text(f"SLURM run ID: {UUID_A}\n")
        job = self._job("1", "/somewhere/else", name=UUID_B)
        assert self._find([job], out_file=str(out)) == []

    def test_empty_run_dir_does_not_match_everything(self):
        with patch("ehio.slurm.queued_jobs", return_value=[self._job("1", "/elsewhere")]):
            assert find_batch_jobs(OUT_DIR, "", "") == []


# ---------------------------------------------------------------------------
# cancel_jobs / cancel_batch_jobs
# ---------------------------------------------------------------------------

class TestCancelJobs:
    def test_no_ids_does_not_call_scancel(self):
        with patch("ehio.slurm.subprocess.run") as run:
            ok, msg = cancel_jobs([])
        assert ok and not run.called

    def test_calls_scancel_with_all_ids(self):
        with patch("ehio.slurm.subprocess.run",
                   return_value=MagicMock(returncode=0, stdout="", stderr="")) as run:
            ok, _ = cancel_jobs(["1", "2"])
        assert ok
        assert run.call_args[0][0] == ["scancel", "1", "2"]

    def test_reports_failure_message(self):
        with patch("ehio.slurm.subprocess.run",
                   return_value=MagicMock(returncode=1, stdout="", stderr="denied")):
            ok, msg = cancel_jobs(["1"])
        assert not ok and msg == "denied"


class TestCancelBatchJobs:
    def _job(self, job_id: str) -> SlurmJob:
        return SlurmJob(job_id, "RUNNING", "n", "cpuqueue", "0:10", OUT_DIR)

    def test_cancels_and_reports_clear_queue(self):
        with patch("ehio.slurm.find_batch_jobs", side_effect=[[self._job("1")], []]), \
             patch("ehio.slurm.cancel_jobs", return_value=(True, "")) as cancel, \
             patch("ehio.slurm.time.sleep"):
            cancelled, remaining, errors = cancel_batch_jobs(OUT_DIR, RUN_DIR, "")
        assert cancelled == ["1"] and remaining == []
        assert cancel.call_args[0][0] == ["1"]

    def test_nothing_queued_calls_no_scancel(self):
        with patch("ehio.slurm.find_batch_jobs", return_value=[]), \
             patch("ehio.slurm.cancel_jobs") as cancel:
            cancelled, remaining, errors = cancel_batch_jobs(OUT_DIR, RUN_DIR, "")
        assert cancelled == [] and remaining == [] and not cancel.called

    def test_resubmitted_jobs_are_cancelled_on_the_next_pass(self):
        # a workflow driver still alive resubmits under the cancellation
        with patch("ehio.slurm.find_batch_jobs",
                   side_effect=[[self._job("1")], [self._job("2")], []]), \
             patch("ehio.slurm.cancel_jobs", return_value=(True, "")) as cancel, \
             patch("ehio.slurm.time.sleep"):
            cancelled, remaining, errors = cancel_batch_jobs(OUT_DIR, RUN_DIR, "")
        assert cancelled == ["1", "2"] and remaining == []
        assert [c[0][0] for c in cancel.call_args_list] == [["1"], ["2"]]

    def test_jobs_left_after_all_attempts_are_returned(self):
        with patch("ehio.slurm.find_batch_jobs", return_value=[self._job("1"), self._job("2")]), \
             patch("ehio.slurm.cancel_jobs", return_value=(True, "")), \
             patch("ehio.slurm.time.sleep"):
            cancelled, remaining, errors = cancel_batch_jobs(OUT_DIR, RUN_DIR, "")
        assert cancelled == ["1", "2"]
        # already-cancelled ids are not reported as leftovers
        assert remaining == []

    def test_scancel_failure_is_reported_and_job_stays_listed(self):
        with patch("ehio.slurm.find_batch_jobs", return_value=[self._job("1")]), \
             patch("ehio.slurm.cancel_jobs", return_value=(False, "Access denied")), \
             patch("ehio.slurm.time.sleep"):
            cancelled, remaining, errors = cancel_batch_jobs(OUT_DIR, RUN_DIR, "")
        assert cancelled == []
        assert [j.job_id for j in remaining] == ["1"]
        assert errors == ["Access denied"]

    def test_new_job_after_last_attempt_is_reported(self):
        with patch("ehio.slurm.find_batch_jobs",
                   side_effect=[[self._job("1")], [self._job("1")], [self._job("1")], [self._job("9")]]), \
             patch("ehio.slurm.cancel_jobs", return_value=(True, "")), \
             patch("ehio.slurm.time.sleep"):
            cancelled, remaining, errors = cancel_batch_jobs(OUT_DIR, RUN_DIR, "")
        assert cancelled == ["1"]
        assert [j.job_id for j in remaining] == ["9"]


# ---------------------------------------------------------------------------
# slurm_available
# ---------------------------------------------------------------------------

class TestSlurmAvailable:
    def test_true_when_both_present(self):
        with patch("ehio.slurm.shutil.which", return_value="/usr/bin/x"):
            assert slurm_available()

    def test_false_when_missing(self):
        with patch("ehio.slurm.shutil.which", return_value=None):
            assert not slurm_available()
