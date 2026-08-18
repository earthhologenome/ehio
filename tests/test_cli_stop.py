"""Tests for the 'ehio stop' and 'ehio jobs' commands."""

from __future__ import annotations

import argparse
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from ehio import cli
from ehio.scanning import STOP_SENTINEL
from ehio.slurm import SlurmJob, SlurmUnavailable

MODULES = ["preprocessing", "binning", "quantifying"]


@pytest.fixture
def batch_dirs(tmp_path: Path):
    """Patch _batch_dirs onto a temporary run/output directory pair."""
    output_dir = tmp_path / "ASB" / "ABB0659"
    run_dir    = tmp_path / "RUN" / "ABB0659"
    run_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)
    out_file = run_dir / "ABB0659.out"
    with patch.object(cli, "_batch_dirs",
                      return_value=(str(output_dir), str(run_dir), str(out_file))):
        yield output_dir, run_dir, out_file


@pytest.fixture
def airtable():
    client = MagicMock()
    client.fetch_batch_record.return_value = {"id": "rec123", "fields": {}}
    with patch("ehio.airtable.AirtableClient", return_value=client), \
         patch.object(cli, "_resolve_token", return_value="tok"), \
         patch.object(cli, "_require_cfg", side_effect=lambda key: f"<{key}>"):
        yield client


def _args(module: str = "binning", keep_jobs: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        module=module, batch="ABB0659", keep_jobs=keep_jobs, airtable_token=None,
    )


# ---------------------------------------------------------------------------
# cmd_stop
# ---------------------------------------------------------------------------

class TestCmdStop:
    @pytest.mark.parametrize("module", MODULES)
    def test_runs_for_every_module(self, module, airtable, batch_dirs):
        # regression: _SET_STATUS_CFG carries five keys, and unpacking four of
        # them raised ValueError before any batch could be stopped
        with patch("subprocess.run", return_value=MagicMock(returncode=0)), \
             patch.object(cli, "_cancel_batch_jobs"):
            assert cli.cmd_stop(_args(module)) == 0

    def test_kills_the_screen_session(self, airtable, batch_dirs):
        with patch("subprocess.run", return_value=MagicMock(returncode=0)) as run, \
             patch.object(cli, "_cancel_batch_jobs"):
            cli.cmd_stop(_args())
        assert run.call_args[0][0] == ["screen", "-S", "ABB0659", "-X", "quit"]

    def test_cancels_jobs_by_default(self, airtable, batch_dirs):
        output_dir, run_dir, out_file = batch_dirs
        with patch("subprocess.run", return_value=MagicMock(returncode=0)), \
             patch.object(cli, "_cancel_batch_jobs") as cancel:
            cli.cmd_stop(_args())
        assert cancel.call_args[0][1:] == (str(output_dir), str(run_dir), str(out_file))

    def test_keep_jobs_skips_cancelling(self, airtable, batch_dirs):
        with patch("subprocess.run", return_value=MagicMock(returncode=0)), \
             patch.object(cli, "_cancel_batch_jobs") as cancel:
            cli.cmd_stop(_args(keep_jobs=True))
        assert not cancel.called

    def test_writes_stop_marker_before_killing_the_session(self, airtable, batch_dirs):
        _, run_dir, _ = batch_dirs
        marker = run_dir / STOP_SENTINEL
        seen = {}

        def _run(*args, **kwargs):
            seen["marker_existed"] = marker.exists()
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=_run), \
             patch.object(cli, "_cancel_batch_jobs"):
            cli.cmd_stop(_args())
        assert seen["marker_existed"] and marker.exists()

    def test_status_written_after_the_jobs_are_cancelled(self, airtable, batch_dirs):
        order = []
        with patch("subprocess.run", return_value=MagicMock(returncode=0)), \
             patch.object(cli, "_cancel_batch_jobs", side_effect=lambda *a: order.append("cancel")):
            airtable.update_records.side_effect = lambda *a, **k: order.append("status")
            cli.cmd_stop(_args())
        assert order == ["cancel", "status"]

    def test_unknown_batch_exits(self, airtable, batch_dirs):
        airtable.fetch_batch_record.return_value = None
        with pytest.raises(SystemExit):
            cli.cmd_stop(_args())


# ---------------------------------------------------------------------------
# _cancel_batch_jobs
# ---------------------------------------------------------------------------

class TestCancelBatchJobs:
    def test_missing_slurm_is_reported_not_raised(self, capsys):
        with patch("ehio.slurm.cancel_batch_jobs", side_effect=SlurmUnavailable("no squeue")):
            cli._cancel_batch_jobs("ABB0659", "/out", "/run", "/run/x.out")
        assert "no squeue" in capsys.readouterr().err

    def test_leftover_jobs_are_warned_about(self, capsys):
        leftover = SlurmJob("42", "RUNNING", "n", "cpuqueue", "1:00", "/out")
        with patch("ehio.slurm.cancel_batch_jobs", return_value=(["41"], [leftover], [])):
            cli._cancel_batch_jobs("ABB0659", "/out", "/run", "/run/x.out")
        err = capsys.readouterr().err
        assert "Cancelled 1 Slurm job(s): 41" in err
        assert "still queued" in err and "42" in err


# ---------------------------------------------------------------------------
# cmd_jobs
# ---------------------------------------------------------------------------

class TestCmdJobs:
    def _args(self, module: str = "binning") -> argparse.Namespace:
        return argparse.Namespace(module=module, batch="ABB0659")

    def test_lists_jobs(self, batch_dirs, capsys):
        jobs = [
            SlurmJob("12345", "RUNNING", "uuid-a", "cpuqueue", "1:02:03", "/out"),
            SlurmJob("12346", "PENDING", "uuid-a", "cpuqueue", "0:00", "/out"),
        ]
        with patch("ehio.slurm.find_batch_jobs", return_value=jobs):
            assert cli.cmd_jobs(self._args()) == 0
        out = capsys.readouterr()
        assert "JOBID" in out.out and "12345" in out.out and "12346" in out.out
        assert "1 pending, 1 running" in out.err

    def test_no_jobs(self, batch_dirs, capsys):
        with patch("ehio.slurm.find_batch_jobs", return_value=[]):
            assert cli.cmd_jobs(self._args()) == 0
        assert "No queued or running Slurm jobs" in capsys.readouterr().err

    def test_missing_slurm_exits(self, batch_dirs):
        with patch("ehio.slurm.find_batch_jobs", side_effect=SlurmUnavailable("no squeue")):
            with pytest.raises(SystemExit):
                cli.cmd_jobs(self._args())
