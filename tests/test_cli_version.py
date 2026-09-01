"""Tests for the drakkar version recorded on a finished batch."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

from ehio import cli


def _metadata(output_dir: Path, run_id: str, version: str, legacy: bool = False) -> Path:
    """Write the run metadata drakkar leaves behind for one run."""
    directory = output_dir if legacy else output_dir / "logging"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"drakkar_{run_id}.yaml"
    path.write_text(
        f"run_id: '{run_id}'\n"
        f"drakkar_version: {version}\n"
        "command: cataloging\n"
        "status: success\n"
    )
    return path


def _installed(version: str = "9.9.9"):
    """Patch the 'drakkar --version' fallback."""
    result = MagicMock(stdout=f"drakkar {version}\n", stderr="")
    return patch.object(subprocess, "run", return_value=result)


class TestGetDrakkarVersion:
    def test_reads_the_version_that_produced_the_output(self, tmp_path: Path):
        _metadata(tmp_path, "20260901-101500", "2.5.0")
        with _installed() as run:
            assert cli._get_drakkar_version(tmp_path) == "2.5.0"
        run.assert_not_called()

    def test_reads_the_legacy_layout(self, tmp_path: Path):
        _metadata(tmp_path, "20260830-101500", "2.4.4", legacy=True)
        with _installed():
            assert cli._get_drakkar_version(tmp_path) == "2.4.4"

    def test_reports_every_version_of_a_batch_that_spanned_an_upgrade(self, tmp_path: Path):
        _metadata(tmp_path, "20260830-101500", "2.4.4", legacy=True)
        _metadata(tmp_path, "20260831-143000", "2.4.5", legacy=True)
        with _installed():
            assert cli._get_drakkar_version(tmp_path) == "2.4.4/2.4.5"

    def test_versions_are_reported_oldest_run_first(self, tmp_path: Path):
        _metadata(tmp_path, "20260901-143000", "2.5.0")
        _metadata(tmp_path, "20260830-101500", "2.4.4", legacy=True)
        with _installed():
            assert cli._get_drakkar_version(tmp_path) == "2.4.4/2.5.0"

    def test_several_runs_of_one_version_report_it_once(self, tmp_path: Path):
        _metadata(tmp_path, "20260901-101500", "2.5.0")
        _metadata(tmp_path, "20260901-143000", "2.5.0")
        with _installed():
            assert cli._get_drakkar_version(tmp_path) == "2.5.0"

    def test_falls_back_to_the_installed_drakkar_without_metadata(self, tmp_path: Path):
        with _installed("2.5.0"):
            assert cli._get_drakkar_version(tmp_path) == "2.5.0"

    def test_falls_back_when_the_output_directory_is_gone(self, tmp_path: Path):
        with _installed("2.5.0"):
            assert cli._get_drakkar_version(tmp_path / "deleted") == "2.5.0"

    def test_asks_the_installed_drakkar_when_no_directory_is_given(self):
        with _installed("2.5.0") as run:
            assert cli._get_drakkar_version() == "2.5.0"
        assert run.called

    def test_unknown_when_drakkar_cannot_be_reached(self, tmp_path: Path):
        with patch.object(subprocess, "run", side_effect=FileNotFoundError):
            assert cli._get_drakkar_version(tmp_path) == "unknown"
