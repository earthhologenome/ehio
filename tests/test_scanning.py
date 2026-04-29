"""Tests for ehio.scanning — command builder and session detection."""

from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock

from ehio.scanning import build_command, session_exists, MODULES, DRAKKAR_CMD


# ---------------------------------------------------------------------------
# MODULES / DRAKKAR_CMD constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_modules_list(self):
        assert set(MODULES) == {"preprocessing", "binning", "quantifying"}

    def test_drakkar_cmd_mapping(self):
        assert DRAKKAR_CMD["preprocessing"] == "preprocessing"
        assert DRAKKAR_CMD["binning"]       == "cataloging"
        assert DRAKKAR_CMD["quantifying"]   == "profiling"


# ---------------------------------------------------------------------------
# build_command
# ---------------------------------------------------------------------------

RUN_DIR = "/projects/ehi/data/RUN/PPR001"
OUT_DIR = "/projects/ehi/data/PPR/PPR001"


class TestBuildCommand:
    def test_preprocessing_contains_required_parts(self):
        cmd = build_command("preprocessing", "PPR001", RUN_DIR, OUT_DIR, "slurm")
        assert "mkdir -p" in cmd
        assert "ehio preprocessing --input" in cmd
        assert "-b" in cmd
        assert "PPR001" in cmd
        assert "samples.tsv" in cmd
        assert "drakkar preprocessing" in cmd
        assert "-p" in cmd
        assert "slurm" in cmd

    def test_samples_tsv_in_run_dir(self):
        """samples.tsv should live in the run dir, not the output dir."""
        cmd = build_command("preprocessing", "PPR001", RUN_DIR, OUT_DIR, "slurm")
        assert RUN_DIR in cmd
        assert f"{RUN_DIR}/samples.tsv" in cmd

    def test_drakkar_output_points_to_output_dir(self):
        """-o flag must use the data output dir, not the run dir."""
        cmd = build_command("preprocessing", "PPR001", RUN_DIR, OUT_DIR, "slurm")
        assert f"-o \"{OUT_DIR}\"" in cmd

    def test_both_dirs_created(self):
        """mkdir -p should create both run_dir and output_dir."""
        cmd = build_command("preprocessing", "PPR001", RUN_DIR, OUT_DIR, "slurm")
        mkdir_part = cmd.split("&&")[0]
        assert RUN_DIR in mkdir_part
        assert OUT_DIR in mkdir_part

    def test_binning_uses_cataloging_subcommand(self):
        cmd = build_command("binning", "ASB001",
                            "/projects/ehi/data/RUN/ASB001",
                            "/projects/ehi/data/ASB/ASB001", "slurm")
        assert "drakkar cataloging" in cmd
        assert "ehio binning --input" in cmd

    def test_quantifying_uses_profiling_subcommand(self):
        cmd = build_command("quantifying", "DMB001",
                            "/projects/ehi/data/RUN/DMB001",
                            "/projects/ehi/data/DMB/DMB001", "slurm")
        assert "drakkar profiling" in cmd
        assert "ehio quantifying --input" in cmd
        assert "bins.txt" in cmd
        assert "samples.tsv" in cmd
        assert "-B" in cmd
        assert "-R" in cmd

    def test_output_dir_quoted(self):
        """Paths with spaces must be safe inside the shell command."""
        cmd = build_command("preprocessing", "PPR001",
                            "/run/my batch/PPR001",
                            "/out/my batch/PPR001", "slurm")
        assert '"/run/my batch/PPR001"' in cmd
        assert '"/out/my batch/PPR001"' in cmd

    def test_batch_name_quoted(self):
        cmd = build_command("preprocessing", 'batch "x"', "/run", "/out", "slurm")
        assert r'"batch \"x\""' in cmd

    def test_unknown_module_raises(self):
        with pytest.raises(ValueError, match="Unknown module"):
            build_command("unknown", "B001", "/run", "/out", "slurm")

    def test_mkdir_comes_first(self):
        cmd = build_command("preprocessing", "PPR001", RUN_DIR, OUT_DIR, "slurm")
        assert cmd.startswith("mkdir -p")

    def test_steps_joined_with_and(self):
        """All steps must be chained with &&."""
        cmd = build_command("preprocessing", "PPR001", RUN_DIR, OUT_DIR, "slurm")
        parts = [p.strip() for p in cmd.split("&&")]
        assert len(parts) == 3


# ---------------------------------------------------------------------------
# session_exists
# ---------------------------------------------------------------------------

class TestSessionExists:
    def _mock_screen(self, stdout: str):
        mock_result = MagicMock()
        mock_result.stdout = stdout
        return mock_result

    def test_detects_running_session(self):
        output = "\t12345.PPR001\t(Detached)\n"
        with patch("ehio.scanning.subprocess.run", return_value=self._mock_screen(output)):
            assert session_exists("PPR001") is True

    def test_no_match_returns_false(self):
        output = "\t12345.OTHER_SESSION\t(Detached)\n"
        with patch("ehio.scanning.subprocess.run", return_value=self._mock_screen(output)):
            assert session_exists("PPR001") is False

    def test_partial_name_does_not_match(self):
        """PPR001X should not match a search for PPR001."""
        output = "\t12345.PPR001EXTRA\t(Detached)\n"
        with patch("ehio.scanning.subprocess.run", return_value=self._mock_screen(output)):
            assert session_exists("PPR001") is False

    def test_empty_output(self):
        with patch("ehio.scanning.subprocess.run", return_value=self._mock_screen("")):
            assert session_exists("PPR001") is False
