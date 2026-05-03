"""Tests for ehio.scanning — script builder and session detection."""

from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock

from ehio.scanning import build_script_content, session_exists, MODULES, DRAKKAR_CMD


RUN_DIR = "/projects/ehi/data/RUN/PPR001"
OUT_DIR = "/projects/ehi/data/PPR/PPR001"


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
# build_script_content
# ---------------------------------------------------------------------------

class TestBuildScriptContent:
    def _script(self, module="preprocessing", batch="PPR001",
                run_dir=RUN_DIR, out_dir=OUT_DIR, profile="slurm",
                error_status="Error"):
        return build_script_content(module, batch, run_dir, out_dir, profile, error_status)

    # --- bash boilerplate ---

    def test_starts_with_shebang(self):
        assert self._script().startswith("#!/usr/bin/env bash")

    def test_has_strict_mode(self):
        assert "set -euo pipefail" in self._script()

    # --- exit trap ---

    def test_has_exit_trap(self):
        script = self._script()
        assert "trap _on_exit EXIT" in script

    def test_exit_trap_calls_set_status_on_failure(self):
        script = self._script(batch="PPR001", error_status="Error")
        assert "_EHIO_SUCCESS=0" in script
        assert '_EHIO_SUCCESS" -ne 1' in script
        assert "ehio set-status" in script
        assert "--module preprocessing" in script
        assert "--batch" in script
        assert "PPR001" in script
        assert "--status" in script
        assert "Error" in script

    def test_success_sentinel_set_after_output_step(self):
        script = self._script()
        assert "_EHIO_SUCCESS=1" in script
        output_pos  = script.index("ehio preprocessing --output")
        sentinel_pos = script.index("_EHIO_SUCCESS=1")
        assert sentinel_pos > output_pos

    def test_error_status_is_configurable(self):
        script = self._script(error_status="Failed")
        assert "Failed" in script
        assert "Error" not in script.split("_on_exit")[1]  # not in the trap body

    # --- directories ---

    def test_mkdir_creates_both_dirs(self):
        script = self._script()
        mkdir_line = next(l for l in script.splitlines() if l.startswith("mkdir"))
        assert RUN_DIR in mkdir_line
        assert OUT_DIR in mkdir_line

    def test_run_dir_and_output_dir_quoted(self):
        script = build_script_content(
            "preprocessing", "PPR001",
            "/run/my batch/PPR001",
            "/out/my batch/PPR001",
            "slurm",
        )
        assert "'/run/my batch/PPR001'" in script
        assert "'/out/my batch/PPR001'" in script

    # --- TSV file naming ---

    def test_tsv_named_after_batch(self):
        script = self._script(batch="PPR001")
        assert "PPR001.tsv" in script
        assert "samples.tsv" not in script

    def test_tsv_in_run_dir(self):
        script = self._script()
        assert f"{RUN_DIR}/PPR001.tsv" in script

    # --- drakkar -o points to output_dir ---

    def test_drakkar_output_flag(self):
        script = self._script()
        # shlex.quote only adds quotes for paths with special characters
        assert f"-o {OUT_DIR}" in script

    # --- module-specific commands ---

    def test_preprocessing_calls_ehio_and_drakkar(self):
        script = self._script(module="preprocessing")
        assert "ehio preprocessing --input" in script
        assert "drakkar preprocessing" in script

    def test_preprocessing_ref_flag_hardwired_indexed(self):
        script = build_script_content(
            "preprocessing", "PPR001", RUN_DIR, OUT_DIR, "slurm",
            ref_flag="-x 'https://example.com/ref.tar.gz'",
        )
        assert "-x 'https://example.com/ref.tar.gz'" in script
        assert "source" not in script
        assert "$DRAKKAR_REF_FLAG" not in script

    def test_preprocessing_ref_flag_hardwired_raw(self):
        script = build_script_content(
            "preprocessing", "PPR001", RUN_DIR, OUT_DIR, "slurm",
            ref_flag="-g 'https://example.com/ref.fna.gz'",
        )
        assert "-g 'https://example.com/ref.fna.gz'" in script

    def test_preprocessing_no_ref_flag_when_empty(self):
        script = self._script(module="preprocessing")
        drakkar_line = next(l for l in script.splitlines() if l.startswith("drakkar"))
        assert "-x" not in drakkar_line
        assert "-g" not in drakkar_line

    def test_binning_uses_cataloging(self):
        script = build_script_content(
            "binning", "ASB001",
            "/projects/ehi/data/RUN/ASB001",
            "/projects/ehi/data/ASB/ASB001",
            "slurm",
        )
        assert "ehio binning --input" in script
        assert "drakkar cataloging" in script

    def test_binning_has_no_m_flag(self):
        """Assembly mode is now driven by the 'assembly' column in the TSV, not -m."""
        script = build_script_content(
            "binning", "ASB001",
            "/projects/ehi/data/RUN/ASB001",
            "/projects/ehi/data/ASB/ASB001",
            "slurm",
        )
        drakkar_line = next(l for l in script.splitlines() if "drakkar cataloging" in l)
        assert "-m " not in drakkar_line

    def test_quantifying_uses_profiling_and_bins_file(self):
        script = build_script_content(
            "quantifying", "DMB001",
            "/projects/ehi/data/RUN/DMB001",
            "/projects/ehi/data/DMB/DMB001",
            "slurm",
        )
        assert "ehio quantifying --input" in script
        assert "drakkar profiling" in script
        assert "DMB001_bins.txt" in script
        assert "-B" in script
        assert "-R" in script

    def test_quantifying_tsv_and_bins_in_run_dir(self):
        run = "/projects/ehi/data/RUN/DMB001"
        script = build_script_content(
            "quantifying", "DMB001", run,
            "/projects/ehi/data/DMB/DMB001", "slurm",
        )
        assert f"{run}/DMB001.tsv" in script
        assert f"{run}/DMB001_bins.txt" in script

    def test_unknown_module_raises(self):
        with pytest.raises(ValueError, match="Unknown module"):
            build_script_content("unknown", "B001", "/run", "/out", "slurm")

    def test_batch_name_with_spaces_is_quoted(self):
        script = build_script_content(
            "preprocessing", "PPR 001", "/run/PPR 001", "/out/PPR 001", "slurm"
        )
        assert "'PPR 001'" in script


# ---------------------------------------------------------------------------
# resume flag (skip --input step)
# ---------------------------------------------------------------------------

class TestResumeFlag:
    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_resume_skips_input_step(self, module: str):
        script = build_script_content(
            module, "BATCH001", "/run/BATCH001", "/out/BATCH001", "slurm",
            resume=True,
        )
        assert f"ehio {module} --input" not in script

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_resume_keeps_drakkar_and_output_steps(self, module: str):
        script = build_script_content(
            module, "BATCH001", "/run/BATCH001", "/out/BATCH001", "slurm",
            resume=True,
        )
        drakkar_sub = DRAKKAR_CMD[module]
        assert f"drakkar {drakkar_sub}" in script
        assert f"ehio {module} --output" in script

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_no_resume_includes_input_step(self, module: str):
        script = build_script_content(
            module, "BATCH001", "/run/BATCH001", "/out/BATCH001", "slurm",
            resume=False,
        )
        assert f"ehio {module} --input" in script

    def test_resume_tsv_path_still_passed_to_drakkar(self):
        script = build_script_content(
            "binning", "ASB001", "/run/ASB001", "/out/ASB001", "slurm",
            resume=True,
        )
        assert "ASB001.tsv" in script


# ---------------------------------------------------------------------------
# boost flags
# ---------------------------------------------------------------------------

class TestBoostFlags:
    def _script(self, module="preprocessing", boost_time=None, boost_memory=None):
        return build_script_content(
            module, "PPR001", RUN_DIR, OUT_DIR, "slurm",
            boost_time=boost_time, boost_memory=boost_memory,
        )

    def test_no_boost_flags_by_default(self):
        script = self._script()
        assert "--time-multiplier"   not in script
        assert "--memory-multiplier" not in script

    def test_time_multiplier_appended(self):
        script = self._script(boost_time=4)
        assert "--time-multiplier 4" in script

    def test_memory_multiplier_appended(self):
        script = self._script(boost_memory=2)
        assert "--memory-multiplier 2" in script

    def test_both_multipliers_appended(self):
        script = self._script(boost_time=3, boost_memory=2)
        assert "--time-multiplier 3"   in script
        assert "--memory-multiplier 2" in script

    def test_value_of_1_is_omitted(self):
        script = self._script(boost_time=1, boost_memory=1)
        assert "--time-multiplier"   not in script
        assert "--memory-multiplier" not in script

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_boost_applied_to_all_modules(self, module: str):
        script = build_script_content(
            module, "BATCH001",
            "/run/BATCH001", "/out/BATCH001", "slurm",
            boost_time=2, boost_memory=4,
        )
        assert "--time-multiplier 2"   in script
        assert "--memory-multiplier 4" in script


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
        output = "\t12345.PPR001EXTRA\t(Detached)\n"
        with patch("ehio.scanning.subprocess.run", return_value=self._mock_screen(output)):
            assert session_exists("PPR001") is False

    def test_empty_output(self):
        with patch("ehio.scanning.subprocess.run", return_value=self._mock_screen("")):
            assert session_exists("PPR001") is False
