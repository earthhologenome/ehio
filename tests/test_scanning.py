"""Tests for ehio.scanning — script builder and session detection."""

from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock

from ehio.scanning import (
    BatchLaunchError,
    build_script_content,
    session_exists,
    STOP_SENTINEL,
    _verify_reference,
    MODULES,
    DRAKKAR_CMD,
)


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

    def test_exit_trap_reports_failure_before_set_status(self):
        script = self._script()
        report_pos = script.index("_ehio_report_failure || true")
        status_pos = script.index("ehio set-status")
        assert report_pos < status_pos

    def test_failure_report_tails_out_file(self):
        script = self._script()
        assert f"tail -n 80 {RUN_DIR}/PPR001.out" in script

    def test_failure_report_tails_referenced_job_logs(self):
        script = self._script()
        assert "grep -hoE" in script
        assert '.log' in script
        assert 'tail -n 40 "$_EHIO_LOG"' in script

    def test_failure_report_writes_to_stderr(self):
        script = self._script()
        report = script.split("_ehio_report_failure() {")[1].split("}\n")[0]
        assert all(">&2" in line for line in report.splitlines()
                   if line.strip().startswith(("echo", "tail")))

    def test_success_sentinel_set_after_output_step(self):
        script = self._script()
        assert "_EHIO_SUCCESS=1" in script
        output_pos  = script.index("ehio preprocessing --output")
        sentinel_pos = script.index("_EHIO_SUCCESS=1")
        assert sentinel_pos > output_pos

    def test_exit_trap_passes_failures_dir_to_set_status(self):
        script = self._script()
        status_line = next(l for l in script.splitlines() if "ehio set-status" in l)
        assert f"--failures-dir {OUT_DIR}" in status_line
        assert '--failures-since "$_EHIO_STARTED"' in status_line

    def test_start_timestamp_recorded_before_trap(self):
        script = self._script()
        assert "_EHIO_STARTED=$(date +%s)" in script
        assert script.index("_EHIO_STARTED=$(date") < script.index("trap _on_exit EXIT")

    # --- stop marker ---

    def test_stop_marker_short_circuits_the_trap(self):
        script = self._script()
        trap = script.split("_on_exit() {")[1].split("\n}")[0]
        assert f"[ -f {RUN_DIR}/{STOP_SENTINEL} ]" in trap
        # the check comes before the failure handling, and returns without it
        assert trap.index(STOP_SENTINEL) < trap.index("_EHIO_SUCCESS")
        assert "return 0" in trap

    def test_stop_marker_removed_at_launch(self):
        script = self._script()
        assert f"rm -f {RUN_DIR}/{STOP_SENTINEL}" in script
        assert script.index("rm -f") < script.index("trap _on_exit EXIT")

    def test_stop_marker_lives_in_the_run_dir(self):
        script = build_script_content(
            "preprocessing", "PPR001", "/other/run", OUT_DIR, "slurm",
        )
        assert f"/other/run/{STOP_SENTINEL}" in script

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
            ref_flag="-r 'https://example.com/ref.fna.gz'",
        )
        assert "-r 'https://example.com/ref.fna.gz'" in script

    def test_preprocessing_no_ref_flag_when_empty(self):
        script = self._script(module="preprocessing")
        drakkar_line = next(l for l in script.splitlines() if l.startswith("drakkar"))
        assert "-x" not in drakkar_line
        assert "-r" not in drakkar_line

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

    def test_binning_no_multicoverage_flag_by_default(self):
        script = build_script_content(
            "binning", "ASB001",
            "/projects/ehi/data/RUN/ASB001",
            "/projects/ehi/data/ASB/ASB001",
            "slurm",
        )
        drakkar_line = next(l for l in script.splitlines() if "drakkar cataloging" in l)
        assert " -c" not in drakkar_line

    def test_binning_multicoverage_adds_c_flag(self):
        script = build_script_content(
            "binning", "ASB001",
            "/projects/ehi/data/RUN/ASB001",
            "/projects/ehi/data/ASB/ASB001",
            "slurm",
            multicoverage=True,
        )
        drakkar_line = next(l for l in script.splitlines() if "drakkar cataloging" in l)
        assert " -c" in drakkar_line

    def test_multicoverage_ignored_for_other_modules(self):
        script = build_script_content(
            "preprocessing", "PPR001", RUN_DIR, OUT_DIR, "slurm",
            multicoverage=True,
        )
        drakkar_line = next(l for l in script.splitlines() if "drakkar preprocessing" in l)
        assert " -c" not in drakkar_line

    def test_quantifying_uses_profiling_and_bins_file(self):
        script = build_script_content(
            "quantifying", "DMB001",
            "/projects/ehi/data/RUN/DMB001",
            "/projects/ehi/data/DMB/DMB001",
            "slurm",
        )
        assert "ehio quantifying --input" in script
        assert "drakkar profiling" in script
        assert "DMB001_mags.tsv" in script
        assert "-B" in script
        assert "-R" in script

    def test_quantifying_tsv_and_bins_in_run_dir(self):
        run = "/projects/ehi/data/RUN/DMB001"
        script = build_script_content(
            "quantifying", "DMB001", run,
            "/projects/ehi/data/DMB/DMB001", "slurm",
        )
        assert f"{run}/DMB001_mags.tsv" in script
        assert f"{run}/DMB001_reads.tsv" in script

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
    _INPUT_FILE = {
        "preprocessing": "/run/BATCH001/BATCH001.tsv",
        "binning":       "/run/BATCH001/BATCH001.tsv",
        "quantifying":   "/run/BATCH001/BATCH001_mags.tsv",
    }

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_resume_reuses_the_existing_input_file(self, module: str):
        script = build_script_content(
            module, "BATCH001", "/run/BATCH001", "/out/BATCH001", "slurm",
            resume=True,
        )
        input_line = next(l for l in script.splitlines() if f"ehio {module} --input" in l)
        assert input_line.startswith(f"[ -s {self._INPUT_FILE[module]} ] || ")

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_resume_clears_a_stale_snakemake_lock(self, module: str):
        script = build_script_content(
            module, "BATCH001", "/run/BATCH001", "/out/BATCH001", "slurm",
            resume=True,
        )
        assert "drakkar unlock -o /out/BATCH001 -p slurm || true" in script

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_no_resume_does_not_unlock(self, module: str):
        script = build_script_content(
            module, "BATCH001", "/run/BATCH001", "/out/BATCH001", "slurm",
        )
        assert "drakkar unlock" not in script

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
        input_line = next(l for l in script.splitlines() if f"ehio {module} --input" in l)
        assert input_line.startswith(f"ehio {module} --input")

    def test_resume_tsv_path_still_passed_to_drakkar(self):
        script = build_script_content(
            "binning", "ASB001", "/run/ASB001", "/out/ASB001", "slurm",
            resume=True,
        )
        assert "ASB001.tsv" in script


# ---------------------------------------------------------------------------
# MAG info table (mags.tsv) on a resumed batch
# ---------------------------------------------------------------------------

class TestResumeMagInfoTable:
    """drakkar gained profiling_genomes/final/mags.tsv after the first batches
    were finished, so a resumed 'Done' batch must regenerate and upload it."""

    _MAGS_TSV = "/out/DMB001/profiling_genomes/final/mags.tsv"
    _SENTINEL = "/run/DMB001/.qfy_output_done"

    def _script(self, **kwargs) -> str:
        return build_script_content(
            "quantifying", "DMB001", "/run/DMB001", "/out/DMB001", "slurm", **kwargs,
        )

    def test_resume_calls_drakkar_when_only_the_mag_table_is_missing(self):
        script = self._script(resume=True)
        assert f"|| [ ! -s {self._MAGS_TSV} ]; then" in script

    def test_resume_reuploads_when_the_mag_table_is_newer_than_the_last_upload(self):
        script = self._script(resume=True)
        assert (
            f"if [ ! -f {self._SENTINEL} ] || [ {self._MAGS_TSV} -nt {self._SENTINEL} ]; then"
            in script
        )

    def test_the_upload_step_comes_after_the_drakkar_call(self):
        script = self._script(resume=True)
        assert script.index(f"[ ! -s {self._MAGS_TSV} ]") < script.index(self._SENTINEL)

    def test_a_fresh_launch_has_no_mag_table_guard(self):
        script = self._script(resume=False)
        assert self._MAGS_TSV not in script
        assert self._SENTINEL not in script


# ---------------------------------------------------------------------------
# drakkar silent no-op guards
# ---------------------------------------------------------------------------

class TestDrakkarNoOpGuards:
    """drakkar exits 0 on several of its own error paths (a stale Snakemake
    lock, a missing input file), so the script must not read that as success."""

    _PRODUCT = {
        "preprocessing": "/out/B001/preprocessing/final",
        "binning":       "/out/B001/cataloging/final",
        "quantifying":   "/out/B001/profiling_genomes/drep/dereplicated_genomes",
    }

    def _script(self, module: str, **kwargs) -> str:
        return build_script_content(
            module, "B001", "/run/B001", "/out/B001", "slurm", **kwargs,
        )

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_every_drakkar_call_is_bracketed_by_the_metadata_check(self, module: str):
        lines = [l.strip() for l in self._script(module).splitlines()]
        for i, line in enumerate(lines):
            if not line.startswith("drakkar ") or line.startswith("drakkar unlock"):
                continue
            assert lines[i - 1] == "_ehio_drakkar_start"
            assert lines[i + 1].startswith("_ehio_drakkar_check")

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_metadata_check_fails_when_no_run_was_started(self, module: str):
        script = self._script(module)
        check = script.split("_ehio_drakkar_check() {")[1].split("\n}")[0]
        assert '-newer "$_EHIO_MARKER"' in check
        assert 'grep -q "^status: success"' in check
        assert check.count("exit 1") == 2

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_metadata_check_is_skipped_for_drakkar_without_run_metadata(self, module: str):
        check = self._script(module).split("_ehio_drakkar_check() {")[1].split("\n}")[0]
        assert "ls /out/B001/drakkar_*.yaml >/dev/null 2>&1" in check
        assert "return 0" in check

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_output_step_runs_only_after_the_product_check(self, module: str):
        script = self._script(module)
        require_pos = script.index(f"_ehio_require {self._PRODUCT[module]}")
        output_pos  = script.index(f"ehio {module} --output")
        assert require_pos < output_pos

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_missing_product_exits_non_zero(self, module: str):
        require = self._script(module).split("_ehio_require() {")[1].split("\n}")[0]
        assert 'if [ ! -e "$1" ]' in require
        assert "exit 1" in require

    def test_marker_lives_in_the_run_dir(self):
        assert "_EHIO_MARKER=/run/B001/.ehio_drakkar_marker" in self._script("binning")

    def test_conditional_drakkar_call_keeps_its_guard(self):
        script = self._script("quantifying", resume=True)
        block = script.split(
            "if [ ! -d /out/B001/profiling_genomes/drep/dereplicated_genomes ]"
            " || [ ! -s /out/B001/profiling_genomes/final/mags.tsv ]; then\n"
        )[1]
        block = block.split("fi\n")[0]
        assert "_ehio_drakkar_start" in block
        assert "drakkar profiling" in block
        assert "_ehio_drakkar_check profiling" in block


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


# ---------------------------------------------------------------------------
# reference genome verification
# ---------------------------------------------------------------------------

class TestVerifyReference:
    URL = "https://ftp.ncbi.nlm.nih.gov/genomes/ref.fna.gz"

    def _noop(self, msg: str) -> None:
        pass

    def test_downloadable_url_passes(self):
        with patch("ehio.urls.check_url", return_value=None):
            _verify_reference(self.URL, "G0001", "EHI_GENOME_URL_RAW", self._noop)

    def test_broken_url_raises(self):
        with patch("ehio.urls.check_url", return_value="HTTP 404 Not Found"):
            with pytest.raises(BatchLaunchError) as exc:
                _verify_reference(self.URL, "G0001", "EHI_GENOME_URL_RAW", self._noop)
        assert "not downloadable" in str(exc.value)
        assert "HTTP 404 Not Found" in str(exc.value)
        assert "G0001" in str(exc.value)

    def test_existing_local_path_passes(self, tmp_path):
        ref = tmp_path / "ref.fna.gz"
        ref.write_text("")
        _verify_reference(str(ref), "G0001", "EHI_GENOME_URL_RAW", self._noop)

    def test_missing_local_path_raises(self, tmp_path):
        with pytest.raises(BatchLaunchError) as exc:
            _verify_reference(str(tmp_path / "nope.fna.gz"), "G0001",
                              "EHI_GENOME_URL_RAW", self._noop)
        assert "not found" in str(exc.value)
