"""Tests for ehio.scanning — script builder and session detection."""

from __future__ import annotations

from contextlib import contextmanager

import pytest
from unittest.mock import patch, MagicMock

from ehio.scanning import (
    BatchLaunchError,
    build_script_content,
    session_exists,
    STOP_SENTINEL,
    _verify_reference,
    _resolve_preprocessing_ref_flag,
    MODULES,
    DRAKKAR_CMD,
    launch_screen,
    _generate_input_files,
)


RUN_DIR = "/projects/ehi/data/RUN/PPR001"
OUT_DIR = "/projects/ehi/data/PPR/PPR001"


# ---------------------------------------------------------------------------
# MODULES / DRAKKAR_CMD constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_modules_list(self):
        assert set(MODULES) == {"preprocessing", "binning", "quantifying", "amr", "ena"}

    def test_every_module_but_ena_runs_a_drakkar_workflow(self):
        assert set(MODULES) - set(DRAKKAR_CMD) == {"ena"}

    def test_drakkar_cmd_mapping(self):
        assert DRAKKAR_CMD["preprocessing"] == "preprocessing"
        assert DRAKKAR_CMD["binning"]       == "cataloging"
        assert DRAKKAR_CMD["quantifying"]   == "profiling"
        assert DRAKKAR_CMD["amr"]           == "amr"


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

    def test_exit_trap_sets_error_status(self):
        script = self._script()
        status_line = next(l for l in script.splitlines() if "ehio set-status" in l)
        assert "--status Error" in status_line
        assert "--failures-dir" not in status_line

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

    _COUNTS_TSV = "/out/DMB001/profiling_genomes/final/counts.tsv"

    def test_resume_calls_drakkar_when_only_the_mag_table_is_missing(self):
        script = self._script(resume=True)
        assert f"|| [ ! -s {self._MAGS_TSV} ]" in script

    def test_resume_calls_drakkar_when_the_counts_table_is_missing(self):
        """The genomes and mags.tsv are written before the catalogue is indexed,
        so a run that stopped at the index or the mapping has both but no counts."""
        script = self._script(resume=True)
        assert f"|| [ ! -s {self._COUNTS_TSV} ]; then" in script

    def test_resume_reuploads_when_the_mag_table_is_newer_than_the_last_upload(self):
        script = self._script(resume=True)
        assert (
            f"if [ ! -f {self._SENTINEL} ] || [ {self._MAGS_TSV} -nt {self._SENTINEL} ]"
            in script
        )

    def test_resume_reuploads_when_the_counts_table_is_newer_than_the_last_upload(self):
        script = self._script(resume=True)
        assert f"|| [ {self._COUNTS_TSV} -nt {self._SENTINEL} ]; then" in script

    @pytest.mark.parametrize("resume", [False, True])
    def test_profiling_requires_the_counts_table(self, resume: bool):
        script = self._script(resume=resume)
        assert f"_ehio_require {self._COUNTS_TSV} profiling" in script
        assert "_ehio_require /out/DMB001/profiling_genomes/drep/dereplicated_genomes profiling" not in script

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
        "quantifying":   "/out/B001/profiling_genomes/final/counts.tsv",
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
        assert 'if [ -z "$(_ehio_drakkar_metadata)" ]' in check
        assert "return 0" in check

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_metadata_is_looked_for_in_both_drakkar_layouts(self, module: str):
        """drakkar 2.5.0 writes into logging/; earlier versions into the root."""
        finder = self._script(module).split("_ehio_drakkar_metadata() {")[1].split("\n}")[0]
        assert "find /out/B001/logging /out/B001 -maxdepth 1" in finder

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_metadata_glob_excludes_the_benchmark_rollup(self, module: str):
        """drakkar_<run id>_resources.yaml sits beside the metadata and has no status."""
        finder = self._script(module).split("_ehio_drakkar_metadata() {")[1].split("\n}")[0]
        assert '-name "drakkar_????????-??????.yaml"' in finder

    @pytest.mark.parametrize("module", ["preprocessing", "binning", "quantifying"])
    def test_metadata_search_survives_a_missing_logging_directory(self, module: str):
        """'find' on an absent directory exits non-zero, which 'pipefail' would
        otherwise turn into a failed script."""
        finder = self._script(module).split("_ehio_drakkar_metadata() {")[1].split("\n}")[0]
        assert "|| true; } | sort" in finder

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
            " || [ ! -s /out/B001/profiling_genomes/final/mags.tsv ]"
            " || [ ! -s /out/B001/profiling_genomes/final/counts.tsv ]; then\n"
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
# Token handoff to scanning subprocesses
# ---------------------------------------------------------------------------

class TestTokenHandoff:
    def test_screen_receives_the_selected_core_token(self, monkeypatch):
        monkeypatch.setenv("EHI_CORE_PIPELINE", "from-codeome")
        monkeypatch.setenv("EHI_CORE_TOKEN", "from-legacy-env")
        with patch("ehio.scanning.subprocess.run") as run:
            launch_screen("PPR001", "/run/PPR001.sh", core_token="selected")
        env = run.call_args.kwargs["env"]
        assert env["EHI_CORE_PIPELINE"] == "selected"
        assert "EHI_CORE_TOKEN" not in set(env)

    def test_screen_carries_a_legacy_token_only_under_the_pipeline_name(self, monkeypatch):
        monkeypatch.setenv("EHI_CORE_TOKEN", "from-legacy-env")
        with patch("ehio.scanning.subprocess.run") as run:
            launch_screen("PPR001", "/run/PPR001.sh", core_token="from-legacy-env")
        env = run.call_args.kwargs["env"]
        assert env["EHI_CORE_PIPELINE"] == "from-legacy-env"
        assert "EHI_CORE_TOKEN" not in set(env)

    def test_dry_run_input_receives_the_selected_core_token(self, monkeypatch):
        monkeypatch.setenv("EHI_CORE_PIPELINE", "from-codeome")
        monkeypatch.setenv("EHI_CORE_TOKEN", "from-legacy-env")
        with patch("ehio.scanning.subprocess.run") as run:
            _generate_input_files("preprocessing", "PPR001", "/run", "selected")
        env = run.call_args.kwargs["env"]
        assert env["EHI_CORE_PIPELINE"] == "selected"
        assert "EHI_CORE_TOKEN" not in set(env)

    def test_dry_run_input_carries_a_legacy_token_only_under_the_pipeline_name(self, monkeypatch):
        monkeypatch.setenv("EHI_CORE_TOKEN", "from-legacy-env")
        with patch("ehio.scanning.subprocess.run") as run:
            _generate_input_files("preprocessing", "PPR001", "/run", "from-legacy-env")
        env = run.call_args.kwargs["env"]
        assert env["EHI_CORE_PIPELINE"] == "from-legacy-env"
        assert "EHI_CORE_TOKEN" not in set(env)

    def test_generated_script_names_the_environment_variable_without_a_token(self, monkeypatch):
        monkeypatch.setenv("EHI_CORE_PIPELINE", "synthetic-token")
        script = build_script_content("preprocessing", "PPR001", RUN_DIR, OUT_DIR, "slurm")
        assert "EHI_CORE_PIPELINE" in script
        assert "synthetic-token" not in script


# ---------------------------------------------------------------------------
# reference genome verification
# ---------------------------------------------------------------------------

class TestVerifyReference:
    URL = "https://ftp.ncbi.nlm.nih.gov/genomes/ref.fna.gz"

    def _noop(self, msg: str) -> None:
        pass

    def test_downloadable_url_passes(self):
        with patch("ehio.urls.check_url", return_value=None):
            _verify_reference(self.URL, "G0001", "url_raw", self._noop)

    def test_broken_url_raises(self):
        with patch("ehio.urls.check_url", return_value="HTTP 404 Not Found"):
            with pytest.raises(BatchLaunchError) as exc:
                _verify_reference(self.URL, "G0001", "url_raw", self._noop)
        assert "not downloadable" in str(exc.value)
        assert "HTTP 404 Not Found" in str(exc.value)
        assert "G0001" in str(exc.value)

    def test_existing_local_path_passes(self, tmp_path):
        ref = tmp_path / "ref.fna.gz"
        ref.write_text("")
        _verify_reference(str(ref), "G0001", "url_raw", self._noop)

    def test_missing_local_path_raises(self, tmp_path):
        with pytest.raises(BatchLaunchError) as exc:
            _verify_reference(str(tmp_path / "nope.fna.gz"), "G0001",
                              "url_raw", self._noop)
        assert "not found" in str(exc.value)


class TestReferenceFlag:
    def _core(self, **genome):
        from ehio.core import CoreSession
        from tests.fake_core import FakeCoreClient

        return CoreSession(FakeCoreClient(genomes={"G0081": genome}))

    def test_indexed_genome_uses_x(self, tmp_path):
        indexed = tmp_path / "G0081.tar.gz"
        indexed.write_bytes(b"index")
        core = self._core(url_indexed=str(indexed), url_raw="https://raw")
        assert _resolve_preprocessing_ref_flag("G0081", core) == f"-x {indexed}"

    def test_raw_only_genome_uses_r(self, tmp_path):
        raw = tmp_path / "G0081.fna.gz"
        raw.write_bytes(b"raw")
        core = self._core(url_raw=str(raw))
        assert _resolve_preprocessing_ref_flag("G0081", core) == f"-r {raw}"

    def test_no_url_names_the_genome(self):
        with pytest.raises(BatchLaunchError, match="G0081"):
            _resolve_preprocessing_ref_flag("G0081", self._core())

    def test_unknown_genome_names_its_code(self):
        from ehio.core import CoreSession
        from tests.fake_core import FakeCoreClient

        with pytest.raises(BatchLaunchError, match="No reference genome G9999"):
            _resolve_preprocessing_ref_flag("G9999", CoreSession(FakeCoreClient()))


def test_launch_reads_the_genome_code_from_batch_entries(tmp_path):
    from ehio.core import CoreSession
    from tests.fake_core import FakeCoreClient

    indexed = tmp_path / "G0081.tar.gz"
    indexed.write_bytes(b"index")
    fake = FakeCoreClient(
        batches={"PPR001": {
            "row": {"code": "PPR001", "status": "Ready", "reference_genome_code": "G9999"},
            "entries": [{"code": "PR00001", "reference_genome_code": "G0081"}],
        }},
        genomes={"G0081": {"url_indexed": str(indexed)}},
    )
    values = {"EHI_PPR_OUTPUT_BASE": str(tmp_path / "PPR"), "RUN_BASE": str(tmp_path / "RUN")}
    with patch("ehio.scanning.cfg.get", side_effect=lambda key, default=None: values.get(key, default)), \
         patch("ehio.scanning.session_exists", return_value=False), \
         patch("ehio.scanning.launch_screen"):
        assert scan_module("preprocessing", core=CoreSession(fake)) == (1, 1)
    assert fake.batch_entries_calls == [("preprocessing_batches", "PPR001")]
    assert fake.reference_reads == ["G0081"]
    script = (tmp_path / "RUN" / "PPR001" / "PPR001.sh").read_text()
    assert f"-x {indexed}" in script


@pytest.mark.parametrize("genomes, code, message", [
    ({"G0081": {}}, "G0081", "neither an indexed nor a raw URL"),
    ({}, "G9999", "No reference genome G9999"),
])
def test_launch_fails_a_batch_with_an_unusable_genome(tmp_path, capsys, genomes, code, message):
    from ehio.core import CoreSession
    from tests.fake_core import FakeCoreClient

    fake = FakeCoreClient(
        batches={"PPR001": {"row": {"code": "PPR001", "status": "Ready"},
                            "entries": [{"code": "PR00001", "reference_genome_code": code}]}},
        genomes=genomes,
    )
    values = {"EHI_PPR_OUTPUT_BASE": str(tmp_path / "PPR"), "RUN_BASE": str(tmp_path / "RUN")}
    with patch("ehio.scanning.cfg.get", side_effect=lambda key, default=None: values.get(key, default)), \
         patch("ehio.scanning.session_exists", return_value=False), \
         patch("ehio.scanning.launch_screen") as launch:
        assert scan_module("preprocessing", core=CoreSession(fake)) == (1, 0)
    launch.assert_not_called()
    assert message in capsys.readouterr().err
    assert fake.rows("preprocessing_batches")[0]["values"]["status"] == "Error"


# ---------------------------------------------------------------------------
# reannotate flag — the annotation half of a finished DMB batch, run again
# ---------------------------------------------------------------------------

class TestReannotateFlag:
    RUN = "/projects/ehi/data/RUN/DMB0157"
    OUT = "/projects/ehi/data/DMB/DMB0157"

    def _script(self, **kwargs):
        return build_script_content(
            "quantifying", "DMB0157", self.RUN, self.OUT, "slurm",
            reannotate=True, **kwargs,
        )

    def test_stages_the_genomes_before_annotating(self):
        script = self._script()
        derep = f"{self.OUT}/profiling_genomes/drep/dereplicated_genomes"
        assert f"ehio annotating --stage -b DMB0157 -d {derep}" in script
        # The catalogue of the batch, not every MAG of it.
        stage = next(line for line in script.splitlines() if "ehio annotating --stage" in line)
        assert "--all-mags" not in stage
        # The staging has to be in place before the genomes are listed for
        # drakkar and before drakkar is asked to read them.
        assert script.index("annotating --stage") < script.index("annotating --input")
        assert script.index("annotating --input") < script.index("drakkar annotating")

    def test_goes_straight_to_the_function_status(self):
        script = self._script()
        assert "Annotating function" in script
        assert "Annotating taxonomy" not in script

    def test_skips_profiling_entirely(self):
        script = self._script()
        assert "drakkar profiling" not in script
        assert "ehio quantifying --input" not in script
        assert "ehio quantifying --output" not in script
        assert "DMB0157_reads.tsv" not in script

    def test_skips_taxonomy(self):
        # A genome's classification is fixed when it is binned; dereplicating
        # it neither changes it nor produces a better one, so GTDB-Tk has no
        # place in a re-annotation.
        script = self._script()
        assert "--annotation-type taxonomy" not in script
        assert "genome_taxonomy.tsv" not in script

    def test_runs_functional_annotation(self):
        assert "--annotation-type function" in self._script()

    def test_annotation_type_still_selects_the_drakkar_flag(self):
        script = self._script(annotation_type="kegg")
        assert "--annotation-type kegg" in script
        assert "--annotation-type function" not in script

    def test_input_step_forces_every_genome(self):
        # After a finished batch every MAG already carries the batch's
        # annotation level, so without --rerun the input step would write an
        # empty file and the whole annotation would be skipped.
        script = self._script()
        assert "ehio annotating --input" in script
        input_line = next(
            line for line in script.splitlines() if "ehio annotating --input" in line
        )
        assert "--rerun" in input_line

    def test_output_step_keeps_the_recorded_drakkar_version(self):
        script = self._script()
        output_line = next(
            line for line in script.splitlines() if "ehio annotating --output" in line
        )
        assert "--tasks function" in output_line
        assert "--rerun" in output_line

    def test_clears_a_stale_lock(self):
        assert "drakkar unlock" in self._script()

    def test_requires_the_staged_directory_and_the_gene_tables(self):
        script = self._script()
        derep = f"{self.OUT}/profiling_genomes/drep/dereplicated_genomes"
        assert f"_ehio_require {derep}" in script
        assert f"_ehio_require {self.OUT}/annotating/final" in script

    def test_marks_success_last(self):
        script = self._script()
        assert script.rstrip().endswith("_EHIO_SUCCESS=1")

    def test_boost_reaches_the_annotation_runs(self):
        script = self._script(boost_time=2, boost_memory=3)
        assert "--time-multiplier 2" in script
        assert "--memory-multiplier 3" in script

    def test_normal_quantifying_is_unchanged(self):
        script = build_script_content(
            "quantifying", "DMB0157", self.RUN, self.OUT, "slurm",
        )
        assert "drakkar profiling" in script
        assert "annotating --stage" not in script
        assert "--reannotate" not in script


# ---------------------------------------------------------------------------
# Tasks — what a DMB batch does: dereplicate, profile, taxonomy, function
# ---------------------------------------------------------------------------

class TestDmbTasks:
    RUN = "/projects/ehi/data/RUN/DMB0200"
    OUT = "/projects/ehi/data/DMB/DMB0200"

    def _script(self, tasks, **kwargs):
        return build_script_content(
            "quantifying", "DMB0200", self.RUN, self.OUT, "slurm", tasks=tasks, **kwargs,
        )

    @staticmethod
    def _line(script, text):
        return next(line for line in script.splitlines() if text in line)

    def test_no_tasks_is_the_whole_batch(self):
        # As before Tasks: profiling dereplicates, then the catalogue it kept
        # is classified and annotated.
        script = self._script(None)
        assert "--skip-derep" not in self._line(script, "drakkar profiling")
        assert "drakkar dereplicating" not in script
        assert "--annotation-type taxonomy" in script
        assert "--annotation-type function" in script
        assert "annotating --stage" not in script
        assert (f"drakkar annotating -b {self.OUT}/profiling_genomes/drep/dereplicated_genomes"
                in script)
        assert ("--tasks dereplicate,profile,taxonomy,function"
                in self._line(script, "ehio annotating --output"))

    def test_dereplicate_and_profile_is_one_profiling_run(self):
        script = self._script(("dereplicate", "profile"))
        assert script.count("drakkar profiling") == 1
        assert "--skip-derep" not in script
        assert "drakkar dereplicating" not in script

    def test_profile_alone_maps_against_every_mag(self):
        script = self._script(("profile",))
        assert "--skip-derep" in self._line(script, "drakkar profiling")
        # Nothing was kept or dropped, so no representatives are recorded.
        assert "--skip-derep" in self._line(script, "ehio quantifying --output")
        assert "drakkar dereplicating" not in script

    def test_profile_alone_ends_the_batch_after_its_counts(self):
        script = self._script(("profile",))
        assert "ehio quantifying --output" in script
        assert "drakkar annotating" not in script
        assert "ehio annotating" not in script
        last_status = [line for line in script.splitlines() if "ehio set-status" in line and "--failures-dir" not in line][-1]
        assert "--status Done" in last_status
        assert script.index("ehio quantifying --output") < script.index(last_status)

    def test_dereplicate_alone_runs_drakkar_dereplicating_without_reads(self):
        script = self._script(("dereplicate",), ani_threshold="0.95")
        assert "drakkar profiling" not in script
        input_line = self._line(script, "ehio quantifying --input")
        assert "--no-reads" in input_line
        assert "--reads-file" not in input_line
        derep = self._line(script, "drakkar dereplicating")
        assert f"-B {self.RUN}/DMB0200_mags.tsv" in derep
        assert f"-q {self.RUN}/DMB0200_quality.tsv" in derep
        assert "-a 0.95" in derep
        assert "_reads.tsv" not in script
        assert script.index("--status Dereplicating") < script.index("ehio quantifying --input")
        assert f"_ehio_require {self.OUT}/dereplicating.tsv" in script
        assert f"ehio quantifying --derep-output -b DMB0200 -l {self.OUT}" in script
        assert "ehio annotating" not in script
        last_status = [line for line in script.splitlines() if "ehio set-status" in line and "--failures-dir" not in line][-1]
        assert "--status Done" in last_status

    def test_dereplicate_and_taxonomy_classify_what_drep_kept(self):
        script = self._script(("dereplicate", "taxonomy"))
        assert "drakkar profiling" not in script
        assert "_reads.tsv" not in script
        assert "annotating --stage" not in script
        assert script.index("drakkar dereplicating") < script.index("ehio quantifying --derep-output")
        assert script.index("ehio quantifying --derep-output") < script.index("--status 'Annotating taxonomy'")
        assert f"drakkar annotating -b {self.OUT}/dereplicating/final" in script
        output = self._line(script, "ehio annotating --output")
        assert "--tasks dereplicate,taxonomy" in output
        assert "--rerun" not in output

    def test_dereplicate_and_function_annotate_what_drep_kept(self):
        script = self._script(("dereplicate", "function"))
        assert f"-d {self.OUT}/dereplicating/final" in self._line(script, "ehio annotating --input")

    def test_taxonomy_alone_stages_the_genomes_and_classifies_them(self):
        script = self._script(("taxonomy",))
        assert "drakkar profiling" not in script
        assert "drakkar dereplicating" not in script
        assert "ehio quantifying" not in script
        assert script.index("--status 'Annotating taxonomy'") < script.index("annotating --stage")
        assert script.index("annotating --stage") < script.index("--annotation-type taxonomy")
        assert "--annotation-type function" not in script
        assert "ehio annotating --input" not in script
        output = self._line(script, "ehio annotating --output")
        assert "--tasks taxonomy" in output
        assert "--rerun" in output

    def test_taxonomy_alone_classifies_every_mag_of_the_batch(self):
        script = self._script(("taxonomy",))
        batch_genomes = f"{self.OUT}/batch_genomes"
        stage = self._line(script, "ehio annotating --stage")
        assert f"-d {batch_genomes}" in stage
        assert "--all-mags" in stage
        assert f"_ehio_require {batch_genomes}" in script
        assert f"drakkar annotating -b {batch_genomes}" in script
        assert "dereplicated_genomes" not in script

    def test_taxonomy_alone_needs_no_reads(self):
        # Nothing is mapped, so neither the samples nor their reads are read.
        script = self._script(("taxonomy",))
        assert "ehio quantifying" not in script
        assert "_reads.tsv" not in script
        assert "_mags.tsv" not in script
        assert "-R " not in script

    def test_taxonomy_and_function_without_dereplicating_share_every_mag(self):
        script = self._script(("taxonomy", "function"))
        batch_genomes = f"{self.OUT}/batch_genomes"
        assert script.count("ehio annotating --stage") == 1
        assert "--all-mags" in self._line(script, "ehio annotating --stage")
        assert f"drakkar annotating -b {batch_genomes}" in script
        input_line = self._line(script, "ehio annotating --input")
        assert f"-d {batch_genomes}" in input_line
        assert "--rerun" in input_line
        assert script.index("--annotation-type taxonomy") < script.index("ehio annotating --input")
        assert "Annotating function" in script
        assert f"_ehio_require {self.OUT}/annotating/final" in script

    def test_function_alone_annotates_every_mag(self):
        stage = self._line(self._script(("function",)), "ehio annotating --stage")
        assert f"-d {self.OUT}/batch_genomes" in stage
        assert "--all-mags" in stage

    def test_a_batch_that_profiles_classifies_its_own_catalogue(self):
        script = self._script(("dereplicate", "profile", "taxonomy"))
        assert "annotating --stage" not in script
        assert ("drakkar annotating -b "
                f"{self.OUT}/profiling_genomes/drep/dereplicated_genomes") in script

    def test_profile_without_dereplicate_classifies_every_mag_it_mapped(self):
        # --skip-derep leaves every MAG in the profiling catalogue directory.
        script = self._script(("profile", "taxonomy"))
        assert "--skip-derep" in self._line(script, "drakkar profiling")
        assert ("drakkar annotating -b "
                f"{self.OUT}/profiling_genomes/drep/dereplicated_genomes") in script

    def test_a_batch_that_profiles_annotates_only_what_is_not_annotated_yet(self):
        script = self._script(("profile", "function"))
        assert "--annotation-type taxonomy" not in script
        assert "genome_taxonomy.tsv" not in script
        assert "--rerun" not in self._line(script, "ehio annotating --input")
        assert "--tasks profile,function" in self._line(script, "ehio annotating --output")

    def test_the_reannotate_status_is_the_function_alone(self):
        script = self._script(("profile", "taxonomy"), reannotate=True)
        assert "drakkar profiling" not in script
        assert "--annotation-type taxonomy" not in script
        assert "--tasks function" in self._line(script, "ehio annotating --output")

    def test_a_batch_without_profiling_clears_a_stale_lock(self):
        assert "drakkar unlock" in self._script(("function",))
        assert "drakkar unlock" not in self._script(("profile", "function"))

    def test_the_tasks_of_a_core_batch_reach_its_script(self, monkeypatch, tmp_path):
        real_get = cfg.get
        monkeypatch.setattr(cfg, "get", lambda key, default=None:
                            str(tmp_path / key) if key in ("MAG_DMB_OUTPUT_BASE", "RUN_BASE")
                            else "" if key == "MAG_DMB_BATCH" else real_get(key, default))
        monkeypatch.setattr("ehio.scanning.session_exists", lambda name: False)
        core = FakeCore([{"code": "DMB0200", "status": "Ready", "tasks": ["Taxonomy"]}])
        found, count = scan_module("quantifying", dry_run=True, core=CoreSession(core))
        assert (found, count) == (1, 1)
        script = (tmp_path / "RUN_BASE" / "DMB0200" / "DMB0200.sh").read_text()
        assert "drakkar profiling" not in script
        assert "--tasks taxonomy" in script


# Core-only batch scanning
# ---------------------------------------------------------------------------

from ehio import config as cfg
from ehio.core import CoreError, CoreSession
from ehio.scanning import CoreBatches, PendingBatch, _set_status, _sources, scan_module


class FakeCore:
    """The pending-batch and write methods the scanner uses."""

    url = "https://core.test/api"

    def __init__(self, rows=(), error=None, fail_upsert=None):
        self.rows = list(rows)
        self.error = error
        self.fail_upsert = fail_upsert
        self.upserts = []

    def ping(self):
        pass

    @contextmanager
    def run(self, label, total=None):
        yield

    def progress(self, done):
        pass

    def batch_entries(self, table, batch):
        return {"batch": batch, "row": {"code": batch}, "entries": []}

    def pending_batches(self, table, statuses):
        if self.error:
            raise self.error
        wanted = {status.lower() for status in statuses}
        return [row for row in self.rows if str(row.get("status", "")).lower() in wanted]

    def upsert(self, changes):
        if self.fail_upsert:
            raise self.fail_upsert
        self.upserts.append(changes)
        return [{"table": table, "code": row["key"].get("code"), "action": "updated"}
                for table, rows in changes for row in rows]

    def statuses(self):
        return [(row["key"]["code"], row["values"].get("status"))
                for changes in self.upserts for _, rows in changes for row in rows]


def _only_core(monkeypatch, **extra):
    real_get = cfg.get
    def get(key, default=None):
        if key == "EHI_PPR_BATCH":
            return ""
        if key in extra:
            return extra[key]
        return real_get(key, default)
    monkeypatch.setattr(cfg, "get", get)


class TestPendingBatch:
    def test_a_core_row_supplies_batch_options(self):
        batch = PendingBatch("DMB0157", "", "Ready",
                             row={"code": "DMB0157", "ani_threshold": 0.98,
                                  "annotation_type": "kegg"})
        assert batch.text("MAG_DMB_BATCH_ANI") == "0.98"
        assert batch.text("MAG_DMB_BATCH_ANNOTATION_TYPE") == "kegg"
        assert batch.where == "ehi-core"

    def test_an_unreadable_boost_is_no_boost(self):
        batch = PendingBatch("PPR001", "", "Ready", row={"code": "PPR001", "boost_time": "soon"})
        assert batch.number("EHI_PPR_BATCH_BOOST_TIME") is None

class TestCoreBatches:
    def _pending(self, rows, statuses=None):
        source = CoreBatches("quantifying", CoreSession(FakeCore(rows)))
        return source.pending(statuses or {"": "Ready", "resume": "Resume",
                                           "rerun": "Rerun", "reannotate": "Reannotate"})

    def test_each_status_becomes_the_requested_launch_kind(self):
        found = self._pending([
            {"code": "DMB0001", "status": "Ready"},
            {"code": "DMB0002", "status": "Resume"},
            {"code": "DMB0003", "status": "Rerun"},
            {"code": "DMB0004", "status": "Reannotate"},
            {"code": "DMB0005", "status": "Running"},
        ])
        assert [(batch.code, batch.kind) for batch in found] == [
            ("DMB0001", ""), ("DMB0002", "resume"),
            ("DMB0003", "rerun"), ("DMB0004", "reannotate"),
        ]

    def test_status_matching_ignores_case(self):
        assert [batch.kind for batch in self._pending([{"code": "DMB0001", "status": "RERUN"}])] == ["rerun"]

    def test_a_batch_without_a_code_is_skipped(self):
        assert self._pending([{"code": "", "status": "Ready"}]) == []

    def test_reannotate_is_not_requested_outside_quantifying(self):
        source = CoreBatches("preprocessing", CoreSession(FakeCore()))
        assert source.pending({"": "", "reannotate": ""}) == []

    def test_an_unreachable_core_stops_the_scan(self):
        core = FakeCore(error=CoreError("ehi-core could not read (404)", status=404))
        with pytest.raises(CoreError):
            CoreBatches("quantifying", CoreSession(core)).pending({"": "Ready"})


def test_only_the_core_is_a_source_when_the_batch_table_is_unconfigured(monkeypatch):
    _only_core(monkeypatch)
    assert [source.name for source in _sources("preprocessing", CoreSession(FakeCore()))] == ["ehi-core"]


class TestSetStatus:
    def test_a_core_batch_status_is_written(self):
        core = FakeCore()
        batch = PendingBatch("PPR001", "", "Ready", row={"code": "PPR001"})
        _set_status("preprocessing", batch, "Running", [CoreBatches("preprocessing", CoreSession(core))])
        assert core.statuses() == [("PPR001", "Running")]

    def test_a_dry_run_does_not_write_status(self):
        core = FakeCore()
        batch = PendingBatch("PPR001", "", "Ready", row={"code": "PPR001"})
        _set_status("preprocessing", batch, "Running", [CoreBatches("preprocessing", CoreSession(core))],
                    dry_run=True)
        assert core.statuses() == []

    def test_a_launched_batch_whose_core_status_fails_stops_the_scan(self, capsys):
        core = FakeCore(fail_upsert=CoreError("ehi-core request failed with HTTP 500"))
        batch = PendingBatch("ABB0729", "resume", "Resume", row={"code": "ABB0729"})
        source = CoreBatches("binning", CoreSession(core))
        with pytest.raises(CoreError) as exc:
            _set_status("binning", batch, "Running", [source], strict=True)
        assert "ehi-core request failed" in str(exc.value)
        assert "status →" not in capsys.readouterr().err

    def test_a_non_strict_status_failure_raises(self):
        core = FakeCore(fail_upsert=CoreError("ehi-core request failed with HTTP 500"))
        batch = PendingBatch("PPR001", "", "Ready", row={"code": "PPR001"})
        with pytest.raises(CoreError):
            _set_status("preprocessing", batch, "Error", [CoreBatches("preprocessing", CoreSession(core))])


class TestScanModule:
    def _scan(self, monkeypatch, tmp_path, rows=(), running=False, **kwargs):
        fake_core = FakeCore(rows)
        _only_core(monkeypatch, EHI_PPR_OUTPUT_BASE=str(tmp_path / "PPR"),
                   RUN_BASE=str(tmp_path / "RUN"))
        monkeypatch.setattr("ehio.scanning.session_exists", lambda name: running)
        launched = []
        monkeypatch.setattr("ehio.scanning.launch_screen",
                            lambda name, script, token="", core_token="": launched.append(name))
        found, count = scan_module("preprocessing", core=CoreSession(fake_core), **kwargs)
        return found, count, launched, fake_core

    def test_a_ready_core_batch_is_launched_and_marked_running(self, monkeypatch, tmp_path):
        found, count, launched, core = self._scan(
            monkeypatch, tmp_path, rows=[{"code": "PPR009", "status": "Ready"}])
        assert (found, count, launched) == (1, 1, ["PPR009"])
        assert core.statuses() == [("PPR009", "Running")]

    def test_a_rerun_clears_its_old_run_directory(self, monkeypatch, tmp_path):
        run_dir = tmp_path / "RUN" / "PPR001"
        run_dir.mkdir(parents=True)
        (run_dir / "stale.txt").write_text("from the failed run")
        self._scan(monkeypatch, tmp_path, rows=[{"code": "PPR001", "status": "Rerun"}])
        assert not (run_dir / "stale.txt").exists()

    def test_a_running_screen_session_is_skipped(self, monkeypatch, tmp_path):
        found, count, launched, core = self._scan(
            monkeypatch, tmp_path, rows=[{"code": "PPR009", "status": "Ready"}], running=True)
        assert (found, count, launched, core.statuses()) == (1, 0, [], [])
