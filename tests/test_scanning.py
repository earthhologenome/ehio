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
        assert set(MODULES) == {"preprocessing", "binning", "quantifying", "amr"}

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
        assert "--reannotate" in output_line
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
# Scanning Airtable and ehi-core together
#
# The same scan that reads Airtable's batch statuses reads ehi-core's, which is
# taking over from it: a batch either database holds is launched, a batch both
# hold is launched once and described by the core, and the status the launch
# sets goes back to whichever of the two holds it.
# ---------------------------------------------------------------------------

from ehio import config as cfg
from ehio.core import CoreError, CoreSession
from ehio.scanning import (
    AirtableBatches,
    CoreBatches,
    PendingBatch,
    _merge,
    _set_status,
    _sources,
    scan_module,
)

PPR_CODE      = cfg.get("EHI_PPR_BATCH_CODE")
PPR_STATUS    = cfg.get("EHI_PPR_BATCH_STATUS")
PPR_REFERENCE = cfg.get("EHI_PPR_BATCH_REFERENCE")
PPR_BOOST     = cfg.get("EHI_PPR_BATCH_BOOST_TIME")
DMB_CODE      = cfg.get("MAG_DMB_BATCH_CODE")
DMB_STATUS    = cfg.get("MAG_DMB_BATCH_STATUS")
DMB_ANI       = cfg.get("MAG_DMB_BATCH_ANI")


def airtable_batch(code="PPR001", status="Ready", **fields):
    return {"id": f"rec{code}", "fields": {PPR_CODE: code, PPR_STATUS: status, **fields}}


class FakeAirtable:
    """ehio.airtable.AirtableClient as the scan uses it."""

    def __init__(self, records=(), fail_update=None):
        self.records = list(records)
        self.updates = []
        self.fail_update = fail_update

    def __call__(self, api_key=None, base_id=None):
        return self

    def fetch_pending_batches(self, batch_table, batch_status_field, trigger_status):
        return [r for r in self.records
                if r["fields"].get(batch_status_field) == trigger_status]

    def update_records(self, table, updates):
        if self.fail_update:
            raise self.fail_update
        self.updates.append((table, updates))


class FakeCore:
    """ehio.core.CoreClient as the scan uses it."""

    def __init__(self, rows=(), error=None, fail_upsert=None):
        self.rows = list(rows)
        self.error = error
        self.fail_upsert = fail_upsert
        self.upserts = []

    url = "https://core.test/api"

    def ping(self):
        pass

    @contextmanager
    def run(self, label, total=None):
        yield

    def progress(self, done):
        pass

    def pending_batches(self, table, statuses):
        if self.error:
            raise self.error
        wanted = {s.lower() for s in statuses}
        return [r for r in self.rows if str(r.get("status", "")).lower() in wanted]

    def upsert(self, changes):
        if self.fail_upsert:
            raise self.fail_upsert
        self.upserts.append(changes)
        return [{"table": t, "code": row["key"].get("code"), "action": "updated"}
                for t, rows in changes for row in rows]

    def statuses(self):
        """Every (code, status) the scan wrote to the core, in order."""
        return [(row["key"]["code"], row["values"].get("status"))
                for changes in self.upserts for _, rows in changes for row in rows]


# --- a batch as the two databases describe it -------------------------------------

class TestPendingBatch:
    def test_the_core_row_is_read_before_the_airtable_record(self):
        batch = PendingBatch("PPR001", "", "Ready",
                             record=airtable_batch(**{PPR_BOOST: 2}),
                             row={"code": "PPR001", "boost_time": 5})
        assert batch.number("EHI_PPR_BATCH_BOOST_TIME") == 5

    def test_airtable_fills_what_the_core_leaves_empty(self):
        batch = PendingBatch("PPR001", "", "Ready",
                             record=airtable_batch(**{PPR_BOOST: 2}),
                             row={"code": "PPR001", "boost_time": None})
        assert batch.number("EHI_PPR_BATCH_BOOST_TIME") == 2

    def test_a_batch_only_airtable_holds_is_read_from_airtable(self):
        batch = PendingBatch("PPR001", "", "Ready", record=airtable_batch(**{PPR_BOOST: 2}))
        assert batch.number("EHI_PPR_BATCH_BOOST_TIME") == 2
        assert batch.where == "Airtable"

    def test_a_batch_only_the_core_holds_is_read_from_the_core(self):
        batch = PendingBatch("DMB0157", "", "Ready",
                             row={"code": "DMB0157", "ani_threshold": 0.98,
                                  "annotation_type": "kegg"})
        assert batch.text("MAG_DMB_BATCH_ANI") == "0.98"
        assert batch.text("MAG_DMB_BATCH_ANNOTATION_TYPE") == "kegg"
        assert batch.where == "ehi-core"

    def test_an_unreadable_boost_is_no_boost(self):
        batch = PendingBatch("PPR001", "", "Ready", row={"code": "PPR001", "boost_time": "soon"})
        assert batch.number("EHI_PPR_BATCH_BOOST_TIME") is None

    def test_the_core_genome_code_is_resolved_as_airtable_would_resolve_it(self):
        batch = PendingBatch("PPR001", "", "Ready", row={"code": "PPR001",
                                                         "reference_genome_code": "G0007"})
        assert batch.reference_record()["fields"][PPR_REFERENCE] == "G0007"

    def test_without_a_core_genome_the_airtable_record_is_resolved(self):
        record = airtable_batch(**{PPR_REFERENCE: ["recGENOME1"]})
        batch = PendingBatch("PPR001", "", "Ready", record=record, row={"code": "PPR001"})
        assert batch.reference_record() is record


# --- reading each database --------------------------------------------------------

class TestCoreBatches:
    def _pending(self, rows, statuses=None):
        core = FakeCore(rows)
        source = CoreBatches("quantifying", CoreSession(core))
        return source.pending(statuses or {"": "Ready", "resume": "Resume",
                                           "rerun": "Rerun", "reannotate": "Reannotate"})

    def test_each_status_becomes_the_kind_of_launch_it_asks_for(self):
        found = self._pending([
            {"code": "DMB0001", "status": "Ready"},
            {"code": "DMB0002", "status": "Resume"},
            {"code": "DMB0003", "status": "Rerun"},
            {"code": "DMB0004", "status": "Reannotate"},
            {"code": "DMB0005", "status": "Running"},
        ])
        assert [(b.code, b.kind) for b in found] == [
            ("DMB0001", ""), ("DMB0002", "resume"),
            ("DMB0003", "rerun"), ("DMB0004", "reannotate"),
        ]

    def test_a_status_matches_however_the_core_capitalises_it(self):
        assert [b.kind for b in self._pending([{"code": "DMB0001", "status": "RERUN"}])] == ["rerun"]

    def test_a_batch_without_a_code_is_left_alone(self):
        assert self._pending([{"code": "", "status": "Ready"}]) == []

    def test_the_reannotate_status_is_not_asked_for_outside_quantifying(self):
        core = CoreBatches("preprocessing", CoreSession(FakeCore()))
        assert core.pending({"": "", "reannotate": ""}) == []

    def test_a_core_that_cannot_be_reached_is_reported_and_gives_nothing(self, capsys):
        core = FakeCore(error=CoreError("ehi-core could not read (404)", status=404))
        found = CoreBatches("quantifying", CoreSession(core)).pending({"": "Ready"})
        assert found == []
        assert "not written to ehi-core" in capsys.readouterr().err

    def test_a_required_core_that_cannot_be_reached_stops_the_scan(self):
        core = FakeCore(error=CoreError("ehi-core could not read (404)", status=404))
        with pytest.raises(CoreError):
            CoreBatches("quantifying", CoreSession(core, required=True)).pending({"": "Ready"})


class TestSources:
    def test_both_databases_are_scanned_when_both_are_configured(self):
        sources = _sources("preprocessing", "tok", CoreSession(FakeCore()))
        assert [s.name for s in sources] == ["Airtable", "ehi-core"]

    def test_without_the_core_the_scan_is_airtable_alone(self):
        assert [s.name for s in _sources("preprocessing", "tok", CoreSession())] == ["Airtable"]

    def test_a_module_airtable_does_not_hold_is_still_scanned_in_the_core(self, monkeypatch):
        monkeypatch.setattr(cfg, "get", lambda key, default=None:
                            "" if key == "EHI_PPR_BATCH" else cfg.load_config().get(key, default))
        sources = _sources("preprocessing", "tok", CoreSession(FakeCore()))
        assert [s.name for s in sources] == ["ehi-core"]


# --- merging the two --------------------------------------------------------------

class TestMerge:
    STATUSES = {"": "Ready", "resume": "Resume", "rerun": "Rerun", "reannotate": ""}

    def _merge(self, monkeypatch, records, rows):
        monkeypatch.setattr("ehio.scanning.AirtableClient", FakeAirtable(records))
        airtable = AirtableBatches("preprocessing", "tok")
        core = CoreBatches("preprocessing", CoreSession(FakeCore(rows)))
        return _merge("preprocessing", [airtable, core], self.STATUSES)

    def test_a_batch_both_databases_hold_is_launched_once(self, monkeypatch):
        merged = self._merge(monkeypatch, [airtable_batch("PPR001")],
                             [{"code": "PPR001", "status": "Ready"}])
        assert [b.code for b in merged] == ["PPR001"]
        assert merged[0].where == "Airtable and ehi-core"

    def test_a_batch_either_database_holds_alone_is_launched_too(self, monkeypatch):
        merged = self._merge(monkeypatch, [airtable_batch("PPR001")],
                             [{"code": "PPR002", "status": "Ready"}])
        assert [(b.code, b.where) for b in merged] == [
            ("PPR001", "Airtable"), ("PPR002", "ehi-core"),
        ]

    def test_when_the_two_disagree_the_core_says_what_the_batch_is_waiting_for(self, monkeypatch, capsys):
        merged = self._merge(monkeypatch, [airtable_batch("PPR001", status="Ready")],
                             [{"code": "PPR001", "status": "Rerun"}])
        assert [(b.code, b.kind) for b in merged] == [("PPR001", "rerun")]
        assert "'Ready' in Airtable but 'Rerun' in ehi-core" in capsys.readouterr().err

    def test_a_batch_both_hold_keeps_the_code_airtable_named_the_run_directory_with(self, monkeypatch):
        merged = self._merge(monkeypatch, [airtable_batch("ppr001")],
                             [{"code": "PPR001", "status": "Ready"}])
        assert [b.code for b in merged] == ["ppr001"]


# --- writing the status back ------------------------------------------------------

class TestSetStatus:
    def _sources(self, monkeypatch, core=None, fail_update=None):
        fake = FakeAirtable(fail_update=fail_update)
        monkeypatch.setattr("ehio.scanning.AirtableClient", fake)
        sources = [AirtableBatches("preprocessing", "tok")]
        if core is not None:
            sources.append(CoreBatches("preprocessing", CoreSession(core)))
        return sources, fake

    def test_a_batch_both_hold_is_set_in_both(self, monkeypatch):
        core = FakeCore()
        sources, fake = self._sources(monkeypatch, core=core)
        batch = PendingBatch("PPR001", "", "Ready", record=airtable_batch(),
                             row={"code": "PPR001"})
        _set_status("preprocessing", batch, "Running", sources)
        assert fake.updates[0][1][0]["fields"][PPR_STATUS] == "Running"
        assert core.statuses() == [("PPR001", "Running")]

    def test_a_batch_only_the_core_holds_never_reaches_airtable(self, monkeypatch):
        core = FakeCore()
        sources, fake = self._sources(monkeypatch, core=core)
        batch = PendingBatch("PPR001", "", "Ready", row={"code": "PPR001"})
        _set_status("preprocessing", batch, "Running", sources)
        assert fake.updates == []
        assert core.statuses() == [("PPR001", "Running")]

    def test_a_dry_run_sets_nothing_anywhere(self, monkeypatch):
        core = FakeCore()
        sources, fake = self._sources(monkeypatch, core=core)
        batch = PendingBatch("PPR001", "", "Ready", record=airtable_batch(), row={"code": "PPR001"})
        _set_status("preprocessing", batch, "Running", sources, dry_run=True)
        assert (fake.updates, core.statuses()) == ([], [])

    def test_a_launched_batch_whose_status_will_not_stick_stops_the_scan(self, monkeypatch):
        from ehio.airtable import AirtableError

        sources, _ = self._sources(monkeypatch, fail_update=AirtableError("403 Forbidden"))
        batch = PendingBatch("PPR001", "", "Ready", record=airtable_batch())
        with pytest.raises(AirtableError) as exc:
            _set_status("preprocessing", batch, "Running", sources, strict=True)
        assert "was launched, but its status" in str(exc.value)

    def test_a_status_that_will_not_stick_elsewhere_is_only_reported(self, monkeypatch, capsys):
        from ehio.airtable import AirtableError

        sources, _ = self._sources(monkeypatch, fail_update=AirtableError("403 Forbidden"))
        batch = PendingBatch("PPR001", "", "Ready", record=airtable_batch())
        _set_status("preprocessing", batch, "Error", sources)
        assert "could not set the status to 'Error' in Airtable" in capsys.readouterr().err

    def test_a_launched_batch_the_core_will_not_take_the_status_of_stops_the_scan(self, monkeypatch, capsys):
        core = FakeCore(fail_upsert=CoreError("ehi-core request failed with HTTP 500"))
        sources, _ = self._sources(monkeypatch, core=core)
        batch = PendingBatch("ABB0729", "resume", "Resume", row={"code": "ABB0729"})
        with pytest.raises(CoreError) as exc:
            _set_status("binning", batch, "Running", sources, strict=True)
        assert "HTTP 500" in str(exc.value)
        assert "was launched, but its status in ehi-core" in str(exc.value)
        assert "status →" not in capsys.readouterr().err

    def test_a_status_the_core_will_not_take_is_not_reported_as_set(self, monkeypatch, capsys):
        core = FakeCore(fail_upsert=CoreError("ehi-core request failed with HTTP 500"))
        sources, _ = self._sources(monkeypatch, core=core)
        batch = PendingBatch("PPR001", "", "Ready", row={"code": "PPR001"})
        _set_status("preprocessing", batch, "Error", sources)
        err = capsys.readouterr().err
        assert "could not set the status to 'Error' in ehi-core" in err
        assert "Airtable has it" not in err
        assert "status →" not in err


# --- the scan end to end ----------------------------------------------------------

class TestScanModule:
    def _scan(self, monkeypatch, tmp_path, records=(), rows=(), running=False, **kwargs):
        """Scan preprocessing over the two fakes, with nothing left to launch."""
        fake_airtable = FakeAirtable(records)
        fake_core     = FakeCore(rows)
        real_get = cfg.get

        def bases(key, default=None):
            if key == "EHI_PPR_OUTPUT_BASE":
                return str(tmp_path / "PPR")
            if key == "RUN_BASE":
                return str(tmp_path / "RUN")
            return real_get(key, default)

        monkeypatch.setattr(cfg, "get", bases)
        monkeypatch.setattr("ehio.scanning.AirtableClient", fake_airtable)
        monkeypatch.setattr("ehio.scanning.session_exists", lambda name: running)
        monkeypatch.setattr("ehio.scanning._resolve_preprocessing_ref_flag",
                            lambda record, token, verbose=False: "")
        launched = []
        monkeypatch.setattr("ehio.scanning.launch_screen",
                            lambda name, script, token="", core_token="": launched.append(name))
        found, count = scan_module("preprocessing", "tok",
                                   core=CoreSession(fake_core), **kwargs)
        return found, count, launched, fake_airtable, fake_core

    def test_a_batch_only_the_core_holds_is_launched(self, monkeypatch, tmp_path):
        found, count, launched, airtable, core = self._scan(
            monkeypatch, tmp_path, rows=[{"code": "PPR009", "status": "Ready"}],
        )
        assert (found, count, launched) == (1, 1, ["PPR009"])
        assert airtable.updates == []
        assert core.statuses() == [("PPR009", "Running")]

    def test_a_batch_both_hold_is_launched_once_and_set_in_both(self, monkeypatch, tmp_path):
        found, count, launched, airtable, core = self._scan(
            monkeypatch, tmp_path,
            records=[airtable_batch("PPR001")],
            rows=[{"code": "PPR001", "status": "Ready"}],
        )
        assert (found, count, launched) == (1, 1, ["PPR001"])
        assert airtable.updates[0][1][0]["fields"][PPR_STATUS] == "Running"
        assert core.statuses() == [("PPR001", "Running")]

    def test_the_core_decides_what_a_batch_both_hold_is_waiting_for(self, monkeypatch, tmp_path):
        run_dir = tmp_path / "RUN" / "PPR001"
        run_dir.mkdir(parents=True)
        (run_dir / "stale.txt").write_text("from the failed run")
        self._scan(
            monkeypatch, tmp_path,
            records=[airtable_batch("PPR001", status="Ready")],
            rows=[{"code": "PPR001", "status": "Rerun"}],
        )
        assert not (run_dir / "stale.txt").exists()

    def test_a_core_too_old_for_the_route_leaves_the_scan_to_airtable(self, monkeypatch, tmp_path):
        fake_core = FakeCore(error=CoreError("404 Not Found", status=404))
        real_get = cfg.get
        monkeypatch.setattr(cfg, "get", lambda key, default=None:
                            str(tmp_path / key) if key in ("EHI_PPR_OUTPUT_BASE", "RUN_BASE")
                            else real_get(key, default))
        monkeypatch.setattr("ehio.scanning.AirtableClient", FakeAirtable([airtable_batch("PPR001")]))
        monkeypatch.setattr("ehio.scanning.session_exists", lambda name: False)
        monkeypatch.setattr("ehio.scanning._resolve_preprocessing_ref_flag",
                            lambda record, token, verbose=False: "")
        launched = []
        monkeypatch.setattr("ehio.scanning.launch_screen",
                            lambda name, script, token="", core_token="": launched.append(name))
        found, count = scan_module("preprocessing", "tok", core=CoreSession(fake_core))
        assert (found, count, launched) == (1, 1, ["PPR001"])

    def test_a_running_screen_session_is_still_skipped(self, monkeypatch, tmp_path):
        found, count, launched, _, core = self._scan(
            monkeypatch, tmp_path, rows=[{"code": "PPR009", "status": "Ready"}], running=True,
        )
        assert (found, count, launched, core.statuses()) == (1, 0, [], [])
