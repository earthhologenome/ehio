"""Tests for ehio.metadata — QC file parsers and Airtable payload builder."""

from __future__ import annotations

import json
import pytest
from pathlib import Path

from ehio.metadata import (
    parse_fastp,
    parse_host_removal,
    parse_singlem_mf,
    parse_nonpareil,
    collect_preprocessing_metadata,
    build_entry_update,
    PREPROCESSING_METRIC_KEYS,
)
from ehio.cli import _rename_preprocessing_files
from tests.conftest import FASTP_JSON, SINGLEM_SMF_TSV, NONPAREIL_TSV


# ---------------------------------------------------------------------------
# parse_fastp
# ---------------------------------------------------------------------------

class TestParseFastp:
    def test_reads_and_bases(self, fastp_json: Path):
        result = parse_fastp(fastp_json)
        assert result["reads_pre_fastp"]  == 10_000_000
        assert result["bases_pre_fastp"]  == 1_500_000_000
        assert result["reads_post_fastp"] == 9_200_000
        assert result["bases_post_fastp"] == 1_380_000_000

    def test_adapter_trimming(self, fastp_json: Path):
        result = parse_fastp(fastp_json)
        assert result["adapter_trimmed_reads"] == 4_100_000
        assert result["adapter_trimmed_bases"] == 82_000_000

    def test_missing_adapter_section(self, tmp_path: Path):
        """Files without adapter_cutting should return None for those keys."""
        data = {k: v for k, v in FASTP_JSON.items() if k != "adapter_cutting"}
        p = tmp_path / "no_adapter.json"
        p.write_text(json.dumps(data))
        result = parse_fastp(p)
        assert result["adapter_trimmed_reads"] is None
        assert result["adapter_trimmed_bases"] is None
        # Other keys still populated
        assert result["reads_pre_fastp"] == 10_000_000

    def test_empty_summary_section(self, tmp_path: Path):
        """Malformed JSON with empty summary returns None values gracefully."""
        p = tmp_path / "empty.json"
        p.write_text(json.dumps({"summary": {}}))
        result = parse_fastp(p)
        assert result["reads_pre_fastp"] is None
        assert result["reads_post_fastp"] is None


# ---------------------------------------------------------------------------
# parse_host_removal
# ---------------------------------------------------------------------------

class TestParseHostRemoval:
    def test_normal_values(self, tmp_path: Path):
        mr = tmp_path / "s.metareads"
        mb = tmp_path / "s.metabases"
        mr.write_text("8500000\n")
        mb.write_text("1275000000\n")
        result = parse_host_removal(mr, mb)
        assert result["metagenomic_reads"] == 8_500_000
        assert result["metagenomic_bases"] == 1_275_000_000

    def test_missing_file(self, tmp_path: Path):
        mr = tmp_path / "missing.metareads"
        mb = tmp_path / "missing.metabases"
        result = parse_host_removal(mr, mb)
        assert result["metagenomic_reads"] is None
        assert result["metagenomic_bases"] is None

    def test_empty_file(self, tmp_path: Path):
        mr = tmp_path / "empty.metareads"
        mb = tmp_path / "empty.metabases"
        mr.write_text("")
        mb.write_text("")
        result = parse_host_removal(mr, mb)
        assert result["metagenomic_reads"] is None
        assert result["metagenomic_bases"] is None

    def test_non_integer_content(self, tmp_path: Path):
        mr = tmp_path / "bad.metareads"
        mb = tmp_path / "bad.metabases"
        mr.write_text("not_a_number\n")
        mb.write_text("also_bad\n")
        result = parse_host_removal(mr, mb)
        assert result["metagenomic_reads"] is None
        assert result["metagenomic_bases"] is None


# ---------------------------------------------------------------------------
# parse_singlem_mf
# ---------------------------------------------------------------------------

class TestParseSinglemMf:
    def test_read_fraction(self, tmp_path: Path):
        p = tmp_path / "EHI00001_smf.tsv"
        p.write_text(SINGLEM_SMF_TSV)
        result = parse_singlem_mf(p)
        assert result["singlem_fraction"] == pytest.approx(0.869565, rel=1e-5)

    def test_missing_file(self, tmp_path: Path):
        result = parse_singlem_mf(tmp_path / "nonexistent_smf.tsv")
        assert result["singlem_fraction"] is None

    def test_missing_column(self, tmp_path: Path):
        p = tmp_path / "no_fraction.tsv"
        p.write_text("sample\tbacterial_archaeal_bases\tmetagenome_size\n"
                     "EHI00001\t1200000000\t1380000000\n")
        result = parse_singlem_mf(p)
        assert result["singlem_fraction"] is None


# ---------------------------------------------------------------------------
# parse_nonpareil
# ---------------------------------------------------------------------------

class TestParseNonpareil:
    def test_all_values(self, tmp_path: Path):
        p = tmp_path / "EHI00001_np.tsv"
        p.write_text(NONPAREIL_TSV)
        result = parse_nonpareil(p)
        assert result["nonpareil_C"]         == pytest.approx(0.92)
        assert result["nonpareil_LR"]        == pytest.approx(14.5)
        assert result["nonpareil_modelR"]    == pytest.approx(16.2)
        assert result["nonpareil_LRstar"]    == pytest.approx(18.1)
        assert result["nonpareil_diversity"] == pytest.approx(21.3)

    def test_absent_file_returns_none(self, tmp_path: Path):
        result = parse_nonpareil(tmp_path / "no_file.tsv")
        assert all(v is None for v in result.values())


# ---------------------------------------------------------------------------
# collect_preprocessing_metadata
# ---------------------------------------------------------------------------

class TestCollectPreprocessingMetadata:
    def test_full_output_tree(self, drakkar_output: Path):
        metrics = collect_preprocessing_metadata("EHI00001", drakkar_output)
        assert metrics["reads_pre_fastp"]    == 10_000_000
        assert metrics["bases_pre_fastp"]    == 1_500_000_000
        assert metrics["reads_post_fastp"]   == 9_200_000
        assert metrics["bases_post_fastp"]   == 1_380_000_000
        assert metrics["adapter_trimmed_reads"] == 4_100_000
        assert metrics["metagenomic_reads"]  == 8_500_000
        assert metrics["metagenomic_bases"]  == 1_275_000_000
        assert metrics["singlem_fraction"]   == pytest.approx(0.869565, rel=1e-5)
        assert metrics["nonpareil_C"]        == pytest.approx(0.92)

    def test_missing_fastp_returns_none(self, drakkar_output: Path):
        """If fastp JSON is absent the metric is None but other parsers still run."""
        (drakkar_output / "preprocessing" / "fastp" / "EHI00001.json").unlink()
        metrics = collect_preprocessing_metadata("EHI00001", drakkar_output)
        assert metrics["reads_pre_fastp"] is None
        assert metrics["metagenomic_reads"] == 8_500_000  # host removal still present

    def test_unknown_sample_returns_none_values(self, drakkar_output: Path):
        metrics = collect_preprocessing_metadata("UNKNOWN", drakkar_output)
        assert metrics["reads_pre_fastp"] is None
        assert metrics["metagenomic_reads"] is None
        assert metrics["singlem_fraction"] is None


# ---------------------------------------------------------------------------
# build_entry_update
# ---------------------------------------------------------------------------

class TestBuildEntryUpdate:
    FIELD_MAP = {
        "reads_pre_fastp":   "fldmD0Z4dNmEc0Uri",
        "metagenomic_reads": "fldzI8PYD9asJYIkN",
        "singlem_fraction":  "fldm3dHUMQ8X9fWoj",
    }

    def test_payload_contains_non_none_values(self):
        metrics = {
            "reads_pre_fastp":   10_000_000,
            "metagenomic_reads": 8_500_000,
            "singlem_fraction":  None,          # should be excluded
        }
        payload = build_entry_update("recENTRY000001", metrics, self.FIELD_MAP)
        assert payload["id"] == "recENTRY000001"
        assert "fldmD0Z4dNmEc0Uri" in payload["fields"]
        assert "fldzI8PYD9asJYIkN" in payload["fields"]
        assert "fldm3dHUMQ8X9fWoj" not in payload["fields"]

    def test_all_none_produces_empty_fields(self):
        metrics = {"reads_pre_fastp": None}
        payload = build_entry_update("recX", metrics, self.FIELD_MAP)
        assert payload["fields"] == {}

    def test_unmapped_metric_is_ignored(self):
        metrics = {"reads_pre_fastp": 10_000_000, "unknown_key": 999}
        payload = build_entry_update("recX", metrics, self.FIELD_MAP)
        assert "unknown_key" not in str(payload)

    def test_metric_keys_constant_covers_all_fields(self):
        """PREPROCESSING_METRIC_KEYS should reference all EHI_PPR_ENTRY_* config keys."""
        config_key_prefixes = {v for v in PREPROCESSING_METRIC_KEYS.values()}
        assert all(k.startswith("EHI_PPR_ENTRY_") for k in config_key_prefixes)


# ---------------------------------------------------------------------------
# _rename_preprocessing_files
# ---------------------------------------------------------------------------

class TestRenamePreprocessingFiles:
    def _make_tree(self, base: Path, sample: str) -> list[Path]:
        dirs = [
            base / "final",
            base / "singlem",
        ]
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)
        files = [
            base / "final" / f"{sample}.bam",
            base / "final" / f"{sample}_1.fq.gz",
            base / "final" / f"{sample}_2.fq.gz",
            base / "singlem" / f"{sample}_cond.tsv",
        ]
        for f in files:
            f.write_text("placeholder")
        return files

    def test_renames_all_four_file_types(self, tmp_path: Path):
        ppr = tmp_path / "preprocessing"
        self._make_tree(ppr, "EHI00001")
        _rename_preprocessing_files(ppr, {"EHI00001": "EHI000001"})
        assert (ppr / "final"   / "EHI000001_G.bam").exists()
        assert (ppr / "final"   / "EHI000001_M_1.fq.gz").exists()
        assert (ppr / "final"   / "EHI000001_M_2.fq.gz").exists()
        assert (ppr / "singlem" / "EHI000001_cond.tsv").exists()
        assert not (ppr / "final" / "EHI00001.bam").exists()

    def test_multiple_samples(self, tmp_path: Path):
        ppr = tmp_path / "preprocessing"
        self._make_tree(ppr, "EHI00001")
        self._make_tree(ppr, "EHI00002")
        _rename_preprocessing_files(ppr, {"EHI00001": "EHI000001", "EHI00002": "EHI000002"})
        assert (ppr / "final" / "EHI000001_G.bam").exists()
        assert (ppr / "final" / "EHI000002_G.bam").exists()

    def test_unrelated_files_are_untouched(self, tmp_path: Path):
        ppr = tmp_path / "preprocessing"
        (ppr / "final").mkdir(parents=True)
        other = ppr / "final" / "unrelated.txt"
        other.write_text("keep me")
        self._make_tree(ppr, "EHI00001")
        _rename_preprocessing_files(ppr, {"EHI00001": "EHI000001"})
        assert other.exists()
