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
    parse_drakkar_stats_tsv,
    parse_drakkar_cataloging_tsv,
    parse_sample_mapping_rates,
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

# ---------------------------------------------------------------------------
# parse_drakkar_stats_tsv
# ---------------------------------------------------------------------------

DRAKKAR_PREPROCESSING_TSV = (
    "sample\treads_pre_fastp\tbases_pre_fastp\thost_reads\thost_bases"
    "\tmetagenomic_reads\tmetagenomic_bases\tsinglem_fraction\tnonpareil_C\n"
    "PR00001\t10000000\t1500000000\t13230\t1686712\t9000000\t1350000000\t77.14\t0.84\n"
    "PR00002\t8000000\t1200000000\tNA\tNA\t7000000\t1050000000\tNA\t0.91\n"
)

# Real drakkar cataloging.tsv column names (with co-assembly example)
DRAKKAR_CATALOGING_TSV = (
    "assembly\tsamples\tassembly_contigs\tassembly_total_length\tassembly_largest_contig"
    "\tassembly_N50\tassembly_L50\tmapping_rate_percent\tfinal_bins\n"
    "EHA05804\tEHI00001,EHI00002\t10928\t43585842\t201785\t5037\t1684\t32.89\t10\n"
    "EHA05805\tEHI00003\t8000\t30000000\tNA\t4000\t2000\tNA\t0\n"
)


class TestParseDrakkarStatsTsv:
    def test_parses_integers(self, tmp_path: Path):
        p = tmp_path / "preprocessing.tsv"
        p.write_text(DRAKKAR_PREPROCESSING_TSV)
        data = parse_drakkar_stats_tsv(p)
        assert data["PR00001"]["reads_pre_fastp"] == 10_000_000
        assert data["PR00001"]["host_reads"]      == 13_230
        assert data["PR00001"]["metagenomic_reads"] == 9_000_000

    def test_parses_floats(self, tmp_path: Path):
        p = tmp_path / "preprocessing.tsv"
        p.write_text(DRAKKAR_PREPROCESSING_TSV)
        data = parse_drakkar_stats_tsv(p)
        assert data["PR00001"]["singlem_fraction"] == pytest.approx(77.14)
        assert data["PR00001"]["nonpareil_C"]      == pytest.approx(0.84)

    def test_na_becomes_none(self, tmp_path: Path):
        p = tmp_path / "preprocessing.tsv"
        p.write_text(DRAKKAR_PREPROCESSING_TSV)
        data = parse_drakkar_stats_tsv(p)
        assert data["PR00002"]["host_reads"]       is None
        assert data["PR00002"]["singlem_fraction"] is None

    def test_missing_file_returns_empty(self, tmp_path: Path):
        data = parse_drakkar_stats_tsv(tmp_path / "nonexistent.tsv")
        assert data == {}

    def test_all_samples_present(self, tmp_path: Path):
        p = tmp_path / "preprocessing.tsv"
        p.write_text(DRAKKAR_PREPROCESSING_TSV)
        data = parse_drakkar_stats_tsv(p)
        assert set(data.keys()) == {"PR00001", "PR00002"}



class TestParseDrakkarCatalogingTsv:
    def test_keyed_by_assembly(self, tmp_path: Path):
        p = tmp_path / "cataloging.tsv"
        p.write_text(DRAKKAR_CATALOGING_TSV)
        data = parse_drakkar_cataloging_tsv(p)
        assert set(data.keys()) == {"EHA05804", "EHA05805"}

    def test_column_names_renamed(self, tmp_path: Path):
        p = tmp_path / "cataloging.tsv"
        p.write_text(DRAKKAR_CATALOGING_TSV)
        data = parse_drakkar_cataloging_tsv(p)
        row = data["EHA05804"]
        assert row["assembly_length"]          == 43_585_842
        assert row["assembly_contigs_largest"] == 201_785
        assert row["assembly_contigs_number"]  == 10_928
        assert row["assembly_n50"]             == 5_037
        assert row["assembly_l50"]             == 1_684
        assert row["assembly_mapping_rate"]    == pytest.approx(32.89)
        assert row["bins_number"]              == 10

    def test_na_becomes_none(self, tmp_path: Path):
        p = tmp_path / "cataloging.tsv"
        p.write_text(DRAKKAR_CATALOGING_TSV)
        data = parse_drakkar_cataloging_tsv(p)
        assert data["EHA05805"]["assembly_contigs_largest"] is None
        assert data["EHA05805"]["assembly_mapping_rate"]    is None

    def test_missing_file_returns_empty(self, tmp_path: Path):
        assert parse_drakkar_cataloging_tsv(tmp_path / "missing.tsv") == {}

    def test_samples_column_preserved(self, tmp_path: Path):
        """The 'samples' column (co-assembly member list) is kept as a string."""
        p = tmp_path / "cataloging.tsv"
        p.write_text(DRAKKAR_CATALOGING_TSV)
        data = parse_drakkar_cataloging_tsv(p)
        assert data["EHA05804"]["samples"] == "EHI00001,EHI00002"


class TestParseSampleMappingRates:
    def test_coassembly_two_samples(self):
        result = parse_sample_mapping_rates("EHI00001:1.96;EHI00002:34.75")
        assert result == {"EHI00001": pytest.approx(1.96), "EHI00002": pytest.approx(34.75)}

    def test_single_sample(self):
        result = parse_sample_mapping_rates("EHI00003:55.0")
        assert result == {"EHI00003": pytest.approx(55.0)}

    def test_empty_string_returns_empty(self):
        assert parse_sample_mapping_rates("") == {}

    def test_none_like_input_returns_empty(self):
        assert parse_sample_mapping_rates(None or "") == {}

    def test_unparseable_rate_becomes_none(self):
        result = parse_sample_mapping_rates("EHI00001:NA")
        assert result["EHI00001"] is None

    def test_whitespace_stripped(self):
        result = parse_sample_mapping_rates(" EHI00001 : 12.3 ; EHI00002 : 45.6 ")
        assert "EHI00001" in result
        assert result["EHI00001"] == pytest.approx(12.3)


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
