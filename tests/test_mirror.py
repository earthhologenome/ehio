"""Tests for what ehio writes to ehi-core (ehio.mirror)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ehio import mirror

CFG = {"ERDA_SHARE_BASE": "https://sid.erda.dk/share_redirect/X/"}


@pytest.fixture(autouse=True)
def config():
    with patch.object(mirror.cfg, "get", side_effect=lambda k, d=None: CFG.get(k, d)):
        yield


def only_row(unit):
    [(table, [row])] = unit
    return table, row


class TestRow:
    def test_empty_values_are_left_out(self):
        row = mirror.row("PR00001", values={"host_reads": None, "reads_pre_fastp": 0}, defaults={"batch_id": ""})
        assert row == {"key": {"code": "PR00001"}, "values": {"reads_pre_fastp": 0}, "defaults": {}}


class TestBatch:

    def test_a_batch_without_its_record_is_still_a_row(self):
        table, row = only_row(mirror.batch("amr", "AMR0001", status="Running"))
        assert (table, row["values"]) == ("amr_batches", {"status": "Running"})


class TestNewMags:
    @pytest.mark.parametrize("genome", ["EHA00405_bin_1.fa", "EHA00405_bin.1.fa"])
    def test_the_assembly_is_read_from_the_bin_name(self, genome):
        assert mirror.assembly_of(genome) == "EHA00405"

    def test_a_mag_is_found_by_name_and_numbered_by_the_core(self):
        bins = [{"genome": "EHA00405_bin_1.fa", "completeness": 91.2, "size": 2_000_000, "N50": 9000, "contig_count": 80}]
        [unit] = mirror.new_mags("ABB0001", bins, {"EHA00405_bin_1.fa.gz"})
        table, row = only_row(unit)
        assert table == "mags"
        assert row["key"] == {"name": "EHA00405_bin_1.fa"}
        assert row["values"] == {
            "completeness": 91.2, "size_bp": 2_000_000, "n50": 9000, "contigs": 80,
            "fasta_url": "https://sid.erda.dk/share_redirect/X/MAG/ABB0001/EHA00405_bin_1.fa.gz",
        }
        # A default, so a MAG whose assembly the core lacks is still recorded.
        assert row["defaults"] == {"assembly_id": "EHA00405"}

    def test_no_fasta_url_for_a_bin_that_was_not_uploaded(self):
        [unit] = mirror.new_mags("ABB0001", [{"genome": "EHA00405_bin_2.fa"}], set())
        assert "fasta_url" not in only_row(unit)[1]["values"]


class TestAnnotatedMag:
    def test_annotation_results_become_core_columns(self):
        unit = mirror.annotated_mag(
            {"code": "EHM000001", "name": "a.fa"},
            {"phylum": "Firmicutes", "class_": "Bacilli", "gtdb_closest_ani": 97.1, "genes_number": 2000,
             "annotated": "kegg"},
        )
        _, row = only_row(unit)
        assert row["key"] == {"name": "a.fa", "code": "EHM000001"}
        assert row["values"] == {
            "tax_phylum": "Firmicutes", "tax_class": "Bacilli", "closest_ani": 97.1, "genes": 2000,
            "annotated": True, "annotation_level": "kegg",
        }

    def test_taxonomy_alone_does_not_mark_a_mag_annotated(self):
        _, row = only_row(mirror.annotated_mag({"code": "EHM000001", "name": "a.fa"}, {"domain": "Bacteria"}))
        assert "annotated" not in row["values"]


class TestMappings:
    def test_a_mapping_is_found_by_batch_and_preprocessing(self):
        [unit] = mirror.mappings("DMB0001", [("PR00001", "DM00009", 71.5), (None, "DM00010", 60.0)])
        _, row = only_row(unit)
        assert row["key"] == {"batch_id": "DMB0001", "preprocessing_id": "PR00001", "code": "DM00009"}
        assert row["values"] == {"mapping_rate": 71.5}
