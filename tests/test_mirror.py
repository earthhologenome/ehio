"""Tests for what ehio writes to ehi-core (ehio.mirror)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ehio import mirror

CFG = {
    "ERDA_SHARE_BASE": "https://sid.erda.dk/share_redirect/X/",
    "EHI_PPR_BATCH_STATUS": "fldPPRSTATUS",
    "EHI_PPR_BATCH_DATE": "fldPPRDATE",
    "EHI_PPR_BATCH_BOOST_TIME": "fldPPRBOOST",
    "EHI_PPR_ENTRY_CODE": "fldPRCODE",
    "EHI_PPR_ENTRY_EHI_NUMBER": "fldEHI",
    "EHI_PPR_ENTRY_RAW_FILE_FORWARD": "fldRAW1",
    "EHI_PPR_ENTRY_RAW_FILE_REVERSE": "fldRAW2",
    "EHI_ASB_ENTRY_CODE": "fldEHA",
    "EHI_ASB_ENTRY_BATCH": "fldABB",
    "EHI_ASB_ENTRY_PREPROCESSING": "fldPR",
    "MAG_DMB_BATCH_TYPE": "fldDMBTYPE",
    "MAG_DMB_BATCH_ANNOTATION_TYPE": "fldANNTYPE",
    "MAG_ENTRY_CODE": "fldMAGCODE",
    "MAG_ENTRY_NAME": "fldMAGNAME",
    "MAG_ENTRY_ASSEMBLY": "fldMAGASB",
    "MAG_ENTRY_CHECKM_COMPLETENESS": "fldCOMP",
    "MAG_ENTRY_URL_FASTA": "fldFASTA",
    "MAG_ENTRY_ANNOTATED": "fldANN",
}


@pytest.fixture(autouse=True)
def config():
    with patch.object(mirror.cfg, "get", side_effect=lambda k, d=None: CFG.get(k, d)):
        yield


def only_row(unit):
    [(table, [row])] = unit
    return table, row


class TestCell:
    def test_a_link_gives_its_first_record(self):
        assert mirror.cell({"fldEHI": ["EHI00001", "EHI00002"]}, "EHI_PPR_ENTRY_EHI_NUMBER") == "EHI00001"

    def test_an_unconfigured_field_gives_nothing(self):
        assert mirror.cell({"fldX": 1}, "NOT_A_KEY") is None

    def test_airtables_nan_gives_nothing(self):
        assert mirror.cell({"fldCOMP": {"specialValue": "NaN"}}, "MAG_ENTRY_CHECKM_COMPLETENESS") is None


class TestRow:
    def test_empty_values_are_left_out(self):
        row = mirror.row("PR00001", values={"host_reads": None, "reads_pre_fastp": 0}, defaults={"batch_id": ""})
        assert row == {"key": {"code": "PR00001"}, "values": {"reads_pre_fastp": 0}, "defaults": {}}

    def test_the_airtable_id_travels_with_the_row(self):
        assert mirror.row("PRB0001", airtable_id="recB")["airtable_record_id"] == "recB"


class TestBatch:
    def test_values_are_set_and_airtable_facts_are_defaults(self):
        record = {"id": "recB", "fields": {"fldPPRSTATUS": "Ready", "fldPPRDATE": "2026-09-01", "fldPPRBOOST": 2}}
        table, row = only_row(mirror.batch("preprocessing", "PRB0042", record, status="Done", ehio_version="0.9.0"))
        assert table == "preprocessing_batches"
        assert row["key"] == {"code": "PRB0042"}
        assert row["values"] == {"status": "Done", "ehio_version": "0.9.0"}
        assert row["defaults"] == {"status": "Ready", "run_on": "2026-09-01", "boost_time": 2}
        assert row["airtable_record_id"] == "recB"

    def test_a_batch_without_its_record_is_still_a_row(self):
        table, row = only_row(mirror.batch("amr", "AMR0001", status="Running"))
        assert (table, row["values"]) == ("amr_batches", {"status": "Running"})

    def test_dereplication_words_are_spelled_the_cores_way(self):
        record = {"id": "recD", "fields": {"fldDMBTYPE": "Genomes", "fldANNTYPE": "KEGG"}}
        _, row = only_row(mirror.batch("quantifying", "DMB0001", record))
        assert row["defaults"] == {"batch_type": "genome", "annotation_type": "kegg"}


class TestPreprocessing:
    ENTRY = {"id": "recPR1", "fields": {
        "fldPRCODE": "PR00001", "fldEHI": ["EHI00007"],
        "fldRAW1": ["https://raw/1.fq.gz"], "fldRAW2": ["https://raw/2.fq.gz"],
    }}

    def test_a_preprocessing_comes_with_its_hologenome(self):
        [unit] = mirror.preprocessings("PRB0001", [self.ENTRY], {"PR00001": {"nonpareil_C": 0.8, "host_reads": 10}})
        (holo_table, [holo]), (ppr_table, [ppr]) = unit
        assert (holo_table, holo["key"]) == ("hologenomes", {"code": "EHI00007"})
        assert holo["defaults"] == {"forward_url": "https://raw/1.fq.gz", "reverse_url": "https://raw/2.fq.gz"}
        assert ppr_table == "preprocessings"
        assert ppr["values"] == {"nonpareil_c": 0.8, "host_reads": 10}
        assert ppr["defaults"] == {"batch_id": "PRB0001", "hologenome_id": "EHI00007"}
        assert ppr["airtable_record_id"] == "recPR1"

    def test_the_file_urls_are_those_of_the_files_on_erda(self):
        [unit] = mirror.preprocessing_files("PRB0001", {"PR00001": "EHI00007"}, {"EHI00007_M_1.fq.gz", "EHI00007_G.bam"})
        _, row = only_row(unit)
        assert row["values"] == {
            "forward_url": "https://sid.erda.dk/share_redirect/X/PPR/PRB0001/EHI00007_M_1.fq.gz",
            "bam_url": "https://sid.erda.dk/share_redirect/X/PPR/PRB0001/EHI00007_G.bam",
        }

    def test_no_files_no_row(self):
        assert mirror.preprocessing_files("PRB0001", {"PR00001": "EHI00007"}, set()) == []


class TestAssemblies:
    def test_an_assembly_links_its_batch(self):
        """The samples it is built from are a link of its own, set for the
        whole batch at once, because a coassembly has several."""
        entry = {"id": "recA", "fields": {"fldEHA": "EHA00405", "fldPR": ["recPR1"]}}
        [unit] = mirror.assemblies("ABB0001", [entry], {"EHA00405": {"assembly_n50": 2100, "bins_number": 3}})
        _, row = only_row(unit)
        assert row["values"] == {"n50": 2100, "num_bins": 3}
        assert row["defaults"] == {"batch_id": "ABB0001"}

    def test_the_fasta_url_keeps_the_erda_name(self):
        [unit] = mirror.assembly_files("ABB0001", {"EHA00405": "EHA00405_contigs.fasta.gz"})
        _, row = only_row(unit)
        assert row["values"]["assembly_url"].endswith("/ASB/ABB0001/EHA00405_contigs.fasta.gz")


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


class TestAirtableMags:
    def test_an_airtable_mag_keeps_its_code_and_fills_the_cores_gaps(self):
        record = {"id": "recM1", "fields": {
            "fldMAGCODE": "EHM000123", "fldMAGNAME": "EHA00405_bin_1.fa", "fldMAGASB": ["recA"],
            "fldCOMP": 88.0, "fldFASTA": "https://erda/x.fa.gz", "fldANN": "genes",
        }}
        [unit] = mirror.airtable_mags([record])
        _, row = only_row(unit)
        assert row["key"] == {"name": "EHA00405_bin_1.fa", "code": "EHM000123"}
        assert row["values"] == {"annotated": True}
        assert row["defaults"] == {
            "assembly_id": "recA", "completeness": 88.0,
            "fasta_url": "https://erda/x.fa.gz", "annotation_level": "genes",
        }
        assert row["airtable_record_id"] == "recM1"

    def test_the_legacy_true_is_a_full_annotation(self):
        assert mirror.annotation_level("true") == "all"
        assert mirror.annotation_level("") is None
        assert mirror.annotation_level("maybe") is None

    def test_an_unannotated_mag_is_not_marked_annotated(self):
        [unit] = mirror.airtable_mags([{"id": "recM2", "fields": {"fldMAGNAME": "a.fa"}}])
        assert only_row(unit)[1]["values"] == {}

    def test_both_sources_give_the_same_shape(self):
        from_airtable = mirror.mag_from_airtable({"id": "recM1", "fields": {"fldMAGNAME": "a.fa", "fldANN": "All"}})
        from_core = mirror.mag_from_core({"code": "EHM1", "name": "a.fa", "annotation_level": "all",
                                          "airtable_record_id": "recM1"})
        assert set(from_airtable) == set(from_core)
        assert from_airtable["annotation_level"] == from_core["annotation_level"] == "all"


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


class TestMappingsAndAmr:
    def test_a_mapping_is_found_by_batch_and_preprocessing(self):
        [unit] = mirror.mappings("DMB0001", [("PR00001", "DM00009", 71.5), (None, "DM00010", 60.0)])
        _, row = only_row(unit)
        assert row["key"] == {"batch_id": "DMB0001", "preprocessing_id": "PR00001", "code": "DM00009"}
        assert row["values"] == {"mapping_rate": 71.5}

    def test_amr_links_the_assembly_to_its_batch_with_its_gene_calls(self):
        record = {"id": "recA", "fields": {"fldEHA": "EHA00405", "fldABB": ["recABB"]}}
        [unit] = mirror.amr_assemblies("AMR0003", [record], {"EHA00405": {"rgi_hits": 4}}, {"EHA00405.faa.gz"})
        _, row = only_row(unit)
        assert row["values"] == {
            "amr_batch_id": "AMR0003", "amr_rgi_hits": 4,
            "faa_url": "https://sid.erda.dk/share_redirect/X/AMR/AMR0003/genes/EHA00405.faa.gz",
        }
        assert row["defaults"] == {"batch_id": "recABB"}
