"""Tests for the drakkar annotating outputs ehio reads back into Airtable."""

from __future__ import annotations

import csv
from pathlib import Path

from ehio.metadata import (
    drakkar_mag_id,
    find_gene_tables,
    parse_annotation_tsv,
    parse_genome_taxonomy_tsv,
)

# The long-form gene table drakkar has written since 2.0.0: one row per
# accepted hit, every gene carrying a 'prodigal' row of its own.
GENE_COLUMNS = [
    "mag", "gene", "contig", "start", "end", "strand", "source", "method",
    "evidence", "hit_rank", "is_primary", "rank_score", "rank_score_type",
    "annotation_id", "annotation", "annotation_type", "evalue", "bitscore",
    "score", "score_type", "threshold", "identity", "coverage",
    "query_coverage", "target_coverage", "confidence", "alignment_length",
    "query_start", "query_end", "target_start", "target_end", "model_start",
    "model_end", "details",
]


def gene_row(gene: str, contig: str, start: int, end: int, source: str,
             annotation_id: str = "", hit_rank: int = 1) -> dict[str, str]:
    row = {column: "" for column in GENE_COLUMNS}
    row.update(
        mag="MAG_A", gene=gene, contig=contig, start=str(start), end=str(end),
        strand="+", source=source, hit_rank=str(hit_rank),
        is_primary="True" if hit_rank == 1 else "False",
        annotation_id=annotation_id, details="{}",
    )
    return row


def write_gene_table(path: Path, rows: list[dict[str, str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=GENE_COLUMNS, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    return path


# ---------------------------------------------------------------------------
# drakkar_mag_id
# ---------------------------------------------------------------------------

class TestDrakkarMagId:
    def test_strips_fa(self):
        assert drakkar_mag_id("EHA00123_bin_1.fa") == "EHA00123_bin_1"

    def test_strips_fna_and_fasta(self):
        assert drakkar_mag_id("genome.fna") == "genome"
        assert drakkar_mag_id("genome.fasta") == "genome"

    def test_strips_a_gzipped_suffix(self):
        assert drakkar_mag_id("genome.fna.gz") == "genome"

    def test_keeps_dots_inside_the_name(self):
        assert drakkar_mag_id("EHA00123_bin.1.fa") == "EHA00123_bin.1"

    def test_name_without_a_suffix_is_unchanged(self):
        assert drakkar_mag_id("EHA00123_bin_1") == "EHA00123_bin_1"

    def test_empty_value(self):
        assert drakkar_mag_id("") == ""


# ---------------------------------------------------------------------------
# parse_annotation_tsv — long-form table (drakkar >= 2.0.0)
# ---------------------------------------------------------------------------

class TestParseGeneTableLongForm:
    def _table(self, tmp_path: Path) -> Path:
        """Three genes: one with two KEGG hits and a Pfam hit, one with CAZy,
        one with nothing but its gene call."""
        return write_gene_table(tmp_path / "MAG_A_genes.tsv", [
            gene_row("c1_1", "c1", 1, 900, "prodigal", "CDS"),
            gene_row("c1_1", "c1", 1, 900, "kegg", "K00001"),
            gene_row("c1_1", "c1", 1, 900, "kegg", "K00002", hit_rank=2),
            gene_row("c1_1", "c1", 1, 900, "pfam", "PF00005"),
            gene_row("c1_2", "c1", 1000, 1600, "prodigal", "CDS"),
            gene_row("c1_2", "c1", 1000, 1600, "cazy", "GH5"),
            gene_row("c2_1", "c2", 1, 500, "prodigal", "CDS"),
        ])

    def test_genes_are_counted_once_each(self, tmp_path: Path):
        """Seven rows, three genes."""
        assert parse_annotation_tsv(self._table(tmp_path))["genes_number"] == 3

    def test_kegg_genes_are_counted_once_per_gene(self, tmp_path: Path):
        """One gene with two accepted KEGG hits is one KEGG-annotated gene."""
        assert parse_annotation_tsv(self._table(tmp_path))["genes_kegg"] == 1

    def test_a_gene_with_only_a_prodigal_row_is_unannotated(self, tmp_path: Path):
        assert parse_annotation_tsv(self._table(tmp_path))["genes_unannotated"] == 1

    def test_coding_density(self, tmp_path: Path):
        """Gene lengths 900 + 601 + 500 over contig ends 1600 + 500."""
        result = parse_annotation_tsv(self._table(tmp_path))
        assert result["coding_density"] == round(2001 / 2100, 6)

    def test_coding_density_stays_below_one(self, tmp_path: Path):
        """Counting hit rows instead of genes used to push it far above 1."""
        assert parse_annotation_tsv(self._table(tmp_path))["coding_density"] <= 1

    def test_pfam_and_cazy_count_as_annotated(self, tmp_path: Path):
        table = write_gene_table(tmp_path / "MAG_A_genes.tsv", [
            gene_row("c1_1", "c1", 1, 900, "prodigal", "CDS"),
            gene_row("c1_1", "c1", 1, 900, "pfam", "PF00005"),
            gene_row("c1_2", "c1", 1000, 1600, "prodigal", "CDS"),
            gene_row("c1_2", "c1", 1000, 1600, "cazy", "GH5"),
        ])
        result = parse_annotation_tsv(table)
        assert result["genes_unannotated"] == 0
        assert result["genes_kegg"] == 0

    def test_other_sources_do_not_count_as_a_functional_annotation(self, tmp_path: Path):
        """A signal peptide or a virulence hit is not what this field counts,
        matching the kegg/ec/pfam/cazy columns of the table it replaced."""
        table = write_gene_table(tmp_path / "MAG_A_genes.tsv", [
            gene_row("c1_1", "c1", 1, 900, "prodigal", "CDS"),
            gene_row("c1_1", "c1", 1, 900, "signalp", "SP"),
            gene_row("c1_1", "c1", 1, 900, "vfdb", "VF0001"),
        ])
        result = parse_annotation_tsv(table)
        assert result["genes_number"] == 1
        assert result["genes_unannotated"] == 1

    def test_the_contig_column_is_used(self, tmp_path: Path):
        """Gene ids no longer have to be split to find the contig."""
        table = write_gene_table(tmp_path / "MAG_A_genes.tsv", [
            gene_row("gene_one", "contig_a", 1, 500, "prodigal", "CDS"),
            gene_row("gene_two", "contig_a", 600, 1000, "prodigal", "CDS"),
        ])
        # One contig ending at 1000: (500 + 401) / 1000
        assert parse_annotation_tsv(table)["coding_density"] == round(901 / 1000, 6)

    def test_rows_without_coordinates(self, tmp_path: Path):
        table = write_gene_table(tmp_path / "MAG_A_genes.tsv", [
            {**gene_row("c1_1", "c1", 0, 0, "prodigal", "CDS"), "start": "", "end": ""},
        ])
        result = parse_annotation_tsv(table)
        assert result["genes_number"] == 1
        assert result["coding_density"] is None

    def test_missing_file(self, tmp_path: Path):
        result = parse_annotation_tsv(tmp_path / "absent_genes.tsv")
        assert result == {
            "coding_density": None, "genes_number": None,
            "genes_unannotated": None, "genes_kegg": None,
        }

    def test_table_with_only_a_header(self, tmp_path: Path):
        table = write_gene_table(tmp_path / "MAG_A_genes.tsv", [])
        assert parse_annotation_tsv(table)["genes_number"] is None


# ---------------------------------------------------------------------------
# parse_annotation_tsv — wide table (drakkar < 2.0.0)
# ---------------------------------------------------------------------------

class TestParseGeneTableWideForm:
    def _table(self, tmp_path: Path) -> Path:
        path = tmp_path / "MAG_A_genes.tsv"
        path.write_text(
            "gene\tstart\tend\tstrand\tkegg\tec\tpfam\tcazy\n"
            "c1_1\t1\t900\t+\tK00001\t1.1.1.1\tPF00005\t\n"
            "c1_2\t1000\t1600\t+\t\t\t\tGH5\n"
            "c2_1\t1\t500\t+\t\t\t\t\n"
        )
        return path

    def test_genes_are_counted(self, tmp_path: Path):
        assert parse_annotation_tsv(self._table(tmp_path))["genes_number"] == 3

    def test_kegg_and_unannotated_genes(self, tmp_path: Path):
        result = parse_annotation_tsv(self._table(tmp_path))
        assert result["genes_kegg"] == 1
        assert result["genes_unannotated"] == 1

    def test_coding_density(self, tmp_path: Path):
        result = parse_annotation_tsv(self._table(tmp_path))
        assert result["coding_density"] == round(2001 / 2100, 6)


# ---------------------------------------------------------------------------
# genome_taxonomy.tsv
# ---------------------------------------------------------------------------

class TestParseGenomeTaxonomy:
    def _table(self, tmp_path: Path) -> Path:
        path = tmp_path / "genome_taxonomy.tsv"
        path.write_text(
            "user_genome\tclassification\tclosest_genome_ani\tclosest_genome_af\t"
            "closest_placement_ani\n"
            "EHA00123_bin_1\td__Bacteria;p__Bacillota;c__Clostridia;o__Lachnospirales;"
            "f__Lachnospiraceae;g__Blautia;s__\t95.2\t0.81\t94.7\n"
        )
        return path

    def test_keys_match_the_mag_entry_name(self, tmp_path: Path):
        data = parse_genome_taxonomy_tsv(self._table(tmp_path))
        assert list(data) == ["EHA00123_bin_1.fa"]

    def test_ranks_are_split(self, tmp_path: Path):
        entry = parse_genome_taxonomy_tsv(self._table(tmp_path))["EHA00123_bin_1.fa"]
        assert entry["domain"] == "Bacteria"
        assert entry["genus"] == "Blautia"
        assert entry["species"] is None

    def test_gtdb_metrics(self, tmp_path: Path):
        entry = parse_genome_taxonomy_tsv(self._table(tmp_path))["EHA00123_bin_1.fa"]
        assert entry["gtdb_fastani"] == 95.2
        assert entry["gtdb_closest_af"] == 0.81
        assert entry["gtdb_closest_ani"] == 94.7


# ---------------------------------------------------------------------------
# find_gene_tables
# ---------------------------------------------------------------------------

class TestFindGeneTables:
    def _final(self, tmp_path: Path) -> Path:
        final = tmp_path / "annotating" / "final"
        final.mkdir(parents=True)
        write_gene_table(final / "EHA00123_bin_1_genes.tsv", [])
        write_gene_table(final / "EHA00123_bin_2_genes.tsv", [])
        (final / "EHA00123_bin_1_clusters.tsv").write_text("mag\tcluster_id\tsource\n")
        (final / "EHA00123_bin_1_genes.qc.json").write_text("{}")
        return final

    def test_keys_are_the_drakkar_mag_ids(self, tmp_path: Path):
        assert sorted(find_gene_tables(self._final(tmp_path))) == [
            "EHA00123_bin_1", "EHA00123_bin_2",
        ]

    def test_a_mag_id_matches_the_mag_entry_name(self, tmp_path: Path):
        tables = find_gene_tables(self._final(tmp_path))
        assert drakkar_mag_id("EHA00123_bin_1.fa") in tables

    def test_cluster_tables_are_not_gene_tables(self, tmp_path: Path):
        """They have a schema of their own and would parse as an empty genome."""
        tables = find_gene_tables(self._final(tmp_path))
        assert not any("clusters" in mag_id for mag_id in tables)

    def test_qc_files_are_ignored(self, tmp_path: Path):
        tables = find_gene_tables(self._final(tmp_path))
        assert all(path.suffix == ".tsv" for path in tables.values())

    def test_missing_directory(self, tmp_path: Path):
        assert find_gene_tables(tmp_path / "absent") == {}
