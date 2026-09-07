"""Tests for the drakkar annotating outputs ehio reads back into Airtable."""

from __future__ import annotations

import csv
from pathlib import Path

from ehio.cli import _merge_drakkar_versions
from ehio.metadata import (
    drakkar_mag_id,
    find_gene_tables,
    parse_annotation_tsv,
    parse_counts_genomes,
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


# ---------------------------------------------------------------------------
# parse_counts_genomes — the dereplicated catalogue of a finished DMB batch
# ---------------------------------------------------------------------------

class TestParseCountsGenomes:
    def _write(self, path: Path, text: str) -> Path:
        path.write_text(text, encoding="utf-8")
        return path

    def test_reads_the_first_column(self, tmp_path):
        counts = self._write(tmp_path / "c.tsv", (
            "genome\tEHI001\tEHI002\n"
            "EHA00123_bin_1\t10\t20\n"
            "EHA00124_bin_3\t30\t40\n"
        ))
        assert parse_counts_genomes(counts) == ["EHA00123_bin_1", "EHA00124_bin_3"]

    def test_strips_the_fasta_suffix(self, tmp_path):
        counts = self._write(tmp_path / "c.tsv", (
            "genome\tEHI001\n"
            "EHA00123_bin_1.fa\t10\n"
        ))
        assert parse_counts_genomes(counts) == ["EHA00123_bin_1"]

    def test_r_style_header_keeps_the_first_genome(self, tmp_path):
        # An R-written table starts with an empty row-name cell; dropping the
        # header must not take the first genome with it.
        counts = self._write(tmp_path / "c.tsv", (
            "\tEHI001\tEHI002\n"
            "EHA00123_bin_1\t10\t20\n"
        ))
        assert parse_counts_genomes(counts) == ["EHA00123_bin_1"]

    def test_reads_gzip(self, tmp_path):
        import gzip
        counts = tmp_path / "DMB0157_counts.tsv.gz"
        with gzip.open(counts, "wt", encoding="utf-8") as fh:
            fh.write("genome\tEHI001\nEHA00123_bin_1\t10\n")
        assert parse_counts_genomes(counts) == ["EHA00123_bin_1"]

    def test_duplicates_kept_once_in_order(self, tmp_path):
        counts = self._write(tmp_path / "c.tsv", (
            "genome\tEHI001\n"
            "b\t1\n"
            "a\t2\n"
            "b\t3\n"
        ))
        assert parse_counts_genomes(counts) == ["b", "a"]

    def test_blank_lines_ignored(self, tmp_path):
        counts = self._write(tmp_path / "c.tsv", "genome\tEHI001\na\t1\n\nb\t2\n")
        assert parse_counts_genomes(counts) == ["a", "b"]

    def test_missing_file_is_empty(self, tmp_path):
        assert parse_counts_genomes(tmp_path / "nope.tsv") == []

    def test_header_only_is_empty(self, tmp_path):
        counts = self._write(tmp_path / "c.tsv", "genome\tEHI001\tEHI002\n")
        assert parse_counts_genomes(counts) == []

    def test_truncated_gzip_is_empty(self, tmp_path):
        counts = tmp_path / "c.tsv.gz"
        counts.write_bytes(b"\x1f\x8b\x08\x00truncated")
        assert parse_counts_genomes(counts) == []


# ---------------------------------------------------------------------------
# _merge_drakkar_versions — a re-annotation must not erase the profiling run
# ---------------------------------------------------------------------------

class TestMergeDrakkarVersions:
    def test_appends_the_new_version(self):
        assert _merge_drakkar_versions("2.4.4", "2.5.0") == "2.4.4/2.5.0"

    def test_keeps_every_recorded_version(self):
        assert _merge_drakkar_versions("2.4.4/2.4.5", "2.5.0") == "2.4.4/2.4.5/2.5.0"

    def test_same_version_twice_is_recorded_once(self):
        assert _merge_drakkar_versions("2.5.0", "2.5.0") == "2.5.0"
        assert _merge_drakkar_versions("2.4.4/2.5.0", "2.5.0") == "2.4.4/2.5.0"

    def test_empty_record_gives_the_new_version(self):
        assert _merge_drakkar_versions("", "2.5.0") == "2.5.0"
        assert _merge_drakkar_versions(None, "2.5.0") == "2.5.0"


# ---------------------------------------------------------------------------
# ehio annotating --stage — the dereplicated genomes of a finished DMB batch,
# put back on disk from the counts table on ERDA and the MAG records
# ---------------------------------------------------------------------------

import argparse
import gzip
from unittest.mock import MagicMock, patch

import pytest

from ehio import cli

STAGE_CFG = {
    "MAG_BASE": "appMAG",
    "MAG_DMB_BATCH": "tblDMB",
    "MAG_ENTRY": "tblMAG",
    "MAG_DMB_BATCH_CODE": "fldBATCHCODE",
    "MAG_DMB_BATCH_LIST_MAGS": "fldMAGS",
    "MAG_ENTRY_NAME": "fldNAME",
    "MAG_ENTRY_URL_FASTA": "fldFASTA",
    "SFTP_HOST": "erda",
    "SFTP_USER": "user",
    "SFTP_PORT": "22",
    "SFTP_REMOTE_BASE": "/Data",
}

# Two of the three linked MAGs survived dereplication.
COUNTS_TABLE = (
    "genome\tEHI001\tEHI002\n"
    "EHA00123_bin_1\t10\t20\n"
    "EHA00124_bin_3\t30\t40\n"
)


@pytest.fixture
def stage_airtable():
    client = MagicMock()
    client.fetch_batch_record.return_value = {
        "id": "recBATCH",
        "fields": {"fldBATCHCODE": "DMB0157", "fldMAGS": ["recM1", "recM2", "recM3"]},
    }
    mags = {
        "recM1": {"id": "recM1", "fields": {
            "fldNAME": "EHA00123_bin_1.fa",
            "fldFASTA": "https://erda/Data/MAG/ABB0659/EHA00123_bin_1.fa.gz"}},
        "recM2": {"id": "recM2", "fields": {
            "fldNAME": "EHA00124_bin_3.fa",
            "fldFASTA": "https://erda/Data/MAG/ABB0659/EHA00124_bin_3.fa.gz"}},
        # Linked to the batch but dereplicated away — never staged.
        "recM3": {"id": "recM3", "fields": {
            "fldNAME": "EHA00125_bin_2.fa",
            "fldFASTA": "https://erda/Data/MAG/ABB0659/EHA00125_bin_2.fa.gz"}},
    }
    client.fetch_record_by_id.side_effect = lambda table, rec_id: mags.get(rec_id)

    with patch("ehio.airtable.AirtableClient", return_value=client), \
         patch.object(cli, "_resolve_token", return_value="tok"), \
         patch.object(cli.cfg, "get", side_effect=lambda k, d=None: STAGE_CFG.get(k, d)), \
         patch.object(cli, "_require_cfg", side_effect=lambda k: STAGE_CFG[k]):
        yield client


def _stage_args(tmp_path: Path, **overrides) -> argparse.Namespace:
    defaults = dict(
        stage=True, input=False, output=False, batch="DMB0157",
        airtable_token=None, verbose=False,
        annotation_dir=str(tmp_path / "dereplicated_genomes"),
        annotation_file="annotation.tsv", genomes_file=None,
        redownload=False, download_timeout=600.0, connect_timeout=300.0,
        host=None, user=None, port=None, identity=None, remote_dir=None,
        local_dir=str(tmp_path), rerun=False, reannotate=False,
    )
    return argparse.Namespace(**{**defaults, **overrides})


def _fake_sftp(counts_body: str | None = COUNTS_TABLE):
    """An SFTPTransfer whose download writes the batch's counts table."""
    xfer = MagicMock()

    def _download(remote_path, local_path, verbose=False):
        if counts_body is None:
            raise FileNotFoundError(remote_path)
        with gzip.open(local_path, "wt", encoding="utf-8") as fh:
            fh.write(counts_body)
        return Path(local_path)

    xfer.download.side_effect = _download
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=xfer)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, xfer


def _fake_download(url, dest, timeout=None, overwrite=False):
    Path(dest).parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(dest, "wb") as fh:
        fh.write(b">contig_1\nACGT\n")
    return Path(dest)


class TestAnnotatingStage:
    def test_stages_only_the_dereplicated_genomes(self, tmp_path, stage_airtable):
        ctx, _ = _fake_sftp()
        args = _stage_args(tmp_path)
        with patch("ehio.transfer.SFTPTransfer", return_value=ctx), \
             patch("ehio.urls.download_url", side_effect=_fake_download):
            assert cli.cmd_annotating(args) == 0

        staged = sorted(p.name for p in Path(args.annotation_dir).glob("*"))
        # The third linked MAG is not in the counts table, so it is not staged.
        assert staged == ["EHA00123_bin_1.fa", "EHA00124_bin_3.fa"]

    def test_staged_genomes_are_decompressed(self, tmp_path, stage_airtable):
        ctx, _ = _fake_sftp()
        args = _stage_args(tmp_path)
        with patch("ehio.transfer.SFTPTransfer", return_value=ctx), \
             patch("ehio.urls.download_url", side_effect=_fake_download):
            cli.cmd_annotating(args)
        text = (Path(args.annotation_dir) / "EHA00123_bin_1.fa").read_text()
        assert text.startswith(">contig_1")

    def test_the_counts_table_of_the_batch_is_read(self, tmp_path, stage_airtable):
        ctx, xfer = _fake_sftp()
        with patch("ehio.transfer.SFTPTransfer", return_value=ctx), \
             patch("ehio.urls.download_url", side_effect=_fake_download):
            cli.cmd_annotating(_stage_args(tmp_path))
        remote = xfer.download.call_args[0][0]
        assert remote == "/Data/DMB/DMB0157/DMB0157_counts.tsv.gz"

    def test_a_genome_already_staged_is_not_downloaded_again(self, tmp_path, stage_airtable):
        args = _stage_args(tmp_path)
        derep = Path(args.annotation_dir)
        derep.mkdir(parents=True)
        (derep / "EHA00123_bin_1.fa").write_text(">already\nACGT\n")

        ctx, _ = _fake_sftp()
        fetched = []

        def _record(url, dest, timeout=None, overwrite=False):
            fetched.append(url)
            return _fake_download(url, dest, timeout, overwrite)

        with patch("ehio.transfer.SFTPTransfer", return_value=ctx), \
             patch("ehio.urls.download_url", side_effect=_record):
            assert cli.cmd_annotating(args) == 0
        assert fetched == ["https://erda/Data/MAG/ABB0659/EHA00124_bin_3.fa.gz"]
        assert (derep / "EHA00123_bin_1.fa").read_text() == ">already\nACGT\n"

    def test_redownload_fetches_everything_again(self, tmp_path, stage_airtable):
        args = _stage_args(tmp_path, redownload=True)
        derep = Path(args.annotation_dir)
        derep.mkdir(parents=True)
        (derep / "EHA00123_bin_1.fa").write_text(">stale\nACGT\n")

        ctx, _ = _fake_sftp()
        with patch("ehio.transfer.SFTPTransfer", return_value=ctx), \
             patch("ehio.urls.download_url", side_effect=_fake_download):
            assert cli.cmd_annotating(args) == 0
        assert (derep / "EHA00123_bin_1.fa").read_text().startswith(">contig_1")

    def test_genomes_file_replaces_the_counts_table(self, tmp_path, stage_airtable):
        listing = tmp_path / "genomes.txt"
        listing.write_text("EHA00124_bin_3.fa\n\nEHA00125_bin_2\n")
        args = _stage_args(tmp_path, genomes_file=str(listing))
        with patch("ehio.transfer.SFTPTransfer", side_effect=AssertionError("connected")), \
             patch("ehio.urls.download_url", side_effect=_fake_download):
            assert cli.cmd_annotating(args) == 0
        staged = sorted(p.name for p in Path(args.annotation_dir).glob("*"))
        assert staged == ["EHA00124_bin_3.fa", "EHA00125_bin_2.fa"]

    def test_a_missing_counts_table_is_reported(self, tmp_path, stage_airtable, capsys):
        ctx, _ = _fake_sftp(counts_body=None)
        with patch("ehio.transfer.SFTPTransfer", return_value=ctx):
            with pytest.raises(SystemExit):
                cli.cmd_annotating(_stage_args(tmp_path))
        err = capsys.readouterr().err
        assert "DMB0157_counts.tsv.gz" in err
        assert "--genomes-file" in err

    def test_an_empty_counts_table_is_reported(self, tmp_path, stage_airtable, capsys):
        ctx, _ = _fake_sftp(counts_body="genome\tEHI001\n")
        with patch("ehio.transfer.SFTPTransfer", return_value=ctx):
            with pytest.raises(SystemExit):
                cli.cmd_annotating(_stage_args(tmp_path))
        assert "no genome names" in capsys.readouterr().err

    def test_a_genome_with_no_mag_record_is_reported(self, tmp_path, stage_airtable, capsys):
        ctx, _ = _fake_sftp(counts_body="genome\tEHI001\nEHA00999_bin_9\t1\n")
        with patch("ehio.transfer.SFTPTransfer", return_value=ctx), \
             patch("ehio.urls.download_url", side_effect=_fake_download):
            with pytest.raises(SystemExit):
                cli.cmd_annotating(_stage_args(tmp_path))
        assert "EHA00999_bin_9" in capsys.readouterr().err

    def test_every_failure_is_reported_at_once(self, tmp_path, stage_airtable, capsys):
        from ehio.urls import DownloadError
        ctx, _ = _fake_sftp()
        with patch("ehio.transfer.SFTPTransfer", return_value=ctx), \
             patch("ehio.urls.download_url", side_effect=DownloadError("HTTP 404 Not Found")):
            with pytest.raises(SystemExit):
                cli.cmd_annotating(_stage_args(tmp_path))
        err = capsys.readouterr().err
        assert "EHA00123_bin_1" in err and "EHA00124_bin_3" in err
        assert "HTTP 404 Not Found" in err

    def test_a_local_fasta_path_is_copied(self, tmp_path, stage_airtable):
        local = tmp_path / "EHA00123_bin_1.fa"
        local.write_text(">local\nACGT\n")
        stage_airtable.fetch_record_by_id.side_effect = lambda t, rid: {
            "id": rid, "fields": {"fldNAME": "EHA00123_bin_1.fa", "fldFASTA": str(local)},
        } if rid == "recM1" else None
        stage_airtable.fetch_batch_record.return_value["fields"]["fldMAGS"] = ["recM1"]

        ctx, _ = _fake_sftp(counts_body="genome\tEHI001\nEHA00123_bin_1\t1\n")
        args = _stage_args(tmp_path)
        with patch("ehio.transfer.SFTPTransfer", return_value=ctx), \
             patch("ehio.urls.download_url", side_effect=AssertionError("downloaded")):
            assert cli.cmd_annotating(args) == 0
        assert (Path(args.annotation_dir) / "EHA00123_bin_1.fa").read_text() == ">local\nACGT\n"

    def test_no_partial_files_are_left_behind(self, tmp_path, stage_airtable):
        ctx, _ = _fake_sftp()
        args = _stage_args(tmp_path)
        with patch("ehio.transfer.SFTPTransfer", return_value=ctx), \
             patch("ehio.urls.download_url", side_effect=_fake_download):
            cli.cmd_annotating(args)
        leftovers = [p.name for p in Path(args.annotation_dir).iterdir()
                     if not p.name.endswith(".fa")]
        assert leftovers == []


# ---------------------------------------------------------------------------
# ehio annotating --output --reannotate — the functional half only
# ---------------------------------------------------------------------------

OUTPUT_CFG = {
    **STAGE_CFG,
    "MAG_DMB_BATCH_ANNOTATION_TYPE": "fldANNTYPE",
    "MAG_DMB_BATCH_STATUS": "fldSTATUS",
    "MAG_DMB_BATCH_DRAKKAR_VERSION": "fldDRAKKAR",
    "MAG_ENTRY_DOMAIN": "fldDOMAIN",
    "MAG_ENTRY_PHYLUM": "fldPHYLUM",
    "MAG_ENTRY_GENES_NUMBER": "fldGENES",
    "MAG_ENTRY_ANNOTATED": "fldANNOTATED",
    "PROCESSING_DONE_STATUS": "Done",
}

TAXONOMY_TABLE = (
    "user_genome\tclassification\tclosest_genome_ani\tclosest_placement_ani\tclosest_genome_af\n"
    "EHA00123_bin_1\td__Bacteria;p__Firmicutes;c__;o__;f__;g__;s__\t95.0\t95.0\t0.8\n"
)


@pytest.fixture
def output_airtable():
    client = MagicMock()
    client.fetch_batch_record.return_value = {
        "id": "recBATCH",
        "fields": {
            "fldBATCHCODE": "DMB0157",
            "fldMAGS": ["recM1"],
            "fldANNTYPE": "all",
            "fldDRAKKAR": "2.4.4",
        },
    }
    client.fetch_record_by_id.side_effect = lambda t, rid: {
        "id": "recM1", "fields": {"fldNAME": "EHA00123_bin_1.fa"},
    } if rid == "recM1" else None

    with patch("ehio.airtable.AirtableClient", return_value=client), \
         patch.object(cli, "_resolve_token", return_value="tok"), \
         patch.object(cli.cfg, "get", side_effect=lambda k, d=None: OUTPUT_CFG.get(k, d)), \
         patch.object(cli, "_require_cfg", side_effect=lambda k: OUTPUT_CFG[k]):
        yield client


def _output_dir(tmp_path: Path, with_taxonomy: bool = True) -> Path:
    """A drakkar output directory holding a finished functional annotation."""
    ann = tmp_path / "out" / "annotating"
    write_gene_table(ann / "final" / "EHA00123_bin_1_genes.tsv", [
        gene_row("g1", "c1", 1, 30, "prodigal"),
        gene_row("g1", "c1", 1, 30, "kegg", annotation_id="K00001"),
    ])
    if with_taxonomy:
        (ann / "genome_taxonomy.tsv").write_text(TAXONOMY_TABLE, encoding="utf-8")
        (ann / "bacteria.tree").write_text("(a,b);", encoding="utf-8")
    return tmp_path / "out"


def _out_args(local_dir: Path, **overrides) -> argparse.Namespace:
    defaults = dict(
        stage=False, input=False, output=True, batch="DMB0157",
        airtable_token=None, verbose=False, local_dir=str(local_dir),
        annotation_dir=".", annotation_file="annotation.tsv", genomes_file=None,
        redownload=False, download_timeout=600.0, connect_timeout=300.0,
        host=None, user=None, port=None, identity=None, remote_dir=None,
        rerun=True, reannotate=True,
    )
    return argparse.Namespace(**{**defaults, **overrides})


def _uploading_sftp():
    xfer = MagicMock()
    xfer.upload_flat.return_value = (0, 0)
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=xfer)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, xfer


class TestAnnotatingOutputReannotate:
    def _run(self, tmp_path, args_overrides=None, with_taxonomy=True):
        local = _output_dir(tmp_path, with_taxonomy=with_taxonomy)
        ctx, xfer = _uploading_sftp()
        with patch("ehio.transfer.SFTPTransfer", return_value=ctx):
            assert cli.cmd_annotating(_out_args(local, **(args_overrides or {}))) == 0
        return xfer

    def test_taxonomy_is_not_written_back(self, tmp_path, output_airtable):
        # A genome_taxonomy.tsv left in the output directory by some earlier
        # run must not be parsed over the taxonomy already on the records.
        self._run(tmp_path)
        written = output_airtable.update_records.call_args_list[0][0][1]
        fields = written[0]["fields"]
        assert "fldDOMAIN" not in fields
        assert "fldPHYLUM" not in fields

    def test_gene_metrics_are_written(self, tmp_path, output_airtable):
        self._run(tmp_path)
        fields = output_airtable.update_records.call_args_list[0][0][1][0]["fields"]
        assert fields["fldGENES"] == 1
        assert fields["fldANNOTATED"] == "all"

    def test_the_taxonomy_table_and_trees_are_not_re_uploaded(self, tmp_path, output_airtable):
        xfer = self._run(tmp_path)
        uploaded = [p.name for call in xfer.upload_flat.call_args_list for p in call[0][0]]
        assert not any("genome_taxonomy" in n for n in uploaded)
        assert "bacteria.tree" not in uploaded

    def test_without_reannotate_taxonomy_is_written(self, tmp_path, output_airtable):
        self._run(tmp_path, {"reannotate": False})
        fields = output_airtable.update_records.call_args_list[0][0][1][0]["fields"]
        assert fields["fldDOMAIN"] == "Bacteria"
        assert fields["fldPHYLUM"] == "Firmicutes"

    def test_without_reannotate_the_taxonomy_table_is_uploaded(self, tmp_path, output_airtable):
        xfer = self._run(tmp_path, {"reannotate": False})
        uploaded = [p.name for call in xfer.upload_flat.call_args_list for p in call[0][0]]
        assert any("genome_taxonomy" in n for n in uploaded)
        assert "bacteria.tree" in uploaded

    def test_the_recorded_drakkar_version_is_kept(self, tmp_path, output_airtable):
        with patch.object(cli, "_get_drakkar_version", return_value="2.5.0"):
            self._run(tmp_path)
        batch_update = output_airtable.update_records.call_args_list[-1][0][1][0]
        assert batch_update["fields"]["fldDRAKKAR"] == "2.4.4/2.5.0"

    def test_without_reannotate_the_version_is_replaced(self, tmp_path, output_airtable):
        with patch.object(cli, "_get_drakkar_version", return_value="2.5.0"):
            self._run(tmp_path, {"reannotate": False})
        batch_update = output_airtable.update_records.call_args_list[-1][0][1][0]
        assert batch_update["fields"]["fldDRAKKAR"] == "2.5.0"

    def test_the_batch_is_marked_done(self, tmp_path, output_airtable):
        self._run(tmp_path)
        batch_update = output_airtable.update_records.call_args_list[-1][0][1][0]
        assert batch_update["fields"]["fldSTATUS"] == "Done"

    def test_an_output_without_taxonomy_still_finishes(self, tmp_path, output_airtable):
        # The normal shape of a re-annotation output directory: GTDB-Tk never
        # ran, so there is no taxonomy table to leave alone in the first place.
        self._run(tmp_path, with_taxonomy=False)
        fields = output_airtable.update_records.call_args_list[0][0][1][0]["fields"]
        assert fields["fldGENES"] == 1
