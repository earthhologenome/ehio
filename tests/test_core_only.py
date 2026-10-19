"""Core-only batches run through input and output commands using a fake ehi-core."""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ehio import cli
from tests.fake_core import FakeCoreClient, using


def run(config: dict, fake: FakeCoreClient, call):
    patches = [
        patch.object(cli.cfg, "get", side_effect=lambda k, d=None: config.get(k, d)),
        patch.object(cli, "_require_cfg", side_effect=lambda k: config.get(k) or f"<{k}>"),
        using(fake),
    ]
    for p in patches:
        p.start()
    try:
        return call()
    finally:
        for p in reversed(patches):
            p.stop()


CONFIG = {}

def rows(path: Path) -> list[list[str]]:
    return [line.split("\t") for line in path.read_text().strip().splitlines()]


# --- preprocessing ----------------------------------------------------------------


@pytest.fixture
def ppr_core():
    return FakeCoreClient(batches={"PRB0021": {
        "row": {"code": "PRB0021", "status": "Ready", "reference_genome_code": "G0007"},
        "entries": [
            {"code": "PR00021", "hologenome_code": "EHI00011",
             "raw_forward_url": "https://erda/EHI00011_1.fq.gz",
             "raw_reverse_url": "https://erda/EHI00011_2.fq.gz",
             "reference_genome_code": "G0007"},
            {"code": "PR00022", "hologenome_code": "EHI00012",
             "raw_forward_url": "https://erda/EHI00012_1.fq.gz",
             "raw_reverse_url": "https://erda/EHI00012_2.fq.gz"},
        ],
    }})


def test_a_preprocessing_batch_only_the_core_holds_writes_its_sample_sheet(tmp_path, ppr_core):
    out = tmp_path / "samples.tsv"
    args = argparse.Namespace(batch="PRB0021", sample_file=str(out), input=True,
                              no_url_check=True, core_token=None)
    assert run(CONFIG, ppr_core,
               lambda: cli._run_preprocessing_input(args)) == 0
    assert rows(out) == [
        ["sample", "rawreads1", "rawreads2"],
        ["PR00021", "https://erda/EHI00011_1.fq.gz", "https://erda/EHI00011_2.fq.gz"],
        ["PR00022", "https://erda/EHI00012_1.fq.gz", "https://erda/EHI00012_2.fq.gz"],
    ]


def test_the_raw_reads_are_the_ones_written_not_what_preprocessing_produces(tmp_path):
    fake = FakeCoreClient(batches={"PRB0021": {"row": {"code": "PRB0021"}, "entries": [
        {"code": "PR00021", "raw_forward_url": "https://erda/raw_1.fq.gz",
         "raw_reverse_url": "https://erda/raw_2.fq.gz",
         "forward_url": "https://erda/PPR/done_1.fq.gz"},
    ]}})
    out = tmp_path / "samples.tsv"
    args = argparse.Namespace(batch="PRB0021", sample_file=str(out), input=True,
                              no_url_check=True, core_token=None)
    assert run(CONFIG, fake, lambda: cli._run_preprocessing_input(args)) == 0
    assert rows(out)[1][1] == "https://erda/raw_1.fq.gz"


def test_a_batch_neither_database_holds_is_reported(tmp_path, ppr_core):
    args = argparse.Namespace(batch="PRB9999", sample_file=str(tmp_path / "s.tsv"), input=True,
                              no_url_check=True, core_token=None)
    with pytest.raises(SystemExit):
        run(CONFIG, ppr_core, lambda: cli._run_preprocessing_input(args))


# --- binning ----------------------------------------------------------------------


def test_a_coassembly_the_core_holds_becomes_one_sample_sheet_row_per_library(tmp_path):
    fake = FakeCoreClient(batches={"ABB0021": {
        "row": {"code": "ABB0021", "batch_type": "Coassembly"},
        "entries": [
            {"assembly_code": "EHA00021", "hologenome_code": "EHI00011",
             "forward_url": "https://erda/PPR/EHI00011_M_1.fq.gz",
             "reverse_url": "https://erda/PPR/EHI00011_M_2.fq.gz"},
            {"assembly_code": "EHA00021", "hologenome_code": "EHI00012",
             "forward_url": "https://erda/PPR/EHI00012_M_1.fq.gz",
             "reverse_url": "https://erda/PPR/EHI00012_M_2.fq.gz"},
        ],
    }})
    out = tmp_path / "samples.tsv"
    args = argparse.Namespace(batch="ABB0021", sample_file=str(out), input=True,
                              reads1_field=None, reads2_field=None,
                              core_token=None)
    assert run(CONFIG, fake, lambda: cli._run_binning_input(args)) == 0
    assert rows(out) == [
        ["sample", "assembly", "rawreads1", "rawreads2"],
        ["EHI00011", "EHA00021", "https://erda/PPR/EHI00011_M_1.fq.gz",
         "https://erda/PPR/EHI00011_M_2.fq.gz"],
        ["EHI00012", "EHA00021", "https://erda/PPR/EHI00012_M_1.fq.gz",
         "https://erda/PPR/EHI00012_M_2.fq.gz"],
    ]


def test_the_batch_type_is_checked_against_the_grouping_from_the_core_too(tmp_path, capsys):
    """The check that keeps a mismatch from surfacing as an empty drakkar run
    reads the batch type from ehi-core."""
    fake = FakeCoreClient(batches={"ABB0022": {
        "row": {"code": "ABB0022", "batch_type": "Individual"},
        "entries": [
            {"assembly_code": "EHA00022", "hologenome_code": "EHI00011",
             "forward_url": "https://erda/a_1.fq.gz", "reverse_url": "https://erda/a_2.fq.gz"},
            {"assembly_code": "EHA00022", "hologenome_code": "EHI00012",
             "forward_url": "https://erda/b_1.fq.gz", "reverse_url": "https://erda/b_2.fq.gz"},
        ],
    }})
    args = argparse.Namespace(batch="ABB0022", sample_file=str(tmp_path / "s.tsv"), input=True,
                              reads1_field=None, reads2_field=None,
                              core_token=None)
    assert run(CONFIG, fake, lambda: cli._run_binning_input(args)) == 0
    err = capsys.readouterr().err
    assert "Batch assembly type: individual" in err
    assert "shared by several entries (EHA00022)" in err


# --- amr --------------------------------------------------------------------------


def test_an_amr_batch_the_core_holds_stages_its_assemblies(tmp_path):
    assembly = tmp_path / "EHA00021.fna"
    assembly.write_text(">contig\nACGT\n")
    fake = FakeCoreClient(batches={"AMR0021": {
        "row": {"code": "AMR0021"},
        "entries": [{"code": "EHA00021", "assembly_url": str(assembly)}],
    }})
    manifest = tmp_path / "assemblies.tsv"
    args = argparse.Namespace(batch="AMR0021", manifest_file=str(manifest), input=True,
                              assemblies_dir=str(tmp_path / "staged"), redownload=False,
                              download_timeout=600.0, core_token=None)
    assert run(CONFIG, fake, lambda: cli._run_amr_input(args)) == 0
    assert rows(manifest)[1][0] == "EHA00021"


# --- quantifying ------------------------------------------------------------------


def test_a_dmb_batch_the_core_holds_writes_its_reads_and_mags(tmp_path):
    fake = FakeCoreClient(
        mags=[{"code": "EHM000001", "name": "EHA00021_bin.1.fa",
               "fasta_url": "https://erda/MAG/EHM000001.fa.gz",
               "completeness": 92.5, "contamination": 1.2, "is_representative": True}],
        batches={"DMB0021": {
            "row": {"code": "DMB0021", "annotation_type": "kegg"},
            "entries": [{"code": "DM000021", "preprocessing_code": "PR00021",
                         "hologenome_code": "EHI00011",
                         "forward_url": "https://erda/PPR/EHI00011_M_1.fq.gz",
                         "reverse_url": "https://erda/PPR/EHI00011_M_2.fq.gz"}],
        }},
    )
    reads = tmp_path / "reads.tsv"
    mags = tmp_path / "mags.tsv"
    quality = tmp_path / "quality.tsv"
    args = argparse.Namespace(batch="DMB0021", input=True, reads_file=str(reads),
                              mags_file=str(mags), quality_file=str(quality),
                              core_token=None)
    assert run(CONFIG, fake, lambda: cli._run_quantifying_input(args)) == 0
    assert rows(reads)[1][0] == "EHI00011"
    assert mags.read_text().strip() == "https://erda/MAG/EHM000001.fa.gz"


# --- core-only dereplication -------------------------------------------------------

DEREP_MAGS = [
    {"code": "EHM000001", "name": "EHA00021_bin_1.fa", "fasta_url": "https://erda/MAG/EHA00021_bin_1.fa.gz",
     "completeness": 92.5, "contamination": 1.2},
    {"code": "EHM000002", "name": "EHA00021_bin_2.fa", "fasta_url": "https://erda/MAG/EHA00021_bin_2.fa.gz",
     "completeness": 80.0, "contamination": 3.0},
]


def test_a_batch_that_only_dereplicates_needs_no_samples(tmp_path):
    fake = FakeCoreClient(mags=DEREP_MAGS, batches={"DMB0040": {"row": {"code": "DMB0040"}, "entries": []}})
    mags, quality = tmp_path / "mags.tsv", tmp_path / "quality.tsv"
    args = argparse.Namespace(batch="DMB0040", input=True, no_reads=True, reads_file=str(tmp_path / "reads.tsv"),
                              mags_file=str(mags), quality_file=str(quality),
                              core_token=None)
    assert run(CONFIG, fake, lambda: cli._run_quantifying_input(args)) == 0
    assert mags.read_text().split() == [m["fasta_url"] for m in DEREP_MAGS]
    assert len(rows(quality)) == 3
    assert not (tmp_path / "reads.tsv").exists()


def _derep_output(tmp_path: Path) -> Path:
    tables = tmp_path / "dereplicating" / "drep" / "data_tables"
    tables.mkdir(parents=True)
    (tables / "Wdb.csv").write_text("genome,cluster,score\nEHA00021_bin_1.fa,1_0,2.3\n")
    (tmp_path / "dereplicating.tsv").write_text(
        "input_bin_number\toutput_bin_number\n2\t1\n")
    return tmp_path


def test_derep_output_records_what_drep_kept(tmp_path):
    fake = FakeCoreClient(mags=DEREP_MAGS, batches={"DMB0040": {"row": {"code": "DMB0040", "drakkar_version": "2.6.1"}}})
    args = argparse.Namespace(batch="DMB0040", derep_output=True, input=False, output=False,
                              local_dir=str(_derep_output(tmp_path)), core_token=None)
    with patch.object(cli, "_get_drakkar_version", return_value="2.6.7"):
        assert run(CONFIG, fake, lambda: cli.cmd_quantifying(args)) == 0
    assert fake.links == [("DMB0040", [], ["EHM000001"])]
    batch = fake.rows("dereplication_batches")[-1]["values"]
    assert batch["drakkar_version"] == "2.6.1/2.6.7"
    # The script sets the status: the annotation may still be to come.
    assert "status" not in batch


def test_derep_output_without_a_winners_table_fails(tmp_path):
    fake = FakeCoreClient(mags=DEREP_MAGS, batches={"DMB0040": {"row": {"code": "DMB0040"}}})
    args = argparse.Namespace(batch="DMB0040", derep_output=True, input=False, output=False,
                              local_dir=str(tmp_path), core_token=None)
    with pytest.raises(SystemExit):
        run(CONFIG, fake, lambda: cli.cmd_quantifying(args))
    assert fake.links == []


def test_profiling_with_skip_derep_marks_no_mag_as_kept(tmp_path):
    (tmp_path / "profiling_genomes.tsv").write_text("sample\tmapping_percentage\nEHI00011\t71.5\n")
    final = tmp_path / "profiling_genomes" / "final"
    final.mkdir(parents=True)
    (final / "counts.tsv").write_text("genome\tEHI00011\nEHA00021_bin_1\t10\nEHA00021_bin_2\t5\n")
    fake = FakeCoreClient(mags=DEREP_MAGS, batches={"DMB0040": {
        "row": {"code": "DMB0040"},
        "entries": [{"code": "PR00021", "preprocessing_code": "PR00021", "hologenome_code": "EHI00011"}],
    }})
    args = argparse.Namespace(batch="DMB0040", output=True, input=False, skip_derep=True, rerun=False,
                              local_dir=str(tmp_path), core_token=None,
                              host=None, user=None, port=None, identity=None, remote_dir=None, verbose=False)
    config = {**CONFIG, "SFTP_HOST": "erda", "SFTP_USER": "u", "SFTP_REMOTE_BASE": "/Data",
              "CLEANUP_OUTPUT_DIR": "false"}
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=MagicMock(upload_flat=MagicMock(return_value=(3, 0))))
    ctx.__exit__ = MagicMock(return_value=False)
    with patch.object(cli, "_get_drakkar_version", return_value="2.6.7"), \
         patch("ehio.transfer.SFTPTransfer", return_value=ctx):
        assert run(config, fake, lambda: cli._run_quantifying_output(args)) == 0
    assert fake.links == []
    assert fake.rows("dereplication_mappings")[0]["values"] == {"mapping_rate": 71.5}
