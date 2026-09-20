"""Batches ehi-core alone holds, run without Airtable.

Airtable is being retired, so a batch can be created in the core and taken all
the way through: the input files a drakkar run needs are written from the core's
own rows, and the results are written back to it. Airtable is looked in first
while it still holds today's batches; a batch it does not have is read from the
core, and emptying a module's Airtable keys leaves the core as the only place
looked at.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ehio import cli
from tests.fake_core import FakeCoreClient, using


def no_airtable() -> MagicMock:
    """An Airtable that holds nothing, as it will once the batches move."""
    client = MagicMock()
    client.fetch_batch_record.return_value = None
    client.fetch_batch_and_entries.return_value = (None, [])
    return client


def run(config: dict, airtable: MagicMock, fake: FakeCoreClient, call):
    patches = [
        patch("ehio.airtable.AirtableClient", return_value=airtable),
        patch.object(cli, "_resolve_token", return_value="tok"),
        patch.object(cli.cfg, "get", side_effect=lambda k, d=None: config.get(k, d)),
        patch.object(cli, "_require_cfg", side_effect=lambda k: config.get(k) or f"<{k}>"),
        patch("ehio.batches.cfg.get", side_effect=lambda k, d=None: config.get(k, d)),
        using(fake),
    ]
    for p in patches:
        p.start()
    try:
        return call()
    finally:
        for p in reversed(patches):
            p.stop()


CONFIG = {
    "EHI_BASE": "app1", "MAG_BASE": "app2",
    "EHI_PPR_BATCH": "tblPPR", "EHI_ASB_BATCH": "tblASB",
    "MAG_DMB_BATCH": "tblDMB", "EHI_AMR_BATCH": "tblAMR",
    "EHI_PPR_BATCH_CODE": "fldPPR", "EHI_ASB_BATCH_CODE": "fldASB",
    "MAG_DMB_BATCH_CODE": "fldDMB", "EHI_AMR_BATCH_CODE": "fldAMR",
}


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
                              no_url_check=True, airtable_token=None, core_token=None)
    assert run(CONFIG, no_airtable(), ppr_core,
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
                              no_url_check=True, airtable_token=None, core_token=None)
    assert run(CONFIG, no_airtable(), fake, lambda: cli._run_preprocessing_input(args)) == 0
    assert rows(out)[1][1] == "https://erda/raw_1.fq.gz"


def test_nothing_of_a_core_batch_is_written_back_to_airtable(tmp_path, ppr_core):
    airtable = no_airtable()
    out = tmp_path / "samples.tsv"
    args = argparse.Namespace(batch="PRB0021", sample_file=str(out), input=True,
                              no_url_check=True, airtable_token=None, core_token=None)
    run(CONFIG, airtable, ppr_core, lambda: cli._run_preprocessing_input(args))
    airtable.update_records.assert_not_called()
    airtable.create_records.assert_not_called()
    # Its rows are already in the core, so the input step has nothing to copy in.
    assert ppr_core.upserts == []


def test_a_batch_neither_database_holds_is_reported(tmp_path, ppr_core):
    args = argparse.Namespace(batch="PRB9999", sample_file=str(tmp_path / "s.tsv"), input=True,
                              no_url_check=True, airtable_token=None, core_token=None)
    with pytest.raises(SystemExit):
        run(CONFIG, no_airtable(), ppr_core, lambda: cli._run_preprocessing_input(args))


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
                              airtable_token=None, core_token=None)
    assert run(CONFIG, no_airtable(), fake, lambda: cli._run_binning_input(args)) == 0
    assert rows(out) == [
        ["sample", "assembly", "rawreads1", "rawreads2"],
        ["EHI00011", "EHA00021", "https://erda/PPR/EHI00011_M_1.fq.gz",
         "https://erda/PPR/EHI00011_M_2.fq.gz"],
        ["EHI00012", "EHA00021", "https://erda/PPR/EHI00012_M_1.fq.gz",
         "https://erda/PPR/EHI00012_M_2.fq.gz"],
    ]


def test_the_batch_type_is_checked_against_the_grouping_from_the_core_too(tmp_path, capsys):
    """The check that keeps a mismatch from surfacing as an empty drakkar run
    reads the batch type from the core the same way it reads Airtable's."""
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
                              airtable_token=None, core_token=None)
    assert run(CONFIG, no_airtable(), fake, lambda: cli._run_binning_input(args)) == 0
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
                              download_timeout=600.0, airtable_token=None, core_token=None)
    assert run(CONFIG, no_airtable(), fake, lambda: cli._run_amr_input(args)) == 0
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
                              airtable_token=None, core_token=None)
    assert run(CONFIG, no_airtable(), fake, lambda: cli._run_quantifying_input(args)) == 0
    assert rows(reads)[1][0] == "EHI00011"
    assert mags.read_text().strip() == "https://erda/MAG/EHM000001.fa.gz"


# --- what the core is told about an Airtable batch ---------------------------------


def test_an_airtable_coassembly_tells_the_core_which_samples_it_groups(tmp_path):
    """Airtable keeps one entry per sample carrying its assembly code; the core
    links the assembly to every sample, so a coassembly keeps all of them."""
    config = {**CONFIG,
              "EHI_ASB_ENTRY": "tblE", "EHI_ASB_ENTRY_BATCH": "fldB",
              "EHI_ASB_ENTRY_CODE": "fldC", "EHI_ASB_ENTRY_EHI_NUMBER": "fldEHI",
              "EHI_ASB_ENTRY_ASSEMBLY_CODE": "fldA", "EHI_ASB_ENTRY_READS1": "fldR1",
              "EHI_ASB_ENTRY_READS2": "fldR2", "EHI_ASB_ENTRY_PREPROCESSING": "fldPR",
              "EHI_ASB_BATCH_TYPE": "fldT"}
    airtable = MagicMock()
    airtable.fetch_batch_and_entries.return_value = (
        {"id": "recB", "fields": {"fldASB": "ABB0030", "fldT": "Coassembly"}},
        [
            {"id": "r1", "fields": {"fldC": "EHA00030", "fldEHI": "EHI00011", "fldA": "EHA00030",
                                    "fldR1": "https://erda/a_1.fq.gz", "fldR2": "https://erda/a_2.fq.gz", "fldPR": "PR00021"}},
            {"id": "r2", "fields": {"fldC": "EHA00030", "fldEHI": "EHI00012", "fldA": "EHA00030",
                                    "fldR1": "https://erda/b_1.fq.gz", "fldR2": "https://erda/b_2.fq.gz", "fldPR": "PR00022"}},
        ],
    )
    fake = FakeCoreClient()
    args = argparse.Namespace(batch="ABB0030", sample_file=str(tmp_path / "s.tsv"), input=True,
                              reads1_field=None, reads2_field=None,
                              airtable_token=None, core_token=None)
    run(config, airtable, fake, lambda: cli._run_binning_input(args))
    assert fake.groupings == [("ABB0030", {"EHA00030": ["PR00021", "PR00022"]})]
