"""The commands that write to ehi-core: what reaches the core, and what still
reaches Airtable, while batches are created in Airtable."""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ehio import cli
from tests.fake_core import FakeCoreClient, using

SFTP = {
    "SFTP_HOST": "erda",
    "SFTP_USER": "user",
    "SFTP_PORT": "22",
    "SFTP_REMOTE_BASE": "/Data",
    "CLEANUP_OUTPUT_DIR": "false",
    "PROCESSING_DONE_STATUS": "Done",
    "ERDA_SHARE_BASE": "https://erda/share",
}


def _patched(config: dict, airtable: MagicMock):
    """Airtable, the token and the config, as each command test needs them."""
    return (
        patch("ehio.airtable.AirtableClient", return_value=airtable),
        patch.object(cli, "_resolve_token", return_value="tok"),
        patch.object(cli.cfg, "get", side_effect=lambda k, d=None: config.get(k, d)),
        patch.object(cli, "_require_cfg", side_effect=lambda k: config[k]),
        patch.object(cli, "_get_drakkar_version", return_value="2.5.0"),
    )


def _sftp():
    xfer = MagicMock()
    xfer.upload.return_value = (1, 0)
    xfer.upload_flat.return_value = (1, 0)
    xfer.remote_exists.return_value = False
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=xfer)
    ctx.__exit__ = MagicMock(return_value=False)
    return patch("ehio.transfer.SFTPTransfer", return_value=ctx)


def _run(patches, call):
    for p in patches:
        p.start()
    try:
        return call()
    finally:
        for p in reversed(patches):
            p.stop()


# ---------------------------------------------------------------------------
# binning --output: new MAGs are created in the core only
# ---------------------------------------------------------------------------

BINNING_CFG = {
    **SFTP,
    "EHI_BASE": "appEHI",
    "MAG_BASE": "appMAG",
    "EHI_ASB_BATCH": "tblABB",
    "EHI_ASB_ENTRY": "tblEHA",
    "EHI_ASB_BATCH_CODE": "fldABBCODE",
    "EHI_ASB_BATCH_STATUS": "fldABBSTATUS",
    "EHI_ASB_ENTRY_BATCH": "fldABBLINK",
    "EHI_ASB_ENTRY_CODE": "fldEHA",
    "EHI_ASB_ENTRY_ASSEMBLY_LENGTH": "fldLEN",
    "MAG_ENTRY": "tblMAG",
    "MAG_ENTRY_NAME": "fldMAGNAME",
}

BIN_METADATA = (
    "genome,completeness,contamination,score,size,N50,contig_count\n"
    "EHA00405_bin_1.fa,91.2,1.1,0.9,2000000,9000,80\n"
)


@pytest.fixture
def cataloging(tmp_path: Path) -> Path:
    megahit = tmp_path / "cataloging" / "megahit" / "EHA00405"
    megahit.mkdir(parents=True)
    (megahit / "EHA00405.fna").write_text(">c\nACGT\n")
    final = tmp_path / "cataloging" / "final"
    (final / "EHA00405").mkdir(parents=True)
    (final / "EHA00405" / "EHA00405_bin_1.fa").write_text(">c\nACGT\n")
    (final / "all_bin_metadata.csv").write_text(BIN_METADATA)
    (final / "all_bin_paths.txt").write_text("cataloging/final/EHA00405/EHA00405_bin_1.fa\n")
    (tmp_path / "cataloging.tsv").write_text("assembly\tassembly_length\nEHA00405\t123456\n")
    return tmp_path


@pytest.fixture
def binning_airtable():
    client = MagicMock()
    client.fetch_batch_and_entries.return_value = (
        {"id": "recABB", "fields": {"fldABBCODE": "ABB0700"}},
        [{"id": "recEHA", "fields": {"fldEHA": "EHA00405", "fldABBLINK": ["recABB"]}}],
    )
    client.fetch_existing_values.return_value = set()
    return client


def _binning_args(local: Path) -> argparse.Namespace:
    return argparse.Namespace(
        batch="ABB0700", local_dir=str(local), airtable_token=None, core_token=None, verbose=False,
        host=None, user=None, port=None, identity=None, remote_dir=None, rerun=False,
        connect_timeout=300.0,
    )


class TestBinningOutput:
    def _run(self, cataloging, airtable, fake=None):
        patches = [*_patched(BINNING_CFG, airtable), _sftp()]
        if fake is not None:
            patches.append(using(fake))
        return _run(patches, lambda: cli._run_binning_output(_binning_args(cataloging)))

    def test_new_mags_go_to_the_core_and_not_to_airtable(self, cataloging, binning_airtable):
        fake = FakeCoreClient()
        assert self._run(cataloging, binning_airtable, fake) == 0
        binning_airtable.create_records.assert_not_called()
        [mag] = fake.rows("mags")
        assert mag["key"] == {"name": "EHA00405_bin_1.fa"}
        assert mag["values"]["completeness"] == 91.2
        assert mag["values"]["fasta_url"] == "https://erda/share/MAG/ABB0700/EHA00405_bin_1.fa.gz"
        assert mag["defaults"] == {"assembly_id": "EHA00405"}

    def test_without_the_core_mags_still_go_to_airtable(self, cataloging, binning_airtable):
        assert self._run(cataloging, binning_airtable) == 0
        created = binning_airtable.create_records.call_args[0][1]
        assert created[0]["fldMAGNAME"] == "EHA00405_bin_1.fa"

    def test_the_assembly_gets_its_metrics_and_its_fasta_url(self, cataloging, binning_airtable):
        fake = FakeCoreClient()
        self._run(cataloging, binning_airtable, fake)
        rows = fake.rows("assemblies")
        assert rows[0]["values"] == {"assembly_length": 123456}
        assert rows[0]["defaults"] == {"batch_id": "ABB0700"}
        assert any(
            row["values"].get("assembly_url") == "https://erda/share/ASB/ABB0700/EHA00405_contigs.fasta.gz"
            for row in rows
        )

    def test_the_batch_is_done_in_the_core_too(self, cataloging, binning_airtable):
        fake = FakeCoreClient()
        self._run(cataloging, binning_airtable, fake)
        done = fake.rows("assembly_batches")[-1]
        assert done["values"] == {"status": "Done", "ehio_version": cli.__version__, "drakkar_version": "2.5.0"}
        assert done["airtable_record_id"] == "recABB"

    def test_a_core_failure_on_the_mags_fails_the_batch(self, cataloging, binning_airtable):
        from ehio.core import CoreError

        fake = FakeCoreClient()
        original = fake.upsert
        fake.upsert = lambda changes: (_ for _ in ()).throw(CoreError("down")) \
            if changes and changes[0][0] == "mags" else original(changes)
        with pytest.raises(CoreError):
            self._run(cataloging, binning_airtable, fake)


# ---------------------------------------------------------------------------
# quantifying --output: mappings under Airtable's codes, and the MAGs kept
# ---------------------------------------------------------------------------

QUANTIFYING_CFG = {
    **SFTP,
    "MAG_BASE": "appMAG",
    "MAG_DMB_BATCH": "tblDMB",
    "MAG_DMB_ENTRY": "tblDM",
    "MAG_PPR": "tblPPR",
    "MAG_DMB_BATCH_CODE": "fldDMBCODE",
    "MAG_DMB_BATCH_LIST_PPR": "fldPPRS",
    "MAG_DMB_BATCH_STATUS": "fldDMBSTATUS",
    "MAG_PPR_EHI": "fldPPREHI",
    "MAG_PPR_CODE": "fldPPRCODE",
    "MAG_DMB_ENTRY_BATCH": "fldDMLINK",
    "MAG_DMB_ENTRY_PPR": "fldDMPPR",
    "MAG_DMB_ENTRY_CODE": "fldDMCODE",
    "MAG_DMB_ENTRY_MAPPING_RATE": "fldRATE",
}


@pytest.fixture
def profiling(tmp_path: Path) -> Path:
    (tmp_path / "profiling_genomes.tsv").write_text("sample\tmapping_percentage\nEHI00001\t71.5\n")
    final = tmp_path / "profiling_genomes" / "final"
    final.mkdir(parents=True)
    (final / "counts.tsv").write_text("genome\tEHI00001\nEHA00405_bin_1\t10\n")
    return tmp_path


@pytest.fixture
def quantifying_airtable():
    client = MagicMock()
    client.fetch_batch_record.return_value = {"id": "recDMB", "fields": {"fldDMBCODE": "DMB0300", "fldPPRS": ["recP1"]}}
    client.fetch_record_by_id.side_effect = lambda table, rec_id: {
        "id": "recP1", "fields": {"fldPPREHI": ["EHI00001"], "fldPPRCODE": "PR00001"},
    }
    client._table.return_value.all.return_value = []
    client.create_records.return_value = [
        {"id": "recDM1", "fields": {"fldDMPPR": ["recP1"], "fldDMCODE": "DM00042"}},
    ]
    return client


class TestQuantifyingOutput:
    def test_mappings_keep_airtables_codes_and_kept_mags_are_marked(self, profiling, quantifying_airtable):
        fake = FakeCoreClient([
            {"code": "EHM000001", "name": "EHA00405_bin_1.fa"},
            {"code": "EHM000002", "name": "EHA00405_bin_2.fa"},
        ])
        args = _binning_args(profiling)
        args.batch = "DMB0300"
        patches = [*_patched(QUANTIFYING_CFG, quantifying_airtable), _sftp(), using(fake)]
        assert _run(patches, lambda: cli._run_quantifying_output(args)) == 0

        [mapping] = fake.rows("dereplication_mappings")
        assert mapping["key"] == {"batch_id": "DMB0300", "preprocessing_id": "PR00001", "code": "DM00042"}
        assert mapping["values"] == {"mapping_rate": 71.5}
        assert fake.links == [("DMB0300", [], ["EHM000001"])]
        assert fake.rows("dereplication_batches")[-1]["values"]["status"] == "Done"


# ---------------------------------------------------------------------------
# set-status and stop
# ---------------------------------------------------------------------------

class TestStatus:
    def test_set_status_reaches_the_core(self):
        airtable = MagicMock()
        airtable.fetch_batch_record.return_value = {"id": "recDMB", "fields": {}}
        fake = FakeCoreClient()
        args = argparse.Namespace(
            module="quantifying", batch="DMB0300", status="Annotating taxonomy",
            failures_dir=None, failures_since=None, airtable_token=None, core_token=None,
        )
        patches = [*_patched({}, airtable), using(fake)]
        patches[3] = patch.object(cli, "_require_cfg", side_effect=lambda k: f"<{k}>")
        assert _run(patches, lambda: cli.cmd_set_status(args)) == 0
        [row] = fake.rows("dereplication_batches")
        assert row["key"] == {"code": "DMB0300"}
        assert row["values"] == {"status": "Annotating taxonomy"}

    def test_a_core_that_is_down_does_not_stop_a_status_change(self, capsys):
        from ehio.core import CoreError

        airtable = MagicMock()
        airtable.fetch_batch_record.return_value = {"id": "recPRB", "fields": {}}
        fake = FakeCoreClient()
        fake.upsert = MagicMock(side_effect=CoreError("ehi-core is down"))
        args = argparse.Namespace(
            module="preprocessing", batch="PRB0001", status="Error",
            failures_dir=None, failures_since=None, airtable_token=None, core_token=None,
        )
        patches = [*_patched({}, airtable), using(fake)]
        patches[3] = patch.object(cli, "_require_cfg", side_effect=lambda k: f"<{k}>")
        assert _run(patches, lambda: cli.cmd_set_status(args)) == 0
        airtable.update_records.assert_called_once()
        assert "ehi-core is down" in capsys.readouterr().err

    def test_a_batch_only_the_core_holds_still_reaches_its_status(self):
        """A batch created in ehi-core has no Airtable record, and the exit trap
        of a failed run has nothing else to report its error to."""
        airtable = MagicMock()
        airtable.fetch_batch_record.return_value = None
        fake = FakeCoreClient()
        args = argparse.Namespace(
            module="preprocessing", batch="PRB0500", status="Error",
            failures_dir=None, failures_since=None, airtable_token=None, core_token=None,
        )
        patches = [*_patched({}, airtable), using(fake)]
        patches[3] = patch.object(cli, "_require_cfg", side_effect=lambda k: f"<{k}>")
        assert _run(patches, lambda: cli.cmd_set_status(args)) == 0
        airtable.update_records.assert_not_called()
        [row] = fake.rows("preprocessing_batches")
        assert (row["key"], row["values"]) == ({"code": "PRB0500"}, {"status": "Error"})

    def test_a_batch_neither_database_holds_is_reported(self, capsys):
        airtable = MagicMock()
        airtable.fetch_batch_record.return_value = None
        fake = FakeCoreClient()   # every upsert comes back as "created"
        args = argparse.Namespace(
            module="preprocessing", batch="PRB9999", status="Error",
            failures_dir=None, failures_since=None, airtable_token=None, core_token=None,
        )
        patches = [*_patched({}, airtable), using(fake)]
        patches[3] = patch.object(cli, "_require_cfg", side_effect=lambda k: f"<{k}>")
        assert _run(patches, lambda: cli.cmd_set_status(args)) == 0
        assert "held no batch 'PRB9999' either" in capsys.readouterr().err

    def test_without_the_core_a_batch_airtable_lacks_is_still_an_error(self):
        airtable = MagicMock()
        airtable.fetch_batch_record.return_value = None
        args = argparse.Namespace(
            module="preprocessing", batch="PRB0500", status="Error",
            failures_dir=None, failures_since=None, airtable_token=None, core_token=None,
        )
        patches = [*_patched({}, airtable)]
        patches[3] = patch.object(cli, "_require_cfg", side_effect=lambda k: f"<{k}>")
        with pytest.raises(SystemExit):
            _run(patches, lambda: cli.cmd_set_status(args))

    def test_a_failure_report_needs_an_airtable_record_to_attach_to(self, capsys):
        airtable = MagicMock()
        airtable.fetch_batch_record.return_value = None
        fake = FakeCoreClient()
        args = argparse.Namespace(
            module="preprocessing", batch="PRB0500", status="Error",
            failures_dir="/tmp/logging", failures_since=None,
            airtable_token=None, core_token=None,
        )
        patches = [*_patched({}, airtable), using(fake)]
        patches[3] = patch.object(cli, "_require_cfg", side_effect=lambda k: f"<{k}>")
        with patch.object(cli, "_upload_failure_report") as upload:
            assert _run(patches, lambda: cli.cmd_set_status(args)) == 0
        upload.assert_not_called()
        assert "needs an Airtable record" in capsys.readouterr().err
