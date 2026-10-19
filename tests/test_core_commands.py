"""Commands that read core-only batches and write their results to ehi-core."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ehio import cli
from ehio.core import CoreError
from tests.fake_core import FakeCoreClient, using

CONFIG = {
    "SFTP_HOST": "erda", "SFTP_USER": "user", "SFTP_PORT": "22",
    "SFTP_REMOTE_BASE": "/Data", "ERDA_SHARE_BASE": "https://erda/share",
    "CLEANUP_OUTPUT_DIR": "false", "PROCESSING_DONE_STATUS": "Done",
}


@contextmanager
def _configured(fake: FakeCoreClient, config: dict | None = None):
    values = {**CONFIG, **(config or {})}
    with patch.object(cli.cfg, "get", side_effect=lambda k, d=None: values.get(k, d)), \
         patch.object(cli, "_require_cfg", side_effect=lambda k: values[k]), \
         patch.object(cli, "_get_drakkar_version", return_value="2.5.0"), using(fake):
        yield


@contextmanager
def _sftp():
    transfer = MagicMock()
    transfer.upload.return_value = (1, 0)
    transfer.upload_flat.return_value = (1, 0)
    transfer.remote_exists.return_value = False
    context = MagicMock()
    context.__enter__ = MagicMock(return_value=transfer)
    context.__exit__ = MagicMock(return_value=False)
    with patch("ehio.transfer.SFTPTransfer", return_value=context):
        yield transfer


@pytest.fixture
def cataloging(tmp_path: Path) -> Path:
    megahit = tmp_path / "cataloging" / "megahit" / "EHA00405"
    megahit.mkdir(parents=True)
    (megahit / "EHA00405.fna").write_text(">c\nACGT\n")
    final = tmp_path / "cataloging" / "final"
    (final / "EHA00405").mkdir(parents=True)
    (final / "EHA00405" / "EHA00405_bin_1.fa").write_text(">c\nACGT\n")
    (final / "all_bin_metadata.csv").write_text(
        "genome,completeness,contamination,score,size,N50,contig_count\n"
        "EHA00405_bin_1.fa,91.2,1.1,0.9,2000000,9000,80\n"
    )
    (final / "all_bin_paths.txt").write_text("cataloging/final/EHA00405/EHA00405_bin_1.fa\n")
    (tmp_path / "cataloging.tsv").write_text("assembly\tassembly_length\nEHA00405\t123456\n")
    return tmp_path


@pytest.fixture
def assembly_core():
    return FakeCoreClient(batches={"ABB0700": {
        "row": {"code": "ABB0700"},
        "entries": [
            {"assembly_code": "EHA00405", "preprocessing_code": "PR00021", "hologenome_code": "EHI00011"},
            {"assembly_code": "EHA00405", "preprocessing_code": "PR00022", "hologenome_code": "EHI00012"},
        ],
    }})


def _output_args(local: Path, batch="ABB0700") -> argparse.Namespace:
    return argparse.Namespace(
        batch=batch, local_dir=str(local), core_token=None, verbose=False,
        host=None, user=None, port=None, identity=None, remote_dir=None, rerun=False,
        connect_timeout=300.0,
    )


def _binning_output(local: Path, fake: FakeCoreClient):
    with _configured(fake), _sftp():
        return cli._run_binning_output(_output_args(local))


class TestBinningOutput:
    def test_new_mags_go_to_the_core(self, cataloging, assembly_core):
        assert _binning_output(cataloging, assembly_core) == 0
        [mag] = assembly_core.rows("mags")
        assert mag["key"] == {"name": "EHA00405_bin_1.fa"}
        assert mag["values"]["completeness"] == 91.2
        assert mag["values"]["fasta_url"] == "https://erda/share/MAG/ABB0700/EHA00405_bin_1.fa.gz"
        assert mag["defaults"] == {"assembly_id": "EHA00405"}

    def test_assembly_metrics_and_fasta_url_reach_the_core(self, cataloging, assembly_core):
        assert _binning_output(cataloging, assembly_core) == 0
        rows = assembly_core.rows("assemblies")
        assert rows[0]["values"] == {"assembly_length": 123456}
        assert rows[0]["key"] == {"code": "EHA00405"}
        assert any(row["values"].get("assembly_url") ==
                   "https://erda/share/ASB/ABB0700/EHA00405_contigs.fasta.gz" for row in rows)

    def test_each_mapping_rate_goes_to_its_preprocessing(self, cataloging, assembly_core):
        (cataloging / "cataloging.tsv").write_text(
            "assembly\tassembly_length\tsample_mapping_rates\n"
            "EHA00405\t123456\tEHI00011:61.0;EHI00012:34.5\n"
        )
        assert _binning_output(cataloging, assembly_core) == 0
        assert [(row["key"]["preprocessing_id"], row["values"])
                for row in assembly_core.rows("assembly_samples")] == [
            ("PR00021", {"mapping_percent": 61.0}),
            ("PR00022", {"mapping_percent": 34.5}),
        ]
        assert assembly_core.groupings == []

    def test_a_sample_without_a_known_preprocessing_gets_no_rate(self, cataloging, assembly_core):
        (cataloging / "cataloging.tsv").write_text(
            "assembly\tassembly_length\tsample_mapping_rates\n"
            "EHA00405\t123456\tEHI99999:87.5\n"
        )
        assert _binning_output(cataloging, assembly_core) == 0
        assert assembly_core.rows("assembly_samples") == []

    def test_the_batch_is_done(self, cataloging, assembly_core):
        assert _binning_output(cataloging, assembly_core) == 0
        done = assembly_core.rows("assembly_batches")[-1]
        assert done["key"] == {"code": "ABB0700"}
        assert done["values"] == {"status": "Done", "ehio_version": cli.__version__,
                                  "drakkar_version": "2.5.0"}

    def test_a_mag_write_failure_fails_the_batch(self, cataloging, assembly_core):
        original = assembly_core.upsert
        def fail_mags(changes):
            if changes and changes[0][0] == "mags":
                raise CoreError("down")
            return original(changes)
        assembly_core.upsert = fail_mags
        with pytest.raises(CoreError):
            _binning_output(cataloging, assembly_core)


@pytest.fixture
def profiling(tmp_path: Path) -> Path:
    (tmp_path / "profiling_genomes.tsv").write_text("sample\tmapping_percentage\nEHI00001\t71.5\n")
    final = tmp_path / "profiling_genomes" / "final"
    final.mkdir(parents=True)
    (final / "counts.tsv").write_text("genome\tEHI00001\nEHA00405_bin_1\t10\n")
    return tmp_path


def test_quantifying_mappings_and_kept_mags_reach_the_core(profiling):
    fake = FakeCoreClient(
        mags=[{"code": "EHM000001", "name": "EHA00405_bin_1.fa"},
              {"code": "EHM000002", "name": "EHA00405_bin_2.fa"}],
        batches={"DMB0300": {"row": {"code": "DMB0300"}, "entries": [
            {"code": "DM00042", "preprocessing_code": "PR00001", "hologenome_code": "EHI00001"},
        ]}},
    )
    with _configured(fake), _sftp():
        assert cli._run_quantifying_output(_output_args(profiling, "DMB0300")) == 0
    [mapping] = fake.rows("dereplication_mappings")
    assert mapping["key"] == {"batch_id": "DMB0300", "preprocessing_id": "PR00001"}
    assert mapping["values"] == {"mapping_rate": 71.5}
    assert fake.links == [("DMB0300", [], ["EHM000001"])]
    assert fake.rows("dereplication_batches")[-1]["values"]["status"] == "Done"


@pytest.mark.parametrize("module,batch,table", [
    ("preprocessing", "PRB0500", "preprocessing_batches"),
    ("binning", "ABB0729", "assembly_batches"),
    ("quantifying", "DMB0300", "dereplication_batches"),
    ("amr", "AMR001", "amr_batches"),
])
def test_set_status_reaches_core(module, batch, table):
    fake = FakeCoreClient()
    args = argparse.Namespace(module=module, batch=batch, status="Error",
                              failures_dir=None, failures_since=None, core_token=None)
    with _configured(fake):
        assert cli.cmd_set_status(args) == 0
    assert fake.rows(table)[0] == {"key": {"code": batch}, "values": {"status": "Error"}, "defaults": {}}


def test_set_status_accepts_legacy_failure_options(capsys):
    args = cli._build_parser().parse_args([
        "set-status", "--module", "preprocessing", "--batch", "PRB0500",
        "--status", "Error", "--failures-dir", "x", "--failures-since", "y",
    ])
    fake = FakeCoreClient()
    with _configured(fake):
        assert args.func(args) == 0
    assert fake.rows("preprocessing_batches")[0]["values"] == {"status": "Error"}
    with pytest.raises(SystemExit) as exc:
        cli._build_parser().parse_args(["set-status", "--help"])
    assert exc.value.code == 0
    help_text = capsys.readouterr().out
    assert "--failures-dir" not in help_text
    assert "--failures-since" not in help_text


def test_a_core_status_failure_is_reported(capsys):
    fake = FakeCoreClient()
    fake.upsert = MagicMock(side_effect=CoreError("ehi-core is down"))
    args = argparse.Namespace(module="binning", batch="ABB0729", status="Error",
                              failures_dir=None, failures_since=None, core_token=None)
    with _configured(fake), pytest.raises(CoreError):
        cli.cmd_set_status(args)
    assert "status →" not in capsys.readouterr().err


def test_set_status_reports_a_batch_missing_from_core(capsys):
    fake = FakeCoreClient()
    args = argparse.Namespace(module="preprocessing", batch="PRB9999", status="Error",
                              failures_dir=None, failures_since=None, core_token=None)
    with _configured(fake):
        assert cli.cmd_set_status(args) == 0
    assert "held no batch 'PRB9999'" in capsys.readouterr().err
    assert fake.rows("preprocessing_batches")[0]["values"] == {"status": "Error"}
