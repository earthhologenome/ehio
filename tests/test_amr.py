"""Tests for the AMR module — manifest input, qc parsing, output and launch script."""

from __future__ import annotations

import argparse
import lzma
import urllib.error
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ehio import cli
from ehio.airtable import ATTACHMENT_MAX_BYTES, attachment_encoded_size
from ehio.drakkar import (
    AMR_MANIFEST_COLUMNS,
    normalise_amr_assembly_type,
    write_amr_manifest,
)
from ehio.metadata import (
    AMR_METRIC_KEYS,
    AMR_OUTPUT_FILES,
    parse_amr_qc_tsv,
    write_amr_output_tsv,
)
from ehio.scanning import build_script_content
from ehio.urls import DownloadError, download_url, filename_from_url


QC_TSV = (
    "assembly_id\tamrfinder_hits\tamrfinder_hits_without_coordinates\trgi_hits\t"
    "rgi_hits_without_coordinates\tmobility_regions\tamr_loci\tmulti_tool_loci\t"
    "mobility_links\tmobile_loci\n"
    "EHA00405\t12\t0\t9\t1\t4\t15\t6\t3\t2\n"
    "EHA00406\t0\t0\t0\t0\t0\t0\t0\t0\t0\n"
)


# ---------------------------------------------------------------------------
# assembly type
# ---------------------------------------------------------------------------

class TestAssemblyType:
    @pytest.mark.parametrize("raw", ["isolate", "Isolate", "ISOLATE", ["Isolate"]])
    def test_isolate_is_recognised_however_it_is_written(self, raw):
        assert normalise_amr_assembly_type(raw) == "isolate"

    @pytest.mark.parametrize("raw", ["metagenome", "Meta-genome", "Meta genome"])
    def test_metagenome_ignores_case_and_punctuation(self, raw):
        assert normalise_amr_assembly_type(raw) == "metagenome"

    @pytest.mark.parametrize("raw", ["", None, [], "nonsense"])
    def test_unset_or_unknown_falls_back_to_the_default(self, raw):
        assert normalise_amr_assembly_type(raw) == "metagenome"
        assert normalise_amr_assembly_type(raw, default="isolate") == "isolate"


# ---------------------------------------------------------------------------
# manifest
# ---------------------------------------------------------------------------

class TestWriteAmrManifest:
    def test_writes_the_columns_drakkar_reads(self, tmp_path: Path):
        path = tmp_path / "assemblies.tsv"
        write_amr_manifest(
            [{"assembly_id": "EHA00405", "assembly_path": "/data/EHA00405.fna.gz"}],
            path,
        )
        header, row = path.read_text().splitlines()
        assert header.split("\t") == AMR_MANIFEST_COLUMNS
        assert row.split("\t") == ["EHA00405", "/data/EHA00405.fna.gz", "metagenome"]

    def test_batch_type_applies_to_every_row(self, tmp_path: Path):
        path = tmp_path / "assemblies.tsv"
        rows = [
            {"assembly_id": "EHA00405", "assembly_path": "/a.fna"},
            {"assembly_id": "EHA00406", "assembly_path": "/b.fna"},
        ]
        assert write_amr_manifest(rows, path, assembly_type="isolate") == 2
        written = path.read_text().splitlines()[1:]
        assert [r.split("\t")[2] for r in written] == ["isolate", "isolate"]

    def test_rows_without_an_id_or_a_path_are_skipped(self, tmp_path: Path):
        path = tmp_path / "assemblies.tsv"
        rows = [
            {"assembly_id": "EHA00405", "assembly_path": "/a.fna"},
            {"assembly_id": "", "assembly_path": "/b.fna"},
            {"assembly_id": "EHA00407", "assembly_path": ""},
        ]
        assert write_amr_manifest(rows, path) == 1

    def test_creates_the_parent_directory(self, tmp_path: Path):
        path = tmp_path / "run" / "AMR001" / "assemblies.tsv"
        write_amr_manifest([{"assembly_id": "E1", "assembly_path": "/a.fna"}], path)
        assert path.is_file()


# ---------------------------------------------------------------------------
# amr_qc.tsv
# ---------------------------------------------------------------------------

class TestParseAmrQc:
    def test_keyed_by_assembly_id_with_numeric_values(self, tmp_path: Path):
        qc = tmp_path / "amr_qc.tsv"
        qc.write_text(QC_TSV)
        parsed = parse_amr_qc_tsv(qc)
        assert set(parsed) == {"EHA00405", "EHA00406"}
        assert parsed["EHA00405"]["amrfinder_hits"] == 12
        assert parsed["EHA00405"]["multi_tool_loci"] == 6
        assert parsed["EHA00405"]["mobile_loci"] == 2

    def test_zero_is_kept_as_a_value(self, tmp_path: Path):
        qc = tmp_path / "amr_qc.tsv"
        qc.write_text(QC_TSV)
        assert parse_amr_qc_tsv(qc)["EHA00406"]["rgi_hits"] == 0

    def test_missing_file_is_empty(self, tmp_path: Path):
        assert parse_amr_qc_tsv(tmp_path / "nope.tsv") == {}

    def test_output_tsv_holds_one_row_per_assembly(self, tmp_path: Path):
        qc = tmp_path / "amr_qc.tsv"
        qc.write_text(QC_TSV)
        out = tmp_path / "AMR001_output.tsv"
        write_amr_output_tsv(parse_amr_qc_tsv(qc), out)
        lines = out.read_text().splitlines()
        assert lines[0].startswith("assembly\tamrfinder_hits")
        assert len(lines) == 3
        # the diagnostic columns of amr_qc.tsv are not carried over
        assert "without_coordinates" not in out.read_text()


class TestMetricKeys:
    def test_every_metric_resolves_to_a_configured_field(self):
        from ehio import config as cfg

        for metric, config_key in AMR_METRIC_KEYS.items():
            assert str(cfg.get(config_key) or "").startswith("fld"), metric

    def test_qc_columns_and_metric_keys_agree(self, tmp_path: Path):
        qc = tmp_path / "amr_qc.tsv"
        qc.write_text(QC_TSV)
        columns = set(parse_amr_qc_tsv(qc)["EHA00405"])
        assert set(AMR_METRIC_KEYS) <= columns

    def test_every_output_file_has_an_attachment_field(self):
        from ehio import config as cfg

        for file_name, config_key in AMR_OUTPUT_FILES.items():
            assert str(cfg.get(config_key) or "").startswith("fld"), file_name


# ---------------------------------------------------------------------------
# downloads
# ---------------------------------------------------------------------------

class TestDownloadUrl:
    def test_writes_the_body_and_leaves_no_part_file(self, tmp_path: Path):
        dest = tmp_path / "EHA00405.fna.gz"
        with patch("urllib.request.urlopen", _fake_response(b"contigs")):
            assert download_url("https://erda/EHA00405.fna.gz", dest) == dest
        assert dest.read_bytes() == b"contigs"
        assert not (tmp_path / "EHA00405.fna.gz.part").exists()

    def test_an_existing_file_is_kept(self, tmp_path: Path):
        dest = tmp_path / "EHA00405.fna.gz"
        dest.write_bytes(b"already here")
        with patch("urllib.request.urlopen", side_effect=AssertionError("downloaded again")):
            download_url("https://erda/EHA00405.fna.gz", dest)
        assert dest.read_bytes() == b"already here"

    def test_overwrite_downloads_again(self, tmp_path: Path):
        dest = tmp_path / "EHA00405.fna.gz"
        dest.write_bytes(b"stale")
        with patch("urllib.request.urlopen", _fake_response(b"fresh")):
            download_url("https://erda/EHA00405.fna.gz", dest, overwrite=True)
        assert dest.read_bytes() == b"fresh"

    def test_http_error_is_reported_not_raised_raw(self, tmp_path: Path):
        dest = tmp_path / "EHA00405.fna.gz"
        error = urllib.error.HTTPError("u", 404, "Not Found", {}, None)
        with patch("urllib.request.urlopen", side_effect=error):
            with pytest.raises(DownloadError, match="404"):
                download_url("https://erda/EHA00405.fna.gz", dest)
        assert not dest.exists()

    def test_an_empty_body_is_rejected(self, tmp_path: Path):
        dest = tmp_path / "EHA00405.fna.gz"
        with patch("urllib.request.urlopen", _fake_response(b"")):
            with pytest.raises(DownloadError, match="empty"):
                download_url("https://erda/EHA00405.fna.gz", dest)
        assert not dest.exists()
        assert not (tmp_path / "EHA00405.fna.gz.part").exists()

    def test_filename_from_url(self):
        assert filename_from_url(
            "https://sid.erda.dk/Data/ASB/ABB0659/EHA00405.fna.gz", "x"
        ) == "EHA00405.fna.gz"
        assert filename_from_url("https://sid.erda.dk/", "fallback.fna") == "fallback.fna"


def _fake_response(body: bytes):
    """A urlopen replacement whose context manager yields `body`."""
    import io

    def _open(*_args, **_kwargs):
        response = MagicMock()
        response.__enter__ = lambda self: io.BytesIO(body)
        response.__exit__ = lambda self, *exc: False
        return response

    return _open


# ---------------------------------------------------------------------------
# attachment size guard
# ---------------------------------------------------------------------------

class TestAttachmentSize:
    def test_encoded_size_matches_base64(self):
        import base64

        for n in (0, 1, 2, 3, 4, 1000, 999_983):
            assert attachment_encoded_size(n) == len(base64.b64encode(b"x" * n))

    def test_the_limit_is_reached_below_four_megabytes_of_payload(self):
        assert attachment_encoded_size(4 * 1024 * 1024) > ATTACHMENT_MAX_BYTES
        assert attachment_encoded_size(3 * 1024 * 1024) < ATTACHMENT_MAX_BYTES


# ---------------------------------------------------------------------------
# launch script
# ---------------------------------------------------------------------------

RUN_DIR = "/projects/ehi/data/RUN/AMR001"
OUT_DIR = "/projects/ehi/data/AMR/AMR001"


class TestAmrScript:
    def _script(self, **kwargs) -> str:
        return build_script_content(
            "amr", "AMR001", RUN_DIR, OUT_DIR, "slurm", "Error", **kwargs
        )

    def test_input_step_writes_the_manifest_and_the_assemblies_directory(self):
        script = self._script()
        assert (
            f"ehio amr --input -b AMR001 -f {RUN_DIR}/AMR001_assemblies.tsv "
            f"-d {OUT_DIR}/data/assemblies" in script
        )

    def test_calls_drakkar_amr_with_the_manifest(self):
        assert (
            f"drakkar amr -f {RUN_DIR}/AMR001_assemblies.tsv -o {OUT_DIR} -p slurm"
            in self._script()
        )

    def test_requires_the_qc_summary_before_the_output_step(self):
        script = self._script()
        require = script.index(f"_ehio_require {OUT_DIR}/amr/amr_qc.tsv")
        output  = script.index("ehio amr --output")
        assert require < output

    def test_output_step_and_success_flag(self):
        script = self._script()
        assert f"ehio amr --output -b AMR001 -l {OUT_DIR}" in script
        assert script.rstrip().endswith("_EHIO_SUCCESS=1")

    def test_error_trap_targets_the_amr_module(self):
        assert "ehio set-status --module amr --batch AMR001" in self._script()

    def test_boosts_are_passed_to_drakkar(self):
        script = self._script(boost_time=2, boost_memory=3)
        assert "--time-multiplier 2 --memory-multiplier 3" in script

    def test_rerun_deletes_the_remote_directory_through_the_output_step(self):
        assert "ehio amr --output -b AMR001 -l " + OUT_DIR + " --rerun" in self._script(rerun=True)

    def test_resume_unlocks_and_reuses_an_existing_manifest(self):
        script = self._script(resume=True)
        assert f"drakkar unlock -o {OUT_DIR}" in script
        assert f"[ -s {RUN_DIR}/AMR001_assemblies.tsv ] || ehio amr --input" in script


# ---------------------------------------------------------------------------
# ehio amr --input
# ---------------------------------------------------------------------------

CFG_VALUES = {
    "EHI_BASE": "appEHI",
    "EHI_AMR_BATCH": "tblAMR",
    "EHI_ASB_ENTRY": "tblASB",
    "EHI_AMR_BATCH_CODE": "fldBATCHCODE",
    "EHI_AMR_BATCH_STATUS": "fldSTATUS",
    "EHI_AMR_BATCH_LIST_ASSEMBLIES": "fldLIST",
    "EHI_ASB_ENTRY_ASSEMBLY_URL": "fldURL",
    "EHI_ASB_ENTRY_ASSEMBLY_CODE": "fldCODE",
    "EHI_ASB_ENTRY_CODE": "fldCODE",
    "EHI_AMR_BATCH_FILE_HITS": "fldHITS",
    "EHI_AMR_BATCH_FILE_LOCI": "fldLOCI",
    "EHI_AMR_BATCH_FILE_DRUG_CLASSES": "fldDRUGS",
    "EHI_AMR_BATCH_FILE_MOBILITY": "fldMOB",
    "EHI_AMR_BATCH_FILE_MOBILITY_REGIONS": "fldREGIONS",
    "EHI_ASB_ENTRY_AMR_AMRFINDER_HITS": "fldAMRFINDER",
    "EHI_ASB_ENTRY_AMR_RGI_HITS": "fldRGI",
    "EHI_ASB_ENTRY_AMR_MOBILITY_REGIONS": "fldMOBREGIONS",
    "EHI_ASB_ENTRY_AMR_LOCI": "fldAMRLOCI",
    "EHI_ASB_ENTRY_AMR_MULTI_TOOL_LOCI": "fldMULTI",
    "EHI_ASB_ENTRY_AMR_MOBILITY_LINKS": "fldLINKS",
    "EHI_ASB_ENTRY_AMR_MOBILE_LOCI": "fldMOBILE",
    "SFTP_HOST": "erda",
    "SFTP_USER": "user",
    "SFTP_PORT": "22",
    "SFTP_REMOTE_BASE": "/Data",
    "PROCESSING_DONE_STATUS": "Done",
}


@pytest.fixture
def amr_airtable(tmp_path: Path):
    """Patch config lookups and the Airtable client for the AMR commands."""
    values = {**CFG_VALUES, "RUN_BASE": str(tmp_path / "RUN")}
    client = MagicMock()
    client.fetch_batch_record.return_value = {
        "id": "recBATCH",
        "fields": {"fldBATCHCODE": "AMR001", "fldLIST": ["recA1", "recA2"]},
    }
    entries = {
        "recA1": {"id": "recA1", "fields": {
            "fldCODE": "EHA00405",
            "fldURL": "https://erda/Data/ASB/ABB0659/EHA00405.fna.gz"}},
        "recA2": {"id": "recA2", "fields": {
            "fldCODE": "EHA00406",
            "fldURL": "https://erda/Data/ASB/ABB0659/EHA00406.fna.gz"}},
    }
    client.fetch_record_by_id.side_effect = lambda table, rec_id: entries.get(rec_id)

    with patch("ehio.airtable.AirtableClient", return_value=client), \
         patch.object(cli, "_resolve_token", return_value="tok"), \
         patch.object(cli.cfg, "get", side_effect=lambda k, d=None: values.get(k, d)), \
         patch.object(cli, "_require_cfg", side_effect=lambda k: values[k]):
        yield client


def _input_args(tmp_path: Path, **overrides) -> argparse.Namespace:
    defaults = dict(
        input=True, output=False, batch="AMR001", airtable_token=None, verbose=False,
        manifest_file=str(tmp_path / "AMR001_assemblies.tsv"),
        assemblies_dir=str(tmp_path / "assemblies"),
        redownload=False, download_timeout=600.0,
    )
    return argparse.Namespace(**{**defaults, **overrides})


class TestAmrInput:
    def test_downloads_every_assembly_and_writes_the_manifest(self, tmp_path, amr_airtable):
        args = _input_args(tmp_path)
        downloaded = []

        def _download(url, dest, timeout=None, overwrite=False):
            downloaded.append(url)
            Path(dest).parent.mkdir(parents=True, exist_ok=True)
            Path(dest).write_bytes(b"contigs")
            return Path(dest)

        with patch("ehio.urls.download_url", side_effect=_download):
            assert cli.cmd_amr(args) == 0

        assert len(downloaded) == 2
        rows = Path(args.manifest_file).read_text().splitlines()[1:]
        assert [r.split("\t")[0] for r in rows] == ["EHA00405", "EHA00406"]
        assert all(
            r.split("\t")[1].startswith(str(tmp_path / "assemblies")) for r in rows
        )
        assert all(r.split("\t")[2] == "metagenome" for r in rows)

    def test_local_paths_are_used_without_downloading(self, tmp_path, amr_airtable):
        local = tmp_path / "EHA00405.fna.gz"
        local.write_bytes(b"contigs")
        amr_airtable.fetch_record_by_id.side_effect = lambda t, rid: {
            "id": rid, "fields": {"fldCODE": "EHA00405", "fldURL": str(local)},
        } if rid == "recA1" else None
        amr_airtable.fetch_batch_record.return_value["fields"]["fldLIST"] = ["recA1"]

        args = _input_args(tmp_path)
        with patch("ehio.urls.download_url", side_effect=AssertionError("downloaded")):
            assert cli.cmd_amr(args) == 0
        assert str(local) in Path(args.manifest_file).read_text()

    def test_a_link_field_that_is_really_a_text_field_is_reported(self, tmp_path, amr_airtable, capsys):
        # EHI_AMR_BATCH_LIST_ASSEMBLIES pointed at the batch code: Airtable
        # returns a string, which would otherwise be walked character by
        # character and leave the batch looking empty
        amr_airtable.fetch_batch_record.return_value["fields"]["fldLIST"] = "AMR001"
        with pytest.raises(SystemExit):
            cli.cmd_amr(_input_args(tmp_path))
        assert "EHI_AMR_BATCH_LIST_ASSEMBLIES" in capsys.readouterr().err

    def test_a_link_field_holding_no_record_ids_is_reported(self, tmp_path, amr_airtable, capsys):
        amr_airtable.fetch_batch_record.return_value["fields"]["fldLIST"] = ["AMR001"]
        with pytest.raises(SystemExit):
            cli.cmd_amr(_input_args(tmp_path))
        assert "EHI_AMR_BATCH_LIST_ASSEMBLIES" in capsys.readouterr().err

    def test_a_batch_with_no_linked_assemblies_is_reported(self, tmp_path, amr_airtable):
        amr_airtable.fetch_batch_record.return_value["fields"]["fldLIST"] = []
        with pytest.raises(SystemExit):
            cli.cmd_amr(_input_args(tmp_path))

    def test_a_failed_download_stops_the_batch(self, tmp_path, amr_airtable):
        args = _input_args(tmp_path)
        with patch("ehio.urls.download_url", side_effect=DownloadError("HTTP 404 Not Found")):
            with pytest.raises(SystemExit):
                cli.cmd_amr(args)
        assert not Path(args.manifest_file).exists()

    def test_an_assembly_without_a_file_stops_the_batch(self, tmp_path, amr_airtable):
        amr_airtable.fetch_record_by_id.side_effect = lambda t, rid: {
            "id": rid, "fields": {"fldCODE": "EHA00405", "fldURL": ""},
        }
        with pytest.raises(SystemExit):
            cli.cmd_amr(_input_args(tmp_path))

    def test_the_download_is_named_after_the_assembly(self, tmp_path, amr_airtable):
        # two assemblies whose URLs share a basename must not collide
        amr_airtable.fetch_record_by_id.side_effect = lambda t, rid: {
            "id": rid,
            "fields": {"fldCODE": "EHA00405" if rid == "recA1" else "EHA00406",
                       "fldURL": "https://erda/Data/ASB/ABB0659/final.contigs.fa.gz"},
        }
        args = _input_args(tmp_path)
        with patch("ehio.urls.download_url", side_effect=lambda url, dest, **kw: Path(dest)):
            assert cli.cmd_amr(args) == 0
        rows = Path(args.manifest_file).read_text().splitlines()[1:]
        assert [Path(r.split("\t")[1]).name for r in rows] == [
            "EHA00405.fa.gz", "EHA00406.fa.gz",
        ]

    def test_a_file_drakkar_cannot_read_is_reported_before_the_run(self, tmp_path, amr_airtable):
        amr_airtable.fetch_record_by_id.side_effect = lambda t, rid: {
            "id": rid,
            "fields": {"fldCODE": "EHA00405", "fldURL": "https://erda/EHA00405.txt"},
        }
        with patch("ehio.urls.download_url", side_effect=AssertionError("downloaded")):
            with pytest.raises(SystemExit):
                cli.cmd_amr(_input_args(tmp_path))

    def test_an_assembly_linked_twice_is_kept_once(self, tmp_path, amr_airtable):
        amr_airtable.fetch_batch_record.return_value["fields"]["fldLIST"] = [
            "recA1", "recA1", "recA2",
        ]
        args = _input_args(tmp_path)
        with patch("ehio.urls.download_url",
                   side_effect=lambda url, dest, **kw: Path(dest)):
            assert cli.cmd_amr(args) == 0
        assert len(Path(args.manifest_file).read_text().splitlines()) == 3


# ---------------------------------------------------------------------------
# ehio amr --output
# ---------------------------------------------------------------------------

@pytest.fixture
def amr_output_dir(tmp_path: Path) -> Path:
    """Build a finished drakkar amr output directory."""
    amr = tmp_path / "out" / "amr"
    amr.mkdir(parents=True)
    (amr / "amr_qc.tsv").write_text(QC_TSV)
    (amr / "assembly_summary.tsv").write_text("assembly_id\tamrfinder_hits\nEHA00405\t12\n")
    (amr / "manifest.yaml").write_text("schema_version: drakkar-amr-manifest-v1\n")
    for name in AMR_OUTPUT_FILES:
        with lzma.open(amr / name, "wb") as fh:
            fh.write(b"assembly_id\tgene\nEHA00405\tblaTEM\n")
    return tmp_path / "out"


def _output_args(local_dir: Path, **overrides) -> argparse.Namespace:
    defaults = dict(
        input=False, output=True, batch="AMR001", airtable_token=None, verbose=False,
        local_dir=str(local_dir), host=None, user=None, port=None, identity=None,
        remote_dir=None, rerun=False, connect_timeout=300.0,
    )
    return argparse.Namespace(**{**defaults, **overrides})


@pytest.fixture
def sftp():
    transfer = MagicMock()
    transfer.upload_flat.return_value = (8, 0)
    ctx = MagicMock()
    ctx.__enter__ = lambda self: transfer
    ctx.__exit__ = lambda self, *exc: False
    with patch("ehio.transfer.SFTPTransfer", return_value=ctx):
        yield transfer


class TestAmrOutput:
    def test_writes_the_stats_to_the_assembly_records(self, amr_output_dir, amr_airtable, sftp):
        assert cli.cmd_amr(_output_args(amr_output_dir)) == 0
        table, updates = amr_airtable.update_records.call_args_list[0][0]
        assert table == "tblASB"
        by_id = {u["id"]: u["fields"] for u in updates}
        assert by_id["recA1"]["fldAMRFINDER"] == 12
        assert by_id["recA1"]["fldMULTI"] == 6
        assert by_id["recA1"]["fldMOBILE"] == 2
        assert by_id["recA2"]["fldRGI"] == 0

    def test_uploads_every_table_batch_prefixed(self, amr_output_dir, amr_airtable, sftp):
        cli.cmd_amr(_output_args(amr_output_dir))
        files, remote = sftp.upload_flat.call_args[0]
        assert remote == "/Data/AMR/AMR001"
        names = sorted(p.name for p in files)
        assert names == sorted([
            "AMR001_amr_drug_classes.tsv.xz", "AMR001_amr_hits.tsv.xz",
            "AMR001_amr_loci.tsv.xz", "AMR001_amr_mobility.tsv.xz",
            "AMR001_mobility_regions.tsv.xz", "AMR001_amr_qc.tsv.gz",
            "AMR001_assembly_summary.tsv.gz", "AMR001_amr_manifest.yaml",
        ])

    def test_attaches_the_five_tables_to_the_batch_record(self, amr_output_dir, amr_airtable, sftp):
        cli.cmd_amr(_output_args(amr_output_dir))
        attached = {
            call[0][2]: call[0][3].name
            for call in amr_airtable.upload_attachment.call_args_list
        }
        assert attached == {
            "fldDRUGS":  "AMR001_amr_drug_classes.tsv.xz",
            "fldHITS":   "AMR001_amr_hits.tsv.xz",
            "fldLOCI":   "AMR001_amr_loci.tsv.xz",
            "fldMOB":    "AMR001_amr_mobility.tsv.xz",
            "fldREGIONS": "AMR001_mobility_regions.tsv.xz",
        }

    def test_an_oversized_table_is_left_on_erda_only(self, amr_output_dir, amr_airtable, sftp):
        big = amr_output_dir / "amr" / "amr_hits.tsv.xz"
        big.write_bytes(b"x" * (4 * 1024 * 1024))
        cli.cmd_amr(_output_args(amr_output_dir))
        attached = [c[0][2] for c in amr_airtable.upload_attachment.call_args_list]
        assert "fldHITS" not in attached
        assert "fldLOCI" in attached
        # it still reaches ERDA
        assert any(p.name == "AMR001_amr_hits.tsv.xz"
                   for p in sftp.upload_flat.call_args[0][0])

    def test_an_already_attached_table_is_not_attached_twice(self, amr_output_dir, amr_airtable, sftp):
        amr_airtable.fetch_batch_record.return_value["fields"]["fldHITS"] = [
            {"filename": "AMR001_amr_hits.tsv.xz"},
        ]
        cli.cmd_amr(_output_args(amr_output_dir))
        attached = [c[0][2] for c in amr_airtable.upload_attachment.call_args_list]
        assert "fldHITS" not in attached

    def test_rerun_clears_the_attachment_fields_first(self, amr_output_dir, amr_airtable, sftp):
        cli.cmd_amr(_output_args(amr_output_dir, rerun=True))
        cleared = [
            call[0][1][0]["fields"]
            for call in amr_airtable.update_records.call_args_list
            if call[0][0] == "tblAMR" and all(v == [] for v in call[0][1][0]["fields"].values())
        ]
        assert cleared and set(cleared[0]) == {
            "fldHITS", "fldLOCI", "fldDRUGS", "fldMOB", "fldREGIONS",
        }
        sftp.remove_remote_dir.assert_called_once_with("/Data/AMR/AMR001")

    def test_temporary_copies_are_removed(self, amr_output_dir, amr_airtable, sftp):
        cli.cmd_amr(_output_args(amr_output_dir))
        left = sorted(p.name for p in (amr_output_dir / "amr").iterdir())
        assert not any(name.startswith("AMR001_") for name in left)

    def test_sets_the_batch_to_done(self, amr_output_dir, amr_airtable, sftp):
        cli.cmd_amr(_output_args(amr_output_dir))
        status_updates = [
            call[0][1][0]["fields"]
            for call in amr_airtable.update_records.call_args_list
            if call[0][0] == "tblAMR"
        ]
        assert any(f.get("fldSTATUS") == "Done" for f in status_updates)

    def test_an_unfinished_batch_is_not_marked_done(self, tmp_path, amr_airtable, sftp):
        empty = tmp_path / "empty"
        (empty / "amr").mkdir(parents=True)
        with pytest.raises(SystemExit):
            cli.cmd_amr(_output_args(empty))
        amr_airtable.update_records.assert_not_called()

    def test_writes_the_run_summary_tsv(self, amr_output_dir, amr_airtable, sftp, tmp_path):
        cli.cmd_amr(_output_args(amr_output_dir))
        summary = tmp_path / "RUN" / "AMR001" / "AMR001_output.tsv"
        assert summary.is_file()
        assert "EHA00405" in summary.read_text()
