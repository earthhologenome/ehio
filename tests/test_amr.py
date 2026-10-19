"""Tests for the AMR module — manifest input, qc parsing, output and launch script."""

from __future__ import annotations

import argparse
import lzma
import urllib.error
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ehio import cli
from ehio.drakkar import (
    AMR_MANIFEST_COLUMNS,
    normalise_amr_assembly_type,
    write_amr_manifest,
)
from ehio.metadata import (
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
    "SFTP_HOST": "erda", "SFTP_USER": "user", "SFTP_PORT": "22",
    "SFTP_REMOTE_BASE": "/Data", "PROCESSING_DONE_STATUS": "Done",
}

@pytest.fixture
def amr_core(tmp_path: Path):
    """An AMR batch and assemblies held only in ehi-core."""
    from tests.fake_core import FakeCoreClient, using

    values = {**CFG_VALUES, "RUN_BASE": str(tmp_path / "RUN")}
    fake = FakeCoreClient(batches={"AMR001": {
        "row": {"code": "AMR001"},
        "entries": [
            {"code": "EHA00405", "assembly_url": "https://erda/Data/ASB/ABB0659/EHA00405.fna.gz"},
            {"code": "EHA00406", "assembly_url": "https://erda/Data/ASB/ABB0659/EHA00406.fna.gz"},
        ],
    }})
    with patch.object(cli.cfg, "get", side_effect=lambda k, d=None: values.get(k, d)), \
         patch.object(cli, "_require_cfg", side_effect=lambda k: values[k]), \
         patch.object(cli, "_get_drakkar_version", return_value="2.5.0"), using(fake):
        yield fake


def _input_args(tmp_path: Path, **overrides) -> argparse.Namespace:
    defaults = dict(
        input=True, output=False, batch="AMR001", verbose=False,
        manifest_file=str(tmp_path / "AMR001_assemblies.tsv"),
        assemblies_dir=str(tmp_path / "assemblies"),
        redownload=False, download_timeout=600.0,
    )
    return argparse.Namespace(**{**defaults, **overrides})


class TestAmrInput:
    def test_downloads_every_assembly_and_writes_the_manifest(self, tmp_path, amr_core):
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

    def test_local_paths_are_used_without_downloading(self, tmp_path, amr_core):
        local = tmp_path / "EHA00405.fna.gz"
        local.write_bytes(b"contigs")
        amr_core.batches["AMR001"]["entries"] = [{"code": "EHA00405", "assembly_url": str(local)}]

        args = _input_args(tmp_path)
        with patch("ehio.urls.download_url", side_effect=AssertionError("downloaded")):
            assert cli.cmd_amr(args) == 0
        assert str(local) in Path(args.manifest_file).read_text()



    def test_a_batch_with_no_linked_assemblies_is_reported(self, tmp_path, amr_core):
        amr_core.batches["AMR001"]["entries"] = []
        with pytest.raises(SystemExit):
            cli.cmd_amr(_input_args(tmp_path))

    def test_a_failed_download_stops_the_batch(self, tmp_path, amr_core):
        args = _input_args(tmp_path)
        with patch("ehio.urls.download_url", side_effect=DownloadError("HTTP 404 Not Found")):
            with pytest.raises(SystemExit):
                cli.cmd_amr(args)
        assert not Path(args.manifest_file).exists()

    def test_an_assembly_without_a_file_stops_the_batch(self, tmp_path, amr_core):
        amr_core.batches["AMR001"]["entries"] = [{"code": "EHA00405", "assembly_url": ""}]
        with pytest.raises(SystemExit):
            cli.cmd_amr(_input_args(tmp_path))

    def test_the_download_is_named_after_the_assembly(self, tmp_path, amr_core):
        # two assemblies whose URLs share a basename must not collide
        for entry in amr_core.batches["AMR001"]["entries"]:
            entry["assembly_url"] = "https://erda/Data/ASB/ABB0659/final.contigs.fa.gz"
        args = _input_args(tmp_path)
        with patch("ehio.urls.download_url", side_effect=lambda url, dest, **kw: Path(dest)):
            assert cli.cmd_amr(args) == 0
        rows = Path(args.manifest_file).read_text().splitlines()[1:]
        assert [Path(r.split("\t")[1]).name for r in rows] == [
            "EHA00405.fa.gz", "EHA00406.fa.gz",
        ]

    def test_a_file_drakkar_cannot_read_is_reported_before_the_run(self, tmp_path, amr_core):
        amr_core.batches["AMR001"]["entries"] = [
            {"code": "EHA00405", "assembly_url": "https://erda/EHA00405.txt"},
        ]
        with patch("ehio.urls.download_url", side_effect=AssertionError("downloaded")):
            with pytest.raises(SystemExit):
                cli.cmd_amr(_input_args(tmp_path))

    def test_an_assembly_linked_twice_is_kept_once(self, tmp_path, amr_core):
        entries = amr_core.batches["AMR001"]["entries"]
        amr_core.batches["AMR001"]["entries"] = [entries[0], entries[0], entries[1]]
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
        input=False, output=True, batch="AMR001", verbose=False,
        local_dir=str(local_dir), host=None, user=None, port=None, identity=None,
        remote_dir=None, rerun=False, connect_timeout=300.0,
    )
    return argparse.Namespace(**{**defaults, **overrides})


@pytest.fixture
def sftp():
    transfer = MagicMock()
    transfer.upload_flat.return_value = (8, 0)
    transfer.remote_exists.return_value = False
    ctx = MagicMock()
    ctx.__enter__ = lambda self: transfer
    ctx.__exit__ = lambda self, *exc: False
    with patch("ehio.transfer.SFTPTransfer", return_value=ctx):
        yield transfer


class TestAmrOutput:
    def test_writes_the_stats_to_the_assembly_records(self, amr_output_dir, amr_core, sftp):
        assert cli.cmd_amr(_output_args(amr_output_dir)) == 0
        values = {code: {} for code in ("EHA00405", "EHA00406")}
        for row in amr_core.rows("assemblies"):
            values[row["key"]["code"]].update(row["values"])
        assert values["EHA00405"]["amr_amrfinder_hits"] == 12
        assert values["EHA00405"]["amr_multi_tool_loci"] == 6
        assert values["EHA00405"]["amr_mobile_loci"] == 2
        assert values["EHA00406"]["amr_rgi_hits"] == 0

    def test_uploads_every_table_batch_prefixed(self, amr_output_dir, amr_core, sftp):
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



    def test_a_run_without_a_manifest_still_finishes(self, amr_output_dir, amr_core, sftp):
        (amr_output_dir / "amr" / "manifest.yaml").unlink()
        assert cli.cmd_amr(_output_args(amr_output_dir)) == 0
        uploaded = [p.name for p in sftp.upload_flat.call_args[0][0]]
        assert "AMR001_amr_manifest.yaml" not in uploaded




    def test_temporary_copies_are_removed(self, amr_output_dir, amr_core, sftp):
        cli.cmd_amr(_output_args(amr_output_dir))
        left = sorted(p.name for p in (amr_output_dir / "amr").iterdir())
        assert not any(name.startswith("AMR001_") for name in left)

    def test_sets_the_batch_to_done(self, amr_output_dir, amr_core, sftp):
        cli.cmd_amr(_output_args(amr_output_dir))
        assert amr_core.rows("amr_batches")[-1]["values"]["status"] == "Done"

    def test_an_unfinished_batch_is_not_marked_done(self, tmp_path, amr_core, sftp):
        empty = tmp_path / "empty"
        (empty / "amr").mkdir(parents=True)
        with pytest.raises(SystemExit):
            cli.cmd_amr(_output_args(empty))
        assert amr_core.rows("amr_batches") == []

    def test_writes_the_run_summary_tsv(self, amr_output_dir, amr_core, sftp, tmp_path):
        cli.cmd_amr(_output_args(amr_output_dir))
        summary = tmp_path / "RUN" / "AMR001" / "AMR001_output.tsv"
        assert summary.is_file()
        assert "EHA00405" in summary.read_text()


def _with_gene_calls(output_dir: Path) -> Path:
    """Add the prodigal outputs drakkar amr leaves in amr/raw/prodigal."""
    prodigal = output_dir / "amr" / "raw" / "prodigal"
    prodigal.mkdir(parents=True)
    for assembly in ("EHA00405", "EHA00406"):
        (prodigal / f"{assembly}.faa").write_text(f">{assembly}_1\nMKV\n")
        (prodigal / f"{assembly}.ffn").write_text(f">{assembly}_1\nATGAAAGTT\n")
        (prodigal / f"{assembly}.gff").write_text("##gff-version 3\n")
        (prodigal / f"{assembly}.amrfinder.gff").write_text("##gff-version 3\n")
    return prodigal


class TestAmrGeneCalls:
    def test_faa_and_ffn_go_gzipped_to_the_genes_folder(self, amr_output_dir, amr_core, sftp):
        _with_gene_calls(amr_output_dir)
        assert cli.cmd_amr(_output_args(amr_output_dir)) == 0
        remote = sorted(c[0][0] for c in sftp.upload_stream.call_args_list)
        assert remote == [
            "/Data/AMR/AMR001/genes/EHA00405.faa.gz",
            "/Data/AMR/AMR001/genes/EHA00405.ffn.gz",
            "/Data/AMR/AMR001/genes/EHA00406.faa.gz",
            "/Data/AMR/AMR001/genes/EHA00406.ffn.gz",
        ]

    def test_the_uploaded_content_is_the_gzipped_file(self, amr_output_dir, amr_core, sftp):
        import gzip
        import io

        prodigal = _with_gene_calls(amr_output_dir)
        sent: dict[str, bytes] = {}

        def _stream(remote_path, writer, verbose=False):
            buffer = io.BytesIO()
            writer(buffer)
            sent[remote_path] = gzip.decompress(buffer.getvalue())

        sftp.upload_stream.side_effect = _stream
        cli.cmd_amr(_output_args(amr_output_dir))
        assert sent["/Data/AMR/AMR001/genes/EHA00405.ffn.gz"] == (
            prodigal / "EHA00405.ffn"
        ).read_bytes()

    def test_gff_intermediates_are_not_sent(self, amr_output_dir, amr_core, sftp):
        _with_gene_calls(amr_output_dir)
        cli.cmd_amr(_output_args(amr_output_dir))
        remote = [c[0][0] for c in sftp.upload_stream.call_args_list]
        assert not any(".gff" in path for path in remote)
        flat = [p.name for p in sftp.upload_flat.call_args[0][0]]
        assert not any(name.endswith((".faa", ".ffn", ".gff")) for name in flat)

    def test_files_already_on_erda_are_skipped(self, amr_output_dir, amr_core, sftp):
        _with_gene_calls(amr_output_dir)
        sftp.remote_exists.side_effect = lambda path: path.endswith(".faa.gz")
        cli.cmd_amr(_output_args(amr_output_dir))
        remote = sorted(c[0][0] for c in sftp.upload_stream.call_args_list)
        assert remote == [
            "/Data/AMR/AMR001/genes/EHA00405.ffn.gz",
            "/Data/AMR/AMR001/genes/EHA00406.ffn.gz",
        ]

    def test_rerun_clears_the_batch_folder_before_the_gene_calls_go_up(
            self, amr_output_dir, amr_core, sftp):
        _with_gene_calls(amr_output_dir)
        cli.cmd_amr(_output_args(amr_output_dir, rerun=True))
        calls = [c[0] for c in sftp.method_calls if c[0] in ("remove_remote_dir", "upload_stream")]
        assert calls[0] == "remove_remote_dir"
        assert calls.count("upload_stream") == 4

    def test_a_run_without_gene_calls_still_finishes(self, amr_output_dir, amr_core, sftp):
        assert cli.cmd_amr(_output_args(amr_output_dir)) == 0
        sftp.upload_stream.assert_not_called()
        sftp.upload_flat.assert_called_once()
