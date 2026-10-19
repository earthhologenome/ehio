"""Tests for ehio.reference — reference index discovery and archiving."""

from __future__ import annotations

import io
import tarfile
import argparse

from unittest.mock import MagicMock, patch

import pytest

from ehio.core import CoreError, CoreSession
from tests.fake_core import FakeCoreClient, using

from ehio.reference import (
    BT2_SUFFIXES,
    BT2L_SUFFIXES,
    find_index_sets,
    locate_references_dir,
    write_index_archive,
    upload_reference_index_status,
)


def _make_index(directory, stem="reference", suffixes=BT2_SUFFIXES, fasta=True):
    directory.mkdir(parents=True, exist_ok=True)
    if fasta:
        (directory / f"{stem}.fna").write_text(">chr1\nACGT\n", encoding="utf-8")
    for suffix in suffixes:
        (directory / f"{stem}{suffix}").write_bytes(b"index-" + suffix.encode())


# ---------------------------------------------------------------------------
# find_index_sets
# ---------------------------------------------------------------------------

class TestFindIndexSets:
    def test_finds_complete_small_index(self, tmp_path):
        _make_index(tmp_path)
        found = find_index_sets(tmp_path)
        assert len(found) == 1
        fasta, files = found[0]
        assert fasta.name == "reference.fna"
        assert [f.name for f in files] == [f"reference{s}" for s in BT2_SUFFIXES]

    def test_finds_complete_large_index(self, tmp_path):
        _make_index(tmp_path, suffixes=BT2L_SUFFIXES)
        found = find_index_sets(tmp_path)
        assert len(found) == 1
        assert [f.name for f in found[0][1]] == [f"reference{s}" for s in BT2L_SUFFIXES]

    def test_incomplete_index_is_not_returned(self, tmp_path):
        _make_index(tmp_path, suffixes=BT2_SUFFIXES[:-1])
        assert find_index_sets(tmp_path) == []

    def test_fasta_without_index_is_not_returned(self, tmp_path):
        tmp_path.mkdir(parents=True, exist_ok=True)
        (tmp_path / "reference.fna").write_text(">chr1\nACGT\n", encoding="utf-8")
        assert find_index_sets(tmp_path) == []

    def test_index_without_fasta_is_not_returned(self, tmp_path):
        _make_index(tmp_path, fasta=False)
        assert find_index_sets(tmp_path) == []

    def test_missing_directory(self, tmp_path):
        assert find_index_sets(tmp_path / "nope") == []

    def test_two_references_are_both_reported(self, tmp_path):
        _make_index(tmp_path, stem="reference")
        _make_index(tmp_path, stem="other")
        assert len(find_index_sets(tmp_path)) == 2


# ---------------------------------------------------------------------------
# write_index_archive
# ---------------------------------------------------------------------------

class TestWriteIndexArchive:
    def _archive(self, tmp_path, code="G0001", **kwargs):
        _make_index(tmp_path, **kwargs)
        fasta, files = find_index_sets(tmp_path)[0]
        buffer = io.BytesIO()
        write_index_archive(buffer, fasta, files, code)
        buffer.seek(0)
        return tarfile.open(fileobj=buffer, mode="r:gz")

    def test_members_are_renamed_to_the_genome_code(self, tmp_path):
        with self._archive(tmp_path) as tar:
            names = sorted(tar.getnames())
        assert names == sorted(["G0001.fna"] + [f"G0001{s}" for s in BT2_SUFFIXES])

    def test_large_index_suffixes_are_preserved(self, tmp_path):
        with self._archive(tmp_path, suffixes=BT2L_SUFFIXES) as tar:
            names = sorted(tar.getnames())
        assert names == sorted(["G0001.fna"] + [f"G0001{s}" for s in BT2L_SUFFIXES])

    def test_file_contents_survive(self, tmp_path):
        with self._archive(tmp_path) as tar:
            fasta = tar.extractfile("G0001.fna").read().decode()
            first = tar.extractfile("G0001.1.bt2").read()
        assert fasta == ">chr1\nACGT\n"
        assert first == b"index-.1.bt2"

    def test_no_compresslevel_is_passed_to_tarfile(self, tmp_path):
        """Python 3.11's tarfile.open forwards compresslevel to TarFile.__init__.

        Only 3.12 and later pop it for stream modes, so passing one raises
        TypeError on the interpreter the cluster runs.  This stands in for 3.11
        by rejecting the keyword outright.
        """
        real_open = tarfile.open

        def strict_open(*args, **kwargs):
            if "compresslevel" in kwargs:
                raise TypeError(
                    "TarFile.__init__() got an unexpected keyword argument 'compresslevel'"
                )
            return real_open(*args, **kwargs)

        _make_index(tmp_path)
        fasta, files = find_index_sets(tmp_path)[0]
        buffer = io.BytesIO()
        with patch("ehio.reference.tarfile.open", side_effect=strict_open):
            write_index_archive(buffer, fasta, files, "G0114")
        with tarfile.open(fileobj=io.BytesIO(buffer.getvalue()), mode="r:gz") as tar:
            assert "G0114.fna" in tar.getnames()

    def test_archive_is_readable_as_a_stream(self, tmp_path):
        """drakkar opens the tarball with tarfile — it must be a valid tar.gz."""
        _make_index(tmp_path)
        fasta, files = find_index_sets(tmp_path)[0]
        buffer = io.BytesIO()
        write_index_archive(buffer, fasta, files, "G0002")
        assert tarfile.is_tarfile(io.BytesIO(buffer.getvalue()))


# ---------------------------------------------------------------------------
# locate_references_dir
# ---------------------------------------------------------------------------

class TestLocateReferencesDir:
    def test_prefers_the_drakkar_layout(self, tmp_path):
        nested = tmp_path / "data" / "references"
        nested.mkdir(parents=True)
        assert locate_references_dir(tmp_path) == nested

    def test_falls_back_to_the_directory_itself(self, tmp_path):
        """'ehio reference -l' may be pointed straight at a references dir."""
        assert locate_references_dir(tmp_path) == tmp_path


def test_fake_core_reference_routes():
    fake = FakeCoreClient(genomes={"G0081": {"url_raw": "https://raw"}})
    assert fake.reference_genome("G0081") == {
        "code": "G0081", "species": None, "accession": None,
        "url_raw": "https://raw", "url_indexed": None, "indexed": False,
    }
    url = "https://share/GEN/G0081.tar.gz"
    assert fake.set_genome_indexed("G0081", url)["indexed"] is True
    assert fake.set_genome_indexed("G0081", url)["url_indexed"] == url
    with pytest.raises(CoreError, match="No reference genome G9999") as exc:
        fake.reference_genome("G9999")
    assert exc.value.status == 404
    with pytest.raises(CoreError) as exc:
        fake.set_genome_indexed("G0081", "")
    assert exc.value.status == 422


def _upload(tmp_path, fake, *, upload_error=None, force=False, remote_exists=False):
    _make_index(tmp_path)
    transfer = MagicMock()
    transfer.remote_exists.return_value = remote_exists
    transfer.upload_stream.side_effect = upload_error
    transfer.__enter__.return_value = transfer
    transfer.__exit__.return_value = False
    settings = {"ERDA_SHARE_BASE": "https://share", "SFTP_REMOTE_REFERENCE_DIR": "GEN"}
    with patch("ehio.transfer.SFTPTransfer", return_value=transfer), \
         patch("ehio.reference.cfg.get", side_effect=lambda key: settings.get(key)):
        status = upload_reference_index_status(
            "G0081", tmp_path, CoreSession(fake), host="host", user="user",
            remote_base="/Data", force=force,
        )
    return status, transfer


def test_successful_upload_registers_index_once(tmp_path):
    fake = FakeCoreClient(genomes={"G0081": {"url_raw": "https://raw"}})
    status, transfer = _upload(tmp_path, fake)
    assert status == "uploaded"
    transfer.upload_stream.assert_called_once()
    assert fake.index_updates == [("G0081", "https://share/GEN/G0081.tar.gz")]


def test_failed_upload_does_not_register_index(tmp_path):
    fake = FakeCoreClient(genomes={"G0081": {"url_raw": "https://raw"}})
    with pytest.raises(OSError, match="transfer failed"):
        _upload(tmp_path, fake, upload_error=OSError("transfer failed"))
    assert fake.index_updates == []


def test_existing_index_url_is_not_overwritten(tmp_path):
    fake = FakeCoreClient(genomes={"G0081": {"url_indexed": "https://existing/index.tar.gz"}})
    with patch("ehio.transfer.SFTPTransfer", side_effect=AssertionError("uploaded")):
        assert upload_reference_index_status(
            "G0081", tmp_path, CoreSession(fake), host="host", user="user",
            remote_base="/Data",
        ) == "already-indexed"
    assert fake.index_updates == []


def test_force_reuploads_without_overwriting_an_existing_index_url(tmp_path):
    original_url = "https://existing/index.tar.gz"
    fake = FakeCoreClient(genomes={"G0081": {"url_indexed": original_url}})
    status, transfer = _upload(tmp_path, fake, force=True, remote_exists=True)
    assert status == "uploaded"
    transfer.upload_stream.assert_called_once()
    assert fake.index_updates == []
    assert fake.genomes["G0081"]["url_indexed"] == original_url


def test_force_registers_url_when_genome_has_no_index_url(tmp_path):
    fake = FakeCoreClient(genomes={"G0081": {"url_indexed": None}})
    status, transfer = _upload(tmp_path, fake, force=True, remote_exists=True)
    assert status == "uploaded"
    transfer.upload_stream.assert_called_once()
    assert fake.index_updates == [("G0081", "https://share/GEN/G0081.tar.gz")]


def test_reference_command_uses_the_batch_genome_code(tmp_path):
    from ehio import cli

    fake = FakeCoreClient(batches={"PPR001": {
        "row": {"code": "PPR001"},
        "entries": [{"code": "PR00001", "reference_genome_code": "G0081"}],
    }})
    args = argparse.Namespace(batch="PPR001", local_dir=str(tmp_path), force=False)
    with using(fake), \
         patch.object(cli, "_conf", side_effect=lambda args, attr, key, required=False:
                      "22" if attr == "port" else "value"), \
         patch("ehio.reference.upload_reference_index_status", return_value="uploaded") as upload:
        assert cli.cmd_reference(args) == 0
    assert upload.call_args.args[0] == "G0081"
    assert upload.call_args.args[2].client is fake


def test_reference_command_names_an_unknown_genome_without_traceback(tmp_path, capsys):
    from ehio import cli

    fake = FakeCoreClient(batches={"PPR001": {
        "row": {"code": "PPR001"},
        "entries": [{"code": "PR00001", "reference_genome_code": "G9999"}],
    }})
    fake.reference_genome = MagicMock(side_effect=CoreError("Not Found", status=404))
    with patch.object(cli, "_core", return_value=CoreSession(fake)), \
         patch.object(cli, "_conf", side_effect=lambda args, attr, key, required=False:
                      "22" if attr == "port" else "value"), \
         pytest.raises(SystemExit) as exc:
        cli.main(["reference", "--batch", "PPR001", "--local-dir", str(tmp_path)])
    assert exc.value.code == 1
    stderr = capsys.readouterr().err
    assert len([line for line in stderr.splitlines() if "Error:" in line]) == 1
    assert "No reference genome G9999" in stderr
    assert "Traceback" not in stderr
