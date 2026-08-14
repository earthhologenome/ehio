"""Tests for ehio.reference — reference index discovery, archiving, registration."""

from __future__ import annotations

import io
import tarfile

import pytest
from unittest.mock import MagicMock, patch

from ehio.reference import (
    BT2_SUFFIXES,
    BT2L_SUFFIXES,
    find_index_sets,
    flag_genome_indexed,
    genome_code,
    indexed_url,
    locate_references_dir,
    resolve_genome_record,
    write_index_archive,
    upload_reference_index,
    upload_reference_index_status,
)


CONFIG = {
    "EHI_BASE":                 "appEHI",
    "EHI_GENOME":               "tblGENOME",
    "EHI_GENOME_CODE":          "fldCODE",
    "EHI_GENOME_URL_INDEXED":   "fldINDEXEDURL",
    "EHI_PPR_BATCH_REFERENCE":  "fldBATCHREF",
    "EHI_GENOME_INDEX_BASE":    "appMASTER",
    "EHI_GENOME_INDEX_TABLE":   "tblMASTER",
    "EHI_GENOME_INDEX_CODE":    "Code",
    "EHI_GENOME_INDEXED":       "fldINDEXED",
    "EHI_GENOME_INDEXED_VALUE": "YES",
    "SFTP_REMOTE_REFERENCE_DIR": "GEN",
}


@pytest.fixture
def config():
    """Patch ehio.config.get with the test config above."""
    with patch("ehio.reference.cfg.get", side_effect=lambda k, d=None: CONFIG.get(k, d)):
        yield CONFIG


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

    def test_archive_is_readable_as_a_stream(self, tmp_path):
        """drakkar opens the tarball with tarfile — it must be a valid tar.gz."""
        _make_index(tmp_path)
        fasta, files = find_index_sets(tmp_path)[0]
        buffer = io.BytesIO()
        write_index_archive(buffer, fasta, files, "G0002")
        assert tarfile.is_tarfile(io.BytesIO(buffer.getvalue()))


# ---------------------------------------------------------------------------
# genome record helpers
# ---------------------------------------------------------------------------

class TestGenomeRecordHelpers:
    def test_genome_code(self, config):
        assert genome_code({"fields": {"fldCODE": "G0001"}}) == "G0001"

    def test_genome_code_from_list_field(self, config):
        assert genome_code({"fields": {"fldCODE": ["G0001"]}}) == "G0001"

    def test_genome_code_missing(self, config):
        assert genome_code({"fields": {}}) == ""

    def test_indexed_url(self, config):
        record = {"fields": {"fldINDEXEDURL": "https://sid.erda.dk/x/G0001.tar.gz"}}
        assert indexed_url(record) == "https://sid.erda.dk/x/G0001.tar.gz"

    def test_indexed_url_empty(self, config):
        assert indexed_url({"fields": {"fldINDEXEDURL": ""}}) == ""


class TestResolveGenomeRecord:
    def test_linked_record_is_fetched_by_id(self, config):
        client = MagicMock()
        client.fetch_record_by_id.return_value = {"id": "recG1", "fields": {}}
        with patch("ehio.reference.AirtableClient", return_value=client):
            record = resolve_genome_record({"fields": {"fldBATCHREF": ["recG1"]}}, "tok")
        assert record == {"id": "recG1", "fields": {}}
        client.fetch_record_by_id.assert_called_once_with("tblGENOME", "recG1")

    def test_text_field_is_looked_up_by_code(self, config):
        client = MagicMock()
        client.fetch_records_by_value.return_value = [{"id": "recG1", "fields": {}}]
        with patch("ehio.reference.AirtableClient", return_value=client):
            record = resolve_genome_record({"fields": {"fldBATCHREF": "G0001"}}, "tok")
        assert record["id"] == "recG1"
        client.fetch_records_by_value.assert_called_once_with("tblGENOME", "fldCODE", "G0001")

    def test_empty_reference_field_returns_none(self, config):
        assert resolve_genome_record({"fields": {"fldBATCHREF": ""}}, "tok") is None

    def test_missing_reference_field_returns_none(self, config):
        assert resolve_genome_record({"fields": {}}, "tok") is None

    def test_record_not_found_returns_none(self, config):
        client = MagicMock()
        client.fetch_record_by_id.return_value = None
        with patch("ehio.reference.AirtableClient", return_value=client):
            assert resolve_genome_record({"fields": {"fldBATCHREF": "recG1"}}, "tok") is None


# ---------------------------------------------------------------------------
# flag_genome_indexed
# ---------------------------------------------------------------------------

class TestFlagGenomeIndexed:
    def test_sets_the_configured_value(self, config):
        client = MagicMock()
        client.fetch_records_by_value.return_value = [{"id": "recM1", "fields": {}}]
        with patch("ehio.reference.AirtableClient", return_value=client):
            assert flag_genome_indexed("G0001", "tok") is True
        client.fetch_records_by_value.assert_called_once_with("tblMASTER", "Code", "G0001")
        client.update_records.assert_called_once_with(
            "tblMASTER", [{"id": "recM1", "fields": {"fldINDEXED": "YES"}}]
        )

    def test_already_flagged_is_not_rewritten(self, config):
        client = MagicMock()
        client.fetch_records_by_value.return_value = [
            {"id": "recM1", "fields": {"fldINDEXED": "YES"}}
        ]
        with patch("ehio.reference.AirtableClient", return_value=client):
            assert flag_genome_indexed("G0001", "tok") is True
        client.update_records.assert_not_called()

    def test_no_matching_record(self, config):
        client = MagicMock()
        client.fetch_records_by_value.return_value = []
        with patch("ehio.reference.AirtableClient", return_value=client):
            assert flag_genome_indexed("G0001", "tok") is False
        client.update_records.assert_not_called()

    def test_ambiguous_match_updates_nothing(self, config):
        client = MagicMock()
        client.fetch_records_by_value.return_value = [
            {"id": "recM1", "fields": {}}, {"id": "recM2", "fields": {}}
        ]
        with patch("ehio.reference.AirtableClient", return_value=client):
            assert flag_genome_indexed("G0001", "tok") is False
        client.update_records.assert_not_called()

    def test_unconfigured_table_is_a_no_op(self):
        with patch("ehio.reference.cfg.get", side_effect=lambda k, d=None: ""):
            assert flag_genome_indexed("G0001", "tok") is False


# ---------------------------------------------------------------------------
# upload_reference_index
# ---------------------------------------------------------------------------

class TestUploadReferenceIndex:
    BATCH = {"fields": {"fldBATCHREF": ["recG1"]}}

    def _genome(self, url=""):
        return {"id": "recG1", "fields": {"fldCODE": "G0001", "fldINDEXEDURL": url}}

    def _run(self, tmp_path, genome, xfer):
        with patch("ehio.reference.resolve_genome_record", return_value=genome), \
             patch("ehio.reference.flag_genome_indexed", return_value=True) as flag, \
             patch("ehio.transfer.SFTPTransfer") as transfer_cls:
            transfer_cls.return_value.__enter__.return_value = xfer
            result = upload_reference_index(
                self.BATCH, tmp_path, "tok",
                host="io.erda.dk", user="me",
                remote_base="/EarthHologenomeInitiative/Data",
            )
        return result, flag

    def test_uploads_and_flags_a_new_index(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references")
        xfer = MagicMock()
        xfer.remote_exists.return_value = False
        result, flag = self._run(tmp_path, self._genome(), xfer)

        assert result is True
        remote_path = xfer.upload_stream.call_args[0][0]
        assert remote_path == "/EarthHologenomeInitiative/Data/GEN/G0001.tar.gz"
        flag.assert_called_once_with("G0001", "tok")

    def test_streams_a_valid_archive(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references")
        xfer = MagicMock()
        xfer.remote_exists.return_value = False
        self._run(tmp_path, self._genome(), xfer)

        buffer = io.BytesIO()
        xfer.upload_stream.call_args[0][1](buffer)
        with tarfile.open(fileobj=io.BytesIO(buffer.getvalue()), mode="r:gz") as tar:
            assert "G0001.fna" in tar.getnames()

    def test_already_indexed_genome_is_skipped(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references")
        xfer = MagicMock()
        result, flag = self._run(tmp_path, self._genome(url="https://erda/G0001.tar.gz"), xfer)
        assert result is False
        xfer.upload_stream.assert_not_called()
        flag.assert_not_called()

    def test_no_index_on_disk_is_skipped(self, config, tmp_path):
        xfer = MagicMock()
        result, flag = self._run(tmp_path, self._genome(), xfer)
        assert result is False
        xfer.upload_stream.assert_not_called()
        flag.assert_not_called()

    def test_batch_without_reference_is_skipped(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references")
        xfer = MagicMock()
        result, flag = self._run(tmp_path, None, xfer)
        assert result is False
        xfer.upload_stream.assert_not_called()

    def test_ambiguous_references_dir_is_skipped(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references", stem="reference")
        _make_index(tmp_path / "data" / "references", stem="other")
        xfer = MagicMock()
        xfer.remote_exists.return_value = False
        result, flag = self._run(tmp_path, self._genome(), xfer)
        assert result is False
        xfer.upload_stream.assert_not_called()

    def test_existing_remote_archive_is_not_reuploaded_but_still_flagged(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references")
        xfer = MagicMock()
        xfer.remote_exists.return_value = True
        result, flag = self._run(tmp_path, self._genome(), xfer)
        assert result is True
        xfer.upload_stream.assert_not_called()
        flag.assert_called_once_with("G0001", "tok")

    def test_genome_without_code_is_skipped(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references")
        xfer = MagicMock()
        genome = {"id": "recG1", "fields": {"fldINDEXEDURL": ""}}
        result, flag = self._run(tmp_path, genome, xfer)
        assert result is False
        xfer.upload_stream.assert_not_called()


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


# ---------------------------------------------------------------------------
# upload_reference_index_status — the outcomes 'ehio reference' reports on
# ---------------------------------------------------------------------------

class TestUploadReferenceIndexStatus:
    BATCH = {"fields": {"fldBATCHREF": ["recG1"]}}

    def _genome(self, url=""):
        return {"id": "recG1", "fields": {"fldCODE": "G0001", "fldINDEXEDURL": url}}

    def _run(self, tmp_path, genome, xfer, *, force=False, flagged=True):
        with patch("ehio.reference.resolve_genome_record", return_value=genome), \
             patch("ehio.reference.flag_genome_indexed", return_value=flagged), \
             patch("ehio.transfer.SFTPTransfer") as transfer_cls:
            transfer_cls.return_value.__enter__.return_value = xfer
            return upload_reference_index_status(
                self.BATCH, tmp_path, "tok",
                host="io.erda.dk", user="me",
                remote_base="/EarthHologenomeInitiative/Data",
                force=force,
            )

    def test_uploaded(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references")
        xfer = MagicMock()
        xfer.remote_exists.return_value = False
        assert self._run(tmp_path, self._genome(), xfer) == "uploaded"

    def test_index_directly_under_local_root(self, config, tmp_path):
        """A batch whose output dir is gone can be retried from the index alone."""
        _make_index(tmp_path)
        xfer = MagicMock()
        xfer.remote_exists.return_value = False
        assert self._run(tmp_path, self._genome(), xfer) == "uploaded"

    def test_no_index(self, config, tmp_path):
        assert self._run(tmp_path, self._genome(), MagicMock()) == "no-index"

    def test_no_reference(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references")
        assert self._run(tmp_path, None, MagicMock()) == "no-reference"

    def test_no_code(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references")
        genome = {"id": "recG1", "fields": {"fldINDEXEDURL": ""}}
        assert self._run(tmp_path, genome, MagicMock()) == "no-code"

    def test_ambiguous(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references", stem="reference")
        _make_index(tmp_path / "data" / "references", stem="other")
        assert self._run(tmp_path, self._genome(), MagicMock()) == "ambiguous"

    def test_already_indexed(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references")
        genome = self._genome(url="https://erda/G0001.tar.gz")
        assert self._run(tmp_path, genome, MagicMock()) == "already-indexed"

    def test_force_uploads_an_already_indexed_genome(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references")
        xfer = MagicMock()
        xfer.remote_exists.return_value = True
        genome = self._genome(url="https://erda/G0001.tar.gz")
        assert self._run(tmp_path, genome, xfer, force=True) == "uploaded"
        xfer.upload_stream.assert_called_once()

    def test_upload_without_the_flag_is_not_uploaded(self, config, tmp_path):
        _make_index(tmp_path / "data" / "references")
        xfer = MagicMock()
        xfer.remote_exists.return_value = False
        status = self._run(tmp_path, self._genome(), xfer, flagged=False)
        assert status == "not-flagged"
