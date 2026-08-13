"""Tests for ehio.airtable — formula construction and client logic.

The pyairtable API is fully mocked so no network calls are made.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call

from tests.conftest import BATCH_RECORD, ENTRY_RECORDS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _http_error(status: int, message: str = "denied"):
    """Return an HTTPError shaped like the ones pyairtable raises."""
    from requests.exceptions import HTTPError

    response = MagicMock()
    response.status_code = status
    response.json.return_value = {"error": {"type": "TEST", "message": message}}
    return HTTPError(f"{status} Client Error", response=response)


def _make_client(base_id: str = "appTEST"):
    """Return an AirtableClient with a mocked underlying Api."""
    with patch("ehio.airtable._AVAILABLE", True), \
         patch("ehio.airtable.Api") as mock_api_cls:
        from ehio.airtable import AirtableClient
        client = AirtableClient(api_key="patTEST", base_id=base_id)
        mock_api_cls.assert_called_once_with("patTEST", use_field_ids=True)
        return client, mock_api_cls.return_value


# ---------------------------------------------------------------------------
# AirtableClient.__init__
# ---------------------------------------------------------------------------

class TestInit:
    def test_use_field_ids_is_true(self):
        with patch("ehio.airtable._AVAILABLE", True), \
             patch("ehio.airtable.Api") as mock_api_cls:
            from ehio.airtable import AirtableClient
            AirtableClient(api_key="patABC", base_id="appXYZ")
            mock_api_cls.assert_called_once_with("patABC", use_field_ids=True)

    def test_missing_pyairtable_exits(self, capsys):
        with patch("ehio.airtable._AVAILABLE", False):
            from ehio.airtable import AirtableClient
            with pytest.raises(SystemExit):
                AirtableClient(api_key="x", base_id="y")


# ---------------------------------------------------------------------------
# fetch_batch_record
# ---------------------------------------------------------------------------

class TestFetchBatchRecord:
    def test_returns_first_match(self):
        client, mock_api = _make_client()
        mock_table = mock_api.table.return_value
        mock_table.all.return_value = [BATCH_RECORD]

        result = client.fetch_batch_record(
            batch_table="tblPPR_BATCH",
            batch_code_field="fldeNHpDmJDinU1Uc",
            batch_code="PPR001",
        )
        assert result == BATCH_RECORD
        mock_table.all.assert_called_once_with(
            formula='{fldeNHpDmJDinU1Uc} = "PPR001"'
        )

    def test_returns_none_when_not_found(self):
        client, mock_api = _make_client()
        mock_api.table.return_value.all.return_value = []

        result = client.fetch_batch_record("tblX", "fldY", "MISSING")
        assert result is None


# ---------------------------------------------------------------------------
# fetch_batch_and_entries
# ---------------------------------------------------------------------------

class TestFetchBatchAndEntries:
    def test_returns_batch_and_entries(self):
        client, mock_api = _make_client()
        mock_table = mock_api.table.return_value
        # First call → batch lookup; second call → entries
        mock_table.all.side_effect = [[BATCH_RECORD], ENTRY_RECORDS]

        batch, entries = client.fetch_batch_and_entries(
            batch_table="tblPPR_BATCH",
            batch_code_field="fldeNHpDmJDinU1Uc",
            batch_code="PPR001",
            entry_table="tblPPR_ENTRY",
            entry_batch_field="fld2lF4Tj0MQ82HIg",
        )
        assert batch == BATCH_RECORD
        assert len(entries) == 2

    def test_entry_formula_uses_batch_code(self):
        """The FIND+ARRAYJOIN formula must use the batch code (primary field value), not the record ID."""
        client, mock_api = _make_client()
        mock_table = mock_api.table.return_value
        mock_table.all.side_effect = [[BATCH_RECORD], ENTRY_RECORDS]

        client.fetch_batch_and_entries(
            batch_table="tblPPR_BATCH",
            batch_code_field="fldeNHpDmJDinU1Uc",
            batch_code="PPR001",
            entry_table="tblPPR_ENTRY",
            entry_batch_field="fld2lF4Tj0MQ82HIg",
        )
        calls = mock_table.all.call_args_list
        entry_call_formula = calls[1].kwargs["formula"]
        assert "PPR001" in entry_call_formula
        assert "recBATCH000001" not in entry_call_formula
        assert "FIND" in entry_call_formula
        assert "ARRAYJOIN" in entry_call_formula
        assert "fld2lF4Tj0MQ82HIg" in entry_call_formula

    def test_returns_none_and_empty_when_batch_missing(self):
        client, mock_api = _make_client()
        mock_api.table.return_value.all.return_value = []

        batch, entries = client.fetch_batch_and_entries(
            batch_table="tblX", batch_code_field="fldY", batch_code="NOPE",
            entry_table="tblZ", entry_batch_field="fldW",
        )
        assert batch is None
        assert entries == []


# ---------------------------------------------------------------------------
# fetch_pending_batches
# ---------------------------------------------------------------------------

class TestFetchPendingBatches:
    def test_formula_matches_status(self):
        client, mock_api = _make_client()
        mock_api.table.return_value.all.return_value = [BATCH_RECORD]

        result = client.fetch_pending_batches(
            batch_table="tblPPR_BATCH",
            batch_status_field="fldhFIPsoslCbCyfo",
            trigger_status="Ready",
        )
        assert result == [BATCH_RECORD]
        mock_api.table.return_value.all.assert_called_once_with(
            formula='{fldhFIPsoslCbCyfo} = "Ready"'
        )


# ---------------------------------------------------------------------------
# update_records
# ---------------------------------------------------------------------------

class TestUpdateRecords:
    def test_calls_batch_update(self):
        client, mock_api = _make_client()
        updates = [{"id": "recX", "fields": {"fldA": "val"}}]

        client.update_records("tblPPR_ENTRY", updates)
        mock_api.table.return_value.batch_update.assert_called_once_with(updates)

    def test_empty_list_skips_call(self):
        client, mock_api = _make_client()
        client.update_records("tblPPR_ENTRY", [])
        mock_api.table.return_value.batch_update.assert_not_called()

    def test_forbidden_raises_airtable_error_without_retrying(self):
        from ehio.airtable import AirtableError

        client, mock_api = _make_client(base_id="appXYZ")
        mock_api.table.return_value.batch_update.side_effect = _http_error(403)

        with pytest.raises(AirtableError) as excinfo:
            client.update_records("tblPPR_ENTRY", [
                {"id": "recX", "fields": {"fldA": "val"}},
                {"id": "recY", "fields": {"fldA": "val"}},
            ])
        assert "403" in str(excinfo.value)
        assert "appXYZ" in str(excinfo.value)
        # One attempt only: a permission error is not per-record.
        assert mock_api.table.return_value.batch_update.call_count == 1

    def test_field_error_names_the_failing_record(self):
        from ehio.airtable import AirtableError

        client, mock_api = _make_client()
        mock_api.table.return_value.batch_update.side_effect = _http_error(
            422, "Unknown field name"
        )

        with pytest.raises(AirtableError) as excinfo:
            client.update_records("tblPPR_ENTRY", [{"id": "recX", "fields": {"fldA": "v"}}])
        message = str(excinfo.value)
        assert "recX" in message
        assert "Unknown field name" in message


# ---------------------------------------------------------------------------
# Error handling on reads
# ---------------------------------------------------------------------------

class TestReadErrors:
    def test_unauthorized_read_raises_airtable_error(self):
        from ehio.airtable import AirtableError

        client, mock_api = _make_client()
        mock_api.table.return_value.all.side_effect = _http_error(401)

        with pytest.raises(AirtableError) as excinfo:
            client.fetch_pending_batches("tblX", "fldY", "Ready")
        assert "401" in str(excinfo.value)

    def test_missing_record_still_returns_none(self):
        client, mock_api = _make_client()
        mock_api.table.return_value.get.side_effect = _http_error(404)

        assert client.fetch_record_by_id("tblX", "recMISSING") is None

    def test_forbidden_record_lookup_raises(self):
        from ehio.airtable import AirtableError

        client, mock_api = _make_client()
        mock_api.table.return_value.get.side_effect = _http_error(403)

        with pytest.raises(AirtableError):
            client.fetch_record_by_id("tblX", "recX")


# ---------------------------------------------------------------------------
# verify_token
# ---------------------------------------------------------------------------

class TestVerifyToken:
    def test_empty_token_raises(self):
        from ehio.airtable import AirtableError, verify_token

        with patch("ehio.airtable._AVAILABLE", True):
            with pytest.raises(AirtableError):
                verify_token("   ")

    def test_valid_token_calls_whoami_once_per_process(self):
        from ehio.airtable import verify_token

        with patch("ehio.airtable._AVAILABLE", True), \
             patch("ehio.airtable._VERIFIED_TOKENS", set()), \
             patch("ehio.airtable.Api") as mock_api_cls:
            verify_token("patGOOD")
            verify_token("patGOOD")
            assert mock_api_cls.return_value.whoami.call_count == 1

    def test_invalid_token_raises_with_actionable_message(self):
        from ehio.airtable import AirtableError, verify_token

        with patch("ehio.airtable._AVAILABLE", True), \
             patch("ehio.airtable._VERIFIED_TOKENS", set()), \
             patch("ehio.airtable.Api") as mock_api_cls:
            mock_api_cls.return_value.whoami.side_effect = _http_error(
                401, "Invalid authentication token"
            )
            with pytest.raises(AirtableError) as excinfo:
                verify_token("patBAD")
        message = str(excinfo.value)
        assert "401" in message
        assert "AIRTABLE_TOKEN" in message

    def test_token_without_metadata_scope_is_accepted(self):
        """whoami may 403 for a token with no meta scopes — records still work."""
        from ehio.airtable import verify_token

        with patch("ehio.airtable._AVAILABLE", True), \
             patch("ehio.airtable._VERIFIED_TOKENS", set()), \
             patch("ehio.airtable.Api") as mock_api_cls:
            mock_api_cls.return_value.whoami.side_effect = _http_error(403)
            verify_token("patNOSCOPE")  # must not raise

    def test_unreachable_api_raises(self):
        from ehio.airtable import AirtableError, verify_token
        from requests.exceptions import ConnectionError as RequestsConnectionError

        with patch("ehio.airtable._AVAILABLE", True), \
             patch("ehio.airtable._VERIFIED_TOKENS", set()), \
             patch("ehio.airtable.Api") as mock_api_cls:
            mock_api_cls.return_value.whoami.side_effect = RequestsConnectionError("no route")
            with pytest.raises(AirtableError) as excinfo:
                verify_token("patOFFLINE")
        assert "Could not reach" in str(excinfo.value)
