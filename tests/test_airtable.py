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

    def test_entry_formula_uses_record_id(self):
        """The FIND+ARRAYJOIN formula must use the batch's recXXX id."""
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
        assert "recBATCH000001" in entry_call_formula
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
