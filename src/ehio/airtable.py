"""Airtable API client for ehio."""

from __future__ import annotations

import sys
from typing import Any

try:
    from pyairtable import Api
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False


def _require() -> None:
    if not _AVAILABLE:
        print("Error: pyairtable is required. Run: pip install pyairtable", file=sys.stderr)
        sys.exit(1)


class AirtableClient:
    def __init__(self, api_key: str, base_id: str) -> None:
        _require()
        self._api = Api(api_key, use_field_ids=True)
        self._base_id = base_id

    def _table(self, table_name: str):
        return self._api.table(self._base_id, table_name)

    def fetch_batch_record(
        self,
        batch_table: str,
        batch_code_field: str,
        batch_code: str,
    ) -> dict[str, Any] | None:
        """Return the single batch record where batch_code_field == batch_code, or None."""
        formula = f'{{{batch_code_field}}} = "{batch_code}"'
        records = self._table(batch_table).all(formula=formula)
        return records[0] if records else None

    def fetch_batch_and_entries(
        self,
        batch_table: str,
        batch_code_field: str,
        batch_code: str,
        entry_table: str,
        entry_batch_field: str,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """Fetch a batch record and all its linked entry records.

        1. Find the batch record in batch_table by batch_code.
        2. Use FIND+ARRAYJOIN on the entry_batch_field reverse-link to get all entries.

        Returns (batch_record, [entry_records]).
        """
        batch_record = self.fetch_batch_record(batch_table, batch_code_field, batch_code)
        if not batch_record:
            return None, []

        formula = f'FIND("{batch_code}", ARRAYJOIN({{{entry_batch_field}}}))'
        entries = self._table(entry_table).all(formula=formula)
        return batch_record, entries

    def fetch_record_by_id(
        self,
        table_name: str,
        record_id: str,
    ) -> dict[str, Any] | None:
        """Fetch a single record by its Airtable record ID (recXXX)."""
        try:
            return self._table(table_name).get(record_id)
        except Exception:
            return None

    def fetch_pending_batches(
        self,
        batch_table: str,
        batch_status_field: str,
        trigger_status: str,
    ) -> list[dict[str, Any]]:
        """Return all batch records where batch_status_field == trigger_status."""
        formula = f'{{{batch_status_field}}} = "{trigger_status}"'
        return self._table(batch_table).all(formula=formula)

    def update_records(
        self,
        table_name: str,
        updates: list[dict[str, Any]],
    ) -> None:
        """Batch-update records. Each item must have 'id' and 'fields' keys."""
        if not updates:
            return
        self._table(table_name).batch_update(updates)
