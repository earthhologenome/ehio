"""Airtable API client for ehio."""

from __future__ import annotations

import sys
from typing import Any

try:
    from pyairtable import Api
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False

try:
    from requests.exceptions import HTTPError, RequestException
except ImportError:  # requests ships with pyairtable; only hit if that is missing too
    class RequestException(Exception):  # type: ignore[no-redef]
        """Placeholder used when requests is not installed."""

    class HTTPError(RequestException):  # type: ignore[no-redef]
        """Placeholder used when requests is not installed."""


TOKEN_HINT = (
    "Provide a valid token with --airtable-token or export AIRTABLE_TOKEN."
)


class AirtableError(RuntimeError):
    """An Airtable request failed for a reason the user can act on.

    Raised instead of letting a raw HTTPError traceback reach the terminal.
    """


def _require() -> None:
    if not _AVAILABLE:
        print("Error: pyairtable is required. Run: pip install pyairtable", file=sys.stderr)
        sys.exit(1)


def _status_of(exc: Exception) -> int | None:
    return getattr(getattr(exc, "response", None), "status_code", None)


def _detail_of(exc: Exception) -> str:
    """Extract Airtable's own error message from a failed response."""
    response = getattr(exc, "response", None)
    if response is None:
        return str(exc)
    try:
        payload = response.json()
    except Exception:
        return (getattr(response, "text", "") or str(exc)).strip()
    error = payload.get("error", payload) if isinstance(payload, dict) else payload
    if isinstance(error, dict):
        return str(error.get("message") or error.get("type") or error)
    return str(error)


def _explain(
    exc: Exception,
    *,
    action: str,
    base_id: str = "",
    table_name: str = "",
) -> str:
    """Turn an Airtable HTTP failure into an actionable one-paragraph message."""
    status = _status_of(exc)
    detail = _detail_of(exc)
    if table_name and base_id:
        where = f"table {table_name} of base {base_id}"
    elif base_id:
        where = f"base {base_id}"
    else:
        where = "Airtable"

    if status == 401:
        return (
            "Airtable rejected the token (401 Unauthorized): it is invalid, expired "
            f"or revoked. {TOKEN_HINT}"
        )
    if status == 403:
        return (
            f"Airtable denied permission to {action} {where} (403 Forbidden). "
            "The token is recognised but not allowed to do this: check that it has "
            "the data.records:read and data.records:write scopes, and that "
            f"{base_id or 'the base'} is in the token's list of accessible bases. "
            f"Airtable said: {detail}"
        )
    if status == 404:
        return (
            f"Airtable could not find {where} (404 Not Found). Check the base and "
            f"table ids in the config (ehio config --edit). Airtable said: {detail}"
        )
    if status == 422:
        return (
            f"Airtable rejected the request to {action} {where} (422 Unprocessable). "
            "This usually means a field id or value does not match the table schema. "
            f"Airtable said: {detail}"
        )
    if status == 429:
        return (
            f"Airtable rate limit hit while trying to {action} {where} (429). "
            "Wait a few seconds and run the command again."
        )
    if status is not None:
        return (
            f"Airtable request to {action} {where} failed with HTTP {status}. "
            f"Airtable said: {detail}"
        )
    return f"Could not reach the Airtable API to {action} {where}: {exc}"


_VERIFIED_TOKENS: set[str] = set()


def verify_token(api_key: str) -> None:
    """Check that `api_key` is a token Airtable accepts, before any real work runs.

    Raises AirtableError with an actionable message when the token is empty,
    rejected, or when Airtable cannot be reached.  A token is only checked once
    per process, so calling this from every command costs a single request.
    """
    _require()
    key = (api_key or "").strip()
    if not key:
        raise AirtableError(f"No Airtable token provided. {TOKEN_HINT}")
    if key in _VERIFIED_TOKENS:
        return
    try:
        Api(key).whoami()
    except HTTPError as exc:
        # A token with no metadata scopes is still usable for record operations.
        if _status_of(exc) != 403:
            raise AirtableError(_explain(exc, action="verify the token")) from exc
    except RequestException as exc:
        raise AirtableError(
            f"Could not reach the Airtable API to verify the token: {exc}"
        ) from exc
    _VERIFIED_TOKENS.add(key)


class AirtableClient:
    def __init__(self, api_key: str, base_id: str) -> None:
        _require()
        self._api = Api(api_key, use_field_ids=True)
        self._base_id = base_id

    def _table(self, table_name: str):
        return self._api.table(self._base_id, table_name)

    def _guard(self, action: str, table_name: str, func, *args, **kwargs):
        """Run an Airtable call, converting HTTP failures into AirtableError."""
        try:
            return func(*args, **kwargs)
        except (HTTPError, RequestException) as exc:
            raise AirtableError(
                _explain(exc, action=action, base_id=self._base_id, table_name=table_name)
            ) from exc

    def fetch_batch_record(
        self,
        batch_table: str,
        batch_code_field: str,
        batch_code: str,
    ) -> dict[str, Any] | None:
        """Return the single batch record where batch_code_field == batch_code, or None."""
        formula = f'{{{batch_code_field}}} = "{batch_code}"'
        records = self._guard(
            "read", batch_table, self._table(batch_table).all, formula=formula
        )
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
        entries = self._guard(
            "read", entry_table, self._table(entry_table).all, formula=formula
        )
        return batch_record, entries

    def fetch_record_by_id(
        self,
        table_name: str,
        record_id: str,
    ) -> dict[str, Any] | None:
        """Fetch a single record by its Airtable record ID (recXXX).

        Returns None when the record does not exist; permission and connection
        failures are raised as AirtableError so they are not mistaken for a
        missing record.
        """
        try:
            return self._table(table_name).get(record_id)
        except (HTTPError, RequestException) as exc:
            if _status_of(exc) in (404, None):
                return None
            raise AirtableError(
                _explain(exc, action="read", base_id=self._base_id, table_name=table_name)
            ) from exc
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
        return self._guard(
            "read", batch_table, self._table(batch_table).all, formula=formula
        )

    def update_records(
        self,
        table_name: str,
        updates: list[dict[str, Any]],
    ) -> None:
        """Batch-update records. Each item must have 'id' and 'fields' keys."""
        if not updates:
            return
        try:
            self._table(table_name).batch_update(updates)
            return
        except (HTTPError, RequestException) as exc:
            # Authentication and permission failures apply to the whole table,
            # so there is nothing to learn from retrying record by record.
            if _status_of(exc) in (401, 403):
                raise AirtableError(
                    _explain(
                        exc, action="update records in",
                        base_id=self._base_id, table_name=table_name,
                    )
                ) from exc

        # Retry one record at a time to surface which record and field caused the error.
        for record in updates:
            try:
                self._table(table_name).batch_update([record])
            except (HTTPError, RequestException) as exc:
                raise AirtableError(
                    _explain(
                        exc, action="update records in",
                        base_id=self._base_id, table_name=table_name,
                    )
                    + f"\n  Failing record: {record['id']}"
                    + f"\n  Fields: {record['fields']}"
                ) from exc

    def create_records(
        self,
        table_name: str,
        fields_list: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Batch-create records. fields_list is a list of field dicts (no 'id').

        Returns the created records with their new Airtable record IDs.
        """
        if not fields_list:
            return []
        return self._guard(
            "create records in",
            table_name,
            self._table(table_name).batch_create,
            fields_list,
        )

    def fetch_existing_values(
        self,
        table_name: str,
        field_id: str,
        values: list[str],
        batch_size: int = 100,
    ) -> set[str]:
        """Return which of `values` already exist in field_id of table_name.

        Queries in batches to stay within Airtable formula URL limits.
        """
        existing: set[str] = set()
        for i in range(0, len(values), batch_size):
            batch = values[i : i + batch_size]
            parts = [f'{{{field_id}}} = "{v}"' for v in batch]
            formula = f"OR({', '.join(parts)})" if len(parts) > 1 else parts[0]
            records = self._guard(
                "read", table_name, self._table(table_name).all, formula=formula
            )
            for rec in records:
                val = str(rec.get("fields", {}).get(field_id, "") or "").strip()
                if val:
                    existing.add(val)
        return existing
