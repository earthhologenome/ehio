"""Read batches and their entries from ehi-core."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

CORE = "ehi-core"

CORE_BATCH_TABLE = {
    "preprocessing": "preprocessing_batches",
    "binning":       "assembly_batches",
    "quantifying":   "dereplication_batches",
    "amr":           "amr_batches",
}

@dataclass
class Fields:
    """Which key of an entry holds each input value."""

    sample: str = ""
    reads1: str = ""
    reads2: str = ""
    assembly: str = ""
    code: str = ""
    url: str = ""


_CORE_FIELDS = {
    "preprocessing": Fields(sample="code", reads1="raw_forward_url", reads2="raw_reverse_url",
                            code="code"),
    "binning":       Fields(sample="hologenome_code", assembly="assembly_code",
                            reads1="forward_url", reads2="reverse_url", code="assembly_code"),
    "quantifying":   Fields(sample="hologenome_code", reads1="forward_url", reads2="reverse_url",
                            code="code"),
    "amr":           Fields(code="code", url="assembly_url", sample="code"),
}


@dataclass
class Batch:
    """One batch and the entries it works on."""

    module: str
    code: str
    source: str
    record: dict[str, Any]
    fields: Fields
    entries: list[dict[str, Any]] = field(default_factory=list)

    def value(self, config_key: str, column: str = "") -> Any:
        """A value from the batch row; config_key is kept for callers."""
        held = self.record.get(column or config_key)
        return held if held not in (None, "") else None


def open_batch(module: str, code: str, core) -> Batch:
    """Read a batch and its entries, raising LookupError for an unknown code."""
    from ehio.core import CoreError

    if not core:
        raise CoreError("ehi-core is required to read batches.")
    try:
        found = core.client.batch_entries(CORE_BATCH_TABLE[module], code)
    except CoreError as exc:
        if exc.status == 404:
            raise LookupError(f"Batch '{code}' is not in ehi-core.") from exc
        raise
    return Batch(
        module, found.get("batch") or code, CORE,
        found.get("row") or {"code": code},
        _CORE_FIELDS[module], found.get("entries") or [],
    )
