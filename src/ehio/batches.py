"""Where a batch and the entries it works on are read from.

Airtable is running out of room and ehi-core is taking over from it, so a batch
can live in either. Every command opens its batch through `open_batch`, which
looks in Airtable and then, for a batch Airtable does not hold, in the core.
With the Airtable keys left empty in the config — which is how Airtable is
switched off once it is gone — the core is the only place looked at.

The entries come back as plain dicts either way: Airtable's keyed by field id,
the core's by column name. `Fields` says which key holds what, and the writers
in ehio.drakkar already read a record as ``rec.get("fields", rec)``, so they
take both without knowing which database the rows came from.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Any

from ehio import config as cfg

AIRTABLE = "Airtable"
CORE = "ehi-core"

# The core table behind each module's Airtable batch table.
CORE_BATCH_TABLE = {
    "preprocessing": "preprocessing_batches",
    "binning":       "assembly_batches",
    "quantifying":   "dereplication_batches",
    "amr":           "amr_batches",
}

_PRIMARY_BASE = {
    "preprocessing": "EHI_BASE",
    "binning":       "EHI_BASE",
    "quantifying":   "MAG_BASE",
    "amr":           "EHI_BASE",
}
_BATCH_TABLE_CFG = {
    "preprocessing": "EHI_PPR_BATCH",
    "binning":       "EHI_ASB_BATCH",
    "quantifying":   "MAG_DMB_BATCH",
    "amr":           "EHI_AMR_BATCH",
}
_BATCH_CODE_CFG = {
    "preprocessing": "EHI_PPR_BATCH_CODE",
    "binning":       "EHI_ASB_BATCH_CODE",
    "quantifying":   "MAG_DMB_BATCH_CODE",
    "amr":           "EHI_AMR_BATCH_CODE",
}
# The entry table a module's entries are read from, and the link back to the
# batch.  quantifying and amr have none: their batches link straight out to
# records of another table, so their entries are gathered record by record.
_ENTRY_TABLE_CFG = {
    "preprocessing": ("EHI_PPR_ENTRY", "EHI_PPR_ENTRY_BATCH"),
    "binning":       ("EHI_ASB_ENTRY", "EHI_ASB_ENTRY_BATCH"),
}


@dataclass
class Fields:
    """Which key of an entry holds what, whichever database it came from."""

    sample: str = ""
    reads1: str = ""
    reads2: str = ""
    assembly: str = ""
    code: str = ""
    url: str = ""


# What the core calls each of them.  A preprocessing batch reads the raw reads
# of its libraries; every later batch reads what preprocessing produced.
_CORE_FIELDS = {
    "preprocessing": Fields(sample="code", reads1="raw_forward_url", reads2="raw_reverse_url",
                            code="code"),
    "binning":       Fields(sample="hologenome_code", assembly="assembly_code",
                            reads1="forward_url", reads2="reverse_url", code="assembly_code"),
    "quantifying":   Fields(sample="hologenome_code", reads1="forward_url", reads2="reverse_url",
                            code="code"),
    "amr":           Fields(code="code", url="assembly_url", sample="code"),
}


def _cfg(key: str) -> str:
    return str(cfg.get(key) or "").strip()


def _airtable_fields(module: str) -> Fields:
    """What Airtable calls each of them, by the config keys naming its fields."""
    if module == "preprocessing":
        return Fields(
            sample=_cfg("EHI_PPR_ENTRY_CODE"),
            reads1=_cfg("EHI_PPR_ENTRY_RAW_FILE_FORWARD"),
            reads2=_cfg("EHI_PPR_ENTRY_RAW_FILE_REVERSE"),
            code=_cfg("EHI_PPR_ENTRY_CODE"),
        )
    if module == "binning":
        return Fields(
            sample=_cfg("EHI_ASB_ENTRY_EHI_NUMBER"),
            assembly=_cfg("EHI_ASB_ENTRY_ASSEMBLY_CODE"),
            reads1=_cfg("EHI_ASB_ENTRY_READS1"),
            reads2=_cfg("EHI_ASB_ENTRY_READS2"),
            code=_cfg("EHI_ASB_ENTRY_CODE"),
        )
    if module == "quantifying":
        return Fields(
            sample=_cfg("MAG_PPR_EHI"),
            reads1=_cfg("MAG_PPR_READS1"),
            reads2=_cfg("MAG_PPR_READS2"),
        )
    return Fields(
        code=_cfg("EHI_ASB_ENTRY_ASSEMBLY_CODE") or _cfg("EHI_ASB_ENTRY_CODE"),
        url=_cfg("EHI_ASB_ENTRY_ASSEMBLY_URL"),
        sample=_cfg("EHI_ASB_ENTRY_ASSEMBLY_CODE") or _cfg("EHI_ASB_ENTRY_CODE"),
    )


@dataclass
class Batch:
    """One batch and the entries it works on, as one database describes it."""

    module: str
    code: str
    source: str
    record: dict[str, Any]
    fields: Fields
    entries: list[dict[str, Any]] = field(default_factory=list)

    @property
    def from_core(self) -> bool:
        return self.source == CORE

    def value(self, config_key: str, column: str = "") -> Any:
        """A field of the batch record itself, by whichever name it has here."""
        if self.from_core:
            held = self.record.get(column or config_key)
            return held if held not in (None, "") else None
        key = _cfg(config_key)
        if not key:
            return None
        held = (self.record.get("fields") or {}).get(key)
        if isinstance(held, list):
            held = held[0] if held else None
        return held if held not in (None, "") else None


def airtable_configured(module: str) -> bool:
    """Whether Airtable is still set up for this module.

    Emptying a module's Airtable keys is how Airtable is switched off: the
    commands then read the batch and its entries from the core alone.
    """
    keys = [_PRIMARY_BASE[module], _BATCH_TABLE_CFG[module], _BATCH_CODE_CFG[module]]
    return all(_cfg(key) for key in keys)


def _airtable_batch(module: str, code: str, token: str) -> Batch | None:
    """The batch and its entries as Airtable holds them, or None."""
    from ehio.airtable import AirtableClient

    client = AirtableClient(api_key=token, base_id=_cfg(_PRIMARY_BASE[module]))
    batch_table = _cfg(_BATCH_TABLE_CFG[module])
    code_field = _cfg(_BATCH_CODE_CFG[module])

    entry_cfg = _ENTRY_TABLE_CFG.get(module)
    if entry_cfg:
        entry_table, entry_batch_field = (_cfg(key) for key in entry_cfg)
        record, entries = client.fetch_batch_and_entries(
            batch_table=batch_table,
            batch_code_field=code_field,
            batch_code=code,
            entry_table=entry_table,
            entry_batch_field=entry_batch_field,
        )
    else:
        record, entries = client.fetch_batch_record(batch_table, code_field, code), []
    if record is None:
        return None
    return Batch(module, code, AIRTABLE, record, _airtable_fields(module), entries)


def _core_batch(module: str, code: str, core) -> Batch:  # noqa: D401
    """The batch and its entries as ehi-core holds them.

    The batch's own row comes with the entries, because the run is launched
    with what it holds: the assembly type, the annotation type, the ANI
    threshold, the reference genome.
    """
    found = core.client.batch_entries(CORE_BATCH_TABLE[module], code)
    return Batch(
        module, found.get("batch") or code, CORE,
        found.get("row") or {"code": code},
        _CORE_FIELDS[module], found.get("entries") or [],
    )


def open_batch(module: str, code: str, token: str, core=None) -> Batch:
    """The batch `code`, from Airtable when it holds it, else from ehi-core.

    Raises LookupError when neither does, so the caller can report it the way
    it reports every other missing batch.
    """
    found = _airtable_batch(module, code, token) if airtable_configured(module) and token else None
    if found is not None:
        return found
    if not core:
        where = f"{_cfg(_BATCH_TABLE_CFG[module])}" if airtable_configured(module) else "Airtable"
        raise LookupError(f"Batch '{code}' not found in {where}, and ehi-core is not in use.")
    if airtable_configured(module):
        print(
            f"  Batch '{code}' is not in Airtable — reading it from ehi-core.",
            file=sys.stderr,
        )
    from ehio.core import CoreError

    try:
        return _core_batch(module, code, core)
    except CoreError as exc:
        # A batch neither database holds is a missing batch, not a broken core.
        if exc.status == 404:
            where = "Airtable or ehi-core" if airtable_configured(module) else "ehi-core"
            raise LookupError(f"Batch '{code}' is in neither {where}.") from exc
        raise
