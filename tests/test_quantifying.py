"""Tests for the MAG_DMB_ENTRY records 'ehio quantifying --output' writes."""

from __future__ import annotations

from unittest.mock import MagicMock

from ehio import config as cfg
from ehio.cli import _create_airtable_mappings


BATCH = {"id": "recBATCH"}


def _ppr(rec_id: str, sample: str, ehi_field: str) -> dict:
    return {"id": rec_id, "fields": {ehi_field: sample}}


def _client(existing: list[dict]) -> MagicMock:
    client = MagicMock()
    client._table.return_value.all.return_value = existing
    return client


def test_existing_records_without_a_rate_get_the_measured_one():
    """A batch whose output step ran before its reads were mapped left its
    records without a rate; running the step again fills them in."""
    ehi_field  = "EHI_NUMBER"
    ppr_field  = cfg.get("MAG_DMB_ENTRY_PPR")
    rate_field = cfg.get("MAG_DMB_ENTRY_MAPPING_RATE")
    ppr_records = [_ppr("recP1", "EHI001", ehi_field), _ppr("recP2", "EHI002", ehi_field)]
    existing = [
        {"id": "recD1", "fields": {ppr_field: ["recP1"]}},
        {"id": "recD2", "fields": {ppr_field: ["recP2"], rate_field: 55.0}},
    ]
    metrics = {"EHI001": {"mapping_rate": 71.2}, "EHI002": {"mapping_rate": 55.0}}
    client = _client(existing)

    _create_airtable_mappings(client, "DMB001", BATCH, ppr_records, metrics, ehi_field)

    client.create_records.assert_not_called()
    client.update_records.assert_called_once_with(
        cfg.get("MAG_DMB_ENTRY"),
        [{"id": "recD1", "fields": {rate_field: 71.2}}],
    )


def test_a_missing_rate_does_not_blank_an_existing_one():
    ehi_field  = "EHI_NUMBER"
    ppr_field  = cfg.get("MAG_DMB_ENTRY_PPR")
    rate_field = cfg.get("MAG_DMB_ENTRY_MAPPING_RATE")
    existing = [{"id": "recD1", "fields": {ppr_field: ["recP1"], rate_field: 60.0}}]
    client = _client(existing)

    _create_airtable_mappings(
        client, "DMB001", BATCH, [_ppr("recP1", "EHI001", ehi_field)], {}, ehi_field,
    )

    client.update_records.assert_not_called()
