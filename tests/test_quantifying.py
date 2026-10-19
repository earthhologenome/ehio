"""Core-only quantifying output keeps measured rates on preprocessing entries."""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import patch

from ehio import cli
from tests.fake_core import FakeCoreClient
from tests.test_core_only import CONFIG, run


def _args(directory: Path) -> argparse.Namespace:
    return argparse.Namespace(
        batch="DMB001", local_dir=str(directory), input=False, output=True,
        skip_derep=False, rerun=False, core_token=None,
        host=None, user=None, port=None, identity=None, remote_dir=None, verbose=False,
    )


def _core() -> FakeCoreClient:
    return FakeCoreClient(batches={"DMB001": {
        "row": {"code": "DMB001"},
        "entries": [
            {"preprocessing_code": "PR00001", "hologenome_code": "EHI001"},
            {"preprocessing_code": "PR00002", "hologenome_code": "EHI002"},
        ],
    }})


def test_measured_rates_are_written_for_each_preprocessing(tmp_path):
    (tmp_path / "profiling_genomes.tsv").write_text(
        "sample\tmapping_percentage\nEHI001\t71.2\nEHI002\t55.0\n")
    fake = _core()
    with patch.object(cli, "_get_drakkar_version", return_value="2.5.0"):
        assert run(CONFIG, fake, lambda: cli._run_quantifying_output(_args(tmp_path))) == 0
    assert [(row["key"]["preprocessing_id"], row["values"])
            for row in fake.rows("dereplication_mappings")] == [
        ("PR00001", {"mapping_rate": 71.2}),
        ("PR00002", {"mapping_rate": 55.0}),
    ]


def test_missing_rate_does_not_write_a_mapping_value(tmp_path):
    (tmp_path / "profiling_genomes.tsv").write_text(
        "sample\tmapping_percentage\nEHI001\tNA\n")
    fake = _core()
    with patch.object(cli, "_get_drakkar_version", return_value="2.5.0"):
        assert run(CONFIG, fake, lambda: cli._run_quantifying_output(_args(tmp_path))) == 0
    assert all("mapping_rate" not in row["values"] for row in fake.rows("dereplication_mappings"))
