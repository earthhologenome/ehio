"""Tests for the binning output helpers — assembly discovery and gzip streaming."""

from __future__ import annotations

import argparse
import gzip
import io
from pathlib import Path

import pytest

from ehio.cli import (
    _assembly_remote_name,
    _find_assembly_fastas,
    _gzip_into,
    _run_binning_output,
)


CONTIGS = ">EHA00405_1\nACGTACGTACGT\n>EHA00405_2\nTTTTGGGGCCCC\n"


@pytest.fixture
def cataloging_output(tmp_path: Path) -> Path:
    """Build a minimal drakkar cataloging output tree for two assemblies."""
    for assembly in ("EHA00405", "EHA00406"):
        megahit_dir = tmp_path / "cataloging" / "megahit" / assembly
        megahit_dir.mkdir(parents=True)
        (megahit_dir / f"{assembly}.fna").write_text(CONTIGS)
        # megahit's own output, kept next to the renamed assembly
        (megahit_dir / "final.contigs.raw.fa").write_text(CONTIGS)

        final_dir = tmp_path / "cataloging" / "final" / assembly
        final_dir.mkdir(parents=True)
        (final_dir / f"{assembly}_bin_1.fa").write_text(CONTIGS)

    final_root = tmp_path / "cataloging" / "final"
    (final_root / "all_bin_paths.txt").write_text("")
    (final_root / "all_bin_metadata.csv").write_text("")
    return tmp_path


def test_find_assembly_fastas_returns_one_per_assembly(cataloging_output: Path) -> None:
    found = _find_assembly_fastas(cataloging_output)
    assert [p.name for p in found] == ["EHA00405.fna", "EHA00406.fna"]


def test_assembly_remote_name_keeps_the_erda_convention(cataloging_output: Path) -> None:
    found = _find_assembly_fastas(cataloging_output)
    assert [_assembly_remote_name(p) for p in found] == [
        "EHA00405_contigs.fasta.gz",
        "EHA00406_contigs.fasta.gz",
    ]


def test_find_assembly_fastas_ignores_bins_and_raw_contigs(cataloging_output: Path) -> None:
    found = _find_assembly_fastas(cataloging_output)
    assert all(p.suffix == ".fna" for p in found)
    assert not any("bin" in p.name or "raw" in p.name for p in found)


def test_find_assembly_fastas_without_megahit_dir(tmp_path: Path) -> None:
    assert _find_assembly_fastas(tmp_path) == []


def test_gzip_into_writes_a_readable_archive(tmp_path: Path) -> None:
    source = tmp_path / "EHA00405.fna"
    source.write_text(CONTIGS)

    handle = io.BytesIO()
    _gzip_into(source, handle)

    assert gzip.decompress(handle.getvalue()).decode() == CONTIGS


def test_binning_output_refuses_a_batch_without_cataloging_output(tmp_path: Path) -> None:
    """drakkar exits 0 on some of its own error paths; an empty output
    directory must fail the step instead of marking the batch as done."""
    args = argparse.Namespace(
        batch="ASB001", local_dir=str(tmp_path), airtable_token="tok", verbose=False,
    )
    with pytest.raises(SystemExit):
        _run_binning_output(args)

