"""A stand-in for ehi-core, for the tests of commands that use it."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any
from unittest.mock import patch


class FakeCoreClient:
    """Records what ehio sends and serves a dereplication batch's MAGs, the
    way ehio.core.CoreClient talks to the real core."""

    url = "https://core.test/api"

    def __init__(
        self,
        mags: list[dict[str, Any]] | None = None,
        batches: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.mags = list(mags or [])
        self.upserts: list[list] = []
        self.links: list[tuple] = []
        # {batch code: {"row": {...}, "entries": [...]}}, as the core answers.
        self.batches = dict(batches or {})
        self.groupings: list[tuple] = []
        # (label, total) of every run written in, and each progress report.
        self.runs: list[tuple] = []
        self.reports: list[int] = []

    def ping(self) -> None:
        pass

    @contextmanager
    def run(self, label: str, total: int | None = None):
        self.runs.append((label, total))
        yield

    def progress(self, done: int) -> None:
        self.reports.append(done)

    def upsert(self, changes):
        self.upserts.append(changes)
        return [
            {"table": table, "code": row["key"].get("code") or row["key"].get("name"), "action": "created"}
            for table, rows in changes for row in rows
        ]

    def batch_entries(self, table: str, batch: str):
        from ehio.core import CoreError

        found = self.batches.get(batch)
        if found is None:
            raise CoreError(f"ehi-core could not read the entries of {batch} (404)", status=404)
        return {"table": table, "batch": batch,
                "row": found.get("row") or {"code": batch},
                "entries": list(found.get("entries") or [])}

    def link_assembly_samples(self, batch: str, assemblies):
        self.groupings.append((batch, {k: list(v) for k, v in assemblies.items()}))
        return {"batch": batch, "assemblies": len(assemblies),
                "samples": sum(len(v) for v in assemblies.values())}

    def batch_mags(self, batch: str):
        return self.mags

    def link_batch_mags(self, batch, mags, representatives=None):
        self.links.append((batch, list(mags), representatives))
        return {"batch": batch, "added": len(mags), "mag_count": len(self.mags),
                "representative_count": len(representatives or [])}

    def rows(self, table: str) -> list[dict[str, Any]]:
        """Every row sent for `table`, in the order sent."""
        return [row for changes in self.upserts for name, rows in changes if name == table for row in rows]


def using(fake: FakeCoreClient, required: bool = False):
    """Make every ehio command use `fake` as its ehi-core."""
    from ehio import cli
    from ehio.core import CoreSession

    return patch.object(cli, "_core", lambda args, holds=False: CoreSession(fake, required=required))
