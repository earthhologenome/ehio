"""A stand-in for ehi-core, for the tests of commands that use it."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch


class FakeCoreClient:
    """Records what ehio sends and serves a dereplication batch's MAGs, the
    way ehio.core.CoreClient talks to the real core."""

    url = "https://core.test/api"

    def __init__(self, mags: list[dict[str, Any]] | None = None) -> None:
        self.mags = list(mags or [])
        self.upserts: list[list] = []
        self.links: list[tuple] = []

    def ping(self) -> None:
        pass

    def upsert(self, changes):
        self.upserts.append(changes)
        return [
            {"table": table, "code": row["key"].get("code") or row["key"].get("name"), "action": "created"}
            for table, rows in changes for row in rows
        ]

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
