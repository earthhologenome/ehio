"""ehi-core API client for ehio.

ehi-core is the EHI's own database for the bioinformatic pipeline, taking over
from the Airtable tables as they run out of room. While batches are still
created in Airtable, ehio writes everything it writes there into the core as
well, and the MAGs, which Airtable's MAG table can no longer take, live in the
core alone.

ehio reaches the core's pipeline routes (/api/pipeline/*) at EHI_CORE_URL with
the pipeline token, which is never stored in the config:

    export EHI_CORE_TOKEN="..."   # the value of the core-pipeline-token secret

What is written, and when, is built in ehio.mirror; this module only sends it.

Several ehio jobs can finish at once on the cluster, and one of them may be
bringing thousands of MAGs.  So that they write one after another rather than
all at once, every write happens inside a run: ehio asks the core for its turn
(POST /pipeline/runs), waits while another run holds the core, names its run on
each write, reports how far it has got, and lets go at the end.  Staff see the
run in the core's editor meanwhile, and are told to wait.
"""

from __future__ import annotations

import random
import sys
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator
from urllib.parse import quote

try:
    import requests
except ImportError:  # requests ships with pyairtable
    requests = None  # type: ignore[assignment]


TOKEN_HINT = "Export EHI_CORE_TOKEN (the core-pipeline-token secret) or pass --core-token."

# Cloud Run starts the core on demand, so the first request after a quiet spell
# can meet a gateway error while an instance boots.  A 409 is two batches
# numbering new MAGs at the same moment: the losing request was rolled back as
# a whole, so sending it again is safe.
_RETRY_STATUSES = {409, 502, 503, 504}

# A DMB batch can bring thousands of MAGs; they are sent in requests of this
# many records, so no single request runs into the core's timeout.
UNITS_PER_REQUEST = 250

# While another ehio run holds the core, a write waits this long at the most
# for its turn (EHI_CORE_WAIT_MINUTES), asking again after a pause that grows
# from the first to the longest, and says it is still waiting every so often.
WAIT_MINUTES = 120.0
WAIT_PAUSE_FIRST = 15.0
WAIT_PAUSE_LONGEST = 60.0
WAIT_NOTE_EVERY = 600.0
# The core lets go of a run that stops reporting after this long.
RUN_LAPSES_MINUTES = 10

# One (table, rows) pair per core table, applied in order.
Change = tuple[str, list[dict[str, Any]]]
# The changes that belong to one record — a preprocessing and the hologenome
# it links to — and so succeed or fail together.
Unit = list[Change]


class CoreError(RuntimeError):
    """A request to ehi-core failed for a reason the user can act on."""

    def __init__(self, message: str, status: int | None = None, detail: str = "") -> None:
        super().__init__(message)
        self.status = status
        # What the core said, as it said it.
        self.detail = detail


def _detail(response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return (getattr(response, "text", "") or "").strip()[:500]
    if isinstance(payload, dict) and "detail" in payload:
        return str(payload["detail"])
    return str(payload)[:500]


def _explain(status: int, detail: str, action: str, base: str) -> str:
    if status == 401:
        return (
            "ehi-core rejected the pipeline token (401 Unauthorized): it does not "
            f"match the core-pipeline-token secret. {TOKEN_HINT}"
        )
    if status == 503 and "not configured" in detail:
        return (
            "ehi-core has no pipeline token configured (503): add a version to the "
            "core-pipeline-token secret and redeploy the core (scripts/deploy.sh core "
            "in ehi-main)."
        )
    if status == 404:
        return f"ehi-core could not {action} (404 Not Found): {detail}. Check EHI_CORE_URL ({base})."
    if status == 422:
        return f"ehi-core refused to {action} (422): {detail}"
    if status == 409:
        return f"ehi-core refused to {action} because of a conflict (409): {detail}"
    if status == 423:
        return f"ehi-core is busy (423): {detail}"
    return f"ehi-core request to {action} failed with HTTP {status}: {detail}"


class CoreClient:
    def __init__(
        self,
        url: str,
        token: str,
        *,
        timeout: float = 120.0,
        attempts: int = 4,
        sleep: Callable[[float], None] = time.sleep,
        wait_minutes: float = WAIT_MINUTES,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if requests is None:
            raise CoreError("The requests package is required to reach ehi-core. Run: pip install requests")
        base = url.strip().rstrip("/")
        self._base = base if base.endswith("/api") else f"{base}/api"
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {token.strip()}"
        self._timeout = timeout
        self._attempts = max(1, attempts)
        self._sleep = sleep
        self._wait = max(0.0, wait_minutes) * 60
        self._clock = clock
        # The run this client writes in, once the core has given it its turn;
        # None outside a run, and inside one on a core that predates runs.
        self._run: int | None = None
        self._run_depth = 0
        self._run_label = ""
        self._run_total: int | None = None
        # False once the core has said it does not know runs.
        self._takes_turns = True

    @property
    def url(self) -> str:
        return self._base

    def _call(self, method: str, path: str, action: str, body: Any = None) -> Any:
        url = f"{self._base}/pipeline{path}"
        for attempt in range(1, self._attempts + 1):
            try:
                response = self._session.request(method, url, json=body, timeout=self._timeout)
            except requests.RequestException as exc:
                if attempt == self._attempts:
                    raise CoreError(f"Could not reach ehi-core at {self._base} to {action}: {exc}") from exc
            else:
                if response.ok:
                    return None if response.status_code == 204 else response.json()
                detail = _detail(response)
                retry = response.status_code in _RETRY_STATUSES and "not configured" not in detail
                if not retry or attempt == self._attempts:
                    raise CoreError(_explain(response.status_code, detail, action, self._base),
                                    status=response.status_code, detail=detail)
            self._sleep(min(8.0, 2.0 ** (attempt - 1)) + random.random())
        raise AssertionError("unreachable")

    def ping(self) -> None:
        """Check that the core answers and accepts the token."""
        self._call("GET", "/ping", "check the pipeline token")

    # --- Runs: one ehio writing at a time ---------------------------------------

    @contextmanager
    def run(self, label: str, total: int | None = None) -> Iterator[None]:
        """Write in a run of its own: wait for the core to be free, hold it
        while writing, and let go at the end.

        `label` is what staff read in the editor ("MAGs of batch 'DMB0042'"),
        `total` how many records the run writes, when known.  A run inside a
        run is part of it, so each write method can take a run of its own and
        still join the one it is called in.
        """
        if self._run_depth:
            self._run_depth += 1
            try:
                yield
            finally:
                self._run_depth -= 1
            return
        self._begin(label, total)
        self._run_depth = 1
        try:
            yield
        finally:
            self._run_depth = 0
            self._end()

    def progress(self, done: int) -> None:
        """Say how many records the run has written, which also keeps the core
        from letting go of it between writes."""
        if self._run is None:
            return
        try:
            self._call("PATCH", f"/runs/{self._run}", f"report on writing {self._run_label}", {"done": done})
        except CoreError:
            # Only a report: a run that lost the core (423) waits for a new
            # turn at its next write, and a core that is down fails that write.
            pass

    def _begin(self, label: str, total: int | None) -> None:
        self._run_label, self._run_total = label, total
        self._run = None
        if not self._takes_turns:
            return
        body = {"label": label, "total": total}
        waiting_since: float | None = None
        noted = 0.0
        pause = WAIT_PAUSE_FIRST
        while True:
            try:
                started = self._call("POST", "/runs", f"start writing {label}", body)
            except CoreError as exc:
                if exc.status == 404:
                    # A core from before runs: write as ehio always did.
                    self._takes_turns = False
                    return
                if exc.status != 423:
                    raise
                now = self._clock()
                if waiting_since is None:
                    waiting_since = noted = now
                    _note(f"ehi-core is busy: {exc.detail}. Waiting for it before writing {label} "
                          f"(up to {self._wait / 60:.0f} min)...")
                elif now - noted >= WAIT_NOTE_EVERY:
                    noted = now
                    _note(f"Still waiting for ehi-core ({(now - waiting_since) / 60:.0f} min so far): {exc.detail}.")
                if now - waiting_since + pause > self._wait:
                    raise CoreError(
                        f"ehi-core stayed busy for {(now - waiting_since) / 60:.0f} min, so {label} "
                        f"was not written: {exc.detail}. Set EHI_CORE_WAIT_MINUTES to wait longer.",
                        status=423, detail=exc.detail,
                    ) from exc
                self._sleep(pause + random.random())
                pause = min(pause * 1.5, WAIT_PAUSE_LONGEST)
                continue
            self._run = started["run"]["id"]
            self._session.headers["X-Pipeline-Run"] = str(self._run)
            if waiting_since is not None:
                _note(f"ehi-core is free: writing {label}.")
            return

    def _end(self) -> None:
        run, self._run = self._run, None
        self._session.headers.pop("X-Pipeline-Run", None)
        if run is None:
            return
        try:
            self._call("DELETE", f"/runs/{run}", f"finish writing {self._run_label}")
        except CoreError as exc:
            _warn(f"Could not tell ehi-core that ehio is done writing ({exc}); "
                  f"the core lets go of the run by itself within {RUN_LAPSES_MINUTES} minutes.")

    def _write(self, method: str, path: str, action: str, body: Any, label: str) -> Any:
        """A write, in the run it is called in or else in a run of its own."""
        with self.run(label):
            try:
                return self._call(method, path, action, body)
            except CoreError as exc:
                if exc.status != 423 or self._run is None:
                    raise
            # This run went quiet for so long that another took the core
            # meanwhile: wait for a new turn, and write in it.
            self._begin(self._run_label, self._run_total)
            return self._call(method, path, action, body)

    def upsert(self, changes: list[Change]) -> list[dict[str, Any]]:
        """Write rows table by table, in the order given, in one transaction.

        Returns one result per row, in order: {"table", "code", "action"} plus
        "skipped" when some of the row's defaults were not taken.
        """
        changes = [(table, rows) for table, rows in changes if rows]
        if not changes:
            return []
        tables = ", ".join(dict.fromkeys(table for table, _ in changes))
        result = self._write(
            "POST", "/upsert", f"write {tables}",
            {"changes": [{"table": table, "rows": rows} for table, rows in changes]},
            tables.replace("_", " "),
        )
        return [{"table": group["table"], **row} for group in result["results"] for row in group["rows"]]

    def pending_batches(self, table: str, statuses: list[str]) -> list[dict[str, Any]]:
        """The batches of one batch table whose status is one of `statuses`.

        This is the core's side of the scan that reads Airtable's batch
        statuses: each batch comes back as the editor holds it, so the scan
        reads the reference genome, the boosts and the batch type from it.
        """
        query = "&".join(f"status={quote(status, safe='')}" for status in statuses)
        path = f"/{quote(table, safe='')}/pending?{query}"
        return self._call("GET", path, f"read the pending {table}")["batches"]

    def batch_entries(self, table: str, batch: str) -> dict[str, Any]:
        """What a batch works on, with the batch's own row.

        An assembly batch comes back one row per sample, the way Airtable's
        entries are and the sample sheet needs, so a coassembly gives one row
        per library with the assembly code they share.
        """
        path = f"/{quote(table, safe='')}/{quote(batch, safe='')}/entries"
        return self._call("GET", path, f"read the entries of {batch}")

    def link_assembly_samples(
        self, batch: str, assemblies: dict[str, list[str]]
    ) -> dict[str, Any]:
        """Set which preprocessed samples each assembly of a batch was built
        from — the grouping a binning run was launched with."""
        path = f"/assembly_batches/{quote(batch, safe='')}/assemblies"
        return self._write("POST", path, f"group the assemblies of {batch}",
                           {"assemblies": {k: list(v) for k, v in assemblies.items()}},
                           f"the assemblies of batch '{batch}'")

    def batch_mags(self, batch: str) -> list[dict[str, Any]]:
        """The MAGs a dereplication batch took in, with 'is_representative'."""
        path = f"/dereplication_batches/{quote(batch, safe='')}/mags"
        return self._call("GET", path, f"read the MAGs of {batch}")["mags"]

    def link_batch_mags(
        self, batch: str, mags: list[str], representatives: list[str] | None = None
    ) -> dict[str, Any]:
        """Add MAGs (EHM codes, bin names or Airtable record ids) to a batch and,
        when `representatives` is given, make them exactly the ones it kept."""
        body: dict[str, Any] = {"mags": list(mags)}
        if representatives is not None:
            body["representatives"] = list(representatives)
        path = f"/dereplication_batches/{quote(batch, safe='')}/mags"
        return self._write("POST", path, f"link MAGs to {batch}", body, f"the MAGs of batch '{batch}'")


_VERIFIED: set[tuple[str, str]] = set()


def verify(client: CoreClient, token: str) -> None:
    """Ping the core once per process and token, like the Airtable token check."""
    if (client.url, token) in _VERIFIED:
        return
    client.ping()
    _VERIFIED.add((client.url, token))


def _warn(message: str) -> None:
    print(f"\033[1;31mWarning:\033[0m {message}", file=sys.stderr)


def _note(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _run_label(what: str | None, units: list[Unit]) -> str:
    """What a write is called in the core's editor: "ehio is writing <label>".

    A command's own name for it reads on in the middle of a sentence ("Batch
    'X'" becomes "batch 'X'", "QC metrics" stays), and without one the tables
    it writes stand in.
    """
    if what:
        what = what.strip()
        if len(what) > 1 and what[0].isupper() and what[1].islower():
            what = what[0].lower() + what[1:]
        return what
    tables = dict.fromkeys(table for unit in units for table, rows in unit if rows)
    return ", ".join(tables).replace("_", " ") or "records"


class CoreSession:
    """ehi-core as one command uses it.

    Falsy when the core is not in use for this command (EHI_CORE_URL empty, or
    no token yet), so callers write ``if core:`` around core-only work.

    ``mirror`` writes what ehio also writes to Airtable: while Airtable is still
    the record, a failure there is reported and the batch carries on, unless
    ``required`` (EHI_CORE_REQUIRED, or a command whose data only the core
    holds).  ``write`` is for data the core alone holds, and raises.
    """

    def __init__(self, client: CoreClient | None = None, required: bool = False) -> None:
        self.client = client
        self.required = required

    def __bool__(self) -> bool:
        return self.client is not None

    def write(self, units: list[Unit], what: str | None = None) -> list[dict[str, Any]]:
        """Send the units a few hundred to a request, in one run, so no other
        ehio writes in between.  When the core refuses a request (422), its
        units are sent one by one, so that one bad record costs only itself;
        the records that failed are then raised together.  `what` names the
        write for the core's editor, which shows it while it is under way."""
        assert self.client is not None
        units = [unit for unit in units if any(rows for _, rows in unit)]
        results: list[dict[str, Any]] = []
        failures: list[str] = []
        if not units:
            return results
        with self.client.run(_run_label(what, units), total=len(units)):
            for start in range(0, len(units), UNITS_PER_REQUEST):
                chunk = units[start:start + UNITS_PER_REQUEST]
                try:
                    results += self.client.upsert([change for unit in chunk for change in unit])
                except CoreError as exc:
                    if exc.status != 422 or len(chunk) < 2:
                        raise
                    for unit in chunk:
                        try:
                            results += self.client.upsert(unit)
                        except CoreError as unit_exc:
                            failures.append(str(unit_exc))
                self.client.progress(start + len(chunk))
        _report_skipped(results)
        if failures:
            raise CoreError(
                f"{len(failures)} of {len(units)} records were not written to ehi-core:\n  "
                + "\n  ".join(failures),
                status=422,
            )
        return results

    def mirror(self, what: str, units: list[Unit]) -> list[dict[str, Any]]:
        """``write``, reporting a failure instead of raising it unless required."""
        if not self.client:
            return []
        try:
            return self.write(units, what)
        except CoreError as exc:
            if self.required:
                raise
            _warn(f"{what} not written to ehi-core (Airtable has it): {exc}")
            return []

    def mirror_call(self, what: str, call: Callable[[CoreClient], Any]) -> Any:
        """Any other client call, under the same rule as ``mirror``."""
        if not self.client:
            return None
        try:
            return call(self.client)
        except CoreError as exc:
            if self.required:
                raise
            _warn(f"{what} not written to ehi-core: {exc}")
            return None


def _report_skipped(results: list[dict[str, Any]]) -> None:
    """Name the Airtable facts a newly created row could not take.

    On a row the core already held they only mean the core knew better; on a
    new one they are gaps worth seeing, such as a MAG left without its assembly.
    """
    for result in results:
        if result.get("action") == "created" and result.get("skipped"):
            for column, why in result["skipped"].items():
                print(
                    f"  ehi-core: {result['code']} was added without its {column}: {why}",
                    file=sys.stderr,
                )
