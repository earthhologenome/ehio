"""Tests for the ehi-core client and for how a command decides to use the core."""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from ehio import cli
from ehio import core
from ehio.core import CoreClient, CoreError, CoreSession


def _response(status: int, payload=None, text: str = ""):
    response = MagicMock()
    response.status_code = status
    response.ok = 200 <= status < 300
    if payload is None:
        response.json.side_effect = ValueError("no json")
    else:
        response.json.return_value = payload
    response.text = text
    return response


def _client(*responses, **options) -> tuple[CoreClient, MagicMock]:
    """A client answered by `responses` in turn; `session.sent_headers` holds
    the headers each request went with."""
    client = CoreClient("https://core.test", "secret", sleep=lambda _: None, **options)
    session = MagicMock()
    session.headers = {}
    session.sent_headers = []
    queue = list(responses)

    def request(*args, **kwargs):
        session.sent_headers.append(dict(session.headers))
        answer = queue.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    session.request.side_effect = request
    client._session = session
    return client, session


def _sent(session) -> list[tuple[str, str]]:
    """Each request as (method, path under /api/pipeline)."""
    return [(c.args[0], c.args[1].split("/api/pipeline", 1)[1]) for c in session.request.call_args_list]


def _begun(run: int = 7):
    return _response(201, {"run": {"id": run}})


ENDED = _response(204)
BUSY = _response(423, {"detail": "ehio is already writing MAGs of batch 'DMB0042' to the core; wait for it to finish"})


# ---------------------------------------------------------------------------
# CoreClient
# ---------------------------------------------------------------------------

class TestCoreClient:
    def test_the_api_prefix_is_added_once(self):
        assert CoreClient("https://core.test/", "t").url == "https://core.test/api"
        assert CoreClient("https://core.test/api", "t").url == "https://core.test/api"

    def test_the_token_is_sent_as_a_bearer(self):
        client = CoreClient("https://core.test", " secret ")
        assert client._session.headers["Authorization"] == "Bearer secret"

    def test_upsert_sends_the_tables_in_order_and_flattens_the_results(self):
        client, session = _client(_begun(), _response(200, {"results": [
            {"table": "hologenomes", "rows": [{"code": "EHI00001", "action": "created"}]},
            {"table": "preprocessings", "rows": [{"code": "PR00001", "action": "updated"}]},
        ]}), ENDED)
        results = client.upsert([
            ("hologenomes", [{"key": {"code": "EHI00001"}}]),
            ("preprocessings", [{"key": {"code": "PR00001"}}]),
            ("assemblies", []),
        ])
        sent = session.request.call_args_list[1]
        body = sent.kwargs["json"]
        assert [change["table"] for change in body["changes"]] == ["hologenomes", "preprocessings"]
        assert sent.args == ("POST", "https://core.test/api/pipeline/upsert")
        assert results == [
            {"table": "hologenomes", "code": "EHI00001", "action": "created"},
            {"table": "preprocessings", "code": "PR00001", "action": "updated"},
        ]

    def test_nothing_to_write_sends_nothing(self):
        client, session = _client()
        assert client.upsert([("mags", [])]) == []
        session.request.assert_not_called()

    def test_a_cold_start_is_retried(self):
        client, session = _client(_response(503, text="Service Unavailable"), _response(200, {"actor": "ehio"}))
        client.ping()
        assert session.request.call_count == 2

    def test_two_batches_numbering_mags_at_once_is_retried(self):
        client, session = _client(
            _begun(),
            _response(409, {"detail": "EHM000123 conflicts with another row of MAGs"}),
            _response(200, {"results": []}),
            ENDED,
        )
        client.upsert([("mags", [{"key": {"name": "EHA1_bin_1.fa"}}])])
        assert _sent(session).count(("POST", "/upsert")) == 2

    def test_a_refused_value_is_not_retried(self):
        client, session = _client(_begun(), _response(422, {"detail": "MAGs, row 1: Completeness must be a number"}), ENDED)
        with pytest.raises(CoreError) as excinfo:
            client.upsert([("mags", [{"key": {"name": "x"}}])])
        assert _sent(session) == [("POST", "/runs"), ("POST", "/upsert"), ("DELETE", "/runs/7")]
        assert excinfo.value.status == 422
        assert "Completeness must be a number" in str(excinfo.value)

    def test_a_rejected_token_says_where_the_token_comes_from(self):
        client, _ = _client(_response(401, {"detail": "Invalid pipeline token"}))
        with pytest.raises(CoreError) as excinfo:
            client.ping()
        assert "core-pipeline-token" in str(excinfo.value)
        assert "EHI_CORE_TOKEN" in str(excinfo.value)

    def test_a_core_without_a_token_configured_is_not_retried(self):
        client, session = _client(_response(503, {"detail": "Pipeline access is not configured on this server"}))
        with pytest.raises(CoreError) as excinfo:
            client.ping()
        assert session.request.call_count == 1
        assert "deploy" in str(excinfo.value)

    def test_an_unreachable_core_is_reported_after_the_retries(self):
        import requests

        client, session = _client(*[requests.ConnectionError("refused")] * 4)
        with pytest.raises(CoreError) as excinfo:
            client.ping()
        assert session.request.call_count == 4
        assert "Could not reach ehi-core" in str(excinfo.value)

    def test_batch_mags_quotes_the_code(self):
        client, session = _client(_response(200, {"batch": "DMB0001", "mags": [{"code": "EHM000001"}]}))
        assert client.batch_mags("DMB0001") == [{"code": "EHM000001"}]
        assert session.request.call_args.args[1].endswith("/pipeline/dereplication_batches/DMB0001/mags")

    def test_representatives_are_only_sent_when_given(self):
        client, session = _client(_begun(), _response(200, {}), ENDED, _begun(8), _response(200, {}), ENDED)
        client.link_batch_mags("DMB0001", ["recM1"])
        assert "representatives" not in session.request.call_args_list[1].kwargs["json"]
        client.link_batch_mags("DMB0001", [], representatives=["EHM000001"])
        assert session.request.call_args_list[4].kwargs["json"]["representatives"] == ["EHM000001"]


# ---------------------------------------------------------------------------
# Runs — one ehio writing at a time
# ---------------------------------------------------------------------------

class TestRuns:
    def test_a_write_takes_the_core_names_its_run_and_lets_go(self):
        client, session = _client(_begun(7), _response(200, {"results": []}), ENDED)
        client.upsert([("mags", [{"key": {"name": "x"}}])])
        assert _sent(session) == [("POST", "/runs"), ("POST", "/upsert"), ("DELETE", "/runs/7")]
        assert session.request.call_args_list[0].kwargs["json"] == {"label": "mags", "total": None}
        assert session.sent_headers[1]["X-Pipeline-Run"] == "7"
        assert "X-Pipeline-Run" not in session.headers

    def test_a_busy_core_is_waited_for(self, capsys):
        pauses = []
        client, session = _client(BUSY, BUSY, _begun(), _response(200, {"results": []}), ENDED)
        client._sleep = pauses.append
        client.upsert([("mags", [{"key": {"name": "x"}}])])
        assert _sent(session)[:4] == [("POST", "/runs")] * 3 + [("POST", "/upsert")]
        assert len(pauses) == 2 and pauses[0] >= core.WAIT_PAUSE_FIRST and pauses[1] > pauses[0]
        err = capsys.readouterr().err
        assert "ehi-core is busy: ehio is already writing MAGs of batch 'DMB0042'" in err
        assert "ehi-core is free: writing mags." in err

    def test_a_core_busy_for_too_long_is_given_up_on(self):
        now = [0.0]
        client, session = _client(*[BUSY] * 20, wait_minutes=1, clock=lambda: now[0])
        client._sleep = lambda seconds: now.__setitem__(0, now[0] + seconds)
        with pytest.raises(CoreError) as excinfo:
            client.upsert([("mags", [{"key": {"name": "x"}}])])
        assert excinfo.value.status == 423
        assert "stayed busy" in str(excinfo.value) and "EHI_CORE_WAIT_MINUTES" in str(excinfo.value)
        assert ("POST", "/upsert") not in _sent(session)
        assert now[0] <= 60

    def test_a_core_from_before_runs_is_written_to_as_before(self):
        client, session = _client(
            _response(404, {"detail": "Not Found"}), _response(200, {"results": []}), _response(200, {"results": []}),
        )
        client.upsert([("mags", [{"key": {"name": "x"}}])])
        client.upsert([("mags", [{"key": {"name": "y"}}])])
        assert _sent(session) == [("POST", "/runs"), ("POST", "/upsert"), ("POST", "/upsert")]
        assert "X-Pipeline-Run" not in session.sent_headers[1]

    def test_a_progress_report_that_fails_does_not_stop_the_write(self):
        units = [_unit(f"PR{n:05d}") for n in range(core.UNITS_PER_REQUEST + 1)]
        ok = _response(200, {"results": []})
        client, session = _client(_begun(), ok, _response(500, text="oops"), ok, _response(200, {"run": {}}), ENDED)
        CoreSession(client).write(units)
        assert _sent(session).count(("POST", "/upsert")) == 2

    def test_a_run_that_lost_the_core_waits_for_a_new_turn(self):
        gone = _response(423, {"detail": "Run 7 no longer holds the core; open another before writing"})
        client, session = _client(_begun(7), gone, _begun(8), _response(200, {"results": []}), ENDED)
        client.upsert([("mags", [{"key": {"name": "x"}}])])
        assert _sent(session) == [
            ("POST", "/runs"), ("POST", "/upsert"), ("POST", "/runs"), ("POST", "/upsert"), ("DELETE", "/runs/8"),
        ]
        assert session.sent_headers[3]["X-Pipeline-Run"] == "8"

    def test_a_run_the_core_could_not_be_told_about_is_left_to_lapse(self, capsys):
        import requests

        client, _ = _client(_begun(), _response(200, {"results": []}), *[requests.ConnectionError("refused")] * 4)
        client.upsert([("mags", [{"key": {"name": "x"}}])])
        assert "lets go of the run by itself" in capsys.readouterr().err

    def test_a_large_write_is_one_run_that_reports_its_progress(self):
        units = [_unit(f"PR{n:05d}") for n in range(core.UNITS_PER_REQUEST + 1)]
        ok, reported = _response(200, {"results": []}), _response(200, {"run": {}})
        client, session = _client(_begun(), ok, reported, ok, reported, ENDED)
        CoreSession(client).write(units, "QC metrics of batch 'PRB0003'")
        assert _sent(session) == [
            ("POST", "/runs"), ("POST", "/upsert"), ("PATCH", "/runs/7"),
            ("POST", "/upsert"), ("PATCH", "/runs/7"), ("DELETE", "/runs/7"),
        ]
        calls = session.request.call_args_list
        assert calls[0].kwargs["json"] == {"label": "QC metrics of batch 'PRB0003'", "total": len(units)}
        assert [calls[i].kwargs["json"]["done"] for i in (2, 4)] == [core.UNITS_PER_REQUEST, len(units)]

    def test_nothing_to_write_takes_no_run(self):
        client, session = _client()
        assert CoreSession(client).write([[("preprocessings", [])]]) == []
        session.request.assert_not_called()

    def test_a_run_is_named_to_read_on_in_a_sentence(self):
        assert core._run_label("Batch 'DMB0042'", []) == "batch 'DMB0042'"
        assert core._run_label("QC metrics of batch 'PRB0003'", []) == "QC metrics of batch 'PRB0003'"
        assert core._run_label("MAGs of batch 'DMB0042'", []) == "MAGs of batch 'DMB0042'"
        units = [[("mags", [{}])], [("dereplication_batches", [{}]), ("hologenomes", [])]]
        assert core._run_label(None, units) == "mags, dereplication batches"


# ---------------------------------------------------------------------------
# CoreSession
# ---------------------------------------------------------------------------

def _unit(code: str):
    return [("preprocessings", [{"key": {"code": code}}])]


class TestCoreSession:
    def test_an_absent_core_is_falsy_and_mirrors_nothing(self):
        session = CoreSession()
        assert not session
        assert session.mirror("anything", [_unit("PR00001")]) == []

    def test_one_bad_record_costs_only_itself(self):
        client = MagicMock()

        def upsert(changes):
            codes = [row["key"]["code"] for _, rows in changes for row in rows]
            if "PR00002" in codes:
                raise CoreError("PR00002 refused", status=422)
            return [{"table": "preprocessings", "code": c, "action": "updated"} for c in codes]

        client.upsert.side_effect = upsert
        with pytest.raises(CoreError) as excinfo:
            CoreSession(client).write([_unit("PR00001"), _unit("PR00002"), _unit("PR00003")])
        written = [call.args[0] for call in client.upsert.call_args_list]
        assert len(written) == 4          # the batch, then each unit on its own
        assert "1 of 3 records" in str(excinfo.value)

    def test_a_core_that_cannot_be_reached_is_not_retried_unit_by_unit(self):
        client = MagicMock()
        client.upsert.side_effect = CoreError("down", status=None)
        with pytest.raises(CoreError):
            CoreSession(client).write([_unit("PR00001"), _unit("PR00002")])
        assert client.upsert.call_count == 1

    def test_large_writes_are_split(self):
        from ehio import core

        client = MagicMock()
        client.upsert.return_value = []
        CoreSession(client).write([_unit(f"PR{n:05d}") for n in range(core.UNITS_PER_REQUEST + 1)])
        assert client.upsert.call_count == 2

    def test_mirror_reports_a_failure_and_carries_on(self, capsys):
        client = MagicMock()
        client.upsert.side_effect = CoreError("down")
        assert CoreSession(client).mirror("QC metrics", [_unit("PR00001")]) == []
        assert "QC metrics not written to ehi-core" in capsys.readouterr().err

    def test_mirror_raises_when_the_core_is_required(self):
        client = MagicMock()
        client.upsert.side_effect = CoreError("down")
        with pytest.raises(CoreError):
            CoreSession(client, required=True).mirror("QC metrics", [_unit("PR00001")])

    def test_a_new_row_missing_a_fact_is_reported(self, capsys):
        client = MagicMock()
        client.upsert.return_value = [
            {"table": "mags", "code": "EHM000009", "action": "created",
             "skipped": {"assembly_id": "Assembly: there is no EHA99999 in Assemblies"}},
            {"table": "mags", "code": "EHM000001", "action": "updated", "skipped": {"status": "x"}},
        ]
        CoreSession(client).write([_unit("x")])
        err = capsys.readouterr().err
        assert "EHM000009 was added without its assembly_id" in err
        assert "EHM000001" not in err


# ---------------------------------------------------------------------------
# cli._core — whether a command uses the core
# ---------------------------------------------------------------------------

def _cfg(**values):
    return patch.object(cli.cfg, "get", side_effect=lambda k, d=None: values.get(k, d))


def _args(**values) -> argparse.Namespace:
    return argparse.Namespace(**{"core_token": None, **values})


class TestCoreForACommand:
    def test_no_url_means_airtable_alone(self):
        with _cfg():
            assert not cli._core(_args())

    def test_no_token_yet_means_airtable_alone_with_a_warning(self, capsys):
        with _cfg(EHI_CORE_URL="https://core.test"):
            assert not cli._core(_args())
        assert "EHI_CORE_TOKEN" in capsys.readouterr().err

    def test_no_token_stops_the_command_when_the_core_is_required(self):
        with _cfg(EHI_CORE_URL="https://core.test", EHI_CORE_REQUIRED="true"):
            with pytest.raises(SystemExit):
                cli._core(_args())

    def test_a_token_is_taken_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("EHI_CORE_TOKEN", "from-env")
        with _cfg(EHI_CORE_URL="https://core.test"), patch("ehio.core.verify") as verify:
            core = cli._core(_args())
        assert core
        assert verify.call_args.args[1] == "from-env"
        assert not core.required

    def test_the_flag_wins_over_the_environment(self, monkeypatch):
        monkeypatch.setenv("EHI_CORE_TOKEN", "from-env")
        with _cfg(EHI_CORE_URL="https://core.test"), patch("ehio.core.verify") as verify:
            cli._core(_args(core_token="from-flag"))
        assert verify.call_args.args[1] == "from-flag"

    def test_an_unreachable_core_is_left_out_of_a_mirroring_command(self, capsys):
        with _cfg(EHI_CORE_URL="https://core.test"), \
             patch("ehio.core.verify", side_effect=CoreError("ehi-core is down")):
            assert not cli._core(_args(core_token="t"))
        assert "ehi-core is down" in capsys.readouterr().err

    def test_an_unreachable_core_stops_a_command_working_on_mags(self):
        with _cfg(EHI_CORE_URL="https://core.test"), \
             patch("ehio.core.verify", side_effect=CoreError("ehi-core is down")):
            with pytest.raises(SystemExit):
                cli._core(_args(core_token="t"), holds=True)

    def test_required_makes_every_write_required(self):
        with _cfg(EHI_CORE_URL="https://core.test", EHI_CORE_REQUIRED="true"), patch("ehio.core.verify"):
            assert cli._core(_args(core_token="t")).required
