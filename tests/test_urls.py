"""Tests for ehio.urls and drakkar.verify_remote_urls — URL reachability checks."""

from __future__ import annotations

import socket
import urllib.error

import pytest

from ehio import urls as urls_mod
from ehio.drakkar import verify_remote_urls
from ehio.urls import check_url, check_urls, is_remote_url


class TestIsRemoteUrl:
    @pytest.mark.parametrize("value", [
        "https://example.com/a.fq.gz",
        "http://example.com/a.fq.gz",
        "ftp://ftp.ncbi.nlm.nih.gov/genomes/ref.fna.gz",
        "sftp://host/path/a.fq.gz",
    ])
    def test_remote(self, value: str):
        assert is_remote_url(value)

    @pytest.mark.parametrize("value", [
        "/projects/ehi/data/a.fq.gz",
        "relative/a.fq.gz",
        "",
    ])
    def test_local(self, value: str):
        assert not is_remote_url(value)


class TestCheckUrl:
    def test_reachable_url_returns_none(self, monkeypatch):
        monkeypatch.setattr(urls_mod, "_check_http", lambda url, timeout: None)
        assert check_url("https://example.com/a.fq.gz") is None

    def test_http_error_reports_status(self, monkeypatch):
        def _fake_urlopen(req, timeout=None):
            raise urllib.error.HTTPError(req.full_url, 404, "Not Found", {}, None)

        monkeypatch.setattr(urls_mod.urllib.request, "urlopen", _fake_urlopen)
        reason = check_url("https://example.com/missing.fq.gz")
        assert reason == "HTTP 404 Not Found"

    def test_head_rejected_falls_back_to_get(self, monkeypatch):
        seen = []

        class _Resp:
            def __enter__(self): return self
            def __exit__(self, *exc): return False
            def read(self, n): return b"x"

        def _fake_urlopen(req, timeout=None):
            seen.append(req.get_method())
            if req.get_method() == "HEAD":
                raise urllib.error.HTTPError(req.full_url, 405, "Method Not Allowed", {}, None)
            return _Resp()

        monkeypatch.setattr(urls_mod.urllib.request, "urlopen", _fake_urlopen)
        assert check_url("https://example.com/a.fq.gz") is None
        assert seen == ["HEAD", "GET"]

    def test_unreachable_host(self, monkeypatch):
        def _fake_urlopen(req, timeout=None):
            raise urllib.error.URLError(socket.gaierror("Name or service not known"))

        monkeypatch.setattr(urls_mod.urllib.request, "urlopen", _fake_urlopen)
        reason = check_url("https://nowhere.invalid/a.fq.gz")
        assert reason is not None and "unreachable" in reason

    def test_empty_url(self):
        assert check_url("   ") == "empty URL"

    def test_unsupported_scheme(self):
        assert check_url("s3://bucket/a.fq.gz") == "unsupported URL scheme 's3'"

    def test_sftp_is_skipped(self):
        """sftp cannot be probed anonymously — treated as reachable."""
        assert check_url("sftp://host/path/a.fq.gz") is None


class TestCheckUrls:
    def test_duplicates_checked_once(self, monkeypatch):
        calls: list[str] = []

        def _fake_check(url, timeout=20.0):
            calls.append(url)
            return None

        monkeypatch.setattr(urls_mod, "check_url", _fake_check)
        assert check_urls(["https://a/1", "https://a/1", "https://a/2"]) == {}
        assert sorted(calls) == ["https://a/1", "https://a/2"]

    def test_only_failures_returned(self, monkeypatch):
        monkeypatch.setattr(
            urls_mod, "check_url",
            lambda url, timeout=20.0: "HTTP 404 Not Found" if url.endswith("2") else None,
        )
        result = check_urls(["https://a/1", "https://a/2"])
        assert result == {"https://a/2": "HTTP 404 Not Found"}


class TestVerifyRemoteUrls:
    SAMPLE_FIELD = "fldNGN3g6Lvqo4ySR"
    READS1_FIELD = "fldSsNtgHYgxRaYxN"
    READS2_FIELD = "fldMmJiQLVVoJOAjF"

    RECORDS = [
        {"id": "rec1", "fields": {
            SAMPLE_FIELD: "EHI00001",
            READS1_FIELD: "https://example.com/EHI00001_1.fq.gz",
            READS2_FIELD: "https://example.com/EHI00001_2.fq.gz",
        }},
        {"id": "rec2", "fields": {
            SAMPLE_FIELD: "EHI00002",
            READS1_FIELD: "/projects/ehi/local_1.fq.gz",
            READS2_FIELD: "",
        }},
    ]

    def test_all_reachable(self, monkeypatch):
        monkeypatch.setattr(urls_mod, "check_urls", lambda urls, timeout=20.0, workers=8: {})
        assert verify_remote_urls(self.RECORDS, self.SAMPLE_FIELD,
                                  [self.READS1_FIELD, self.READS2_FIELD]) == []

    def test_failure_is_attributed_to_sample(self, monkeypatch):
        bad = "https://example.com/EHI00001_2.fq.gz"
        monkeypatch.setattr(
            urls_mod, "check_urls",
            lambda urls, timeout=20.0, workers=8: {bad: "HTTP 404 Not Found"},
        )
        result = verify_remote_urls(self.RECORDS, self.SAMPLE_FIELD,
                                     [self.READS1_FIELD, self.READS2_FIELD])
        assert result == [("EHI00001", bad, "HTTP 404 Not Found")]

    def test_local_paths_are_not_checked(self, monkeypatch):
        seen: list[list[str]] = []

        def _fake(urls, timeout=20.0, workers=8):
            seen.append(list(urls))
            return {}

        monkeypatch.setattr(urls_mod, "check_urls", _fake)
        verify_remote_urls(self.RECORDS, self.SAMPLE_FIELD,
                           [self.READS1_FIELD, self.READS2_FIELD])
        assert "/projects/ehi/local_1.fq.gz" not in seen[0]
        assert len(seen[0]) == 2
