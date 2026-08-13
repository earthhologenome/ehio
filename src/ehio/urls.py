"""Reachability checks for remote input files (reads and reference genomes).

Airtable holds URLs that drakkar downloads at run time.  A typo or an expired
link only surfaces deep inside a Snakemake job, so ehio verifies the URLs up
front and fails with a controlled error instead.
"""

from __future__ import annotations

import socket
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

REMOTE_SCHEMES = ("http", "https", "ftp", "ftps", "sftp")

# Schemes that cannot be probed anonymously — skipped rather than reported.
_UNCHECKABLE_SCHEMES = ("sftp", "ftps")

_USER_AGENT = "ehio/url-check"

DEFAULT_TIMEOUT = 20.0


def is_remote_url(value: str) -> bool:
    """Return True if value looks like a remote URL rather than a local path."""
    return str(value).strip().lower().startswith(tuple(f"{s}://" for s in REMOTE_SCHEMES))


def _check_http(url: str, timeout: float) -> str | None:
    def _open(method: str, headers: dict[str, str]) -> None:
        req = urllib.request.Request(url, method=method, headers={"User-Agent": _USER_AGENT, **headers})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if method == "GET":
                resp.read(1)

    try:
        _open("HEAD", {})
        return None
    except urllib.error.HTTPError as exc:
        # Some servers reject HEAD outright — retry with a 1-byte ranged GET.
        if exc.code not in (403, 405, 501):
            return f"HTTP {exc.code} {exc.reason}"
    except (urllib.error.URLError, socket.timeout, OSError):
        pass  # fall through to the GET attempt

    try:
        _open("GET", {"Range": "bytes=0-0"})
        return None
    except urllib.error.HTTPError as exc:
        return f"HTTP {exc.code} {exc.reason}"
    except urllib.error.URLError as exc:
        return f"unreachable ({exc.reason})"
    except socket.timeout:
        return f"timed out after {timeout:g}s"
    except OSError as exc:
        return f"unreachable ({exc})"


def _check_ftp(url: str, timeout: float) -> str | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            resp.read(1)
        return None
    except urllib.error.URLError as exc:
        return f"unreachable ({exc.reason})"
    except socket.timeout:
        return f"timed out after {timeout:g}s"
    except OSError as exc:
        return f"unreachable ({exc})"


def check_url(url: str, timeout: float = DEFAULT_TIMEOUT) -> str | None:
    """Return None if the URL can be downloaded, else a short reason string.

    Only the headers (or the first byte) are fetched — the file itself is never
    downloaded.  URLs whose scheme cannot be probed anonymously (sftp, ftps)
    are reported as reachable.
    """
    url = str(url).strip()
    if not url:
        return "empty URL"

    scheme = urlparse(url).scheme.lower()
    if scheme in _UNCHECKABLE_SCHEMES:
        return None
    if scheme in ("http", "https"):
        return _check_http(url, timeout)
    if scheme == "ftp":
        return _check_ftp(url, timeout)
    return f"unsupported URL scheme '{scheme}'"


def check_urls(
    urls: list[str],
    timeout: float = DEFAULT_TIMEOUT,
    workers: int = 8,
) -> dict[str, str]:
    """Check many URLs concurrently; return {url: reason} for the failing ones.

    Duplicate URLs are checked once.
    """
    unique = sorted({str(u).strip() for u in urls if str(u).strip()})
    if not unique:
        return {}

    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(unique)))) as pool:
        reasons = list(pool.map(lambda u: check_url(u, timeout), unique))

    return {url: reason for url, reason in zip(unique, reasons) if reason}
