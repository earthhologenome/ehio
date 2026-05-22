"""File transfer helpers for ehio (SFTP via paramiko and bulk mirror via lftp)."""

from __future__ import annotations

import shlex
import shutil
import subprocess
import sys
from pathlib import Path, PurePosixPath

try:
    import paramiko
    _PARAMIKO_AVAILABLE = True
except ImportError:
    _PARAMIKO_AVAILABLE = False


def _require_paramiko() -> None:
    if not _PARAMIKO_AVAILABLE:
        print("Error: paramiko is required. Run: pip install paramiko", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# lftp bulk mirror
# ---------------------------------------------------------------------------

def upload_with_lftp(
    local_dir: str | Path,
    remote_dir: str,
    host: str,
    user: str,
    port: int = 22,
    identity: str | None = None,
    verbose: bool = False,
) -> None:
    """Mirror a local directory tree to a remote path via lftp over SFTP.

    Requires lftp on PATH. Raises subprocess.CalledProcessError on failure.
    """
    if shutil.which("lftp") is None:
        print("Error: lftp is not available on PATH.", file=sys.stderr)
        sys.exit(1)

    local_str = str(Path(local_dir).resolve())
    verbosity = "--verbose" if verbose else "--quiet"

    parts: list[str] = []
    if identity:
        parts.append(f'set sftp:connect-program "ssh -a -x -i {identity}"')
    parts.append(f"open sftp://{user}@{host}:{port}")
    parts.append(
        f"mirror --reverse {verbosity} "
        f"{shlex.quote(local_str)} {shlex.quote(remote_dir)}"
    )
    parts.append("bye")

    cmd = ["lftp", "-e", "; ".join(parts)]
    if verbose:
        print(f"  lftp command: {cmd}", file=sys.stderr)
    subprocess.run(cmd, check=True)


# ---------------------------------------------------------------------------
# paramiko SFTP (per-file transfers)
# ---------------------------------------------------------------------------

class SFTPTransfer:
    def __init__(
        self,
        host: str,
        username: str,
        port: int = 22,
        key_path: str | None = None,
        timeout: float = 300.0,
    ) -> None:
        _require_paramiko()
        self._host = host
        self._username = username
        self._port = port
        self._key_path = key_path or None
        self._timeout = timeout
        self._client: paramiko.SSHClient | None = None
        self._sftp: paramiko.SFTPClient | None = None

    def connect(self) -> None:
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        kwargs: dict = {
            "hostname": self._host,
            "username": self._username,
            "port": self._port,
            "timeout": self._timeout,
            "banner_timeout": self._timeout,
            "auth_timeout": self._timeout,
        }
        if self._key_path:
            kwargs["key_filename"] = self._key_path
        self._client.connect(**kwargs)
        self._sftp = self._client.open_sftp()

    def disconnect(self) -> None:
        if self._sftp:
            self._sftp.close()
        if self._client:
            self._client.close()

    def _ensure_remote_dir(self, remote_dir: str) -> None:
        parts = PurePosixPath(remote_dir).parts
        current = ""
        for part in parts:
            current = str(PurePosixPath(current) / part)
            try:
                self._sftp.stat(current)
            except FileNotFoundError:
                self._sftp.mkdir(current)

    def _remote_exists(self, remote_path: str) -> bool:
        try:
            self._sftp.stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def upload(
        self,
        files: list[Path],
        base_dir: Path,
        remote_dir: str,
        verbose: bool = False,
        skip_existing: bool = True,
    ) -> tuple[int, int]:
        """Upload a list of files, preserving their path relative to base_dir.

        Returns (uploaded, skipped) counts.
        """
        remote_root = PurePosixPath(remote_dir)
        uploaded = 0
        skipped = 0
        for file_path in sorted(files):
            rel = file_path.relative_to(base_dir)
            remote_path = str(remote_root / PurePosixPath(rel.as_posix()))
            if skip_existing and self._remote_exists(remote_path):
                skipped += 1
                if verbose:
                    print(f"  SKIP {file_path} (already exists remotely)", file=sys.stderr)
                continue
            self._ensure_remote_dir(str(PurePosixPath(remote_path).parent))
            size_mb = file_path.stat().st_size / (1024 * 1024)
            self._sftp.put(str(file_path), remote_path)
            uploaded += 1
            if verbose:
                print(f"  PUT {file_path} -> {remote_path} ({size_mb:.1f} MB)", file=sys.stderr)
        return uploaded, skipped

    def upload_dir(
        self,
        local_dir: Path,
        remote_dir: str,
        verbose: bool = False,
        include_suffixes: list[str] | None = None,
        skip_existing: bool = True,
    ) -> tuple[int, int]:
        """Upload files under local_dir, preserving directory structure.

        include_suffixes: if given, only files whose names end with one of
        the listed strings are uploaded (e.g. [".bam", ".fq.gz", "_output.tsv"]).
        Returns (uploaded, skipped) counts.
        """
        files = [p for p in sorted(local_dir.rglob("*")) if p.is_file()]
        if include_suffixes:
            files = [f for f in files if any(f.name.endswith(s) for s in include_suffixes)]
        return self.upload(files, local_dir, remote_dir, verbose=verbose, skip_existing=skip_existing)

    def remove_remote_dir(self, remote_dir: str) -> None:
        """Recursively remove a remote directory and all its contents.

        Silently does nothing if the directory does not exist.
        """
        import stat as _stat
        try:
            entries = self._sftp.listdir_attr(remote_dir)
        except FileNotFoundError:
            return
        for entry in entries:
            child = f"{remote_dir}/{entry.filename}"
            if _stat.S_ISDIR(entry.st_mode):
                self.remove_remote_dir(child)
            else:
                self._sftp.remove(child)
        self._sftp.rmdir(remote_dir)

    def upload_flat(
        self,
        files: list[Path],
        remote_dir: str,
        verbose: bool = False,
        skip_existing: bool = True,
    ) -> tuple[int, int]:
        """Upload a list of files directly into remote_dir, without subdirectories.

        All files land as remote_dir/{filename} regardless of their local location.
        Returns (uploaded, skipped) counts.
        """
        self._ensure_remote_dir(remote_dir)
        remote_root = PurePosixPath(remote_dir)
        uploaded = 0
        skipped = 0
        for file_path in sorted(files):
            remote_path = str(remote_root / file_path.name)
            if skip_existing and self._remote_exists(remote_path):
                skipped += 1
                if verbose:
                    print(f"  SKIP {file_path} (already exists remotely)", file=sys.stderr)
                continue
            size_mb = file_path.stat().st_size / (1024 * 1024)
            self._sftp.put(str(file_path), remote_path)
            uploaded += 1
            if verbose:
                print(f"  PUT {file_path} -> {remote_path} ({size_mb:.1f} MB)", file=sys.stderr)
        return uploaded, skipped

    def __enter__(self) -> SFTPTransfer:
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.disconnect()
