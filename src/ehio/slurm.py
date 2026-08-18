"""Slurm job discovery and cancellation for ehio batches.

A batch runs as a detached screen session holding a drakkar/snakemake
process, which submits the real work to Slurm as individual sbatch jobs.
Killing the screen session only kills the workflow driver: every job it
already submitted keeps running.  This module finds those jobs again from
the batch code alone, so `ehio stop` can cancel them and `ehio jobs` can
list them.

Two independent handles identify the jobs of a batch, and both are used:

  * working directory — the generated launch script cds into the output
    directory before calling drakkar, so every job submitted from there
    carries it as its Slurm WorkDir;
  * snakemake run id — the Slurm executor plugin names every job of a run
    after a UUID it prints as "SLURM run ID: <uuid>" on stdout, which the
    launch script appends to {run_dir}/{batch}.out.  A batch can hold
    several of them (quantifying calls drakkar more than once, and a
    resumed batch adds another run).
"""

from __future__ import annotations

import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

# JobID|State|Name|Partition|TimeUsed|WorkDir — no width given, so squeue
# prints every field at its full length instead of truncating it.
_SQUEUE_FORMAT = "%i|%T|%j|%P|%M|%Z"

_RUN_ID_RE = re.compile(r"SLURM run ID:\s*([0-9a-fA-F-]{36})")

class SlurmUnavailable(Exception):
    """Slurm client commands are not on PATH."""


@dataclass(frozen=True)
class SlurmJob:
    job_id:    str
    state:     str
    name:      str
    partition: str
    elapsed:   str
    workdir:   str


def slurm_available() -> bool:
    return shutil.which("squeue") is not None and shutil.which("scancel") is not None


def read_run_ids(out_file: str | Path) -> set[str]:
    """Return every snakemake Slurm run id logged in the batch .out file."""
    path = Path(out_file)
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return set()
    return {m.lower() for m in _RUN_ID_RE.findall(text)}


def queued_jobs() -> list[SlurmJob]:
    """Return every queued or running job of the current user."""
    if not slurm_available():
        raise SlurmUnavailable("squeue/scancel are not available on PATH.")
    result = subprocess.run(
        ["squeue", "--me", "--noheader", "-o", _SQUEUE_FORMAT],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise SlurmUnavailable(
            f"squeue failed (exit {result.returncode}): {result.stderr.strip()}"
        )

    jobs: list[SlurmJob] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("|", 5)
        if len(parts) < 6:
            continue
        job_id, state, name, partition, elapsed, workdir = (p.strip() for p in parts)
        jobs.append(SlurmJob(job_id, state.upper(), name, partition, elapsed, workdir))
    return jobs


def _under(workdir: str, directory: str) -> bool:
    if not directory:
        return False
    base = directory.rstrip("/")
    work = workdir.rstrip("/")
    return work == base or work.startswith(base + "/")


def find_batch_jobs(
    output_dir: str,
    run_dir: str = "",
    out_file: str | Path = "",
) -> list[SlurmJob]:
    """Return the queued/running jobs belonging to one batch.

    A job matches when it was submitted from the batch output or run
    directory, or when its name is a snakemake run id found in the batch
    .out file.  Raises SlurmUnavailable if Slurm cannot be queried.
    """
    run_ids = read_run_ids(out_file) if out_file else set()
    matched = []
    for job in queued_jobs():
        if (
            _under(job.workdir, output_dir)
            or _under(job.workdir, run_dir)
            or job.name.lower() in run_ids
        ):
            matched.append(job)
    return matched


def cancel_jobs(job_ids: list[str]) -> tuple[bool, str]:
    """Cancel the given jobs. Returns (ok, message from scancel)."""
    if not job_ids:
        return True, ""
    result = subprocess.run(
        ["scancel", *job_ids],
        capture_output=True, text=True,
    )
    return result.returncode == 0, (result.stderr or result.stdout).strip()


def cancel_batch_jobs(
    output_dir: str,
    run_dir: str = "",
    out_file: str | Path = "",
    attempts: int = 3,
    wait: float = 3.0,
) -> tuple[list[str], list[SlurmJob], list[str]]:
    """Cancel every job of a batch, retrying until the queue is clear.

    A single pass can leave jobs behind: a workflow driver that has not
    died yet resubmits them (the drakkar slurm profile sets retries and
    keep-going), and jobs submitted between the squeue call and scancel
    are missed entirely.

    Returns (cancelled job ids, jobs still queued, scancel error messages).
    """
    cancelled: list[str] = []
    errors: list[str] = []
    for attempt in range(attempts):
        queued = find_batch_jobs(output_dir, run_dir, out_file)
        if not queued:
            return cancelled, [], errors
        ids = [j.job_id for j in queued if j.job_id not in cancelled]
        if ids:
            ok, message = cancel_jobs(ids)
            if ok:
                cancelled.extend(ids)
            elif message and message not in errors:
                errors.append(message)
        if attempt < attempts - 1:
            time.sleep(wait)
    still_there = [
        j for j in find_batch_jobs(output_dir, run_dir, out_file)
        if j.job_id not in cancelled
    ]
    return cancelled, still_there, errors
