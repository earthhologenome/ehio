"""Depositing hologenomes in the European Nucleotide Archive (ENA).

An ENA submission (EHS…) is a batch of ehi-core like the others: staff put
hologenomes in it, name the ENA study it goes under and set it Ready, and
`ehio ena` deposits the raw reads of each hologenome at ENA — a sample, an
experiment and a run — and writes the accessions onto the hologenome.

ENA is reached through ena-upload-cli, which uploads the reads to the Webin
upload area and submits the XML ENA wants. ehio writes its tables one
hologenome at a time, so a hologenome ENA refuses costs only itself, and a
submission that stops can be launched again: hologenomes that already hold a
run accession are skipped.

What ENA is told comes from ehi-core alone:

- the library, from the hologenome: library name, source, platform,
  instrument, and the raw reads on ERDA;
- the sample, from the sample the hologenome links to, the core's copy of the
  laboratory's table: host, place, date and environment (checklist ERC000013);
- the study, from the submission's own row, which is also the sample's
  'project name'.

A lab sample is registered at ENA once. A hologenome of a sample ENA already
knows — registered for another hologenome, in this submission or an earlier
one, or by the pipelines before ehio — is added to that sample. ena-upload-cli
refers to an existing sample or study by the alias it was registered under
(``refname``), and those aliases differ from one pipeline to the next, so the
alias is looked up at ENA from the accession.

A new sample is registered under its lab code, and an experiment and a run
under ``ena_{hologenome}``, the aliases the earlier pipeline used. When ENA
answers that one of them already exists — deposited by a run that stopped
before the accession was written back — the accession ENA names is taken and
only the rest is submitted.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import os
import re
import socket
import subprocess
import sys
import threading
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

import yaml

PRODUCTION = "https://www.ebi.ac.uk"
TEST_SERVER = "https://wwwdev.ebi.ac.uk"

DEFAULT_CHECKLIST = "ERC000013"
DEFAULT_CENTER = "Earth Hologenome Initiative"

# What ENA is told about the library, the same for every EHI hologenome: every
# one of the 4,018 records of Airtable's SE table held these values.
DEFAULT_LIBRARY = {
    "library_strategy":  "WGS",
    "library_selection": "RANDOM",
    "library_layout":    "PAIRED",
    "insert_size":       "400",
}

# Checklist column → column of the sample in ehi-core, the attributes the
# pipeline before ehio sent. The columns are the ones of ena-upload-cli's
# ERC000013 template, which is also what ENA calls each attribute; a column the
# template does not know is not sent. The earlier pipeline sent the host's life
# stage as 'host lifestage', which the template does not know, so it never
# reached ENA: it goes out as 'host life stage' here. No scientific name is
# sent: ena-upload-cli looks it up from the taxon id, which is the metagenome's
# (feces metagenome), whereas the laboratory's table names the host there.
DEFAULT_SAMPLE_FIELDS = {
    "sample_description":                       "description",
    "host subject id":                          "specimen_code",
    "host common name":                         "host_common_name",
    "host taxid":                               "host_taxid",
    "host life stage":                          "host_life_stage",
    "taxon_id":                                 "taxon_id",
    "collection date":                          "collection_date",
    "geographic location (country and/or sea)": "country",
    "geographic location (latitude)":           "latitude",
    "geographic location (longitude)":          "longitude",
    "broad-scale environmental context":        "broad_scale_environment",
    "local environmental context":              "local_environment",
    "environmental medium":                     "environmental_medium",
}

# The columns ena-upload-cli will not submit an ERC000013 sample without. They
# are checked before any reads are downloaded, so a sample the laboratory has
# not described yet is reported at once rather than after a 10 GB download.
MANDATORY_SAMPLE_COLUMNS = (
    "taxon_id",
    "project name",
    "collection date",
    "geographic location (country and/or sea)",
    "geographic location (latitude)",
    "geographic location (longitude)",
    "broad-scale environmental context",
    "local environmental context",
    "environmental medium",
)

# A hologenome's library source, when ehi-core holds only its data type.
_LIBRARY_SOURCE_OF = {"metagenome": "METAGENOMIC", "genome": "GENOMIC"}

# ENA's answer when an object with the alias ehio chose is already there:
#   In sample, alias: "AGP08". The object being added already exists in the
#   submission account with accession: "ERS22455981".
_ALREADY_THERE = re.compile(
    r'In (?P<kind>sample|experiment|run|study|project),\s*alias:?\s*"(?P<alias>[^"]+)"'
    r'.*?already exists.*?accession:?\s*"(?P<accession>[^"]+)"',
    re.IGNORECASE | re.DOTALL,
)

# How many times a hologenome is submitted when ENA answers that part of it is
# already there: once for what is new, once more in case the first answer did
# not name everything that was.
_ATTEMPTS = 3

_USER_AGENT = "ehio/ena"


class EnaError(RuntimeError):
    """A hologenome, or the whole submission, could not be deposited."""


# ---------------------------------------------------------------------------
# Settings and credentials
# ---------------------------------------------------------------------------

@dataclass
class Credentials:
    username: str
    password: str

    def header(self) -> str:
        token = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        return f"Basic {token}"


def credentials(secret_file: str = "") -> Credentials | None:
    """The Webin account ehio submits with, or None when there is none.

    ENA_USERNAME and ENA_PASSWORD, which ena-upload-cli reads too, come first;
    otherwise the YAML file `secret_file` (``username:`` and ``password:``),
    which is where the cluster kept them for the pipeline before ehio.
    """
    username = os.environ.get("ENA_USERNAME", "").strip()
    password = os.environ.get("ENA_PASSWORD", "").strip()
    if username and password:
        return Credentials(username, password)
    path = Path(secret_file).expanduser() if secret_file else None
    if not path or not path.is_file():
        return None
    try:
        held = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise EnaError(f"Could not read the Webin credentials in {path}: {exc}") from exc
    username = str(held.get("username") or "").strip()
    password = str(held.get("password") or "").strip()
    if not (username and password):
        raise EnaError(f"{path} holds no 'username' and 'password' for the Webin account.")
    return Credentials(username, password)


@dataclass
class Settings:
    """How a submission is run, from the config and the command line."""

    work_dir: Path
    center: str = DEFAULT_CENTER
    checklist: str = DEFAULT_CHECKLIST
    test: bool = False
    keep_reads: bool = False
    parallel: int = 1
    timeout: float = 600.0
    library: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_LIBRARY))
    sample_fields: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_SAMPLE_FIELDS))

    @property
    def server(self) -> str:
        return TEST_SERVER if self.test else PRODUCTION


# ---------------------------------------------------------------------------
# Values as ENA wants them
# ---------------------------------------------------------------------------

def text(value: Any) -> str:
    """A cell as the text of a table cell.

    Airtable's lookup fields come as lists, which are joined; a whole number
    read as a float loses its '.0', so a taxon id stays a taxon id. Tabs and
    line breaks would break the table ena-upload-cli reads, so they become
    spaces.
    """
    if isinstance(value, (list, tuple)):
        return ", ".join(part for part in (text(item) for item in value) if part)
    if value is None or isinstance(value, dict):
        return ""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return re.sub(r"\s+", " ", str(value)).strip()


def _unreadable(row: dict[str, str]) -> list[str]:
    """The columns ena-upload-cli would misread: it reads its tables with '#'
    as the start of a comment, which would cut the rest of the row off."""
    return [column for column, value in row.items() if "#" in value]


# ---------------------------------------------------------------------------
# ENA's own records
# ---------------------------------------------------------------------------

def _fetch(url: str, creds: Credentials | None, timeout: float) -> bytes | None:
    headers = {"User-Agent": _USER_AGENT}
    if creds:
        headers["Authorization"] = creds.header()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=timeout) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code in (400, 401, 403, 404):
            return None
        raise EnaError(f"ENA answered {url} with HTTP {exc.code} {exc.reason}") from exc
    except (urllib.error.URLError, socket.timeout, OSError) as exc:
        raise EnaError(f"Could not reach ENA at {url}: {exc}") from exc


def alias_in(xml: bytes, accession: str) -> str:
    """The alias of the object `accession` names in an ENA XML record."""
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return ""
    found = ""
    for element in root.iter():
        alias = (element.get("alias") or "").strip()
        if not alias:
            continue
        ids = {element.get("accession") or ""}
        ids |= {(child.text or "").strip() for child in element.iter()
                if child.tag in ("PRIMARY_ID", "SECONDARY_ID", "EXTERNAL_ID")}
        if accession in ids:
            return alias
        found = found or alias
    return found


class Aliases:
    """The alias each existing ENA sample and study was registered under.

    The account's own records are read through Webin's report service, which
    also answers for what is not public yet; a public record is found in ENA's
    browser as well, which needs no account.
    """

    def __init__(self, creds: Credentials | None, server: str = PRODUCTION, timeout: float = 60.0,
                 fetch: Callable[[str, Credentials | None, float], bytes | None] | None = None) -> None:
        self._creds = creds
        self._server = server
        self._timeout = timeout
        self._fetch = fetch or _fetch
        self._known: dict[str, str] = {}
        self._lock = threading.Lock()

    def of(self, accession: str, kind: str) -> str:
        """`kind` is 'samples' or 'studies', as Webin's report service names them."""
        accession = accession.strip()
        with self._lock:
            if accession in self._known:
                return self._known[accession]
        places: list[tuple[str, Credentials | None]] = []
        if self._creds:
            places.append((f"{self._server}/ena/submit/report/{kind}/xml/{accession}", self._creds))
        if self._server == PRODUCTION:
            places.append((f"{PRODUCTION}/ena/browser/api/xml/{accession}", None))
        for url, creds in places:
            body = self._fetch(url, creds, self._timeout)
            alias = alias_in(body, accession) if body else ""
            if alias:
                with self._lock:
                    self._known[accession] = alias
                return alias
        where = "the Webin account" if self._creds else "ENA's public records (no Webin account given)"
        raise EnaError(f"{accession} is not in {where}, so its alias cannot be referenced.")


# ---------------------------------------------------------------------------
# What each hologenome is deposited as
# ---------------------------------------------------------------------------

@dataclass
class Sample:
    """A lab sample, registered at ENA once for all its hologenomes.

    `accession` is set once ENA holds the sample; until then `row` is what it
    is registered with.
    """

    code: str
    alias: str
    accession: str | None = None
    row: dict[str, str] | None = None
    hologenomes: list[dict[str, Any]] = field(default_factory=list)
    experiments: dict[str, dict[str, str]] = field(default_factory=dict)


@dataclass
class Deposited:
    code: str
    sample: str
    experiment: str
    run: str
    # The lab sample the ENA sample is.
    sample_code: str = ""


@dataclass
class Plan:
    """A submission, worked out before anything is sent to ENA."""

    samples: list[Sample] = field(default_factory=list)
    # Hologenomes already deposited, and why each of the others cannot be.
    skipped: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)

    @property
    def hologenomes(self) -> int:
        return sum(len(sample.hologenomes) for sample in self.samples)


def experiment_row(entry: dict[str, Any], sample_alias: str, study_alias: str,
                   library: dict[str, str]) -> tuple[dict[str, str], list[str]]:
    """The experiment table row of a hologenome, and what it lacks.

    The platform and instrument must be ENA's own words ('ILLUMINA',
    'Illumina NovaSeq 6000'), so they are taken from ehi-core rather than
    guessed; the editor can give those columns a default value for new
    hologenomes.
    """
    code = text(entry.get("code"))
    data_type = text(entry.get("data_type")).lower()
    source = text(entry.get("library_source")).upper() or _LIBRARY_SOURCE_OF.get(data_type, "")
    row = {
        "alias": f"ena_{code}",
        "title": code,
        "study_alias": study_alias,
        "sample_alias": sample_alias,
        "design_description": data_type if data_type in _LIBRARY_SOURCE_OF else (
            "metagenome" if source == "METAGENOMIC" else "genome" if source == "GENOMIC" else ""),
        "library_name": text(entry.get("library_name")),
        "library_strategy": library["library_strategy"],
        "library_source": source,
        "library_selection": library["library_selection"],
        "library_layout": library["library_layout"],
        "insert_size": library["insert_size"],
        "platform": text(entry.get("platform")),
        "instrument_model": text(entry.get("instrument_model")),
    }
    lacking = [name for name, column in (
        ("library source (or data type)", "library_source"),
        ("platform", "platform"),
        ("instrument model", "instrument_model"),
    ) if not row[column]]
    return row, lacking


def sample_row(code: str, sample: dict[str, Any], mapping: dict[str, str],
               project: str = "") -> tuple[dict[str, str], list[str]]:
    """The sample table row of a lab sample, and the mandatory columns it lacks.

    ENA's 'project name' is the study the sample is deposited under, as the
    earlier pipeline sent it.
    """
    row = {"alias": code, "title": code}
    for column, source in mapping.items():
        value = text(sample.get(source))
        if value:
            row[column] = value
    if project:
        row["project name"] = project
    return row, [column for column in MANDATORY_SAMPLE_COLUMNS if not row.get(column)]


def plan(
    entries: Iterable[dict[str, Any]],
    *,
    study: str,
    study_alias: str,
    settings: Settings,
    aliases: Aliases,
) -> Plan:
    """Work out what each hologenome of a submission is deposited as, from
    what ehi-core answers for the submission: each hologenome with its sample
    and the ENA samples that sample already has."""
    found = Plan()
    samples: dict[str, Sample] = {}
    for entry in entries:
        code = text(entry.get("code"))
        if not code:
            continue
        if text(entry.get("ena_run_accession")):
            found.skipped.append(f"{code}: already deposited as {text(entry.get('ena_run_accession'))}")
            continue
        lab = text(entry.get("sample_code"))
        if not lab:
            found.problems.append(f"{code}: no lab sample code, so ENA cannot be told about its sample")
            continue
        if not (text(entry.get("raw_forward_url")) and text(entry.get("raw_reverse_url"))):
            found.problems.append(f"{code}: no raw forward and reverse reads")
            continue

        sample = samples.get(lab)
        if sample is None:
            try:
                sample = _sample(lab, entry, aliases, settings, study)
            except EnaError as exc:
                found.problems.append(f"{code} (sample {lab}): {exc}")
                continue
            samples[lab] = sample
            found.samples.append(sample)

        row, lacking = experiment_row(entry, sample.alias, study_alias, settings.library)
        if lacking:
            found.problems.append(f"{code}: ehi-core holds no {' or '.join(lacking)} for it")
            continue
        unreadable = _unreadable(row)
        if unreadable:
            found.problems.append(f"{code}: '#' in {', '.join(unreadable)}, which ena-upload-cli cannot read")
            continue
        sample.hologenomes.append(entry)
        sample.experiments[code] = row

    found.samples = [sample for sample in found.samples if sample.hologenomes]
    return found


def _sample(lab: str, entry: dict[str, Any], aliases: Aliases, settings: Settings, study: str) -> Sample:
    """A lab sample: the ENA sample it already has, or what to register it with."""
    known = [text(accession) for accession in entry.get("sample_ena_accessions") or [] if text(accession)]
    if known:
        accession = known[0]
        return Sample(lab, aliases.of(accession, "samples"), accession=accession)
    held = entry.get("sample")
    if not isinstance(held, dict):
        raise EnaError("ehi-core holds no such sample: sync the samples from Airtable")
    row, lacking = sample_row(lab, held, settings.sample_fields, project=study)
    if lacking:
        raise EnaError(f"the sample holds no {', '.join(lacking)} (fill it in Airtable, then sync)")
    unreadable = _unreadable(row)
    if unreadable:
        raise EnaError(f"'#' in {', '.join(unreadable)}, which ena-upload-cli cannot read")
    return Sample(lab, lab, row=row)


# ---------------------------------------------------------------------------
# Talking to ENA through ena-upload-cli
# ---------------------------------------------------------------------------

@dataclass
class Receipt:
    success: bool
    # (kind, alias) → accession, of what the submission registered.
    accessions: dict[tuple[str, str], str] = field(default_factory=dict)
    # (kind, alias) → accession, of what ENA said was already there.
    existing: dict[tuple[str, str], str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


def parse_receipt(xml: bytes) -> Receipt:
    """ENA's receipt of one submission, which ena-upload-cli saves as receipt.xml."""
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as exc:
        raise EnaError(f"ENA's receipt could not be read: {exc}") from exc
    receipt = Receipt(success=(root.get("success") or "").lower() == "true")
    for kind in ("SAMPLE", "EXPERIMENT", "RUN"):
        for element in root.findall(kind):
            alias, accession = element.get("alias"), element.get("accession")
            if alias and accession:
                receipt.accessions[(kind.lower(), alias)] = accession
    for message in root.findall("MESSAGES/ERROR"):
        error = (message.text or "").strip()
        if not error:
            continue
        receipt.errors.append(error)
        match = _ALREADY_THERE.search(error)
        if match:
            receipt.existing[(match["kind"].lower(), match["alias"])] = match["accession"]
    return receipt


def write_table(path: Path, rows: list[dict[str, str]]) -> Path:
    """A tab-separated table of `rows`, as ena-upload-cli reads it."""
    columns = list(dict.fromkeys(column for row in rows for column in row))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return path


def upload_cli_command(tables: dict[str, Path], data: list[Path], settings: Settings) -> list[str]:
    """The ena-upload-cli call for one hologenome.

    It is run as a module of the Python ehio runs in, since it is installed
    with ehio, so it is found whatever the PATH of the screen session.
    """
    cmd = [sys.executable, "-m", "ena_upload.ena_upload", "--action", "add",
           "--center", settings.center, "--checklist", settings.checklist]
    for kind in ("sample", "experiment", "run"):
        if kind in tables:
            cmd += [f"--{kind}", str(tables[kind])]
    if data:
        cmd += ["--data", *(str(path) for path in data)]
    if settings.test:
        cmd.append("--dev")
    return cmd


def run_upload_cli(cmd: list[str], folder: Path, creds: Credentials) -> subprocess.CompletedProcess:
    env = {**os.environ, "ENA_USERNAME": creds.username, "ENA_PASSWORD": creds.password}
    return subprocess.run(cmd, cwd=folder, env=env, capture_output=True, text=True)


def _tail(output: str, lines: int = 15) -> str:
    kept = [line for line in output.strip().splitlines() if line.strip()][-lines:]
    return "\n    ".join(kept)


# ---------------------------------------------------------------------------
# The reads
# ---------------------------------------------------------------------------

def _is_remote(source: str) -> bool:
    return bool(re.match(r"^(https?|ftp)://", source, re.IGNORECASE))


def md5_of(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fetch_reads(source: str, dest: Path, timeout: float) -> str:
    """Put a read file at `dest` and return its MD5, which ENA is told.

    The checksum is taken as the file arrives, rather than by ena-upload-cli
    afterwards, which reads it back 128 bytes at a time. A file already there
    — from a submission that stopped — is kept; the transfer goes to a '.part'
    file first, so a file that is there is whole. A local path is linked.
    """
    if dest.exists() and dest.stat().st_size > 0:
        return md5_of(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not _is_remote(source):
        local = Path(source).expanduser()
        if not local.is_file():
            raise EnaError(f"{source} does not exist")
        dest.unlink(missing_ok=True)
        dest.symlink_to(local.resolve())
        return md5_of(dest)

    part = dest.with_name(dest.name + ".part")
    digest = hashlib.md5()
    request = urllib.request.Request(source, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp, part.open("wb") as fh:
            for chunk in iter(lambda: resp.read(1024 * 1024), b""):
                digest.update(chunk)
                fh.write(chunk)
    except urllib.error.HTTPError as exc:
        part.unlink(missing_ok=True)
        raise EnaError(f"{source} could not be downloaded (HTTP {exc.code} {exc.reason})") from exc
    except (urllib.error.URLError, socket.timeout, OSError) as exc:
        part.unlink(missing_ok=True)
        raise EnaError(f"{source} could not be downloaded ({exc})") from exc
    if part.stat().st_size == 0:
        part.unlink(missing_ok=True)
        raise EnaError(f"{source} is empty")
    part.replace(dest)
    return digest.hexdigest()


def read_names(code: str) -> tuple[str, str]:
    """What a hologenome's reads are called at ENA, as the earlier pipeline
    called them."""
    return f"{code}_raw_1.fq.gz", f"{code}_raw_2.fq.gz"


# ---------------------------------------------------------------------------
# Depositing
# ---------------------------------------------------------------------------

Runner = Callable[[list[str], Path, Credentials], subprocess.CompletedProcess]
Fetcher = Callable[[str, Path, float], str]


def deposit(
    entry: dict[str, Any],
    sample: Sample,
    settings: Settings,
    creds: Credentials,
    *,
    run: Runner | None = None,
    fetch: Fetcher | None = None,
    note: Callable[[str], None] = lambda message: None,
) -> Deposited:
    """Deposit one hologenome: its sample if ENA does not hold it yet, its
    experiment and its run, with its reads.

    ENA takes a submission whole or not at all, so a refused one leaves
    nothing behind. What it answers is already there is taken as it is, and
    only the rest submitted again.
    """
    run = run or run_upload_cli
    fetch = fetch or fetch_reads
    code = text(entry.get("code"))
    folder = settings.work_dir / code
    alias = f"ena_{code}"
    held: dict[str, str] = {}
    if sample.accession:
        held["sample"] = sample.accession
    checksums: dict[str, str] = {}

    for _ in range(_ATTEMPTS):
        tables: dict[str, Path] = {}
        data: list[Path] = []
        if "sample" not in held:
            tables["sample"] = write_table(folder / "sample.tsv", [sample.row or {}])
        if "experiment" not in held:
            tables["experiment"] = write_table(folder / "experiment.tsv", [sample.experiments[code]])
        if "run" not in held:
            rows = []
            for source, name in zip((text(entry.get("raw_forward_url")), text(entry.get("raw_reverse_url"))),
                                    read_names(code)):
                path = folder / name
                if name not in checksums:
                    note(f"{code}: fetching {name} ...")
                    checksums[name] = fetch(source, path, settings.timeout)
                data.append(path)
                rows.append({"alias": alias, "experiment_alias": alias, "file_name": name,
                             "file_type": "fastq", "file_checksum": checksums[name]})
            tables["run"] = write_table(folder / "run.tsv", rows)
        if not tables:
            break

        receipt_path = folder / "receipt.xml"
        receipt_path.unlink(missing_ok=True)
        note(f"{code}: submitting {', '.join(tables)} to ENA{' (test server)' if settings.test else ''} ...")
        cmd = upload_cli_command(tables, data, settings)
        result = run(cmd, folder, creds)
        with (folder / "ena-upload-cli.log").open("a", encoding="utf-8") as log:
            log.write(f"$ {' '.join(cmd)}\n")
            log.write(result.stdout or "")
            log.write(result.stderr or "")
        if not receipt_path.is_file():
            raise EnaError(
                f"ena-upload-cli submitted nothing (exit {result.returncode}):\n    "
                + _tail((result.stdout or "") + "\n" + (result.stderr or ""))
            )
        receipt = parse_receipt(receipt_path.read_bytes())
        ours = {"sample": sample.alias, "experiment": alias, "run": alias}
        if receipt.success:
            for kind in tables:
                accession = receipt.accessions.get((kind, ours[kind]))
                if not accession:
                    raise EnaError(f"ENA took the submission but named no {kind} accession for {ours[kind]}")
                held[kind] = accession
            break
        there = {kind: receipt.existing[(kind, ours[kind])]
                 for kind in tables if (kind, ours[kind]) in receipt.existing}
        if not there:
            raise EnaError("ENA refused the submission:\n    " + "\n    ".join(receipt.errors or ["(no reason given)"]))
        for kind, accession in there.items():
            note(f"{code}: ENA already holds its {kind} ({accession}) — taking it.")
        held.update(there)
    missing = [kind for kind in ("sample", "experiment", "run") if kind not in held]
    if missing:
        raise EnaError(f"ENA kept answering that part of it was already there, and never took its {', '.join(missing)}")

    sample.accession = held["sample"]
    if not settings.keep_reads:
        for name in read_names(code):
            (folder / name).unlink(missing_ok=True)
    return Deposited(code, held["sample"], held["experiment"], held["run"], sample.code)


@dataclass
class Outcome:
    deposited: list[Deposited] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)


def deposit_all(
    found: Plan,
    settings: Settings,
    creds: Credentials,
    *,
    on_deposit: Callable[[Deposited], None] = lambda done: None,
    run: Runner | None = None,
    fetch: Fetcher | None = None,
    note: Callable[[str], None] = lambda message: None,
) -> Outcome:
    """Deposit every hologenome of a plan, a few samples at a time.

    The hologenomes of one lab sample go one after the other, so the first
    registers the sample and the rest are added to it; different samples go
    side by side (`settings.parallel`). `on_deposit` is told about each
    hologenome as soon as ENA holds it, so its accessions are kept even if a
    later one fails.
    """
    outcome = Outcome()
    lock = threading.Lock()

    def one_sample(sample: Sample) -> None:
        for entry in sample.hologenomes:
            code = text(entry.get("code"))
            try:
                done = deposit(entry, sample, settings, creds, run=run, fetch=fetch, note=note)
            except EnaError as exc:
                note(f"{code}: FAILED — {exc}")
                with lock:
                    outcome.failed.append(f"{code}: {exc}")
                continue
            note(f"{code}: deposited — sample {done.sample}, experiment {done.experiment}, run {done.run}")
            with lock:
                outcome.deposited.append(done)
                on_deposit(done)

    workers = max(1, min(settings.parallel, len(found.samples) or 1))
    if workers == 1:
        for sample in found.samples:
            one_sample(sample)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(one_sample, found.samples))
    outcome.deposited.sort(key=lambda done: done.code)
    return outcome


def write_draft(found: Plan, settings: Settings) -> list[Path]:
    """The tables each hologenome would be submitted with, without the reads'
    checksums, which only a download gives: what a dry run leaves to look at."""
    written: list[Path] = []
    for sample in found.samples:
        for index, entry in enumerate(sample.hologenomes):
            code = text(entry.get("code"))
            folder = settings.work_dir / code
            if sample.row and index == 0:
                written.append(write_table(folder / "sample.tsv", [sample.row]))
            written.append(write_table(folder / "experiment.tsv", [sample.experiments[code]]))
            alias = f"ena_{code}"
            written.append(write_table(folder / "run.tsv", [
                {"alias": alias, "experiment_alias": alias, "file_name": name, "file_type": "fastq"}
                for name in read_names(code)
            ]))
    return written


def write_accessions(path: Path, deposited: list[Deposited]) -> Path:
    """A local record of what was deposited, beside the batch's files."""
    return write_table(path, [
        {"hologenome": done.code, "sample": done.sample, "experiment": done.experiment, "run": done.run}
        for done in deposited
    ])
