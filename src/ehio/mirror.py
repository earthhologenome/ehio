"""What ehio writes to ehi-core, built from Airtable records and drakkar output.

Every write to the core is an upsert (ehi-core's app/pipeline/service.py):

- ``values`` are what ehio sets — a status, a version, a metric, a file URL —
  and are always written;
- ``defaults`` are the facts ehio read from an Airtable record, and only fill
  cells the core has empty.

So a batch created in Airtable after the core was loaded is created in the core
the first time ehio touches it, and ehio's output always has a row to land on.
A link names its target by code, or by the Airtable record id a link cell
holds, which the core resolves through the ids its rows were imported with.

Airtable builds some URLs with formulas; the core stores them, so ehio writes
the public ERDA address of every file it uploads (ERDA_SHARE_BASE).

Everything here only builds rows; ehio.core sends them.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

from ehio import config as cfg
from ehio.metadata import drakkar_mag_id

Row = dict[str, Any]
Unit = list[tuple[str, list[Row]]]

BATCH_TABLES = {
    "preprocessing": "preprocessing_batches",
    "binning":       "assembly_batches",
    "quantifying":   "dereplication_batches",
    "amr":           "amr_batches",
}

# Facts of a batch record ehio copies into the core when the core lacks them:
# config key of the Airtable field → core column.
BATCH_FACTS: dict[str, dict[str, str]] = {
    "preprocessing": {
        "EHI_PPR_BATCH_STATUS":       "status",
        "EHI_PPR_BATCH_DATE":         "run_on",
        "EHI_PPR_BATCH_BOOST_TIME":   "boost_time",
        "EHI_PPR_BATCH_BOOST_MEMORY": "boost_memory",
    },
    "binning": {
        "EHI_ASB_BATCH_STATUS":       "status",
        "EHI_ASB_BATCH_DATE":         "run_on",
        "EHI_ASB_BATCH_TYPE":         "batch_type",
        "EHI_ASB_BATCH_BOOST_TIME":   "boost_time",
        "EHI_ASB_BATCH_BOOST_MEMORY": "boost_memory",
    },
    "quantifying": {
        "MAG_DMB_BATCH_STATUS":          "status",
        "MAG_DMB_BATCH_TYPE":            "batch_type",
        "MAG_DMB_BATCH_ANI":             "ani_threshold",
        "MAG_DMB_BATCH_ANNOTATION_TYPE": "annotation_type",
        "MAG_DMB_BATCH_BOOST_TIME":      "boost_time",
        "MAG_DMB_BATCH_BOOST_MEMORY":    "boost_memory",
    },
    "amr": {
        "EHI_AMR_BATCH_STATUS":       "status",
        "EHI_AMR_BATCH_DATE":         "run_on",
        "EHI_AMR_BATCH_BOOST_TIME":   "boost_time",
        "EHI_AMR_BATCH_BOOST_MEMORY": "boost_memory",
    },
}

# Metric keys of ehio's parsers → core columns, beside the config keys the same
# metrics go to in Airtable (ehio.metadata *_METRIC_KEYS).
PREPROCESSING_COLUMNS = {
    "reads_pre_fastp":       "reads_pre_fastp",
    "bases_pre_fastp":       "bases_pre_fastp",
    "adapter_trimmed_reads": "adapter_trimmed_reads",
    "adapter_trimmed_bases": "adapter_trimmed_bases",
    "reads_post_fastp":      "reads_post_fastp",
    "bases_post_fastp":      "bases_post_fastp",
    "host_reads":            "host_reads",
    "host_bases":            "host_bases",
    "metagenomic_reads":     "metagenomic_reads",
    "metagenomic_bases":     "metagenomic_bases",
    "singlem_fraction":      "singlem_fraction",
    "nonpareil_C":           "nonpareil_c",
    "nonpareil_LR":          "nonpareil_lr",
    "nonpareil_modelR":      "nonpareil_model_r",
    "nonpareil_LRstar":      "nonpareil_lr_star",
    "nonpareil_diversity":   "nonpareil_diversity",
}

ASSEMBLY_COLUMNS = {
    "assembly_length":          "assembly_length",
    "assembly_n50":             "n50",
    "assembly_l50":             "l50",
    "assembly_contigs_number":  "num_contigs",
    "assembly_contigs_largest": "largest_contig",
    "assembly_mapping_rate":    "assembly_mapping_percent",
    "bins_number":              "num_bins",
}

AMR_COLUMNS = {
    "amrfinder_hits":   "amr_amrfinder_hits",
    "rgi_hits":         "amr_rgi_hits",
    "mobility_regions": "amr_mobility_regions",
    "amr_loci":         "amr_amr_loci",
    "multi_tool_loci":  "amr_multi_tool_loci",
    "mobility_links":   "amr_mobility_links",
    "mobile_loci":      "amr_mobile_loci",
}

# drakkar's all_bin_metadata.csv columns → core columns of a new MAG.
BIN_COLUMNS = {
    "completeness":  "completeness",
    "contamination": "contamination",
    "size":          "size_bp",
    "N50":           "n50",
    "contig_count":  "contigs",
}

# What 'ehio annotating --output' writes on a MAG.
ANNOTATION_COLUMNS = {
    "domain":            "tax_domain",
    "phylum":            "tax_phylum",
    "class_":            "tax_class",
    "order":             "tax_order",
    "family":            "tax_family",
    "genus":             "tax_genus",
    "species":           "tax_species",
    "gtdb_fastani":      "fastani_ani",
    "gtdb_closest_ani":  "closest_ani",
    "gtdb_closest_af":   "closest_af",
    "coding_density":    "coding_density",
    "genes_number":      "genes",
    "genes_unannotated": "genes_unannotated",
    "genes_kegg":        "kegg_hits",
}

# A MAG record's fields in Airtable (config key) → core columns, for the MAGs
# the core does not hold yet or holds with gaps.
MAG_FACTS = {
    "MAG_ENTRY_ASSEMBLY":                 "assembly_id",
    "MAG_ENTRY_CHECKM_COMPLETENESS":      "completeness",
    "MAG_ENTRY_CHECKM_CONTAMINATION":     "contamination",
    "MAG_ENTRY_SIZE":                     "size_bp",
    "MAG_ENTRY_GC":                       "gc",
    "MAG_ENTRY_N50":                      "n50",
    "MAG_ENTRY_CONTIGS_NUMBER":           "contigs",
    "MAG_ENTRY_CODING_DENSITY":           "coding_density",
    "MAG_ENTRY_DOMAIN":                   "tax_domain",
    "MAG_ENTRY_PHYLUM":                   "tax_phylum",
    "MAG_ENTRY_CLASS":                    "tax_class",
    "MAG_ENTRY_ORDER":                    "tax_order",
    "MAG_ENTRY_FAMILY":                   "tax_family",
    "MAG_ENTRY_GENUS":                    "tax_genus",
    "MAG_ENTRY_SPECIES":                  "tax_species",
    "MAG_ENTRY_GTDBTK_VERSION":           "gtdbtk_version",
    "MAG_ENTRY_GTDB_RELEASE":             "gtdb_release",
    "MAG_ENTRY_GTDB_FASTANI":             "fastani_ani",
    "MAG_ENTRY_GTDB_CLOSEST_ANI":         "closest_ani",
    "MAG_ENTRY_GTDB_CLOSEST_AF":          "closest_af",
    "MAG_ENTRY_GENES_NUMBER":             "genes",
    "MAG_ENTRY_GENES_NUMBER_UNANNOTATED": "genes_unannotated",
    "MAG_ENTRY_GENES_KEGG_NUMBER":        "kegg_hits",
    "MAG_ENTRY_URL_FASTA":                "fasta_url",
}

# How far a MAG was annotated, each level covering the one before.  Airtable's
# older records say "true" for a full annotation.
ANNOTATION_LEVELS = ("kegg", "genes", "all")


# ---------------------------------------------------------------------------
# Building blocks
# ---------------------------------------------------------------------------

def _clean(values: dict[str, Any] | None) -> dict[str, Any]:
    return {k: v for k, v in (values or {}).items() if v is not None and v != ""}


def row(
    code: str | None = None,
    *,
    key: dict[str, Any] | None = None,
    values: dict[str, Any] | None = None,
    defaults: dict[str, Any] | None = None,
    airtable_id: str | None = None,
) -> Row:
    """One upsert row. Empty values are left out: ehio never clears a cell."""
    found_by = _clean(key)
    if code:
        found_by["code"] = code
    out: Row = {"key": found_by, "values": _clean(values), "defaults": _clean(defaults)}
    if airtable_id:
        out["airtable_record_id"] = airtable_id
    return out


def cell(fields: dict[str, Any], config_key: str) -> Any:
    """An Airtable cell by the config key naming its field, as a plain value.

    A link or lookup cell gives its first item; an attachment, or Airtable's
    {"specialValue": "NaN"}, gives nothing.
    """
    field_id = str(cfg.get(config_key) or "").strip()
    if not field_id:
        return None
    value = fields.get(field_id)
    if isinstance(value, list):
        value = value[0] if value else None
    if isinstance(value, dict):
        return None
    if isinstance(value, str):
        return value.strip() or None
    return value


def columns(metrics: dict[str, Any] | None, mapping: dict[str, str]) -> dict[str, Any]:
    """Metrics renamed to core columns, the missing ones left out."""
    metrics = metrics or {}
    return _clean({column: metrics.get(key) for key, column in mapping.items()})


def erda_url(*parts: str) -> str | None:
    """Public ERDA URL of a file under SFTP_REMOTE_BASE: erda_url("MAG", batch, name)."""
    base = str(cfg.get("ERDA_SHARE_BASE") or "").strip().rstrip("/")
    return "/".join([base, *parts]) if base else None


def annotation_level(value: Any) -> str | None:
    word = str(value or "").strip().lower()
    if word == "true":
        return "all"
    return word if word in ANNOTATION_LEVELS else None


# ---------------------------------------------------------------------------
# Batches
# ---------------------------------------------------------------------------

# Airtable words the core spells differently.
_DMB_TYPES = {"genomes": "genome", "pangenomes": "pangenome"}


def batch(module: str, code: str, record: dict | None = None, **values: Any) -> Unit:
    """A batch row: `values` set by ehio, the Airtable record's facts as defaults."""
    record = record or {}
    fields = record.get("fields", {})
    facts = {column: cell(fields, key) for key, column in BATCH_FACTS[module].items()}
    if module == "quantifying":
        kind = str(facts.get("batch_type") or "").strip().lower()
        facts["batch_type"] = _DMB_TYPES.get(kind, kind)
        facts["annotation_type"] = str(facts.get("annotation_type") or "").strip().lower()
    return [(BATCH_TABLES[module], [row(code, values=values, defaults=facts, airtable_id=record.get("id"))])]


# ---------------------------------------------------------------------------
# Preprocessing
# ---------------------------------------------------------------------------

def preprocessings(
    batch_code: str,
    entries: Iterable[dict],
    metrics: dict[str, dict] | None = None,
) -> list[Unit]:
    """One unit per entry: its hologenome, created from the raw reads if the
    core lacks it, and its preprocessing with the QC metrics drakkar reported."""
    units: list[Unit] = []
    for entry in entries:
        fields = entry.get("fields", {})
        code = cell(fields, "EHI_PPR_ENTRY_CODE")
        if not code:
            continue
        ehi = cell(fields, "EHI_PPR_ENTRY_EHI_NUMBER")
        unit: Unit = []
        if ehi:
            unit.append(("hologenomes", [row(ehi, defaults={
                "forward_url": cell(fields, "EHI_PPR_ENTRY_RAW_FILE_FORWARD"),
                "reverse_url": cell(fields, "EHI_PPR_ENTRY_RAW_FILE_REVERSE"),
            })]))
        unit.append(("preprocessings", [row(
            code,
            values=columns((metrics or {}).get(code), PREPROCESSING_COLUMNS),
            defaults={"batch_id": batch_code, "hologenome_id": ehi},
            airtable_id=entry.get("id"),
        )]))
        units.append(unit)
    return units


def preprocessing_files(batch_code: str, code_to_ehi: dict[str, str], uploaded: set[str]) -> list[Unit]:
    """The ERDA URLs of each preprocessing's reads and host BAM, for the files
    that reached PPR/{batch}."""
    units: list[Unit] = []
    for code, ehi in code_to_ehi.items():
        urls = {
            column: erda_url("PPR", batch_code, name)
            for column, name in (
                ("forward_url", f"{ehi}_M_1.fq.gz"),
                ("reverse_url", f"{ehi}_M_2.fq.gz"),
                ("bam_url",     f"{ehi}_G.bam"),
            )
            if name in uploaded
        }
        if _clean(urls):
            units.append([("preprocessings", [row(code, values=urls)])])
    return units


# ---------------------------------------------------------------------------
# Assembly and binning
# ---------------------------------------------------------------------------

def assemblies(
    batch_code: str,
    entries: Iterable[dict],
    metrics: dict[str, dict] | None = None,
) -> list[Unit]:
    """One unit per assembly entry, with the metrics drakkar reported for it
    (keyed by entry code)."""
    units: list[Unit] = []
    for entry in entries:
        fields = entry.get("fields", {})
        code = cell(fields, "EHI_ASB_ENTRY_CODE")
        if not code:
            continue
        units.append([("assemblies", [row(
            code,
            values=columns((metrics or {}).get(code), ASSEMBLY_COLUMNS),
            defaults={
                "batch_id": batch_code,
                "preprocessing_id": cell(fields, "EHI_ASB_ENTRY_PREPROCESSING"),
            },
            airtable_id=entry.get("id"),
        )])])
    return units


def assembly_files(batch_code: str, files: dict[str, str]) -> list[Unit]:
    """The ERDA URL of each assembly FASTA that reached ASB/{batch}
    (assembly code → its file name there)."""
    return [
        [("assemblies", [row(code, values={"assembly_url": erda_url("ASB", batch_code, name)})])]
        for code, name in files.items()
    ]


def assembly_of(genome: str) -> str:
    """The assembly a bin came from: EHA00405_bin_1.fa and EHA00405_bin.1.fa → EHA00405."""
    return re.split(r"_bin[._]", drakkar_mag_id(genome), maxsplit=1)[0]


def new_mags(batch_code: str, bins: Iterable[dict], uploaded: set[str]) -> list[Unit]:
    """Binning's MAGs, found by bin name and numbered by the core.

    The assembly link is a default, so a bin whose assembly the core does not
    know is still recorded (and reported) rather than lost.
    """
    units: list[Unit] = []
    for bin_row in bins:
        genome = str(bin_row.get("genome") or "").strip()
        if not genome:
            continue
        values = columns(bin_row, BIN_COLUMNS)
        if f"{genome}.gz" in uploaded:
            values["fasta_url"] = erda_url("MAG", batch_code, f"{genome}.gz")
        units.append([("mags", [row(
            key={"name": genome},
            values=values,
            defaults={"assembly_id": assembly_of(genome)},
        )])])
    return units


# ---------------------------------------------------------------------------
# AMR
# ---------------------------------------------------------------------------

def amr_assemblies(
    amr_code: str,
    records: Iterable[dict],
    stats: dict[str, dict] | None = None,
    gene_calls: set[str] | None = None,
) -> list[Unit]:
    """The assemblies of an AMR batch: linked to it, with their AMR metrics
    and the ERDA URLs of their gene calls (file names under AMR/{batch}/genes)."""
    code_key = "EHI_ASB_ENTRY_ASSEMBLY_CODE" if cfg.get("EHI_ASB_ENTRY_ASSEMBLY_CODE") else "EHI_ASB_ENTRY_CODE"
    units: list[Unit] = []
    for record in records:
        fields = record.get("fields", {})
        code = cell(fields, code_key)
        if not code:
            continue
        values = {"amr_batch_id": amr_code, **columns((stats or {}).get(code), AMR_COLUMNS)}
        for column, name in (("faa_url", f"{code}.faa.gz"), ("ffn_url", f"{code}.ffn.gz")):
            if name in (gene_calls or set()):
                values[column] = erda_url("AMR", amr_code, "genes", name)
        units.append([("assemblies", [row(
            code,
            values=values,
            defaults={"batch_id": cell(fields, "EHI_ASB_ENTRY_BATCH")},
            airtable_id=record.get("id"),
        )])])
    return units


# ---------------------------------------------------------------------------
# MAGs of a dereplication batch
# ---------------------------------------------------------------------------

def airtable_mags(records: Iterable[dict]) -> list[Unit]:
    """MAG records read from Airtable, for the core to fill what it lacks.

    Found by bin name (else code), so each keeps the code Airtable gave it.
    The annotation depth fills the core's too; that the MAG is annotated at
    all is set outright, since Airtable's word is the one ehio wrote.
    """
    units: list[Unit] = []
    for record in records:
        fields = record.get("fields", {})
        name = cell(fields, "MAG_ENTRY_NAME")
        code = cell(fields, "MAG_ENTRY_CODE")
        if not (name or code):
            continue
        facts = {column: cell(fields, key) for key, column in MAG_FACTS.items()}
        level = annotation_level(cell(fields, "MAG_ENTRY_ANNOTATED"))
        facts["annotation_level"] = level
        units.append([("mags", [row(
            code,
            key={"name": name},
            values={"annotated": True} if level else None,
            defaults=facts,
            airtable_id=record.get("id"),
        )])])
    return units


def mag_from_airtable(record: dict) -> dict[str, Any]:
    """An Airtable MAG record in the shape 'ehio quantifying' and 'annotating' use."""
    fields = record.get("fields", {})
    return {
        "code":             cell(fields, "MAG_ENTRY_CODE"),
        "name":             str(cell(fields, "MAG_ENTRY_NAME") or ""),
        "fasta_url":        cell(fields, "MAG_ENTRY_URL_FASTA"),
        "completeness":     cell(fields, "MAG_ENTRY_CHECKM_COMPLETENESS"),
        "contamination":    cell(fields, "MAG_ENTRY_CHECKM_CONTAMINATION"),
        # Kept as Airtable says it, "true" included: 'annotating --input'
        # knows what the legacy word means.
        "annotation_level": str(cell(fields, "MAG_ENTRY_ANNOTATED") or "").lower(),
        "airtable_id":      record.get("id"),
    }


def mag_from_core(mag: dict) -> dict[str, Any]:
    """A MAG as the core returns it, in the same shape."""
    return {
        "code":             mag.get("code"),
        "name":             str(mag.get("name") or ""),
        "fasta_url":        mag.get("fasta_url"),
        "completeness":     mag.get("completeness"),
        "contamination":    mag.get("contamination"),
        "annotation_level": str(mag.get("annotation_level") or ""),
        "airtable_id":      mag.get("airtable_record_id"),
    }


def annotated_mag(mag: dict, metrics: dict[str, Any]) -> Unit:
    """What 'ehio annotating --output' found about one MAG."""
    values = columns(metrics, ANNOTATION_COLUMNS)
    if "annotated" in metrics:
        values["annotated"] = True
        values["annotation_level"] = annotation_level(metrics["annotated"])
    key = {"name": mag["name"]} if mag.get("name") else {}
    return [("mags", [row(mag.get("code"), key=key, values=values)])]


def mappings(batch_code: str, entries: Iterable[tuple[str | None, str | None, Any]]) -> list[Unit]:
    """A dereplication batch's mappings, one per preprocessed sample, found by
    batch and preprocessing: (preprocessing code, Airtable's DM code, mapping rate)."""
    units: list[Unit] = []
    for preprocessing, code, rate in entries:
        if not preprocessing:
            continue
        units.append([("dereplication_mappings", [row(
            key={"batch_id": batch_code, "preprocessing_id": preprocessing, "code": code},
            values={"mapping_rate": rate},
        )])])
    return units
