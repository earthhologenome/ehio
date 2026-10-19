"""Build ehi-core rows from drakkar output and ERDA file URLs."""

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
    "ena":           "ena_submissions",
}

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
    "bins_number":              "num_bins",
}

# What drakkar measures of each sample of an assembly, which the core keeps on
# that sample rather than on the assembly, since a coassembly has several.
ASSEMBLY_SAMPLE_COLUMNS = {
    "assembly_mapping_rate": "mapping_percent",
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
    "gtdbtk_version":    "gtdbtk_version",
    "gtdb_release":      "gtdb_release",
    "coding_density":    "coding_density",
    "genes_number":      "genes",
    "genes_unannotated": "genes_unannotated",
    "genes_kegg":        "kegg_hits",
}

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
) -> Row:
    """One upsert row. Empty values are left out: ehio never clears a cell."""
    found_by = _clean(key)
    if code:
        found_by["code"] = code
    out: Row = {"key": found_by, "values": _clean(values), "defaults": _clean(defaults)}
    return out


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

def batch(module: str, code: str, **values: Any) -> Unit:
    """The values written to one batch row."""
    return [(BATCH_TABLES[module], [row(code, values=values)])]


# ---------------------------------------------------------------------------
# Preprocessing
# ---------------------------------------------------------------------------

def preprocessing_metrics(metrics: dict[str, dict]) -> list[Unit]:
    """The QC metrics of each preprocessing, by its code.

    The core already holds the batch and its libraries.
    """
    return [
        [("preprocessings", [row(code, values=columns(found, PREPROCESSING_COLUMNS))])]
        for code, found in metrics.items() if code
    ]


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

def assembly_metrics(metrics: dict[str, dict]) -> list[Unit]:
    """The metrics of each assembly, by its code — the core's own batches."""
    return [
        [("assemblies", [row(code, values=columns(found, ASSEMBLY_COLUMNS))])]
        for code, found in metrics.items() if code
    ]


def assembly_samples(samples: Iterable[tuple[str | None, str | None, dict]]) -> list[Unit]:
    """What drakkar measured of each sample of an assembly, on the sample's row:
    (assembly code, preprocessing code, metrics).

    The core only updates these rows, because which samples an assembly holds is
    set for the whole batch at once (CoreClient.link_assembly_samples); a sample
    whose preprocessing is not known is left out.
    """
    units: list[Unit] = []
    for assembly, preprocessing, metrics in samples:
        values = columns(metrics, ASSEMBLY_SAMPLE_COLUMNS)
        if assembly and preprocessing and values:
            units.append([("assembly_samples", [row(
                key={"assembly_id": assembly, "preprocessing_id": preprocessing},
                values=values,
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

def _amr_values(
    amr_code: str,
    code: str,
    stats: dict[str, dict] | None,
    gene_calls: set[str] | None,
) -> dict[str, Any]:
    """One assembly's AMR metrics and the ERDA URLs of its gene calls
    (file names under AMR/{batch}/genes)."""
    values = {"amr_batch_id": amr_code, **columns((stats or {}).get(code), AMR_COLUMNS)}
    for column, name in (("faa_url", f"{code}.faa.gz"), ("ffn_url", f"{code}.ffn.gz")):
        if name in (gene_calls or set()):
            values[column] = erda_url("AMR", amr_code, "genes", name)
    return values


def amr_metrics(
    amr_code: str,
    codes: Iterable[str],
    stats: dict[str, dict] | None = None,
    gene_calls: set[str] | None = None,
) -> list[Unit]:
    """The same, by assembly code — the core's own batches, which already hold
    the assemblies and which batch produced them."""
    return [
        [("assemblies", [row(code, values=_amr_values(amr_code, code, stats, gene_calls))])]
        for code in codes if code
    ]


# ---------------------------------------------------------------------------
# MAGs of a dereplication batch
# ---------------------------------------------------------------------------

def mag_from_core(mag: dict) -> dict[str, Any]:
    """A MAG as the core returns it, in the same shape."""
    return {
        "code":             mag.get("code"),
        "name":             str(mag.get("name") or ""),
        "fasta_url":        mag.get("fasta_url"),
        "completeness":     mag.get("completeness"),
        "contamination":    mag.get("contamination"),
        "annotation_level": str(mag.get("annotation_level") or ""),
        # Whether the batch's dereplication kept it.
        "is_representative": bool(mag.get("is_representative")),
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
    batch and preprocessing: (preprocessing code, mapping code, mapping rate)."""
    units: list[Unit] = []
    for preprocessing, code, rate in entries:
        if not preprocessing:
            continue
        units.append([("dereplication_mappings", [row(
            key={"batch_id": batch_code, "preprocessing_id": preprocessing, "code": code},
            values={"mapping_rate": rate},
        )])])
    return units
