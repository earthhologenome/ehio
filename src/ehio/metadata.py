"""Parse drakkar preprocessing output files and build Airtable update payloads."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any

OUTPUT_TSV_COLUMNS: list[str] = [
    "sample",
    "reads_pre_fastp",
    "bases_pre_fastp",
    "adapter_trimmed_reads",
    "adapter_trimmed_bases",
    "reads_post_fastp",
    "bases_post_fastp",
    "host_reads",
    "host_bases",
    "metagenomic_reads",
    "metagenomic_bases",
    "singlem_fraction",
    "nonpareil_C",
    "nonpareil_LR",
    "nonpareil_modelR",
    "nonpareil_LRstar",
    "nonpareil_diversity",
]


# ---------------------------------------------------------------------------
# Per-file parsers
# ---------------------------------------------------------------------------

def parse_fastp(json_path: Path) -> dict[str, Any]:
    """Parse a fastp JSON report. Returns a dict with read/base counts."""
    with json_path.open() as fh:
        data = json.load(fh)

    before = data.get("summary", {}).get("before_filtering", {})
    after  = data.get("summary", {}).get("after_filtering", {})
    adapter = data.get("adapter_cutting", {})

    return {
        "reads_pre_fastp":        before.get("total_reads"),
        "bases_pre_fastp":        before.get("total_bases"),
        "reads_post_fastp":       after.get("total_reads"),
        "bases_post_fastp":       after.get("total_bases"),
        "adapter_trimmed_reads":  adapter.get("adapter_trimmed_reads"),
        "adapter_trimmed_bases":  adapter.get("adapter_trimmed_bases"),
    }


def _read_int_file(path: Path) -> int | None:
    """Read the first non-empty line of a text file and return it as int."""
    try:
        text = path.read_text().strip()
        return int(text) if text else None
    except (OSError, ValueError):
        return None


def parse_host_removal(
    metareads_path: Path,
    metabases_path: Path,
) -> dict[str, Any]:
    """Parse .metareads and .metabases files produced by the split_reads rule."""
    return {
        "metagenomic_reads": _read_int_file(metareads_path),
        "metagenomic_bases": _read_int_file(metabases_path),
    }


def parse_singlem_mf(smf_path: Path) -> dict[str, Any]:
    """Parse a SingleM microbial_fraction TSV.

    Expected columns: sample, bacterial_archaeal_bases, metagenome_size, read_fraction, ...
    Returns {"singlem_fraction": float | None}.
    """
    try:
        with smf_path.open(newline="") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                frac = row.get("read_fraction", "").strip()
                try:
                    return {"singlem_fraction": float(frac)}
                except ValueError:
                    pass
    except OSError:
        pass
    return {"singlem_fraction": None}


def parse_nonpareil(summary_path: Path) -> dict[str, Any]:
    """Parse a Nonpareil summary TSV produced by drakkar nonpareil_stats.R.

    Expected file: preprocessing/nonpareil/{sample}_np.tsv
    Columns: sample, kappa, C, LR, modelR, LRstar, diversity
    Returns all values as None if the file is absent or unparseable.
    """
    result: dict[str, Any] = {
        "nonpareil_C":         None,
        "nonpareil_LR":        None,
        "nonpareil_modelR":    None,
        "nonpareil_LRstar":    None,
        "nonpareil_diversity": None,
    }
    if not summary_path.exists():
        return result

    try:
        with summary_path.open(newline="") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                def _f(key: str) -> float | None:
                    v = row.get(key, "").strip()
                    try:
                        return float(v)
                    except ValueError:
                        return None

                result["nonpareil_C"]         = _f("C")
                result["nonpareil_LR"]        = _f("LR")
                result["nonpareil_modelR"]    = _f("modelR")
                result["nonpareil_LRstar"]    = _f("LRstar")
                result["nonpareil_diversity"] = _f("diversity")
                break  # one row per sample file
    except OSError:
        pass

    return result


# ---------------------------------------------------------------------------
# Per-sample collector
# ---------------------------------------------------------------------------

def collect_preprocessing_metadata(
    sample: str,
    output_dir: Path,
) -> dict[str, Any]:
    """Collect all preprocessing QC metrics for one sample.

    Looks for output files under output_dir using the path layout produced
    by the drakkar preprocessing_ref workflow:

        preprocessing/fastp/{sample}.json
        preprocessing/final/{sample}.metareads
        preprocessing/final/{sample}.metabases
        preprocessing/singlem/{sample}_smf.tsv
        preprocessing/nonpareil/{sample}_np.tsv          (optional)

    Returns a flat dict of metric_key → value (None if file missing).
    """
    base = output_dir / "preprocessing"

    fastp_file    = base / "fastp"    / f"{sample}.json"
    metareads     = base / "final"    / f"{sample}.metareads"
    metabases     = base / "final"    / f"{sample}.metabases"
    singlem_file  = base / "singlem"  / f"{sample}_smf.tsv"
    nonpareil_tsv = base / "nonpareil" / f"{sample}_np.tsv"

    result: dict[str, Any] = {}

    if fastp_file.exists():
        result.update(parse_fastp(fastp_file))
    else:
        result.update({k: None for k in [
            "reads_pre_fastp", "bases_pre_fastp",
            "reads_post_fastp", "bases_post_fastp",
            "adapter_trimmed_reads", "adapter_trimmed_bases",
        ]})
        print(f"  Warning: fastp JSON not found: {fastp_file}", file=sys.stderr)

    result.update(parse_host_removal(metareads, metabases))
    result.update(parse_singlem_mf(singlem_file))
    result.update(parse_nonpareil(nonpareil_tsv))

    return result


# ---------------------------------------------------------------------------
# Airtable payload builder
# ---------------------------------------------------------------------------

def build_entry_update(
    record_id: str,
    metrics: dict[str, Any],
    field_map: dict[str, str],
) -> dict[str, Any]:
    """Build an Airtable update payload for one entry record.

    field_map maps metric keys (e.g. "reads_pre_fastp") to Airtable field IDs
    (e.g. "fldmD0Z4dNmEc0Uri"). Only fields with non-None values are included.
    """
    fields: dict[str, Any] = {}
    for metric_key, field_id in field_map.items():
        value = metrics.get(metric_key)
        if value is not None:
            fields[field_id] = value
    return {"id": record_id, "fields": fields}


def parse_drakkar_stats_tsv(tsv_path: Path) -> dict[str, dict[str, Any]]:
    """Read a drakkar summary TSV (e.g. preprocessing.tsv or cataloging.tsv).

    Returns {sample_code: {metric_name: value, ...}}.
    Numeric strings are coerced to int (if whole-valued) or float.
    'NA', empty, and similar sentinel strings become None.
    """
    result: dict[str, dict[str, Any]] = {}
    if not tsv_path.exists():
        return result
    try:
        with tsv_path.open(newline="") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                sample = (row.get("sample") or "").strip()
                if not sample:
                    continue
                metrics: dict[str, Any] = {}
                for col, raw in row.items():
                    if col == "sample":
                        continue
                    v = (raw or "").strip()
                    if v in ("", "NA", "nan", "NaN", "None", "none"):
                        metrics[col] = None
                    else:
                        try:
                            f = float(v)
                            metrics[col] = int(f) if f == round(f) else f
                        except ValueError:
                            metrics[col] = v
                result[sample] = metrics
    except OSError:
        pass
    return result


def write_output_tsv(
    sample_metrics: dict[str, dict[str, Any]],
    tsv_path: Path,
) -> None:
    """Write a per-sample summary TSV to tsv_path.

    Uses host_reads / host_bases from the metrics dict when present;
    falls back to deriving them as (reads_post_fastp - metagenomic_reads) etc.
    """
    tsv_path.parent.mkdir(parents=True, exist_ok=True)
    with tsv_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_TSV_COLUMNS, delimiter="\t",
                                extrasaction="ignore")
        writer.writeheader()
        for sample, m in sample_metrics.items():
            row = {"sample": sample, **m}
            if row.get("host_reads") is None:
                rp = m.get("reads_post_fastp")
                mr = m.get("metagenomic_reads")
                row["host_reads"] = (rp - mr) if (rp is not None and mr is not None) else None
            if row.get("host_bases") is None:
                bp = m.get("bases_post_fastp")
                mb = m.get("metagenomic_bases")
                row["host_bases"] = (bp - mb) if (bp is not None and mb is not None) else None
            writer.writerow(row)


BINNING_OUTPUT_TSV_COLUMNS: list[str] = [
    "sample",
    "assembly_length",
    "assembly_n50",
    "assembly_l50",
    "assembly_contigs_number",
    "assembly_contigs_largest",
    "assembly_mapping_rate",
    "bins_number",
]

QUANTIFYING_OUTPUT_TSV_COLUMNS: list[str] = [
    "sample",
    "mapping_rate",
]


# ---------------------------------------------------------------------------
# Binning / cataloging parsers
# ---------------------------------------------------------------------------

def parse_quast_report(report_path: Path) -> dict[str, Any]:
    """Parse a QUAST report.tsv file. Returns assembly stats.

    Expected file: cataloging/assembly/quast/{sample}/report.tsv
    """
    result: dict[str, Any] = {
        "assembly_length":          None,
        "assembly_n50":             None,
        "assembly_l50":             None,
        "assembly_contigs_number":  None,
        "assembly_contigs_largest": None,
    }
    if not report_path.exists():
        return result
    _QUAST_MAP = {
        "Total length":   "assembly_length",
        "N50":            "assembly_n50",
        "L50":            "assembly_l50",
        "# contigs":      "assembly_contigs_number",
        "Largest contig": "assembly_contigs_largest",
    }
    try:
        with report_path.open(newline="") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if line.startswith("#") or line.startswith("Assembly"):
                    continue
                parts = line.split("\t", 1)
                if len(parts) < 2:
                    continue
                metric, raw = parts[0].strip(), parts[1].strip()
                if metric in _QUAST_MAP:
                    try:
                        result[_QUAST_MAP[metric]] = int(raw)
                    except ValueError:
                        pass
    except OSError:
        pass
    return result


def parse_flagstat(flagstat_path: Path) -> dict[str, Any]:
    """Parse a samtools flagstat file to extract the overall mapping rate (%).

    Expected file: cataloging/bowtie2/{sample}/{sample}.flagstat.txt
                   profiling/mapping/{sample}.flagstat
    """
    result: dict[str, Any] = {"mapping_rate": None}
    if not flagstat_path.exists():
        return result
    import re
    try:
        text = flagstat_path.read_text()
        m = re.search(r"mapped \((\d+\.\d+)%", text)
        if m:
            result["mapping_rate"] = float(m.group(1))
    except OSError:
        pass
    return result


def parse_dastool_summary(summary_path: Path) -> dict[str, Any]:
    """Count recovered bins from a DAS_Tool summary TSV.

    Expected file: cataloging/binning/dastool/{sample}/{sample}_DASTool_summary.tsv
    Each data row represents one bin; header row is excluded from the count.
    """
    result: dict[str, Any] = {"bins_number": None}
    if not summary_path.exists():
        return result
    try:
        with summary_path.open(newline="") as fh:
            rows = list(csv.reader(fh, delimiter="\t"))
        result["bins_number"] = max(0, len(rows) - 1)
    except OSError:
        pass
    return result


def collect_binning_metadata(
    sample: str,
    output_dir: Path,
) -> dict[str, Any]:
    """Collect assembly and binning QC metrics for one sample.

    Looks for output files under output_dir using the path layout produced
    by the drakkar cataloging workflow:

        cataloging/quast/{sample}/report.tsv                 (QUAST assembly stats)
        cataloging/bowtie2/{sample}/{sample}.flagstat.txt    (samtools flagstat)
        cataloging/final/{sample}.tsv                        (Binette quality report, bins count)

    Returns a flat dict of metric_key → value (None if file missing).
    """
    base = output_dir / "cataloging"

    quast_file   = base / "quast"   / sample / "report.tsv"
    flagstat     = base / "bowtie2" / sample  / f"{sample}.flagstat.txt"
    binette_file = base / "final"   / f"{sample}.tsv"

    result: dict[str, Any] = {}
    result.update(parse_quast_report(quast_file))
    result["assembly_mapping_rate"] = parse_flagstat(flagstat).get("mapping_rate")
    result.update(parse_dastool_summary(binette_file))
    return result


def collect_quantifying_metadata(
    sample: str,
    output_dir: Path,
) -> dict[str, Any]:
    """Collect profiling metrics for one sample.

    Looks for output files under output_dir using the path layout produced
    by the drakkar profiling workflow:

        profiling/mapping/{sample}.flagstat

    Returns a flat dict of metric_key → value (None if file missing).
    """
    flagstat = output_dir / "profiling" / "mapping" / f"{sample}.flagstat"
    return parse_flagstat(flagstat)


def write_binning_output_tsv(
    sample_metrics: dict[str, dict[str, Any]],
    tsv_path: Path,
) -> None:
    """Write a per-sample assembly/binning summary TSV to tsv_path."""
    tsv_path.parent.mkdir(parents=True, exist_ok=True)
    with tsv_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=BINNING_OUTPUT_TSV_COLUMNS, delimiter="\t",
                                extrasaction="ignore")
        writer.writeheader()
        for sample, m in sample_metrics.items():
            writer.writerow({"sample": sample, **m})


def write_quantifying_output_tsv(
    sample_metrics: dict[str, dict[str, Any]],
    tsv_path: Path,
) -> None:
    """Write a per-sample mapping summary TSV to tsv_path."""
    tsv_path.parent.mkdir(parents=True, exist_ok=True)
    with tsv_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=QUANTIFYING_OUTPUT_TSV_COLUMNS, delimiter="\t",
                                extrasaction="ignore")
        writer.writeheader()
        for sample, m in sample_metrics.items():
            writer.writerow({"sample": sample, **m})


# Default mapping: metric key → config key (resolved to field IDs at call time)
PREPROCESSING_METRIC_KEYS: dict[str, str] = {
    "reads_pre_fastp":        "EHI_PPR_ENTRY_READS_PRE_FASTP",
    "bases_pre_fastp":        "EHI_PPR_ENTRY_BASES_PRE_FASTP",
    "adapter_trimmed_reads":  "EHI_PPR_ENTRY_ADAPTER_TRIMMED_READS",
    "adapter_trimmed_bases":  "EHI_PPR_ENTRY_ADAPTER_TRIMMED_BASES",
    "reads_post_fastp":       "EHI_PPR_ENTRY_READS_POST_FASTP",
    "bases_post_fastp":       "EHI_PPR_ENTRY_BASES_POST_FASTP",
    "host_reads":             "EHI_PPR_ENTRY_HOST_READS",
    "host_bases":             "EHI_PPR_ENTRY_HOST_BASES",
    "metagenomic_reads":      "EHI_PPR_ENTRY_METAGENOMIC_READS",
    "metagenomic_bases":      "EHI_PPR_ENTRY_METAGENOMIC_BASES",
    "singlem_fraction":       "EHI_PPR_ENTRY_SINGLEM_FRACTION",
    "nonpareil_C":            "EHI_PPR_ENTRY_NONPAREIL_C",
    "nonpareil_LR":           "EHI_PPR_ENTRY_NONPAREIL_LR",
    "nonpareil_modelR":       "EHI_PPR_ENTRY_NONPAREIL_MODELR",
    "nonpareil_LRstar":       "EHI_PPR_ENTRY_NONPAREIL_LRSTAR",
    "nonpareil_diversity":    "EHI_PPR_ENTRY_NONPAREIL_DIVERSITY",
}

BINNING_METRIC_KEYS: dict[str, str] = {
    "assembly_length":          "EHI_ASB_ENTRY_ASSEMBLY_LENGTH",
    "assembly_n50":             "EHI_ASB_ENTRY_ASSEMBLY_N50",
    "assembly_l50":             "EHI_ASB_ENTRY_ASSEMBLY_L50",
    "assembly_contigs_number":  "EHI_ASB_ENTRY_ASSEMBLY_CONTIGS_NUMBER",
    "assembly_contigs_largest": "EHI_ASB_ENTRY_ASSEMBLY_CONTIGS_LARGEST",
    "assembly_mapping_rate":    "EHI_ASB_ENTRY_ASSEMBLY_MAPPING_RATE",
    "bins_number":              "EHI_ASB_ENTRY_BINS_NUMBER",
}

QUANTIFYING_METRIC_KEYS: dict[str, str] = {
    "mapping_rate": "MAG_DMB_ENTRY_MAPPING_RATE",
}
