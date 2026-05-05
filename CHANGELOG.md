# Changelog

All notable changes to ehio are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- No unreleased changes yet.

## [0.3.4] - 2026-05-05

### Added

- `ehio annotating` subcommand with `--input` and `--output` modes.
  - `--input`: fetches linked MAG records for the batch, skips any where `MAG_ENTRY_ANNOTATED` is already `true`, and writes the remaining genome paths (one per line) to `--annotation-file`. Accepts `--annotation-dir` to locate the dereplicated FASTA files.
  - `--output`: parses `annotating/genome_taxonomy.tsv` (GTDB-Tk classification per genome), per-genome annotation TSVs from `annotating/final/`, and uploads taxonomy/tree files to `DMB/{batch}/` and compressed per-genome TSVs to `ANN/{batch}/` via SFTP. Updates `MAG_ENTRY` in Airtable with taxonomy ranks, GTDB closest-genome metadata, coding density, gene counts, and sets `MAG_ENTRY_ANNOTATED = true` for each processed genome.
- New metadata parsers in `ehio.metadata`: `parse_genome_taxonomy_tsv`, `_parse_gtdb_classification`, `parse_annotation_tsv`.
- New config keys: `MAG_ENTRY_DOMAIN`, `MAG_ENTRY_PHYLUM`, `MAG_ENTRY_CLASS`, `MAG_ENTRY_ORDER`, `MAG_ENTRY_FAMILY`, `MAG_ENTRY_GENUS`, `MAG_ENTRY_SPECIES`, `MAG_ENTRY_GTDB_FASTANI`, `MAG_ENTRY_GTDB_CLOSEST_ANI`, `MAG_ENTRY_GTDB_CLOSEST_AF`, `MAG_ENTRY_CODING_DENSITY`, `MAG_ENTRY_GENES_NUMBER`, `MAG_ENTRY_GENES_NUMBER_UNANNOTATED`, `MAG_ENTRY_GENES_KEGG_NUMBER`, `MAG_ENTRY_ANNOTATED`.

### Changed

- Generated quantifying scripts now include two `drakkar annotating` steps after `ehio quantifying --output`: taxonomy annotation (`--annotation-type taxonomy`, always runs) followed by functional annotation (`--annotation-type function`, only for unannotated MAGs), with `ehio annotating --input` in between to build the filtered genome paths file. `ehio annotating --output` runs last to upload results and update Airtable.
- `ehio binning --output` now sets `MAG_ENTRY_ANNOTATED = false` when creating new `MAG_ENTRY` records, marking freshly binned genomes as not yet functionally annotated.

## [0.3.3] - 2026-05-04

### Changed

- Quantifying output now creates new `MAG_DMB_ENTRY` records instead of updating pre-existing ones. Each record is linked to the batch (`MAG_DMB_ENTRY_BATCH`) and its corresponding PPR record (`MAG_DMB_ENTRY_PPR`), with `MAG_DMB_ENTRY_MAPPING_RATE` populated from `profiling_genomes.tsv`.

## [0.3.2] - 2026-05-04

### Added

- `parse_profiling_genomes_tsv`: reads `profiling_genomes.tsv` and extracts `mapping_percentage` per sample → `MAG_DMB_ENTRY_MAPPING_RATE`.
- `parse_dereplicating_tsv`: reads `dereplicating.tsv` and extracts `output_bin_number` → `MAG_DMB_BATCH_DEREP_MAGS`.

### Changed

- Quantifying output now locates drakkar results under `profiling_genomes/final/` (was `profiling/final/`).
- `counts.tsv` and `bases.tsv` are gzipped and renamed to `{batch}_counts.tsv.gz` / `{batch}_bases.tsv.gz` before transfer to `DMB/{batch}/`; only these two files are uploaded (whole-directory upload removed).
- Batch-level `MAG_DMB_BATCH_DEREP_MAGS` and per-entry `MAG_DMB_ENTRY_MAPPING_RATE` are now populated from drakkar summary TSVs instead of flagstat files.

## [0.3.1] - 2026-05-04

### Changed

- Reads for the quantifying input are now sourced from `MAG_PPR` (via `MAG_DMB_BATCH_LIST_PPR`) instead of `MAG_DMB_ENTRY`. Sample name comes from `MAG_PPR_EHI`; reads from `MAG_PPR_READS1` / `MAG_PPR_READS2`.

## [0.3.0] - 2026-05-04

### Changed

- `ehio quantifying --input` now fetches MAG records by linked IDs from `MAG_DMB_BATCH_LIST_MAGS` and entry records by linked IDs from `MAG_DMB_BATCH_LIST_ENTRY`, replacing the formula-based entry scan.
- Output files renamed: `{batch}_mags.tsv` (MAG URLs, no header), `{batch}_reads.tsv` (sample/rawreads1/rawreads2), `{batch}_quality.tsv` (genome/completeness/contamination).
- `drakkar profiling` command now includes `-a {ani_threshold}` (from `MAG_DMB_BATCH_ANI`), `-t {profiling_type}` (from `MAG_DMB_BATCH_TYPE`, lowercased), and `-q {quality_file}`.

### Added

- New `write_quality_file` helper in `drakkar.py` writes the MAG quality TSV from `MAG_ENTRY_NAME`, `MAG_ENTRY_CHECKM_COMPLETENESS`, `MAG_ENTRY_CHECKM_CONTAMINATION`.

## [0.2.17] - 2026-05-03

### Changed

- `MAG_ENTRY_NAME` now stores the genome filename with its `.fa` extension (e.g. `EHA05803_bin_2253.fa`).
- FASTA files are compressed to `.fa.gz` before uploading to `MAG/{batch}/`; temporary `.gz` files are removed after transfer.

## [0.2.16] - 2026-05-03

### Fixed

- `MAG_ENTRY_URL_FASTA` (`MAG_url`) is no longer written during record creation — it is computed by Airtable automatically.

### Added

- Assembly code (the prefix before `_bin_` in the genome filename, e.g. `EHA05803`) is now written to `MAG_ENTRY_ASSEMBLY` for each created MAG record.

## [0.2.15] - 2026-05-03

### Fixed

- `MAG_ENTRY_CODE` (the auto-number primary key "ID" in Airtable) is no longer written during record creation. The genome filename (without extension) is now stored in `MAG_ENTRY_NAME` instead, which is a writable text field.

## [0.2.14] - 2026-05-03

### Changed

- Removed `score` from `BIN_METRIC_KEYS` and dropped `MAG_ENTRY_SCORE` from config. The Binette score is no longer written to Airtable.

## [0.2.13] - 2026-05-03

### Fixed

- MAG FASTA files are now uploaded flat to `{SFTP_REMOTE_BASE}/MAG/{batch}/EHA05803_bin_2253.fa` instead of into a per-assembly subdirectory. The `MAG_ENTRY_URL_FASTA` values written to Airtable reflect this flat layout.

## [0.2.12] - 2026-05-03

### Added

- `ehio binning --output` now reads `cataloging/final/all_bin_metadata.csv` and creates one `MAG_ENTRY` record per bin in Airtable (`MAG_BASE`), populating: code (genome filename without extension), completeness, contamination, score, size, N50, contig count, and the remote FASTA URL.
- FASTA files listed in `cataloging/final/all_bin_paths.txt` are uploaded to `{SFTP_REMOTE_BASE}/MAG/{batch}/` preserving the assembly subdirectory (e.g. `.../MAG/ABB0650/EHA05803/EHA05803_bin_2253.fa`). The remote URL is written into `MAG_ENTRY_URL_FASTA` at record-creation time.
- New config key `MAG_ENTRY_SCORE` for the Binette composite score field (leave empty to skip).
- `AirtableClient.create_records`: new method for batch-creating records via `batch_create`.

## [0.2.11] - 2026-05-03

### Changed

- `ehio binning --output` now uses per-sample mapping rates from the `sample_mapping_rates` column of `cataloging.tsv` (e.g. `EHI00001:1.96;EHI00002:34.75`) instead of the assembly-level `mapping_rate_percent`. Each entry in Airtable and each row in the output TSV receives its own mapping rate.
- Binning output TSV now includes an `assembly` column (second column, after `sample`) containing the assembly code. For individual assemblies each sample has a unique code; for co-assemblies all members share the same code.

## [0.2.10] - 2026-05-03

### Added

- "Resume" batch status: when a batch is set to `SCANNING_RESUME_STATUS` (default `"Resume"`), the generated script skips the `ehio <module> --input` step and runs drakkar directly against the existing input TSV, then runs `ehio <module> --output` as normal. Useful when drakkar stopped mid-run and the input files are already in place.

## [0.2.9] - 2026-05-03

### Fixed

- `ehio binning --output` now correctly reads `cataloging.tsv` using the `assembly` column as the key (previously it looked for a `sample` column, so all metrics were empty).
- Column names from drakkar's `cataloging.tsv` (`assembly_total_length`, `assembly_N50`, `assembly_L50`, `assembly_contigs`, `assembly_largest_contig`, `mapping_rate_percent`, `final_bins`) are now mapped to ehio's metric keys (`assembly_length`, `assembly_n50`, `assembly_l50`, `assembly_contigs_number`, `assembly_contigs_largest`, `assembly_mapping_rate`, `bins_number`).
- The binning `_output.tsv` now contains one row per sample (EHI number) rather than one row per assembly code, so co-assemblies produce the correct number of rows.

## [0.2.8] - 2026-05-03

### Added

- `--rerun` flag for `ehio preprocessing/binning/quantifying --output`: when set, the remote archive directory is deleted via SFTP before uploading the new output files, replacing the previous run's data cleanly.


### Changed

- `ehio binning --input` now writes an `assembly` column (from `EHI_ASB_ENTRY_ASSEMBLY_CODE`) to the drakkar sample TSV. Drakkar infers co-assembly vs individual assembly automatically from rows that share the same `assembly` value, replacing the old `-m individual` / `-m all` CLI flag.
- `ehio binning --output` now looks up cataloging metrics by `EHI_ASB_ENTRY_ASSEMBLY_CODE` rather than entry code, so co-assembly metrics are correctly applied to all entries sharing the same assembly.
- Generated binning scripts no longer pass `-m` to `drakkar cataloging`.
- `write_sample_file` accepts an optional `assembly_field` parameter; when supplied it adds an `assembly` column as the second column in the TSV.

## [0.2.6] - 2026-05-02

### Changed

- `ehio preprocessing --output` now reads metrics from the drakkar-generated `preprocessing.tsv` (at the root of the output directory) instead of parsing individual per-sample output files. This fixes missing `metagenomic_reads`, `metagenomic_bases`, `host_reads`, and `host_bases` values that arose because the source files are declared `temp()` in snakemake and deleted after the pipeline run.
- `ehio binning --output` likewise reads from `cataloging.tsv` instead of per-sample QUAST/flagstat/DAS_Tool files.
- `write_output_tsv` now uses `host_reads`/`host_bases` from the metrics dict directly when available, falling back to derivation only if absent.

### Added

- `parse_drakkar_stats_tsv`: new function that reads any drakkar summary TSV into a `{sample: metrics}` dict, handling `NA`/empty cells as `None` and coercing numeric strings to `int` or `float`.
- `host_reads` and `host_bases` added to `PREPROCESSING_METRIC_KEYS` and config (`EHI_PPR_ENTRY_HOST_READS`, `EHI_PPR_ENTRY_HOST_BASES`).

## [0.2.5] - 2026-05-02

### Added

- `ehio scanning` now reads `EHI_PPR_BATCH_BOOST_TIME` / `EHI_PPR_BATCH_BOOST_MEMORY` (and the homologous `EHI_ASB_*` and `MAG_DMB_*` keys) from each batch record and passes `--time-multiplier` and `--memory-multiplier` to the corresponding drakkar command. Values of 1 or absent are omitted (default drakkar behaviour).

## [0.2.4] - 2026-05-02

### Fixed

- `ehio binning --input` now uses `EHI_ASB_ENTRY_EHI_NUMBER` as the sample name in the input TSV instead of `EHI_ASB_ENTRY_CODE`.
- `__version__` is now read from package metadata via `importlib.metadata` instead of being hardcoded, so `ehio --version` always reflects the installed version.

### Changed

- Generated batch scripts now use an `EXIT` trap with a success sentinel (`_EHIO_SUCCESS`) instead of an `ERR` trap. `_EHIO_SUCCESS=1` is set only after `ehio --output` completes; any earlier exit (snakemake failure, SIGTERM, or unexpected screen-session termination) triggers `ehio set-status --status Error` in Airtable.

## [0.2.3] - 2026-05-01

### Added

- `ehio scanning` now reads `EHI_ASB_BATCH_TYPE` from each binning batch record and passes `-m individual` or `-m all` to `drakkar cataloging` accordingly. Airtable values `"Individual"` and `"Coassembly"` (case-insensitive) are supported; anything other than a co-assembly variant defaults to `individual`.

## [0.2.2] - 2026-05-01

### Changed

- `ehio preprocessing --output` now transfers files flat (no subdirectories) to the remote archive. Only the renamed `.fq.gz`, `.bam`, `_cond.tsv`, and `{batch}_output.tsv` files are uploaded; the full `preprocessing/` directory tree is no longer mirrored.
- `SFTPTransfer.upload_flat`: new method that uploads a list of files directly into a remote directory without preserving any local subdirectory structure.

## [0.2.1] - 2026-05-01

### Added

- `ehio preprocessing --output` now renames output files to EHI-number-based names before archiving: `{code}.bam` → `{ehi}_G.bam`, `{code}_1.fq.gz` → `{ehi}_M_1.fq.gz`, `{code}_2.fq.gz` → `{ehi}_M_2.fq.gz`, `{code}_cond.tsv` → `{ehi}_cond.tsv`. The `sample` column in `{batch}_output.tsv` also uses the EHI number.
- `ehio preprocessing --input`, `ehio binning --input`, and `ehio quantifying --input` now verify that all local input file paths exist before returning. Remote URLs are skipped. If any file is missing, each path is reported as a warning and the command exits with an error before a screen session can be launched.
- `ehio stop` now requires `--module` and accepts `--airtable-token`. Before killing the screen session it connects to Airtable and sets the batch status to `SCANNING_STOPPED_STATUS` (default `"Stopped"`).
- Config key added: `SCANNING_STOPPED_STATUS` (default `"Stopped"`).

## [0.2.0] - 2026-05-01

### Added

- `ehio binning --output`: collects assembly/binning QC metrics (QUAST, samtools flagstat, Binette bin count), writes `{batch}_output.tsv`, updates `EHI_ASB_ENTRY` in Airtable, transfers `cataloging/final/` via SFTP, logs ehio/drakkar versions to the batch record, and sets status to `Done`.
- `ehio quantifying --output`: collects per-sample mapping rates (samtools flagstat), writes `{batch}_output.tsv`, updates `MAG_DMB_ENTRY`, transfers `profiling/final/` via SFTP, and marks the batch `Done`.
- Generated scripts for binning and quantifying now include `ehio binning/quantifying --output` after the drakkar call, mirroring preprocessing.
- `ehio quantifying --input` now fetches the bins file from MAG records linked via `MAG_DMB_BATCH_LIST_MAGS` → `MAG_ENTRY_URL_FASTA` instead of expecting a bins field on each entry.
- `ehio stop --batch XXXXX`: kills the screen session for a running batch.
- `ehio remove --module MODULE --batch XXXXX`: deletes the output directory without touching `RUN/{batch}`.
- Config keys added: `EHI_ASB_BATCH_EHIO_VERSION`, `EHI_ASB_BATCH_DRAKKAR_VERSION`, `MAG_DMB_BATCH_EHIO_VERSION`, `MAG_DMB_BATCH_DRAKKAR_VERSION`, `MAG_DMB_BATCH_LIST_MAGS`, `MAG_ENTRY_URL_FASTA`.
- Preprocessing SFTP transfer now uploads `.bam`, `.fq.gz`, `_cond.tsv` (SingleM condensed profile), and `_output.tsv` from the full `preprocessing/` tree, excluding intermediate files (`.hostbases`, `.hostreads`, `.metareads`, `.metabases`).

### Fixed

- Nonpareil file path corrected from `{sample}_nonpareil.tsv` to `{sample}_np.tsv` (actual drakkar output name).
- Nonpareil column `LR*` corrected to `LRstar` (actual column name in drakkar's `nonpareil_stats.R` output).
- Binning metadata collection now uses correct drakkar cataloging output paths: `cataloging/quast/{sample}/report.tsv`, `cataloging/bowtie2/{sample}/{sample}.flagstat.txt`, and `cataloging/final/{sample}.tsv` for bin counts.

## [0.1.22] - 2026-05-01

### Added

- `ehio binning --output`: mirrors preprocessing output — collects assembly/binning QC metrics (QUAST, DAS_Tool, flagstat), writes `{batch}_output.tsv` to `RUN/{batch}/`, updates `EHI_ASB_ENTRY` in Airtable, transfers `cataloging/final/` via SFTP, logs ehio/drakkar versions to the batch record, and sets status to `Done`.
- `ehio quantifying --output`: same structure — collects per-sample mapping rates from samtools flagstat, writes `{batch}_output.tsv`, updates `MAG_DMB_ENTRY`, transfers `profiling/final/` via SFTP, and marks the batch `Done`.
- Generated scripts for binning and quantifying now include `ehio binning/quantifying --output` after the drakkar call, matching preprocessing.
- `ehio quantifying --input` now fetches the bins file from MAG records linked via `MAG_DMB_BATCH_LIST_MAGS` → `MAG_ENTRY_URL_FASTA`, rather than expecting a bins field on each entry.
- Config keys `EHI_ASB_BATCH_EHIO_VERSION`, `EHI_ASB_BATCH_DRAKKAR_VERSION`, `MAG_DMB_BATCH_EHIO_VERSION`, `MAG_DMB_BATCH_DRAKKAR_VERSION`, `MAG_DMB_BATCH_LIST_MAGS`, `MAG_ENTRY_URL_FASTA` added.

### Fixed

- Nonpareil file path corrected from `{sample}_nonpareil.tsv` to `{sample}_np.tsv` (actual drakkar output name).
- Nonpareil column `LR*` corrected to `LRstar` (actual column name in drakkar's `nonpareil_stats.R` output).
- Preprocessing SFTP transfer now only uploads `.bam`, `.fq.gz`, and `_output.tsv` files, excluding intermediate files such as `.hostbases`, `.hostreads`, `.metareads`, and `.metabases`.
## [0.1.21] - 2026-04-30

### Added

- `ehio stop --batch XXXXX`: sends a quit signal to the screen session named after the batch, stopping an ongoing job.
- `ehio remove --module MODULE --batch XXXXX`: deletes the output directory (`PPR/ASB/DMB/{batch}`) for the given module without touching the `RUN/{batch}` directory (scripts and logs).
## [0.1.20] - 2026-04-30

### Added

- `DRAKKAR_PPR_FRACTION` config key (default `true`): passes `--fraction` to `drakkar preprocessing` to run SingleM microbial fraction estimation.
- `DRAKKAR_PPR_NONPAREIL` config key (default `true`): passes `--nonpareil` to `drakkar preprocessing` to run Nonpareil coverage estimation.

### Fixed

- Drakkar version stored in Airtable now contains only the version number (e.g. `1.2.1`) instead of the full `drakkar 1.2.1` string.
- `.out` and `.err` log files in `RUN/{batch}/` now include a `=== YYYY-MM-DD HH:MM:SS ===` timestamp separator at the start of each attempt, making it easy to distinguish output from successive runs.
## [0.1.19] - 2026-04-30

### Added

- `ehio preprocessing --output` writes a per-sample summary TSV to `RUN/{batch}/{batch}_output.tsv`. The file includes all QC metrics plus derived `host_reads` and `host_bases` columns (`reads_post_fastp − metagenomic_reads/bases`). The TSV is copied into `preprocessing/final/` before upload so it is transferred to the remote archive alongside the `.fq.gz` and `.bam` files.
- `CLEANUP_OUTPUT_DIR` config key (default `true`): after a successful transfer, the local output directory (`PPR/{batch}`) is deleted. Only `RUN/{batch}` (containing the `.sh`, `.tsv`, `.out`, `.err`, and `_output.tsv` files) is retained.
## [0.1.18] - 2026-04-30

### Changed

- File transfer in `ehio preprocessing --output` now uses `paramiko` (already a declared pip dependency) instead of `lftp`, removing the requirement for lftp to be installed on the system.
- `launch_screen` now forwards `AIRTABLE_TOKEN` into the screen session's environment when the token was supplied via `--airtable-token`, so the generated script's `ehio preprocessing --input` and `--output` calls inherit it without requiring it to be pre-exported in the shell.
## [0.1.17] - 2026-04-30

### Added

- `SCANNING_RESUME_STATUS` config key (default `"Resume"`): behaves identically to `"Ready"` — Snakemake resumes from its checkpoint. Intended as a human-readable signal in Airtable that an error was fixed.
- `SCANNING_RERUN_STATUS` config key (default `"Rerun"`): deletes both the run directory and the output directory before relaunching, forcing a full clean restart from scratch.
## [0.1.16] - 2026-04-30

### Added

- `ehio preprocessing --output` now writes the ehio version (`EHI_PPR_BATCH_EHIO_VERSION`) and drakkar version (`EHI_PPR_BATCH_DRAKKAR_VERSION`) to the batch record alongside the `Done` status. The drakkar version is retrieved via `conda run` using the same `DRAKKAR_CONDA_ENV` path configured for the workflow.
## [0.1.15] - 2026-04-30

### Changed

- Generated preprocessing scripts now include `ehio preprocessing --output -b {batch} -l {output_dir}` after the drakkar call, so Airtable logging and SFTP transfer run automatically. If the output step fails, resetting the batch to `Ready` and re-scanning is safe — drakkar resumes via its `.snakemake` checkpoint and completes instantly, then the output step retries.
## [0.1.14] - 2026-04-30

### Fixed

- `DRAKKAR_CONDA_ENV` paths (starting with `/`, `~`, or `.`) now use `conda run -p` instead of `conda run -n`, avoiding the `CondaValueError: Invalid environment name` error when a full path is specified.
## [0.1.13] - 2026-04-30

### Added

- `EHIO_CONDA_ENV` config key: if set, generated scripts source conda and activate the named environment at startup, ensuring `ehio` (including the ERR trap's `ehio set-status`) is available in the screen session.
- `DRAKKAR_CONDA_ENV` now invokes drakkar via `conda run -n <env>` rather than activating the environment, so the ehio environment stays active for the error trap throughout the script.
## [0.1.12] - 2026-04-30

### Changed

- `scripts/release.py` now stages all tracked modifications with `git add -u` instead of only the three version-metadata files, so source code changes are always included in the release commit.
## [0.1.11] - 2026-04-30

### Added

- `DRAKKAR_CONDA_ENV` config key: if set, generated `{batch}.sh` scripts source the conda profile and activate the named environment before running `ehio` or `drakkar`, so screen sessions launched from a plain shell can still find the drakkar command.
## [0.1.10] - 2026-04-30

### Changed

- Generated `{batch}.sh` scripts now redirect stdout to `{batch}.out` and stderr to `{batch}.err` in the run directory, so failures are captured even when the screen session exits immediately.
## [0.1.9] - 2026-04-30

### Changed

- `scan_module` now always prints the resolved reference flag (or `(no reference)`) for every preprocessing batch regardless of `--verbose`, making it immediately visible in standard scan output whether the flag was resolved or not.
## [0.1.8] - 2026-04-30

### Fixed

- Reference genome resolution now handles both a "Link to another record" field (returns a `rec...` ID, fetched directly) and a plain text/formula field containing a genome code such as `G0001` (looked up by `EHI_GENOME_CODE`). Previously, only linked-record IDs were supported and a genome code silently produced no flag.
## [0.1.7] - 2026-04-30

### Changed

- Reference genome lookup now uses `EHI_GENOME` (synced table inside `EHI_BASE`) instead of the separate `GENOME_BASE` database. Config keys updated: `GENOME_BASE`/`GENOME_ENTRY`/`GENOME_ENTRY_URL_INDEXED`/`GENOME_ENTRY_URL_RAW` → `EHI_BASE`/`EHI_GENOME`/`EHI_GENOME_URL_INDEXED`/`EHI_GENOME_URL_RAW`.
- `ehio scanning --dry-run -v` now prints step-by-step diagnostics for reference genome resolution, with explicit warnings when the linked record ID, genome record, or URL fields cannot be resolved.
## [0.1.6] - 2026-04-30

### Changed

- Reference genome resolution for preprocessing is now performed at scan time (when the batch record is already fetched) and the flag is hardwired directly into the generated `{batch}.sh` script as `-x <url>` (indexed tarball) or `-g <url>` (raw fasta). Removes the `{batch}_ref.env` file, the `source` call, and `$DRAKKAR_REF_FLAG` indirection introduced in 0.1.5.

## [0.1.5] - 2026-04-30

### Added

- `ehio preprocessing --input` now accepts `--ref-flag-file PATH` and writes a bash-sourceable env file (`DRAKKAR_REF_FLAG=...`) containing the resolved drakkar reference flag: `-x <url>` if the genome entry has an indexed tarball (`GENOME_ENTRY_URL_INDEXED`), `-g <url>` if only the raw fasta is available (`GENOME_ENTRY_URL_RAW`), or an empty string if no reference is configured.
- Generated `{batch}.sh` preprocessing scripts now source the `{batch}_ref.env` file produced by `ehio preprocessing --input` and pass `$DRAKKAR_REF_FLAG` to `drakkar preprocessing`, enabling transparent use of both raw (`-g`) and indexed (`-x`) reference genomes.

### Changed

- `write_sample_file` no longer accepts a `reference` parameter or writes a `reference` column to the sample TSV. The reference genome is now communicated to drakkar via a CLI flag (`-g`/`-x`) in the generated script rather than as a per-row TSV value.
## [0.1.4] - 2026-04-30

### Added

- `ehio update` command: reinstalls ehio from GitHub using `pip install --force-reinstall git+<repo>`. Accepts `--repo` to target a fork or branch.
## [0.1.3] - 2026-04-30

### Fixed

- `write_sample_file` now writes columns `rawreads1`/`rawreads2` (was `reads1`/`reads2`) to match the drakkar input spec.
- `write_sample_file` now unwraps single-element lists in URL fields returned by the Airtable API, writing a plain URL string instead of a Python list literal.
## [0.1.2] - 2026-04-30

### Fixed

- `fetch_batch_and_entries` now uses the batch code (primary field value) in the `FIND`+`ARRAYJOIN` formula instead of the internal Airtable record ID. Airtable formulas expand linked-record fields to their primary field values, so the previous `recXXX`-based formula always returned zero entries.

## [0.1.1] - 2026-04-29

### Added

- **Preprocessing module end-to-end wiring**: `ehio preprocessing --input` fetches batch and entry records from Airtable and writes a `{batch}.tsv` input file; `ehio preprocessing --output` parses QC metrics, updates entry records in Airtable, uploads data via lftp, and sets the batch status to `Done`.
- **Metadata parsers** (`ehio.metadata`): `parse_fastp` (reads/bases pre- and post-filtering, adapter-trimmed counts), `parse_host_removal` (metagenomic reads/bases from `.metareads`/`.metabases` files), `parse_singlem_mf` (microbial fraction from SingleM TSV), `parse_nonpareil` (coverage, diversity, and model metrics — optional).
- **Bash script generation with ERR trap**: `scan_module` writes a `{batch}.sh` script into `RUN_BASE/{batch}/` containing `set -euo pipefail` and a `trap _on_error ERR` that calls `ehio set-status` on any drakkar failure.
- **`ehio set-status` CLI command**: sets the Airtable batch status to an arbitrary value; used by the ERR trap in generated scripts and callable directly.
- **HPC directory structure**: data output written to `{PPR|ASB|DMB}_OUTPUT_BASE/{batch}`, drakkar jobs launched from `RUN_BASE/{batch}` (input TSVs, logs, and `.snakemake` state kept separate from output data).
- **Batch status lifecycle**: `Ready` → `Running` (on launch) → `Done` (on successful output) / `Error` (on drakkar failure); all status strings are configurable in `config.yaml`.
- **lftp bulk transfer** (`ehio.transfer.upload_with_lftp`): mirrors a local directory to a remote SFTP path using `mirror --reverse`; remote base is `SFTP_REMOTE_BASE/{PPR|ASB|MAG|DMB}/{batch}`.
- **Dry-run generates artefacts**: `ehio scanning --dry-run` writes the `.sh` script and calls `ehio {module} --input` to generate the input TSV without launching a screen session or updating Airtable.
- **`fetch_batch_and_entries`** in `AirtableClient`: fetches a batch record and all linked entry records using a `FIND("{recID}", ARRAYJOIN({fldXXX}))` formula, compatible with `use_field_ids=True`.
- **`fetch_pending_batches`** in `AirtableClient`: queries a batch table for records matching a given status field value.
- **Test suite**: `tests/test_airtable.py`, `tests/test_drakkar.py`, `tests/test_metadata.py`, and `tests/test_scanning.py` covering formula construction, TSV generation, QC metric parsing, and bash script content.

### Changed

- `AirtableClient` now initialises `pyairtable.Api` with `use_field_ids=True`; all Airtable field names in config must use `fldXXX` IDs.
- `write_sample_file` in `ehio.drakkar` accepts `reference: str | None` (a single batch-level reference genome URL) instead of a per-sample field name.
- Input TSV is named `{batch_code}.tsv` instead of `samples.tsv` to make artefacts easier to identify per batch.
- `scan_module` writes a `.sh` script file and runs `bash {script}` in the screen session instead of building an inline one-liner command.
