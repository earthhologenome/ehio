# Changelog

All notable changes to ehio are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- No unreleased changes yet.

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
