# Changelog

All notable changes to ehio are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- No unreleased changes yet.

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
