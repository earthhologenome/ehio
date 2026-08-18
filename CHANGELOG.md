# Changelog

All notable changes to ehio are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- No unreleased changes yet.

## [0.6.1] - 2026-08-18

### Fixed

- `ehio stop` crashed with `ValueError: too many values to unpack (expected 4)` before doing anything, for every module. `_SET_STATUS_CFG` gained a fifth key (the error-files field) when failure reports started being attached to batch records, and `cmd_set_status` was updated to read it while `cmd_stop` kept unpacking four — so no batch could be stopped through ehio since. `cmd_stop` now ignores the extra key, and the command is covered by tests that run it for all three modules.

## [0.6.0] - 2026-08-18

### Added

- `ehio stop` now cancels the Slurm jobs of the batch, not just its screen session. Killing the session only ever killed the drakkar/snakemake driver: every job it had already submitted kept running, kept writing into the output directory, and kept occupying the queue after the batch was marked `Stopped`. The session is quit first and the jobs cancelled after — the other order makes the still-live workflow resubmit them, since the drakkar slurm profile runs with `retries: 3` and `keep-going`. The queue is re-checked after cancelling, so jobs submitted in the meantime are caught too. `--keep-jobs` restores the old behaviour for the cases where the submitted work is worth finishing.
  - Jobs are matched to a batch in two independent ways, so orphans left behind by an already-dead session are cancelled as well: by the directory they were submitted from (`{MODULE_OUTPUT_BASE}/{batch}` or `{RUN_BASE}/{batch}`, since the launch script cds into the output directory before calling drakkar), and by the snakemake run ids the Slurm executor uses as job names and logs as `SLURM run ID: <uuid>` into `{RUN_BASE}/{batch}/{batch}.out`. A batch can carry several run ids: quantifying calls drakkar more than once, and a resumed batch adds another run.
- `ehio jobs -m MODULE -b BATCH` lists the queued and running Slurm jobs of a batch — job id, state, elapsed time, partition and name, with a per-state summary — using the same matching as `ehio stop` and cancelling nothing.

### Fixed

- A stopped batch is no longer flagged as an error. `ehio stop` set the status to `Stopped` and then killed the session, whose exit trap ran `ehio set-status --status Error` on the way out and overwrote it. `ehio stop` now drops a `.ehio_stopped` marker in the run directory before killing anything and writes the status last; the exit trap of the launch script checks for that marker and reports the stop instead of building a failure report. The marker is deleted at the start of every launch, so it cannot silence the error reporting of a later run.

### Notes

- `ehio jobs` needs `squeue` on `PATH` and `ehio stop` also needs `scancel`. Where they are missing (a laptop, a non-Slurm host), `ehio stop` still kills the session and updates the status, and says that the jobs were left alone.
## [0.5.0] - 2026-08-15

### Added

- Assembly type support in the binning module. `EHI_ASB_BATCH_TYPE` (`fldNQm1mdTIKkGaVC` in `EHI_ASB_BATCH`) was declared in the config but never read; it now drives the run and takes three values: **Individual**, **Coassembly** and **Multicoverage**. Punctuation and case are ignored, so `Co-assembly` is read the same as `Coassembly`, and a batch with the field unset behaves exactly as before.
  - **Multicoverage** adds `-c/--multicoverage` to `drakkar cataloging`. Every sample of the batch is assembled on its own and then mapped against every assembly of the batch, so the binners see a coverage profile per assembly instead of a single depth. No `coverage` column is written to the sample info TSV, which is what makes drakkar map all samples to all assemblies; per-batch coverage subgroups are not exposed.
  - **Individual** and **Coassembly** are unchanged in behaviour: the grouping still comes from the assembly codes of the entries, and entries sharing a code are co-assembled.
- `ehio binning --input` checks the assembly codes of the entries against the declared batch type. A **Multicoverage** batch whose entries share assembly codes is refused before the TSV is written, because drakkar rejects `--multicoverage` on co-assembled samples by printing a message and exiting **0** without running anything — the batch would otherwise look launched, produce no output, and only fail later in `ehio binning --output`. A batch typed **Individual** with shared codes, or **Coassembly** with none, only warns: those run exactly as their codes say.

### Notes

- Multicoverage is *n* assemblies × *n* mappings. A 50-sample batch is 2500 mapping jobs where an individual batch is 50, so batch size matters much more for this type.

## [0.4.6] - 2026-08-15

### Changed

- `ehio binning --output` now uploads the **assemblies** to `Data/ASB/{batch}/`, compressed. drakkar writes them to `cataloging/megahit/{assembly}/{assembly}.fna`, which is outside `cataloging/final/` and was therefore never transferred: the only copy was deleted along with the output directory once `CLEANUP_OUTPUT_DIR` kicked in, so the assemblies behind every existing MAG are gone from ERDA. Each one is now streamed through gzip straight into the SFTP connection, so a multi-GB assembly never needs a second copy on the local disk. An assembly already on ERDA is skipped, which keeps a re-run of `--output` cheap.
  - They land as `Data/ASB/{batch}/{assembly}_contigs.fasta.gz`, the name assemblies have carried on ERDA since before ehio (`ASB/ABB0112/EHA00405_contigs.fasta.gz`), rather than drakkar's own `{assembly}.fna` — so batches uploaded from now on sit under the same naming as everything already there.
- The bin FASTAs are no longer uploaded to `Data/ASB/{batch}/`. The whole `cataloging/final/` tree was mirrored there, and that tree contains the per-assembly subdirectories holding the bins — so every bin was transferred twice: once uncompressed into `ASB/{batch}/{assembly}/{assembly}_bin_{n}.fa`, and once gzipped into `MAG/{batch}/{assembly}_bin_{n}.fa.gz`. Only the batch-level tables at the root of `cataloging/final/` (`all_bin_paths.txt`, `all_bin_metadata.csv`, the per-assembly `{assembly}.tsv` and the `{batch}_output.tsv` summary) now go to `ASB/{batch}/`; the MAG upload is unchanged. Bins already uploaded to `ASB/` by an earlier version have to be removed by hand.

## [0.4.5] - 2026-08-14

### Fixed

- The reference index upload crashed with `TypeError: TarFile.__init__() got an unexpected keyword argument 'compresslevel'` on Python 3.11, which is what the cluster environment runs. `tarfile.open` only pops `compresslevel` for stream modes (`w|gz`) from Python 3.12 onwards; on 3.11 it forwards it to `TarFile.__init__`, which does not take it. The gzip layer is now built explicitly with `gzip.GzipFile`, so the compression level applies on every supported version. This is what left an empty `Data/REF` directory in 0.4.3: the upload was reached, the remote directory created, and the archive then failed on its first write.

## [0.4.4] - 2026-08-14

### Fixed

- Reference genome indexes were uploaded to `{SFTP_REMOTE_BASE}/REF/`, but the genome tarballs on ERDA live in `Data/GEN/`. `SFTP_REMOTE_REFERENCE_DIR` now defaults to `GEN`, so an index uploaded by 0.4.3 into `Data/REF/` has to be moved by hand.
- `ehio preprocessing --output` deleted the drakkar output directory even when the reference index upload had just failed, which destroyed the only copy of the index and left no way to retry short of rebuilding it. The output directory is now kept when the upload raised an error or found more than one reference, and the command prints the `ehio reference` line to retry with. The failing exception type is included in the warning.

### Added

- New `ehio reference -b BATCH [-l DIR]`, which performs only the reference-index step of `ehio preprocessing --output` — archive the index, upload it, flag the genome as indexed — for a batch whose drakkar run has already finished. Nothing is sent through snakemake again.
  - `-l` accepts either the drakkar output directory or its `data/references` directory, so a batch whose output directory was partly cleaned up can still be finished from the index alone.
  - `--force` re-uploads an index that is already on ERDA and ignores an existing indexed URL on the genome record.
  - The command exits non-zero with the reason when there is nothing to upload (no reference on the batch, no complete index under `-l`, several references in the same directory, or the archive uploaded but the Airtable flag not written), so it can be run over a list of batches.

## [0.4.3] - 2026-08-14

### Added

- Reference genomes that are not yet indexed on ERDA are now indexed once instead of once per batch. A preprocessing batch whose genome record has no `EHI_GENOME_URL_INDEXED` is launched with `-r`, so drakkar runs `bowtie2-build` on the raw FASTA — and the index it produced was then thrown away with the output directory. `ehio preprocessing --output` now archives that index (the reference FASTA plus the six Bowtie2 files, renamed to the genome code), uploads it to `{SFTP_REMOTE_BASE}/{SFTP_REMOTE_REFERENCE_DIR}/{genome_code}.tar.gz` and flags the genome record as indexed in the master reference genome table, so every later batch on the same host is launched with `-x` and skips `bowtie2-build` entirely.
  - The archive is streamed straight into the SFTP connection, so a multi-GB index never needs a second copy on the local disk, and it is written under a `.part` name that is only renamed once the transfer completes.
  - Nothing is uploaded when the batch had no reference, when the genome is already indexed, when the archive is already on ERDA (the genome is still flagged), or when no complete Bowtie2 index is found in the output directory. A failure at any point is reported as a warning and never fails the batch, whose own results are already transferred by then.
  - New config keys: `SFTP_REMOTE_REFERENCE_DIR` (default `REF`), `EHI_GENOME_INDEX_BASE`, `EHI_GENOME_INDEX_TABLE`, `EHI_GENOME_INDEX_CODE`, `EHI_GENOME_INDEXED` and `EHI_GENOME_INDEXED_VALUE`.
- The drakkar failure report of a failed batch is now attached to its Airtable batch record, so the source of the error is visible from the record itself without opening a terminal. When drakkar stops after failures it writes `drakkar_<run_id>_failures.tsv` (one row per failed job, with the failure category and a detail line) into the root of the output directory; the exit trap of the generated launch script now passes that directory to `ehio set-status`, which uploads the newest report to the batch's error files attachment field along with setting the error status.
  - New config keys: `EHI_PPR_BATCH_ERROR_FILES` (set to the `error_files` field of `EHI_PPR_BATCH`), `EHI_ASB_BATCH_ERROR_FILES` and `MAG_DMB_BATCH_ERROR_FILES` (both empty — fill in a field id to enable the same for binning and quantifying).
  - `ehio set-status` takes a new `--failures-dir DIR` (and `--failures-since EPOCH`, which ignores reports left by an earlier launch of the same batch). Reports already attached under the same filename are not attached twice, and a missing report, an unconfigured field or a failed upload is reported as a warning — the batch is still flagged as failed in Airtable.

### Fixed

- The `.err` file of a failed batch no longer ends at drakkar's `subprocess.CalledProcessError` traceback with no trace of what actually failed. drakkar merges the snakemake output (including its errors) into its own stdout, so everything explaining a failure went to `{batch}.out` while `{batch}.err` — the file that is normally inspected — only got the traceback. The exit trap of the generated launch script now appends a failure report to `{batch}.err` before setting the Airtable status: the last 80 lines of `{batch}.out` plus the last 40 lines of each log file referenced there (the `log:` entries snakemake prints for a failing rule, i.e. the SLURM job logs), and the path of the full log.

## [0.4.2] - 2026-08-13

### Fixed

- `ehio scanning` passed the raw reference genome to drakkar as `-g`, which `drakkar preprocessing` does not accept (`ERROR: unrecognized arguments: -g …`). The flag is now `-r`. The indexed reference flag (`-x`) is unchanged.

### Added

- `ehio scanning` now verifies that the reference genome of a preprocessing batch can actually be downloaded (`EHI_GENOME_URL_INDEXED` / `EHI_GENOME_URL_RAW`) before launching. A broken link, or a local reference path that does not exist, is reported as a controlled error, the batch status is set to `PROCESSING_ERROR_STATUS` and the scan moves on to the next batch instead of launching a run that would fail inside drakkar.
- `ehio preprocessing --input` now verifies that every raw-read URL in `EHI_PPR_ENTRY` (`EHI_PPR_ENTRY_RAW_FILE_FORWARD` and `EHI_PPR_ENTRY_RAW_FILE_REVERSE`) can be downloaded, in addition to the existing check on local paths. Each unreachable URL is listed with its sample and the reason (e.g. `HTTP 404 Not Found`), and the command exits with a controlled error before drakkar starts. Pass `--no-url-check` to skip it.
  - Checks only fetch the headers (or the first byte) of each file, run concurrently, and check duplicate URLs once. `sftp://` links, which cannot be probed anonymously, are skipped.
- Every command now verifies the Airtable token (via `/meta/whoami`) before doing any work, so an invalid, expired or revoked token fails immediately with `Error: Airtable rejected the token (401 Unauthorized)…` instead of failing halfway through a run. The check costs one request per process, and a token without metadata scopes is still accepted.

### Changed

- Airtable request failures are now reported as controlled errors instead of raw `requests.exceptions.HTTPError` tracebacks. Each status gets an actionable message: 401 (bad token), 403 (missing scopes or base not in the token's access list), 404 (wrong base/table id in the config), 422 (field id or value does not match the schema), and 429 (rate limit). Airtable's own message is appended, and record-level failures still name the offending record and fields.
- `ehio scanning` reports explicitly when a batch's screen session was launched but its Airtable status could not be updated, so the record can be corrected manually.
- `fetch_record_by_id` no longer swallows permission and connection failures as "record not found" — only a genuine 404 returns `None`.

## [0.4.1] - 2026-06-19

### Added

- `ehio annotating --output` now uploads the unified `annotating/gene_annotations.tsv.xz` file and `profiling_genomes/checkm2/quality_report.tsv` to `Data/DMB/{batch}/`, alongside the existing `genome_taxonomy.tsv` and tree files. Missing files are skipped with a log message rather than failing.

## [0.4.0] - 2026-06-03

### Added

- `MAG_DMB_BATCH` now supports an **Annotation type** field (`MAG_DMB_BATCH_ANNOTATION_TYPE`) with single-select options `kegg`, `genes`, and `all`, controlling which drakkar annotation tier is run per batch.
  - `kegg` → `drakkar annotating --annotation-type kegg` (KEGG-only, lightest)
  - `genes` → `drakkar annotating --annotation-type genes` (all gene-level annotation)
  - `all` → `drakkar annotating --annotation-type function` (gene + cluster annotation)
- Annotation skip logic now uses a hierarchy (`kegg ⊂ genes ⊂ all`): a MAG is only re-annotated when the requested tier is higher than its current status. A MAG with `genes` status requested for `all` is upgraded via `--annotation-type clusters` (avoiding redundant gene re-annotation) using a separate `_annotation_clusters.tsv` file.
- `ehio annotating --output` now writes the annotation tier (`kegg`, `genes`, or `all`) to the `annotated` field in `MAG_ENTRY` instead of the old boolean `true`. Legacy `true` values are treated as equivalent to `all` for skip-logic purposes.
## [0.3.21] - 2026-05-26

### Fixed

- `ehio quantifying --output` now checks whether `MAG_DMB_ENTRY` records already exist for the batch before creating them. If any exist, creation is skipped and the remaining steps (SFTP transfer, `MAG_DMB_BATCH` metadata update) still run. This makes the command idempotent on Resume, preventing duplicate entry records when the batch previously failed after record creation but before completion.

## [0.3.20] - 2026-05-26

### Fixed

- `ehio annotating --input` no longer treats the string `"false"` as annotated. The `MAG_ENTRY_ANNOTATED` field is a single-select in Airtable, so Python's truthy check incorrectly flagged `"false"` as truthy and skipped all MAGs. The check now explicitly treats `"false"`, `"0"`, `"no"`, and empty as unannotated.

## [0.3.19] - 2026-05-26

### Fixed

- `ehio annotating --input` now scans the dereplicated genomes directory (`-d`) for `.fa` files as the authoritative source of MAGs to annotate, instead of deriving paths from the Airtable batch MAG list. Airtable is still queried to build the set of already-annotated MAG names (skipped unless `--rerun`), but the directory drives which genomes are actually included. This prevents an empty annotation file when all batch MAGs are already marked as annotated in Airtable from a previous batch.

## [0.3.18] - 2026-05-26

### Fixed

- Resume mode for the `quantifying` module now skips `ehio quantifying --output` (guarded by a `.qfy_output_done` sentinel file in the run directory) and `ehio annotating --input` (guarded by the annotation TSV being non-empty) in addition to the drakkar compute steps already guarded in 0.3.15. A Resume therefore only runs the steps that were not yet completed when the batch failed.

## [0.3.17] - 2026-05-26

### Fixed

- `parse_genome_taxonomy_tsv` now reads `closest_genome_ani` (numeric) for `gtdb_fastani` instead of `closest_genome_reference` (text genome accession), fixing a 422 `INVALID_VALUE_FOR_COLUMN` error on the numeric `fastani_ani` Airtable field. `gtdb_closest_ani` now reads `closest_placement_ani` accordingly.

## [0.3.16] - 2026-05-26

### Fixed

- `AirtableClient.update_records` now retries failed batch updates one record at a time and prints the offending record ID and field values to stderr before re-raising. This makes `INVALID_VALUE_FOR_COLUMN` errors actionable instead of opaque.

## [0.3.15] - 2026-05-25

### Fixed

- `ehio scan` (quantifying module, Resume mode) no longer re-runs `drakkar profiling` when the dereplicated genomes directory already exists, nor `drakkar annotating --annotation-type taxonomy` when `annotating/genome_taxonomy.tsv` already exists. Both steps are now skipped via bash existence guards (`[ -d ... ] ||` and `[ -f ... ] ||`), so a resume after a late-stage failure (e.g. during `ehio annotating --output`) goes straight to the steps that actually need retrying.

## [0.3.14] - 2026-05-25

### Fixed

- `ehio annotating --output` no longer raises a 422 error when `closest_genome_reference` is `N/A` in `genome_taxonomy.tsv`. The string `"N/A"` is now treated as missing (converted to `None` and excluded from the Airtable payload) rather than being forwarded as a literal string to a numeric field.

## [0.3.13] - 2026-05-22

### Fixed

- `ehio binning --output` no longer silently skips the MAG FASTA upload when killed during gzip compression. Files are now compressed and uploaded one at a time: each `.fa` is gzipped, transferred, and the temporary `.gz` is deleted before moving to the next file. This keeps disk usage minimal and makes partial uploads resumable — already-uploaded files are skipped on re-run via `skip_existing`.
- `ehio binning --output` no longer creates duplicate `MAG_ENTRY` records when the output step is re-run after a failure (e.g. via "Resume"). Before creating records, all genome names are checked against the existing `MAG_ENTRY` table in batches of 100; genomes already present are skipped.

### Added

- `AirtableClient.fetch_existing_values`: queries a table for which values from a given list already exist in a field, using batched `OR(...)` formulas to stay within Airtable URL limits.
## [0.3.11] - 2026-05-22

### Fixed

- MAG creation during `ehio binning --output` no longer sets the `annotated` single-select field at creation time. The field is left unset (empty) for new records — only the annotating step sets it to `"true"`. Sending any explicit value (`False` or `"false"`) was rejected by Airtable.

## [0.3.10] - 2026-05-22

### Fixed

- MAG creation during `ehio binning --output` no longer fails with `INVALID_VALUE_FOR_COLUMN` for the `annotated` field. The field is a single-select (options `"true"` / `"false"`), so the code now sends the string `"false"` on record creation instead of a Python boolean `False`.
- `ehio annotating --output` likewise sends the string `"true"` when marking a MAG as annotated, matching the single-select options.

## [0.3.8] - 2026-05-21

### Changed

- `ehio annotating --input` restores the `MAG_ENTRY_ANNOTATED` filter: MAGs already marked as annotated are skipped and the annotation file is left empty, which causes the `[ -s ]` guard in the generated script to skip `drakkar annotating --annotation-type function` gracefully (no error).
- When the batch is launched with `Rerun` status, `--rerun` is now also passed to `ehio annotating --input`, bypassing the annotated filter so all MAGs are force-reannotated.

## [0.3.7] - 2026-05-21

### Changed

- `ehio annotating --input` no longer filters MAGs by `MAG_ENTRY_ANNOTATED`. All MAGs linked to the batch are always written to the annotation paths file. drakkar's own Snakemake checkpointing skips previously-completed genomes, making the Airtable-side filter redundant and harmful (it caused an empty file whenever all MAGs had been annotated in a prior batch).
- Removed `MAG_DMB_BATCH_FORCE_ANNOTATE` config key and the associated Airtable checkbox field (`fldKLTR9hA4okSx2Z`) added in 0.3.6 — no longer needed since the annotated filter is gone entirely.

## [0.3.6] - 2026-05-21

### Fixed

- `ehio annotating --input` now correctly writes paths as `{annotation_dir}/{mag_name}` using `MAG_ENTRY_NAME` (the dereplicated genome FASTA filename). Previously the function was using `MAG_ENTRY_URL_FASTA` for paths, which pointed to the wrong location.
- Generated quantifying scripts now guard the `drakkar annotating --annotation-type function` call with `[ -s {annotation_file} ]`, so drakkar is skipped gracefully when all MAGs in the batch are already annotated (file is empty) instead of failing.

### Added

- New config key `MAG_DMB_BATCH_FORCE_ANNOTATE` (`fldKLTR9hA4okSx2Z`): a checkbox field on `MAG_DMB_BATCH`. When checked, `ehio annotating --input` ignores `MAG_ENTRY_ANNOTATED` and includes all MAGs in the batch in the annotation paths file, forcing a full functional re-annotation.

## [0.3.5] - 2026-05-05

### Added

- New config keys: `QUANTIFYING_RUNNING_STATUS` (default `"Quantifying"`), `ANNOTATING_TAXONOMY_STATUS` (default `"Annotating taxonomy"`), `ANNOTATING_FUNCTION_STATUS` (default `"Annotating function"`).

### Changed

- Generated quantifying scripts now `cd` to the output directory immediately after `mkdir -p`, ensuring drakkar always runs from `DMB/{batch}/` regardless of where the screen session was launched from.
- Quantifying batch status now transitions through explicit stages: `Quantifying` (script start) → `Annotating taxonomy` (after `ehio quantifying --output`) → `Annotating function` (after taxonomy annotation) → `Done` (after `ehio annotating --output`). Inline `ehio set-status` calls in the generated script drive these transitions.
- `ehio annotating --output` now marks the batch status to `Done` (`PROCESSING_DONE_STATUS`) at the end.
- All generated scripts (`cd` fix) now apply to preprocessing and binning modules too.

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
