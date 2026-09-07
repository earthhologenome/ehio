# ehio

**ehio** is a bridge between Airtable metadata databases and [Drakkar](https://github.com/alberdilab/drakkar) bioinformatics workflows. It handles three concerns:

1. **Input** — fetches sample metadata and file URLs from Airtable and generates the input files that Drakkar expects.
2. **Output** — transfers Drakkar result files to remote storage via SFTP and updates Airtable records with processing status.
3. **Scanning** — monitors Airtable batch tables for pending work and automatically launches Drakkar runs in named `screen` sessions.

---

## Installation

```bash
pip install -e .
```

After installation, configure the package before first use:

```bash
ehio config --edit
```

---

## Configuration

All settings live in a single YAML file bundled with the package. Open it with:

```bash
ehio config --edit   # open in terminal editor
ehio config --view   # print to stdout
```

The file is structured in three layers:

### 1. Database structure (fill in once)

```yaml
EHI_BASE:    "appXXXXXXXXXXXXXX"   # EHI Airtable base ID
MAG_BASE:    "appXXXXXXXXXXXXXX"   # MAG Airtable base ID
GENOME_BASE: "appXXXXXXXXXXXXXX"   # Genome Airtable base ID

EHI_PPR_BATCH: "tblXXXXXXXXXXXXXX"  # Preprocessing batch table
EHI_PPR_ENTRY: "tblXXXXXXXXXXXXXX"  # Preprocessing entry table
EHI_ASB_BATCH: "tblXXXXXXXXXXXXXX"  # Assembly/binning batch table
EHI_ASB_ENTRY: "tblXXXXXXXXXXXXXX"  # Assembly/binning entry table
EHI_AMR_BATCH: "tblXXXXXXXXXXXXXX"  # AMR batch table
MAG_DMB_BATCH: "tblXXXXXXXXXXXXXX"  # Dereplication/mapping batch table
MAG_DMB_ENTRY: "tblXXXXXXXXXXXXXX"  # Dereplication/mapping entry table
GENOME_ENTRY:  "tblXXXXXXXXXXXXXX"  # Genome entry table
```

### 2. Field name mappings (fill in once per module)

Each module has a set of keys that map Airtable field names to their role in ehio, for example:

```yaml
PREPROCESSING_BATCH_NAME_FIELD: "batch_id"
PREPROCESSING_READS1_FIELD: "r1_url"
PREPROCESSING_READS2_FIELD: "r2_url"
```

### 3. Runtime settings

SFTP connection, output directories, Drakkar profile, and scanning behaviour:

```yaml
SFTP_HOST: "io.erda.dk"
SFTP_USER: "user@example.com"
DRAKKAR_PROFILE: "slurm"
PREPROCESSING_OUTPUT_BASE: "/home/user/projects"
```

### Airtable token

The token is **never** stored in the config file. Provide it via:

```bash
export AIRTABLE_TOKEN="patXXXXXXXXXXXXXX"
```

or pass it per command with `--airtable-token`.

Every command validates the token against Airtable before doing any work, so an
invalid, expired or revoked token stops the command immediately:

```
Error: Airtable rejected the token (401 Unauthorized): it is invalid, expired or revoked.
```

A token that Airtable accepts can still lack access to a given base or table. In
that case the failing operation reports what was denied and why, instead of
raising a traceback:

```
Error: Airtable denied permission to update records in table tblXXXX of base appXXXX
(403 Forbidden). The token is recognised but not allowed to do this: check that it has
the data.records:read and data.records:write scopes, and that appXXXX is in the token's
list of accessible bases.
```

The token needs the `data.records:read` and `data.records:write` scopes, and both
`EHI_BASE` and `MAG_BASE` must be added to its list of accessible bases.

---

## Airtable database structure

ehio expects two Airtable bases with a shared relational pattern: each base has a **batch table** (one row per batch) and an **entry table** (one row per sample), with a linked field on the batch record pointing to its entries.

```
EHI_BASE
├── EHI_PPR_BATCH  ── linked ──▶  EHI_PPR_ENTRY   (preprocessing)
├── EHI_ASB_BATCH  ── linked ──▶  EHI_ASB_ENTRY   (assembly/binning)
└── EHI_AMR_BATCH  ── linked ──▶  EHI_ASB_ENTRY   (antimicrobial resistance)

MAG_BASE
└── MAG_DMB_BATCH  ── linked ──▶  MAG_DMB_ENTRY   (dereplication/mapping)

GENOME_BASE
└── GENOME_ENTRY                                   (reference genomes)
```

The AMR batch table is the one exception to the batch/entry pattern: it has no
entry table of its own and links straight to the assembly records of
`EHI_ASB_ENTRY`, so an assembly can be run through the AMR workflow at any time
after the binning batch that produced it is done, and the same assembly can
belong to several AMR batches.

**Batch tables** hold batch-level metadata and a status field that ehio reads (for scanning) and updates (after launching or completing a run).

**Entry tables** hold per-sample metadata including file URLs for inputs (reads, reference genomes, bin paths) and status fields that ehio updates after a run completes.

---

## Modules

### `ehio preprocessing`

Bridges the preprocessing step. Connects to `EHI_BASE` only.

| Direction | What it does |
|-----------|-------------|
| `--input` | Looks up the batch in `EHI_PPR_BATCH`, follows the linked field to `EHI_PPR_ENTRY`, and writes a Drakkar sample info TSV with columns `sample`, `reads1`, `reads2`, `reference`. Local read paths must exist and remote read URLs must be downloadable, otherwise the command fails before Drakkar starts (`--no-url-check` skips the download check). |
| `--output` | Transfers the `preprocessing/final/` directory to SFTP and marks all entry records as processed in `EHI_PPR_ENTRY`. If the batch ran against a raw reference genome, the Bowtie2 index Drakkar built is archived, uploaded to `{SFTP_REMOTE_BASE}/GEN/{genome_code}.tar.gz` and the genome record is flagged as indexed, so later batches on the same host are launched with `-x`. |

```bash
# Generate drakkar input file for batch PPR001
ehio preprocessing --input -b PPR001 -f samples.tsv

# Run drakkar (example)
drakkar preprocessing -f samples.tsv -o /projects/PPR001

# Transfer results and update Airtable
ehio preprocessing --output -b PPR001 --local-dir /projects/PPR001
```

---

### `ehio reference`

Repeats the reference-index step of `ehio preprocessing --output` on its own, for a batch whose Drakkar run is already finished. Nothing goes through Snakemake again — the index Drakkar already built is archived, uploaded to `{SFTP_REMOTE_BASE}/{SFTP_REMOTE_REFERENCE_DIR}/{genome_code}.tar.gz` and the genome is flagged as indexed.

```bash
# Finish the reference upload of a batch whose output directory is still there
ehio reference -b PPR001 -l /projects/ehi/data/PPR/PPR001

# Or point -l straight at the references directory
ehio reference -b PPR001 -l /projects/ehi/data/PPR/PPR001/data/references

# Re-upload an index that is already on ERDA
ehio reference -b PPR001 -l /projects/ehi/data/PPR/PPR001 --force
```

The command exits non-zero and says why when there is nothing to upload, so it can be used in a loop over batches. Because the index only exists inside the Drakkar output directory, `ehio preprocessing --output` now keeps that directory instead of deleting it when the reference upload fails, and prints the `ehio reference` line to retry with.

---

### `ehio binning`

Bridges the assembly and binning step. Reads from `EHI_BASE`; on output, also writes MAG metadata to `MAG_BASE`.

| Direction | What it does |
|-----------|-------------|
| `--input` | Looks up the batch in `EHI_ASB_BATCH`, follows links to `EHI_ASB_ENTRY`, and writes a Drakkar sample info TSV with preprocessed read URLs. |
| `--output` | Transfers the assemblies and the batch summary tables to `{SFTP_REMOTE_BASE}/ASB/{batch}` and the bins to `{SFTP_REMOTE_BASE}/MAG/{batch}`, both gzipped; marks entries as processed in `EHI_ASB_ENTRY`; and writes new MAG metadata to `MAG_BASE`. |

```bash
ehio binning --input -b ASB001 -f samples.tsv
drakkar cataloging -f samples.tsv -o /projects/ASB001
ehio binning --output -b ASB001 --local-dir /projects/ASB001
```

**Assembly type.** `EHI_ASB_BATCH_TYPE` on the batch record takes `Individual`, `Coassembly` or `Multicoverage`. The grouping itself always comes from the assembly codes of the entries — entries sharing a code are co-assembled by Drakkar — and the type declares what that grouping is meant to be:

| Type | Effect |
|------|--------|
| `Individual` | One assembly per entry; each sample is mapped only to its own assembly. |
| `Coassembly` | Entries sharing an assembly code are assembled together. |
| `Multicoverage` | One assembly per entry, but every sample of the batch is mapped to every assembly before binning (`drakkar cataloging -c`). Requires one assembly code per entry — `ehio binning --input` refuses the batch if any code is shared, since Drakkar rejects `--multicoverage` on co-assemblies without failing. Note that this is *n* × *n* mappings, so batch size matters. |

A batch with the field unset runs as it always did: the assembly codes decide, and no extra flag is passed.

---

### `ehio quantifying`

Bridges the dereplication and mapping step. Connects to `MAG_BASE` only.

| Direction | What it does |
|-----------|-------------|
| `--input` | Looks up the batch in `MAG_DMB_BATCH`, follows links to `MAG_DMB_ENTRY`, and writes two files: a bins path file (`bins.txt`) and a reads sample file (`samples.tsv`) for Drakkar profiling. |
| `--output` | Transfers the `profiling_genomes/final/` tables to SFTP as `{batch}_counts.tsv.gz`, `{batch}_bases.tsv.gz` and `{batch}_mag_info.tsv.gz` (the per-MAG metrics: size, completeness, contamination, contig count), and marks entries as processed in `MAG_DMB_ENTRY`. |

```bash
ehio quantifying --input -b DMB001 -f samples.tsv --bins-file bins.txt
drakkar profiling -B bins.txt -R samples.tsv -o /projects/DMB001
ehio quantifying --output -b DMB001 --local-dir /projects/DMB001
```


---

### `ehio annotating`

Bridges the taxonomic and functional annotation step of a DMB batch, which runs after `ehio quantifying --output` in the same output directory. Connects to `MAG_BASE` only.

| Direction | What it does |
|-----------|-------------|
| `--stage` | Rebuilds the dereplicated genome directory of a batch whose results are already on ERDA, so an old batch can be annotated again without being profiled again. See [Re-annotating a finished batch](#re-annotating-a-finished-batch). |
| `--input` | Lists the dereplicated genomes that still need annotating, skipping the MAGs whose `MAG_ENTRY_ANNOTATED` value already covers the batch's annotation type (`kegg` ⊂ `genes` ⊂ `all`). A MAG at `genes` in an `all` batch is written to a second file, so it gets cluster annotation only instead of being annotated from scratch. `--rerun` annotates every genome regardless. |
| `--output` | Writes taxonomy and gene metrics back to `MAG_ENTRY`, transfers the batch-level tables to `{SFTP_REMOTE_BASE}/DMB/{batch}` and the per-genome tables to `{SFTP_REMOTE_BASE}/ANN/{batch}`, and marks the batch done. |

`--output` reads two things from `annotating/`:

- `genome_taxonomy.tsv` — the GTDB-Tk summary. The classification string is split into its seven ranks, and `closest_genome_ani`, `closest_placement_ani` and `closest_genome_af` are written alongside them.
- `final/{mag}_genes.tsv` — one gene table per genome. Since drakkar 2.0.0 this is a long-form evidence table with one row per accepted hit, so a gene appears once per source and once per ranked hit within a source, always including a `prodigal` row carrying the gene call itself. ehio counts over distinct genes: `genes_number` is every gene predicted, `genes_kegg` the genes with a KEGG hit, `genes_unannotated` the genes with no KEGG, Pfam or CAZy hit, and `coding_density` the fraction of the genome the gene calls cover. The 1.x wide table, one row per gene and one column per database, is still read.

A MAG is named by its FASTA file in Airtable (`EHA00123_bin_1.fa`) and by that name with the suffix stripped in every drakkar output path (`EHA00123_bin_1_genes.tsv`), so the two are matched on the stripped id. `final/{mag}_clusters.tsv` sits beside the gene tables and holds a different table — the dbCAN gene clusters, antiSMASH regions, geNomad mobile elements and defense systems — so it is transferred but not parsed.

Once a MAG has its metrics, `MAG_ENTRY_ANNOTATED` is set to the batch's annotation type, which is what lets the next batch skip it.

#### Re-annotating a finished batch

A DMB batch that finished long ago can be sent through the current drakkar's annotation without being dereplicated or profiled again: set its status to `SCANNING_REANNOTATE_STATUS` (default `Reannotate`) and `ehio scanning` launches it. Only DMB batches are scanned for this status.

The batch's genomes are no longer on the cluster, so they are put back first. Airtable records how many MAGs came out of dereplication but not which ones, so the catalogue is read from the batch's own counts table on ERDA — `DMB/{batch}/{batch}_counts.tsv.gz`, one row per dereplicated genome — and each of those genomes is downloaded from the FASTA URL on its `MAG_ENTRY` record and staged as `{mag}.fa`, exactly as a profiling run would have left it. A genome already in the directory is kept, so a re-annotation that stopped halfway downloads nothing twice.

```bash
# Stage on its own, without launching anything
ehio annotating --stage -b DMB0157 -d /projects/ehi/data/DMB/DMB0157/profiling_genomes/drep/dereplicated_genomes

# Supply the catalogue by hand when the counts table is missing
ehio annotating --stage -b DMB0157 -d DEREP_DIR --genomes-file genomes.txt
```

A genome in the catalogue with no MAG record, no FASTA URL, or a URL that cannot be downloaded stops the batch before drakkar starts, with every problem reported at once.

What the re-annotation then runs is the **functional** annotation and nothing else: gene annotation over the staged catalogue and, for an `all` batch, cluster annotation. `ehio annotating --input` runs with `--rerun`, because after a finished batch every MAG already carries the batch's annotation level and would otherwise be skipped.

Two steps of a normal DMB batch are deliberately not repeated:

- **`drakkar profiling`**, so the counts, the mapping rates and the dereplicated MAG count already in Airtable are left untouched, and neither the reads nor `MAG_DMB_BATCH_LIST_PPR` are needed.
- **GTDB-Tk taxonomy.** A genome's classification is a property of the genome, fixed when it was binned; dereplication neither changes it nor produces a better one. The taxonomy ranks on the `MAG_ENTRY` records, and `{batch}_genome_taxonomy.tsv.gz` and the trees in `DMB/{batch}` on ERDA, are left exactly as they are — including when the output directory happens to hold a `genome_taxonomy.tsv` from an earlier run, which `--reannotate` makes `ehio annotating --output` ignore rather than write back.

So what a re-annotation rewrites is the gene metrics on every `MAG_ENTRY` record of the catalogue (`coding_density`, `genes_number`, `genes_unannotated`, `genes_kegg`, `annotated`) and `ANN/{batch}` on ERDA, whose per-genome tables the new ones supersede. The drakkar version on the batch record is **kept** and the new one appended to it (`2.4.4/2.5.0`), so the version that dereplicated and profiled the batch is not lost.

---

### `ehio amr`

Bridges the antimicrobial resistance step. Connects to `EHI_BASE` only.

| Direction | What it does |
|-----------|-------------|
| `--input` | Looks up the batch in `EHI_AMR_BATCH`, follows `EHI_AMR_BATCH_LIST_ASSEMBLIES` to the linked `EHI_ASB_ENTRY` records, downloads every assembly FASTA from `EHI_ASB_ENTRY_ASSEMBLY_URL` into the batch staging directory, and writes a drakkar amr manifest (`assembly_id`, `assembly_path`, `assembly_type`) pointing at the local copies. |
| `--output` | Parses `amr/amr_qc.tsv`, writes the per-assembly AMR counts back to the assembly records in `EHI_ASB_ENTRY`, transfers the aggregate tables to `{SFTP_REMOTE_BASE}/AMR/{batch}` and attaches them to the AMR batch record. |

```bash
ehio amr --input -b AMR001 -f assemblies.tsv -d /projects/ehi/data/AMR/AMR001/data/assemblies
drakkar amr -f assemblies.tsv -o /projects/ehi/data/AMR/AMR001
ehio amr --output -b AMR001 --local-dir /projects/ehi/data/AMR/AMR001
```

**Why the assemblies are downloaded.** `drakkar amr` inspects and hashes every
assembly before the run — it takes local files only and has no downloader of its
own, unlike `drakkar preprocessing`, which is handed read URLs. `ehio amr
--input` therefore fetches each URL into `{EHI_AMR_OUTPUT_BASE}/{batch}/data/assemblies`
and writes those paths into the manifest. A file that is already there is kept,
so a resumed batch does not download anything twice; `--redownload` forces a
fresh copy. A cell already holding a local path is used as it is. The batch stops
before drakkar starts if any assembly has no file, cannot be downloaded, or is not
named as a FASTA drakkar accepts (`.fa`, `.fna`, `.fasta`, optionally `.gz`) —
all such problems are reported together rather than one per run.

**Assembly type.** Every batch runs as `metagenome`, which is written into the
`assembly_type` column of the manifest and selects the Prodigal mode and RGI's
`--low_quality` handling. Isolate assemblies are not part of the EHI database,
so there is no Airtable field for it.

**Stats.** One row of `amr_qc.tsv` per assembly goes to `EHI_ASB_ENTRY`:
`amrfinder_hits`, `rgi_hits`, `mobility_regions`, `amr_loci`, `multi_tool_loci`,
`mobility_links` and `mobile_loci`. The two `*_without_coordinates` columns are
diagnostics of the callers rather than results, and are not written.

**Files.** The five aggregate tables (`amr_hits`, `amr_loci`, `amr_drug_classes`,
`amr_mobility`, `mobility_regions`, all `.tsv.xz`) go to `AMR/{batch}` on ERDA
batch-prefixed, together with gzipped copies of `amr_qc.tsv` and
`assembly_summary.tsv` and the `manifest.yaml` provenance record. The same five
tables are attached to the AMR batch record, together with the manifest
(`EHI_AMR_BATCH_FILE_MANIFEST`). Airtable caps an attachment upload
at 5 MB of base64 (~3.7 MB of file), so a table above that is reported and left
on ERDA only — the transfer is never the step that fails. A rerun clears the
attachment fields first, since Airtable's upload endpoint appends rather than
replaces.

---

### `ehio scanning`

Polls all four batch tables for records whose status field matches `SCANNING_TRIGGER_STATUS` (default: `ready`). For each pending batch it finds:

1. Checks whether a `screen` session named after the batch already exists — skips if so.
2. For preprocessing batches, resolves the reference genome (`-x` indexed tarball, else `-r` raw fasta) and verifies it can be downloaded; if not, the batch is marked `PROCESSING_ERROR_STATUS` and skipped instead of being launched.
3. Creates a detached `screen` session: `screen -dmS BATCH_NAME bash -c "..."`.
4. The session runs the full `ehio --input` + `drakkar` command chain.
5. Updates the batch record status to `SCANNING_LAUNCHED_STATUS` (default: `running`).

```bash
# Scan all four modules
ehio scanning

# Scan one module only
ehio scanning --module preprocessing

# Preview without launching anything
ehio scanning --dry-run
```

The command launched inside each screen session follows this pattern:

```bash
# preprocessing
mkdir -p OUTPUT_DIR &&
ehio preprocessing --input -b BATCH -f OUTPUT_DIR/samples.tsv &&
drakkar preprocessing -f OUTPUT_DIR/samples.tsv -o OUTPUT_DIR -p slurm

# binning
mkdir -p OUTPUT_DIR &&
ehio binning --input -b BATCH -f OUTPUT_DIR/samples.tsv &&
drakkar cataloging -f OUTPUT_DIR/samples.tsv -o OUTPUT_DIR -p slurm

# quantifying
mkdir -p OUTPUT_DIR &&
ehio quantifying --input -b BATCH -f OUTPUT_DIR/samples.tsv --bins-file OUTPUT_DIR/bins.txt &&
drakkar profiling -B OUTPUT_DIR/bins.txt -R OUTPUT_DIR/samples.tsv -o OUTPUT_DIR -p slurm

# amr
mkdir -p OUTPUT_DIR &&
ehio amr --input -b BATCH -f RUN_DIR/BATCH_assemblies.tsv -d OUTPUT_DIR/data/assemblies &&
drakkar amr -f RUN_DIR/BATCH_assemblies.tsv -o OUTPUT_DIR -p slurm
```

`OUTPUT_DIR` is constructed as `{MODULE_OUTPUT_BASE}/{BATCH_NAME}`.

If any step fails, the exit trap of the launch script appends a failure report to `{BATCH}.err`, sets the batch status to `PROCESSING_ERROR_STATUS` and attaches drakkar's own failure report — `OUTPUT_DIR/logging/drakkar_<run_id>.failures.tsv`, one row per failed job with its failure category — to the batch record, so the source of the error can be read straight from Airtable. The attachment field is configured per module with `EHI_PPR_BATCH_ERROR_FILES`, `EHI_ASB_BATCH_ERROR_FILES`, `MAG_DMB_BATCH_ERROR_FILES` and `EHI_AMR_BATCH_ERROR_FILES`; leave a key empty to disable uploading for that module.

Every drakkar call in the script is bracketed by a check of the run metadata drakkar writes for the run it starts (`drakkar_<run_id>.yaml`, stamped `status: success` once the workflow ends), because drakkar reports some of its own errors — a Snakemake lock, a missing input file — by printing a message and exiting 0. drakkar 2.5.0 moved that file, the failure table and the Snakemake log into `OUTPUT_DIR/logging/`; before it they sat in the output root and in `OUTPUT_DIR/log/`. ehio reads both layouts everywhere, so a batch launched under one drakkar and resumed under another is still checked, and its failure report is still found.

**Drakkar version recorded on the batch.** When a batch finishes, the drakkar version that produced it is written to the batch record (`EHI_PPR_BATCH_DRAKKAR_VERSION` and its per-module counterparts). It is read from the run metadata in the output directory, not from the installed drakkar, so the field says which version actually did the work. A batch holds one metadata file per drakkar run — quantifying alone calls drakkar four times, and a resumed batch adds more — so a batch that spans an upgrade reports every version that did part of it, oldest run first: `2.4.4/2.4.5`. On a DMB batch the field is written twice: once by `ehio quantifying --output`, and again by `ehio annotating --output` at the end of the batch, by which point the annotation runs have been recorded too. If the output directory holds no run metadata (it was cleaned up, or the drakkar that ran predates the metadata), the installed drakkar is asked instead.

---

### `ehio stop` and `ehio jobs`

A running batch is a screen session holding the drakkar workflow, plus the Slurm jobs that workflow has submitted. `ehio stop` ends both:

1. Writes a stop marker (`{RUN_BASE}/{batch}/.ehio_stopped`), so the exit trap of the dying launch script reports the stop instead of flagging the batch as an error.
2. Quits the screen session. This comes first on purpose — while the workflow is alive it resubmits any job cancelled under it (the drakkar slurm profile runs with `retries` and `keep-going`).
3. Cancels every queued or running Slurm job of the batch, re-checking the queue afterwards in case jobs were submitted while it was cancelling.
4. Sets the batch status to `SCANNING_STOPPED_STATUS` (default: `Stopped`).

The jobs of a batch are recognised in two independent ways, so orphans left by an earlier session are caught as well: by the directory they were submitted from ( `{MODULE_OUTPUT_BASE}/{batch}` or `{RUN_BASE}/{batch}`), and by the snakemake run ids that the Slurm executor uses as job names and logs as `SLURM run ID: <uuid>` in `{RUN_BASE}/{batch}/{batch}.out` — a batch can hold several of those, since quantifying calls drakkar more than once.

```bash
# Stop everything: session, jobs, status
ehio stop -m preprocessing -b PPR001

# Kill the session but let the submitted jobs finish
ehio stop -m preprocessing -b PPR001 --keep-jobs

# List what is queued or running for a batch, cancelling nothing
ehio jobs -m preprocessing -b PPR001
```

`ehio jobs` prints one row per job (job id, state, elapsed time, partition, name) and a per-state summary. It needs `squeue` on `PATH`; `ehio stop` also needs `scancel`, and if either is missing it kills the session and updates the status but reports that the jobs were left alone.

---

## Overall data flow

```
Airtable (EHI_BASE / MAG_BASE)
        │
        │  ehio <module> --input -b BATCH
        ▼
  Drakkar input files
  (samples.tsv, bins.txt,
   assemblies.tsv)
        │
        │  drakkar <cmd> -f ... -o OUTPUT_DIR
        ▼
  Drakkar output directory
        │
        │  ehio <module> --output -b BATCH
        ▼
  SFTP remote storage  +  Airtable status updated
```

`ehio scanning` automates the middle two steps by watching Airtable for batches marked `ready` and launching the sequence in a named screen session.

---

## CLI reference

```
ehio preprocessing  --input  -b BATCH [-f samples.tsv] [--no-url-check] [overrides...]
ehio preprocessing  --output -b BATCH [-l LOCAL_DIR]   [overrides...]

ehio binning        --input  -b BATCH [-f samples.tsv] [overrides...]
ehio binning        --output -b BATCH [-l LOCAL_DIR]   [overrides...]

ehio quantifying    --input  -b BATCH [-f samples.tsv] [--bins-file bins.txt] [overrides...]
ehio quantifying    --output -b BATCH [-l LOCAL_DIR]   [overrides...]

ehio annotating     --input  -b BATCH [-f annotation.tsv] [-d GENOMES_DIR] [overrides...]
ehio annotating     --output -b BATCH [-l LOCAL_DIR]   [overrides...]

ehio amr            --input  -b BATCH [-f assemblies.tsv] [-d ASSEMBLIES_DIR] [--redownload] [overrides...]
ehio amr            --output -b BATCH [-l LOCAL_DIR]   [overrides...]

ehio reference      -b BATCH [-l LOCAL_DIR] [--force] [overrides...]

ehio scanning       [--module preprocessing|binning|quantifying|amr] [--dry-run] [-v]

ehio set-status     -m MODULE -b BATCH -s STATUS [--failures-dir DIR] [--failures-since EPOCH]

ehio stop           -m MODULE -b BATCH [--keep-jobs]
ehio jobs           -m MODULE -b BATCH
ehio remove         -m MODULE -b BATCH

ehio config         --view | --edit
```

Every config file value can be overridden at the command line. Run `ehio <command> --help` for the full list of flags for each subcommand.
