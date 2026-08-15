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
└── EHI_ASB_BATCH  ── linked ──▶  EHI_ASB_ENTRY   (assembly/binning)

MAG_BASE
└── MAG_DMB_BATCH  ── linked ──▶  MAG_DMB_ENTRY   (dereplication/mapping)

GENOME_BASE
└── GENOME_ENTRY                                   (reference genomes)
```

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
| `--output` | Transfers the `profiling_genomes/final/` directory to SFTP and marks entries as processed in `MAG_DMB_ENTRY`. |

```bash
ehio quantifying --input -b DMB001 -f samples.tsv --bins-file bins.txt
drakkar profiling -B bins.txt -R samples.tsv -o /projects/DMB001
ehio quantifying --output -b DMB001 --local-dir /projects/DMB001
```

---

### `ehio scanning`

Polls all three batch tables for records whose status field matches `SCANNING_TRIGGER_STATUS` (default: `ready`). For each pending batch it finds:

1. Checks whether a `screen` session named after the batch already exists — skips if so.
2. For preprocessing batches, resolves the reference genome (`-x` indexed tarball, else `-r` raw fasta) and verifies it can be downloaded; if not, the batch is marked `PROCESSING_ERROR_STATUS` and skipped instead of being launched.
3. Creates a detached `screen` session: `screen -dmS BATCH_NAME bash -c "..."`.
4. The session runs the full `ehio --input` + `drakkar` command chain.
5. Updates the batch record status to `SCANNING_LAUNCHED_STATUS` (default: `running`).

```bash
# Scan all three modules
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
```

`OUTPUT_DIR` is constructed as `{MODULE_OUTPUT_BASE}/{BATCH_NAME}`.

If any step fails, the exit trap of the launch script appends a failure report to `{BATCH}.err`, sets the batch status to `PROCESSING_ERROR_STATUS` and attaches drakkar's own failure report — `OUTPUT_DIR/drakkar_<run_id>_failures.tsv`, one row per failed job with its failure category — to the batch record, so the source of the error can be read straight from Airtable. The attachment field is configured per module with `EHI_PPR_BATCH_ERROR_FILES`, `EHI_ASB_BATCH_ERROR_FILES` and `MAG_DMB_BATCH_ERROR_FILES`; leave a key empty to disable uploading for that module.

---

## Overall data flow

```
Airtable (EHI_BASE / MAG_BASE)
        │
        │  ehio <module> --input -b BATCH
        ▼
  Drakkar input files
  (samples.tsv, bins.txt)
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

ehio reference      -b BATCH [-l LOCAL_DIR] [--force] [overrides...]

ehio scanning       [--module preprocessing|binning|quantifying] [--dry-run] [-v]

ehio set-status     -m MODULE -b BATCH -s STATUS [--failures-dir DIR] [--failures-since EPOCH]

ehio config         --view | --edit
```

Every config file value can be overridden at the command line. Run `ehio <command> --help` for the full list of flags for each subcommand.
