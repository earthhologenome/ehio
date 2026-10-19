# ehio

**ehio** is a bridge between the EHI's metadata databases and [Drakkar](https://github.com/alberdilab/drakkar) bioinformatics workflows. It handles four concerns:

1. **Input** — fetches sample metadata and file URLs and generates the input files that Drakkar expects.
2. **Output** — transfers Drakkar result files to remote storage via SFTP and writes back the processing status and metrics.
3. **Scanning** — monitors the batch tables for pending work and automatically launches Drakkar runs in named `screen` sessions.
4. **Archiving** — deposits the raw reads of the hologenomes in the European Nucleotide Archive (see [`ehio ena`](#ehio-ena)).

ehi-core, the EHI's own database, is the only backend: batches, entries,
reference genomes and MAGs are read from it and results are written back to it
(see [ehi-core](#ehi-core)). Earlier versions also supported Airtable; that
support has been removed.

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

The file holds the ehi-core address, the SFTP connection, the output directories, the scanning statuses, the Drakkar profile and the ENA settings:

```yaml
EHI_CORE_URL: "https://<ehi-core-host>"
EHI_CORE_WAIT_MINUTES: "120"

SFTP_HOST: "io.erda.dk"
SFTP_USER: "user@example.com"
SFTP_REMOTE_BASE: "/EarthHologenomeInitiative/Data"

DRAKKAR_PROFILE: "slurm"
EHI_PPR_OUTPUT_BASE: "/projects/ehi/data/PPR"
```

Batch tables, entry tables and their fields are not configured: ehio talks to
ehi-core's pipeline routes, which know them.

### ehi-core token

ehio reaches ehi-core at `EHI_CORE_URL` with the pipeline token (the routes under
`/api/pipeline/`). Codeome supplies it as `EHI_CORE_PIPELINE`. The token is
**never** stored in the config. Outside Codeome, export it in the environment:

```bash
export EHI_CORE_PIPELINE="..."
```

or pass it per command with `--core-token`, which takes priority. Existing
environments can continue using `EHI_CORE_TOKEN` as a fallback. `ehio scanning`
hands the selected token on to the batches it launches.

Every command checks the token against ehi-core before doing any work, so a
missing, invalid or revoked token stops the command immediately. Without
`EHI_CORE_URL` or a token the command stops and says which is missing.

---

## ehi-core

ehi-core is the EHI's own database for the bioinformatic pipeline, and the only
one ehio reads from and writes to. A batch is created in ehi-core, set to Ready,
and `ehio scanning` launches it (see [`ehio scanning`](#ehio-scanning)). Each
batch, its entries (samples, assemblies, MAGs) and its status live there.

What each step reads from the core:

| Step | Read from the core |
|---|---|
| `preprocessing --input` | each library's raw read URLs, through the preprocessings of the batch |
| `binning --input` | one row per sample of each assembly, with the reads preprocessing produced |
| `quantifying --input` | the batch's MAGs, and the preprocessed samples queued in it |
| `amr --input` | the assemblies the batch runs over, with their ERDA URLs |
| `annotating` | the batch's MAGs and the annotation depth it asks for |
| every `--output` | the same entries, to write the metrics and the file URLs back to |

The core answers with the batch's own row too — the assembly type, the ANI
threshold, the annotation type, the reference genome — because that is what the
run is launched with.

What each step writes to it:

| Step | Written to the core |
|---|---|
| any status change (scanning, `set-status`, `stop`, done) | the batch's status |
| `preprocessing --output` | QC and Nonpareil metrics, the ERDA URLs of the reads and host BAM, versions |
| `binning --output` | assembly metrics, the ERDA URL of each assembly, the new MAGs (the core gives them their EHM codes, with the ERDA URL of their FASTA), versions |
| `amr --output` | AMR metrics, the ERDA URLs of the gene calls and the hits and loci tables, versions |
| `quantifying --output` | the mappings, which MAGs dereplication kept, versions |
| `annotating --output` | taxonomy, GTDB and gene metrics, and how far each MAG was annotated |
| `ena` | each hologenome's ENA study, sample, experiment and run accessions, a new ENA sample's accession on its sample, and the submission's status and log |

**Reference genomes.** A preprocessing batch names its reference genome by code.
ehio resolves it with `GET /reference_genomes/{code}` on the core, which gives
the raw FASTA URL and, when the genome has been indexed, the URL of its Bowtie2
index. After a run that built an index, `preprocessing --output` and
`ehio reference` upload it to ERDA and register it with
`PATCH /reference_genomes/{code}` (`url_indexed`, `indexed`), so later batches on
the same host are launched with `-x`.

**Files stay on ERDA.** Result files go to ERDA over SFTP and the core keeps
their URLs. Failure reports and AMR result tables are not attached to batch
records any more (see [`ehio scanning`](#ehio-scanning) and [`ehio amr`](#ehio-amr)).

**When the core can't be reached,** the command stops: a write that did not reach
the core is reported and fails the batch, since the core is the only record.

ehio jobs write to the core one at a time. Each write takes the core first,
reports its progress as it goes (the core's editor shows staff what is being
written), and lets go at the end. A job that finishes while another is writing
says so, waits for its turn, and then writes:

```
ehi-core is busy: ehio is already writing MAGs of batch 'DMB0042' to the core (1,250 of 3,214 records, since 12:03 UTC); wait for it to finish. Waiting for it before writing status of batch 'PRB0003' (up to 120 min)...
ehi-core is free: writing status of batch 'PRB0003'.
```

`EHI_CORE_WAIT_MINUTES` (120 by default) is how long a job waits at the most.
After that the write counts as a failed one. A job that dies mid-write holds the
core for 10 minutes at most. A core too old to take turns is written to as
before.

---

## Modules

### `ehio preprocessing`

Bridges the preprocessing step.

| Direction | What it does |
|-----------|-------------|
| `--input` | Reads the batch and its entries from ehi-core, and writes a Drakkar sample info TSV with columns `sample`, `reads1`, `reads2`, `reference`. Local read paths must exist and remote read URLs must be downloadable, otherwise the command fails before Drakkar starts (`--no-url-check` skips the download check). |
| `--output` | Transfers the `preprocessing/final/` directory to SFTP and writes the metrics and file URLs of every entry to ehi-core. If the batch ran against a raw reference genome, the Bowtie2 index Drakkar built is archived, uploaded to `{SFTP_REMOTE_BASE}/GEN/{genome_code}.tar.gz` and the genome is flagged as indexed in ehi-core (`PATCH /reference_genomes/{code}`), so later batches on the same host are launched with `-x`. |

```bash
# Generate drakkar input file for batch PPR001
ehio preprocessing --input -b PPR001 -f samples.tsv

# Run drakkar (example)
drakkar preprocessing -f samples.tsv -o /projects/PPR001

# Transfer results and update ehi-core
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

Bridges the assembly and binning step.

| Direction | What it does |
|-----------|-------------|
| `--input` | Reads the batch and its entries from ehi-core, and writes a Drakkar sample info TSV with preprocessed read URLs. |
| `--output` | Transfers the assemblies and the batch summary tables to `{SFTP_REMOTE_BASE}/ASB/{batch}` and the bins to `{SFTP_REMOTE_BASE}/MAG/{batch}`, both gzipped; writes the assembly metrics and URLs to ehi-core; and creates the new MAGs there. |

```bash
ehio binning --input -b ASB001 -f samples.tsv
drakkar cataloging -f samples.tsv -o /projects/ASB001
ehio binning --output -b ASB001 --local-dir /projects/ASB001
```

**Assembly type.** The batch's assembly type in ehi-core takes `Individual`, `Coassembly` or `Multicoverage`. The grouping itself always comes from the assembly codes of the entries — entries sharing a code are co-assembled by Drakkar — and the type declares what that grouping is meant to be:

| Type | Effect |
|------|--------|
| `Individual` | One assembly per entry; each sample is mapped only to its own assembly. |
| `Coassembly` | Entries sharing an assembly code are assembled together. |
| `Multicoverage` | One assembly per entry, but every sample of the batch is mapped to every assembly before binning (`drakkar cataloging -c`). Requires one assembly code per entry — `ehio binning --input` refuses the batch if any code is shared, since Drakkar rejects `--multicoverage` on co-assemblies without failing. Note that this is *n* × *n* mappings, so batch size matters. |

A batch with the field unset runs as it always did: the assembly codes decide, and no extra flag is passed.

---

### `ehio quantifying`

Bridges the dereplication and mapping step.

| Direction | What it does |
|-----------|-------------|
| `--input` | Reads the batch's MAGs and samples from ehi-core, and writes two files: a bins path file (`bins.txt`) and a reads sample file (`samples.tsv`) for Drakkar profiling. |
| `--output` | Transfers the `profiling_genomes/final/` tables to SFTP as `{batch}_counts.tsv.gz`, `{batch}_bases.tsv.gz` and `{batch}_mag_info.tsv.gz` (the per-MAG metrics: size, completeness, contamination, contig count), and writes the mappings to ehi-core. With `--skip-derep` (a batch profiled without dereplicating) no MAG is recorded as kept by dereplication. |
| `--derep-output` | For a batch that dereplicates without profiling: reads which MAGs `drakkar dereplicating` kept from dRep's `Wdb.csv`, marks them in ehi-core and writes their number and the drakkar version on the batch. |

`--input --no-reads` writes the MAG and quality files only, without reading the batch's samples, which is all `drakkar dereplicating` needs.

```bash
ehio quantifying --input -b DMB001 -f samples.tsv --bins-file bins.txt
drakkar profiling -B bins.txt -R samples.tsv -o /projects/DMB001
ehio quantifying --output -b DMB001 --local-dir /projects/DMB001
```


---

### `ehio annotating`

Bridges the taxonomic and functional annotation step of a DMB batch, which runs after `ehio quantifying --output` in the same output directory.

| Direction | What it does |
|-----------|-------------|
| `--stage` | Rebuilds the dereplicated genome directory of a batch whose results are already on ERDA, so an old batch can be annotated again without being profiled again; with `--all-mags`, stages every MAG of the batch instead. See [What a DMB batch does: Tasks](#what-a-dmb-batch-does-tasks). |
| `--input` | Lists the dereplicated genomes that still need annotating, skipping the MAGs whose annotation depth in ehi-core already covers the batch's annotation type (`kegg` ⊂ `genes` ⊂ `all`). A MAG at `genes` in an `all` batch is written to a second file, so it gets cluster annotation only instead of being annotated from scratch. `--rerun` annotates every genome regardless. |
| `--output` | Writes taxonomy and gene metrics back to the MAGs in ehi-core, transfers the batch-level tables to `{SFTP_REMOTE_BASE}/DMB/{batch}` and the per-genome tables to `{SFTP_REMOTE_BASE}/ANN/{batch}`, and marks the batch done. |

`--output` reads two things from `annotating/`:

- `genome_taxonomy.tsv` — the GTDB-Tk summary. The classification string is split into its seven ranks, and `closest_genome_ani`, `closest_placement_ani` and `closest_genome_af` are written alongside them. Every MAG classified also gets the GTDB-Tk version and GTDB release of the run (`gtdbtk_version`, `gtdb_release`, e.g. `2.7.2` and `R232`), read from `gtdbtk/gtdbtk.json`, or from the first lines of `gtdbtk/gtdbtk.log` when the JSON is missing.
- `final/{mag}_genes.tsv` — one gene table per genome. Since drakkar 2.0.0 this is a long-form evidence table with one row per accepted hit, so a gene appears once per source and once per ranked hit within a source, always including a `prodigal` row carrying the gene call itself. ehio counts over distinct genes: `genes_number` is every gene predicted, `genes_kegg` the genes with a KEGG hit, `genes_unannotated` the genes with no KEGG, Pfam or CAZy hit, and `coding_density` the fraction of the genome the gene calls cover. The 1.x wide table, one row per gene and one column per database, is still read.

A MAG is named by its FASTA file in ehi-core (`EHA00123_bin_1.fa`) and by that name with the suffix stripped in every drakkar output path (`EHA00123_bin_1_genes.tsv`), so the two are matched on the stripped id. `final/{mag}_clusters.tsv` sits beside the gene tables and holds a different table — the dbCAN gene clusters, antiSMASH regions, geNomad mobile elements and defense systems — so it is transferred but not parsed.

Once a MAG has its metrics, its annotation depth in ehi-core is set to the batch's annotation type, which is what lets the next batch skip it.

#### What a DMB batch does: Tasks

A DMB batch's **Tasks** in ehi-core say which of its four steps it runs, in this order:

| Task | What it does | What it writes |
|---|---|---|
| `Dereplicate` | dRep on the batch's MAGs, keeping one representative per cluster | which MAGs were kept, and how many |
| `Profile` | the samples mapped against the MAGs; the only task that reads the samples' reads | the counts and the mapping rates |
| `Taxonomy` | `drakkar annotating --annotation-type taxonomy` (GTDB-Tk, run by drakkar) | the taxonomy ranks, GTDB-Tk version and GTDB release of each MAG, `{batch}_genome_taxonomy.tsv.gz` and the trees in `DMB/{batch}` |
| `Function` | `drakkar annotating` at the batch's **Annotation** level (`kegg`, `genes` or `all`), and for `all` the clusters | the gene metrics of each MAG and `ANN/{batch}` |

**`Dereplicate` decides which MAGs the other tasks work on**: the representatives dRep keeps when it is ticked, every MAG of the batch when it is not. `drakkar annotating` itself never dereplicates and never reads reads: it classifies or annotates exactly the genomes ehio hands it.

| Tasks | What ehio runs | Genomes | Reads |
|---|---|---|---|
| `Dereplicate` | `drakkar dereplicating`, `ehio quantifying --derep-output` | — | no |
| `Dereplicate` + `Taxonomy` / `Function` | `drakkar dereplicating`, then `drakkar annotating` on `dereplicating/final` | representatives | no |
| `Taxonomy` / `Function` | every MAG staged into `{output}/batch_genomes`, then `drakkar annotating` | every MAG | no |
| `Dereplicate` + `Profile` (+ …) | `drakkar profiling`, which dereplicates and maps in one run | representatives | yes |
| `Profile` (+ …) | `drakkar profiling --skip-derep` | every MAG | yes |
| none | all four, as every DMB batch did before the column existed | representatives | yes |

- A batch that dereplicates or profiles skips a MAG another batch has already annotated far enough (see `--input` above). A batch that does neither annotates every genome again, and its new results replace the old ones on the records and on ERDA.
- A batch that does not profile leaves its counts and mapping rates untouched, and the drakkar version on the batch record is **kept**, with the new one appended (`2.4.4/2.6.1`).
- A batch with no `Taxonomy` or `Function` is marked Done once its dereplication or profiling output is written.
- `Profile` without `Dereplicate` needs drakkar's `--skip-derep` (drakkar 2.6.7 or later), and only genomes profiling: drakkar refuses it for pangenomes.

`ehio annotating --output --tasks` writes back the results of the tasks it names and nothing else. A `genome_taxonomy.tsv` left in the output directory by an earlier run, for example, is not written over the taxonomy of a batch without `Taxonomy`.

The `SCANNING_REANNOTATE_STATUS` status (default `Reannotate`) predates Tasks and still works: it runs `Function` alone on the dereplicated catalogue of a batch profiled long ago, whatever the batch's Tasks say. `--reannotate` is the same as `--tasks function`, for scripts written by earlier versions.

#### Staging the genomes of a batch

A batch that neither dereplicates nor profiles has no genomes on the cluster, so they are downloaded first, each from the FASTA URL of its MAG, and staged as `{mag}.fa`. A genome already in the directory is kept, so a batch that stopped halfway downloads nothing twice.

- `--all-mags` stages every MAG of the batch, read from the batch's MAG list alone. This is what `Taxonomy` and `Function` use.
- Without it, the dereplicated catalogue is staged, which is what Reannotate uses. It is read from the batch's counts table on ERDA (`DMB/{batch}/{batch}_counts.tsv.gz`, one row per dereplicated genome), or, for a batch dereplicated without profiling, from the MAGs ehi-core records as kept.

```bash
# Every MAG of the batch
ehio annotating --stage -b DMB0157 -d /projects/ehi/data/DMB/DMB0157/batch_genomes --all-mags

# The dereplicated catalogue
ehio annotating --stage -b DMB0157 -d /projects/ehi/data/DMB/DMB0157/profiling_genomes/drep/dereplicated_genomes

# Supply the catalogue by hand when the counts table is missing
ehio annotating --stage -b DMB0157 -d DEREP_DIR --genomes-file genomes.txt
```

A genome in the catalogue with no MAG record, no FASTA URL, or a URL that cannot be downloaded stops the batch before drakkar starts, with every problem reported at once.

---

### `ehio amr`

Bridges the antimicrobial resistance step.

| Direction | What it does |
|-----------|-------------|
| `--input` | Reads the assemblies of the batch from ehi-core, downloads every assembly FASTA from its ERDA URL into the batch staging directory, and writes a drakkar amr manifest (`assembly_id`, `assembly_path`, `assembly_type`) pointing at the local copies. |
| `--output` | Parses `amr/amr_qc.tsv`, writes the per-assembly AMR counts back to the assemblies in ehi-core, transfers the aggregate tables to `{SFTP_REMOTE_BASE}/AMR/{batch}`, and transfers the prodigal gene calls to `{SFTP_REMOTE_BASE}/AMR/{batch}/genes`. |

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
so the batch does not carry a field for it.

**Stats.** One row of `amr_qc.tsv` per assembly goes to the assembly in ehi-core:
`amrfinder_hits`, `rgi_hits`, `mobility_regions`, `amr_loci`, `multi_tool_loci`,
`mobility_links` and `mobile_loci`. The two `*_without_coordinates` columns are
diagnostics of the callers rather than results, and are not written.

**Files.** The five aggregate tables (`amr_hits`, `amr_loci`, `amr_drug_classes`,
`amr_mobility`, `mobility_regions`, all `.tsv.xz`) go to `AMR/{batch}` on ERDA
batch-prefixed, together with gzipped copies of `amr_qc.tsv` and
`assembly_summary.tsv` and the `manifest.yaml` provenance record. The core keeps
their ERDA URLs. The tables are no longer attached to the AMR batch record: they
are on ERDA only.

**Gene calls.** `drakkar amr` calls genes with prodigal before AMRFinderPlus runs,
and keeps one pair of files per assembly in `amr/raw/prodigal/`. Both are gzipped
straight into the SFTP connection, so no temporary `.gz` is written to disk:

| Local | ERDA |
|---|---|
| `amr/raw/prodigal/{assembly}.faa` (proteins) | `AMR/{batch}/genes/{assembly}.faa.gz` |
| `amr/raw/prodigal/{assembly}.ffn` (nucleotides) | `AMR/{batch}/genes/{assembly}.ffn.gz` |

The `.gff` and `.amrfinder.gff` files in the same folder are AMRFinderPlus
intermediates and are not sent. Files already on ERDA are skipped, so a resumed
batch only sends what is missing. `--rerun` deletes `AMR/{batch}`, including
`genes/`, before anything is sent. A run whose output has no prodigal folder
still finishes.

---

### `ehio ena`

Deposits the raw reads of the hologenomes of an **ENA submission** (`EHS…`) in
the [European Nucleotide Archive](https://www.ebi.ac.uk/ena). ENA submissions
live in ehi-core alone — they replace the former Submissions table — and everything ENA is told comes from the core, so this command
needs the core.
Staff create a submission in the core's editor, name the ENA study it goes
under (`PRJEB…`), paste the hologenomes into it and set it Ready.

For each hologenome of the submission that holds no run accession yet, ehio:

1. downloads its raw reads from the URLs the hologenome holds in the core,
   taking their MD5 as they arrive;
2. registers at ENA, with [ena-upload-cli](https://github.com/usegalaxy-eu/ena-upload-cli)
   (installed with ehio), its **sample** if ENA does not hold it yet, its
   **experiment** and its **run**, uploading the reads;
3. writes the four accessions (study, sample, experiment, run) onto the
   hologenome in ehi-core, and the ENA sample's onto its sample, as soon as ENA
   holds them, and deletes the reads.

When every hologenome is deposited the submission is set to Done; otherwise to
Error. Either way its **Log** in the core says what was deposited and, for each
hologenome that was not, why — so a sample the laboratory has not fully described is
fixed in the laboratory's Samples table, the core's copy synced, and the
submission set Ready again. Hologenomes already holding a
run accession are skipped, so a submission can be launched as often as needed.

```bash
# Check a submission and write the tables it would send, without sending anything
ehio ena -b EHS0001 --dry-run

# Try it against ENA's test server, which discards everything within a day
ehio ena -b EHS0001 --test

# Deposit it
ehio ena -b EHS0001
```

**Where ENA's metadata comes from.**

| ENA object | Read from |
|---|---|
| Study | the submission's `study_accession` in ehi-core |
| Sample | the sample the hologenome links to in ehi-core — the core's copy of the laboratory's Samples table, which ehi-core refreshes with its own `python -m airtable_import sync`. Columns are mapped in `ENA_SAMPLE_FIELDS` (checklist column → sample column) and sent under checklist `ENA_CHECKLIST` (ERC000013, host associated). `project name` is the submission's study, and no scientific name is sent: ena-upload-cli looks it up from the taxon id (a metagenome, such as feces metagenome), since the laboratory's table names the host there. |
| Experiment | the hologenome in ehi-core (library name, library source or data type, platform, instrument model), plus `ENA_LIBRARY_STRATEGY`, `ENA_LIBRARY_SELECTION`, `ENA_LIBRARY_LAYOUT` and `ENA_INSERT_SIZE` (WGS, RANDOM, PAIRED, 400: every EHI library so far) |
| Run | the hologenome's raw reads, as `{EHI}_raw_1.fq.gz` and `{EHI}_raw_2.fq.gz` |

Everything is checked before any read is downloaded: a hologenome without a
sample, raw reads, a platform or an instrument model, or whose sample lacks one
of the checklist's mandatory fields, is reported and left out, and the others
go ahead.

**One ENA sample per lab sample.** A lab sample is registered once, under its
sample code as alias. A later library of the same sample — in the same
submission or a later one — is added to the ENA sample it already has: the core
answers each hologenome with its sample's own ENA accession (copied from the
laboratory's table, where the pipeline before ehio wrote it) and those any
hologenome of the sample holds. ena-upload-cli refers to an existing
sample or study by the alias it was registered under, and the earlier
pipelines used different aliases, so ehio looks the alias up at ENA from the
accession (Webin's report service, or ENA's public browser).

**Resuming safely.** Experiments and runs are registered as `ena_{EHI}`, the
aliases the earlier pipeline used. If ENA answers that an object ehio is adding
already exists — a run that stopped after ENA took the hologenome but before
the core was written — ehio takes the accession ENA names and submits only the
rest. The accessions of every hologenome deposited are also kept in
`{ENA_OUTPUT_BASE}/{batch}/{batch}_accessions.tsv`.

**Webin account.** Never stored in the config: export `ENA_USERNAME` and
`ENA_PASSWORD`, or point `ENA_SECRET_FILE` at a YAML file holding `username:`
and `password:` (the cluster's `/projects/ehi/data/.secret.yml`).

**Files.** Each hologenome gets a folder in `{ENA_OUTPUT_BASE}/{batch}` holding
its tables, ENA's receipt and ena-upload-cli's output (`ena-upload-cli.log`).
Its reads are deleted once ENA holds them, unless `--keep-reads` or
`ENA_KEEP_READS`. `ENA_PARALLEL_UPLOADS` lab samples are deposited side by side;
the hologenomes of one lab sample go in turn, so the first registers the sample.

`--test` sends everything to ENA's test server and writes no accession to the
core: only the submission's Log, marked as a test.

---

### `ehio scanning`

Polls the four batch tables and the ENA submissions in ehi-core for batches whose status matches `SCANNING_TRIGGER_STATUS` (default: `ready`). For each pending batch it finds:

1. Checks whether a `screen` session named after the batch already exists — skips if so.
2. For preprocessing batches, resolves the reference genome (`-x` indexed tarball, else `-r` raw fasta) and verifies it can be downloaded; if not, the batch is marked `PROCESSING_ERROR_STATUS` and skipped instead of being launched.
3. Creates a detached `screen` session: `screen -dmS BATCH_NAME bash -c "..."`.
4. The session runs the full `ehio --input` + `drakkar` command chain.
5. Sets the batch status to `SCANNING_LAUNCHED_STATUS` (default: `running`) in ehi-core.

The core is read through `GET /api/pipeline/{table}/pending`, which takes one `status` parameter per status to look for and matches it however it is capitalised.

```bash
# Scan every module
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

# ena (no drakkar: ehio deposits the reads itself)
mkdir -p OUTPUT_DIR &&
ehio ena -b BATCH -d OUTPUT_DIR
```

`OUTPUT_DIR` is constructed as `{MODULE_OUTPUT_BASE}/{BATCH_NAME}`.

If any step fails, the exit trap of the launch script appends a failure report to `{BATCH}.err` and sets the batch status to `PROCESSING_ERROR_STATUS` in ehi-core. Drakkar's own failure report is no longer uploaded to the batch record: it stays in `OUTPUT_DIR/logging/drakkar_<run_id>.failures.tsv` (one row per failed job with its failure category).

Every drakkar call in the script is bracketed by a check of the run metadata drakkar writes for the run it starts (`drakkar_<run_id>.yaml`, stamped `status: success` once the workflow ends), because drakkar reports some of its own errors — a Snakemake lock, a missing input file — by printing a message and exiting 0. drakkar 2.5.0 moved that file, the failure table and the Snakemake log into `OUTPUT_DIR/logging/`; before it they sat in the output root and in `OUTPUT_DIR/log/`. ehio reads both layouts everywhere, so a batch launched under one drakkar and resumed under another is still checked, and its failure report is still found.

**Drakkar version recorded on the batch.** When a batch finishes, the drakkar version that produced it is written to the batch record in ehi-core. It is read from the run metadata in the output directory, not from the installed drakkar, so the field says which version actually did the work. A batch holds one metadata file per drakkar run — quantifying alone calls drakkar four times, and a resumed batch adds more — so a batch that spans an upgrade reports every version that did part of it, oldest run first: `2.4.4/2.4.5`. On a DMB batch the field is written twice: once by `ehio quantifying --output`, and again by `ehio annotating --output` at the end of the batch, by which point the annotation runs have been recorded too. If the output directory holds no run metadata (it was cleaned up, or the drakkar that ran predates the metadata), the installed drakkar is asked instead.

---

### `ehio stop` and `ehio jobs`

A running batch is a screen session holding the drakkar workflow, plus the Slurm jobs that workflow has submitted. `ehio stop` ends both:

1. Writes a stop marker (`{RUN_BASE}/{batch}/.ehio_stopped`), so the exit trap of the dying launch script reports the stop instead of flagging the batch as an error.
2. Quits the screen session. This comes first on purpose — while the workflow is alive it resubmits any job cancelled under it (the drakkar slurm profile runs with `retries` and `keep-going`).
3. Cancels every queued or running Slurm job of the batch, re-checking the queue afterwards in case jobs were submitted while it was cancelling.
4. Sets the batch status to `SCANNING_STOPPED_STATUS` (default: `Stopped`) in ehi-core.

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
ehi-core
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
  SFTP remote storage  +  ehi-core status and metrics updated
```

`ehio scanning` automates the middle two steps by watching ehi-core for batches marked `ready` and launching the sequence in a named screen session.

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

ehio ena            -b BATCH [-d WORK_DIR] [--test] [--dry-run] [--keep-reads] [--parallel N]

ehio reference      -b BATCH [-l LOCAL_DIR] [--force] [overrides...]

ehio scanning       [--module preprocessing|binning|quantifying|amr|ena] [--dry-run] [-v]

ehio set-status     -m MODULE -b BATCH -s STATUS [--failures-dir DIR] [--failures-since EPOCH]

ehio stop           -m MODULE -b BATCH [--keep-jobs]
ehio jobs           -m MODULE -b BATCH
ehio remove         -m MODULE -b BATCH

ehio config         --view | --edit
```

Every config file value can be overridden at the command line. Run `ehio <command> --help` for the full list of flags for each subcommand.
