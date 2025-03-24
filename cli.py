import argparse
import os
import sys
import subprocess
import yaml
import re
import json
import pandas as pd
from pathlib import Path
#from ehio.utils import *

## setup paths (hardcoded)
EHIO_PATH = "/projects/ehi/data/0_Environments/ehio"
EHI_CODE_DIR = "/projects/ehi/data/0_Code/EHI_bioinformatics_1.1/workflow"

    
    # Define workflow launching functions
    ## N.b. using EHI nomenclature:
    ## ppr = preprocessing 
    ## asb = cataloging
    ## dmb = profiling

    #############################################
    ##########   Preprocessing   ################
    #############################################
def run_fetch_input_ppr(batch):

    """ Fetching EHI PPR input """

    Path(f"/projects/ehi/data/RUN/{batch}").mkdir(exist_ok=True)
    Path(f"/projects/ehi/data/RUN/{batch}/logs").mkdir(exist_ok=True)

    os.chdir(f"/projects/ehi/data/RUN/{batch}")

    subprocess.run([
        "python", f"{EHI_CODE_DIR}/airtable/get_preprocessing_input.py", 
        f"--prb={batch}"
    ]) 
    ## output is 'prb_input.tsv', line separated file with EHI numbers of input samples

    subprocess.run([
        "python", f"{EHI_CODE_DIR}/airtable/get_host_genome_id.py", 
        f"--prb={batch}"
    ]) 
    ## output is 'host_genome.tsv', containing a single line with EHI host genome code (e.g. G0001)

    with open(f"/projects/ehi/data/RUN/{batch}/host_genome.tsv", "r") as f:
        HOSTGENOME = f.readline().strip()

    subprocess.run([
        "python", f"{EHI_CODE_DIR}/airtable/get_host_genome_url.py", 
        f"--code={HOSTGENOME}"
    ]) 
    ## output is 'host_genome_url.tsv', containing a single line with the URL to the host genome fasta

def run_preprocessing(batch):

    ## declare variables for config
    CODEDIR = "/projects/ehi/data/0_Environments/ehio/workflow"
    WORKDIR = f"/projects/ehi/data/PPR/{batch}"
    LOGDIR = f"/projects/ehi/data/RUN/{batch}/logs"
    with open(f"/projects/ehi/data/RUN/{batch}/host_genome.tsv", "r") as f:
        HOSTGENOME = f.readline().strip()
    with open(f"/projects/ehi/data/RUN/{batch}/host_genome_url.tsv", "r") as f:
        HOST_GENOME_URL = f.readline().strip()

    """ Run the preprocessing workflow """

    snakemake_command = [
        "/bin/bash", "-c",  # Ensures the module system works properly
        f"module load snakemake/8.25.5 && "
        "snakemake "
        f"--workflow-profile {EHIO_PATH}/profile/local/ "
        "--resources load=7 " # for rules that create an ERDA connection, I've added a load of 1 to prevent exceeding the ERDA limit (~15) [download_raw.smk, get_filesize_erda.smk, upload_prb.smk]
        "--conda-prefix /projects/ehi/data/0_Environments/conda "
        f"-s {EHIO_PATH}/workflow/preprocessing.smk "
        f"--config codedir={CODEDIR} workdir={WORKDIR} logdir={LOGDIR} hostgenome={HOSTGENOME} host_genome_url={HOST_GENOME_URL} ehi_code_dir={EHI_CODE_DIR} batch={batch} "
    ]

    try:
        subprocess.run(snakemake_command, shell=False, check=True)
    except subprocess.CalledProcessError as e:
        error_message = e.stderr
        if "LockException" in error_message:
            display_unlock()
        else:
            print(f"\nERROR: Snakemake failed with exit code {e.returncode}!", file=sys.stderr)
            print(f"ERROR: Check the Snakemake logs for more details.", file=sys.stderr)
            sys.exit(1)


    #############################################
    #############   Cataloging   ################
    #############################################
def run_fetch_input_asb(batch):

    """ Fetching EHI ASB input """

    Path(f"/projects/ehi/data/RUN/{batch}").mkdir(exist_ok=True)
    Path(f"/projects/ehi/data/RUN/{batch}/logs").mkdir(exist_ok=True)

    os.chdir(f"/projects/ehi/data/RUN/{batch}")

    subprocess.run([
        "python", f"{EHI_CODE_DIR}/airtable/get_asb_input.py", 
        f"--abb={batch}"
    ]) 
    ## output is 'asb_input.tsv', tab-separated file with (EHI_number, metagenomic_bases, r1_URL of preprocessed reads, r2_URL of preprocessed reads)

def run_cataloging(batch):

    ## declare variables for config
    CODEDIR = "/projects/ehi/data/0_Environments/ehio/workflow"
    WORKDIR = f"/projects/ehi/data/PPR/{batch}"
    LOGDIR = f"/projects/ehi/data/RUN/{batch}/logs"

    """ Run the cataloging workflow """

    snakemake_command = [
        "/bin/bash", "-c",  # Ensures the module system works properly
        f"module load snakemake/8.25.5 && "
        "snakemake "
        f"--workflow-profile {EHIO_PATH}/profile/local/ "
        "--conda-prefix /projects/ehi/data/0_Environments/conda "
        f"-s {EHIO_PATH}/workflow/assembly_binning.smk "
        f"--config codedir={CODEDIR} workdir={WORKDIR} logdir={LOGDIR} ehi_code_dir={EHI_CODE_DIR} batch={batch} "
    ]

    try:
        subprocess.run(snakemake_command, shell=False, check=True)
    except subprocess.CalledProcessError as e:
        error_message = e.stderr
        if "LockException" in error_message:
            display_unlock()
        else:
            print(f"\nERROR: Snakemake failed with exit code {e.returncode}!", file=sys.stderr)
            print(f"ERROR: Check the Snakemake logs for more details.", file=sys.stderr)
            sys.exit(1)


    #############################################
    ##############   Profiling   ################
    #############################################

def run_fetch_input_dmb(batch):

    """ Fetching EHI DMB input """

    Path(f"/projects/ehi/data/RUN/{batch}").mkdir(exist_ok=True)
    Path(f"/projects/ehi/data/RUN/{batch}/logs").mkdir(exist_ok=True)

    os.chdir(f"/projects/ehi/data/RUN/{batch}")

    print("Fetching MAGs information from AirTable, this may take a while")

    subprocess.run([
        "python", f"{EHI_CODE_DIR}/airtable/get_dmb_mags.py", 
        f"--dmb={batch}"
    ]) 
    ## output is 'mags.csv', comma-separated file with (MAG_name, completeness, contamination, URL)

    print("Fetching sample read information from AirTable")

    subprocess.run([
        "python", f"{EHI_CODE_DIR}/airtable/get_dmb_reads.py", 
        f"--dmb={batch}"
    ]) 
    ## output is 'reads.tsv', tab-separated file with (EHI_number, r1_URL of preprocessed reads, r2_URL of preprocessed reads)


def run_profiling(batch):

    ## declare variables for config
    CODEDIR = "/projects/ehi/data/0_Environments/ehio/workflow"
    WORKDIR = f"/projects/ehi/data/PPR/{batch}"
    LOGDIR = f"/projects/ehi/data/RUN/{batch}/logs"

    """ Run the cataloging workflow """

    snakemake_command = [
        "/bin/bash", "-c",  # Ensures the module system works properly
        f"module load snakemake/8.25.5 && "
        "snakemake "
        f"--workflow-profile {EHIO_PATH}/profile/local/ "
        "--conda-prefix /projects/ehi/data/0_Environments/conda "
        f"-s {EHIO_PATH}/workflow/dereplication_mapping.smk "
        f"--config codedir={CODEDIR} workdir={WORKDIR} logdir={LOGDIR} ehi_code_dir={EHI_CODE_DIR} batch={batch} "
    ]

    try:
        subprocess.run(snakemake_command, shell=False, check=True)
    except subprocess.CalledProcessError as e:
        error_message = e.stderr
        if "LockException" in error_message:
            display_unlock()
        else:
            print(f"\nERROR: Snakemake failed with exit code {e.returncode}!", file=sys.stderr)
            print(f"ERROR: Check the Snakemake logs for more details.", file=sys.stderr)
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="EHio: Input-output of EHI data between ERDA, Mjolnir and Airtable.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    subparsers = parser.add_subparsers(dest="command", help="Available workflows")

    # Define subcommands for each workflow
    subparser_preprocessing = subparsers.add_parser("preprocessing", help="Process preprocessing data")
    subparser_preprocessing.add_argument("-b", "--batch", required=False, help="EHI preprocessing batch")

    subparser_cataloging = subparsers.add_parser("cataloging", help="Process cataloging data")
    subparser_cataloging.add_argument("-b", "--batch", required=False, help="EHI assembly+binning batch")

    subparser_profiling = subparsers.add_parser("profiling", help="Process profiling data")
    subparser_profiling.add_argument("-b", "--batch", required=False, help="EHI assembly+binning batch")


    args = parser.parse_args()

    if args.command == "preprocessing":
        run_fetch_input_ppr(args.batch)
        run_preprocessing(args.batch)
    elif args.command == "cataloging":
        run_fetch_input_asb(args.batch)
        run_cataloging(args.batch)
    elif args.command == "profiling":
        run_fetch_input_dmb(args.batch)
        run_profiling(args.batch)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
