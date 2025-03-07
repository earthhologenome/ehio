import argparse
import os
import sys
import subprocess
import yaml
import re
import json
import pandas as pd
from pathlib import Path
from ehio.utils import *

## setup variables
EHI_SOFTWARE_PATH = "/projects/ehi/data/0_Code/EHI_bioinformatics_1.1"


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

    
    # Define workflow launching functions

def run_fetch_input_ppr(batch):

    """ Fetching EHI PPR input """

    mkdir -p "/projects/ehi/data/RUN/"{args.BATCH} 

    mkdir -p "/projects/ehi/data/RUN/"{args.BATCH}"/logs"

    cd "/projects/ehi/data/RUN/"{args.BATCH}

    subprocess.run([
        "python", f"{EHI_SOFTWARE_PATH}/workflow/airtable/get_preprocessing_input.py", 
        "--prb=", {args.BATCH}
    ]) 
    ## output is 'prb_input.tsv', line separated file with EHI numbers of input samples

    subprocess.run([
        "python", f"{EHI_SOFTWARE_PATH}/workflow/airtable/get_host_genome_id.py", 
        "--prb=", {args.BATCH}
    ]) 
    ## output is 'host_genome.tsv', containing a single line with EHI host genome code (e.g. G0001)

    ##setup variables for snakefile
    CODEDIR = "/projects/ehi/data/0_Code/EHI_bioinformatics_1.1/workflow/"
    WORKDIR = "/projects/ehi/data/PPR/{args.batch}"
    LOGDIR = "/projects/ehi/data/RUN/{args.batch}/logs"
    with open("/projects/ehi/data/RUN/host_genome.tsv", "r") as f:
        HOST_GENOME = [line.strip() for line in f]

    subprocess.run([
        "python", f"{EHI_SOFTWARE_PATH}/workflow/airtable/get_host_genome_id.py", 
        "--code=", {HOST_GENOME}
    ]) 
    ## output is 'host_genome_url.tsv', containing a single line with the URL to the host genome fasta
    
    with open("/projects/ehi/data/RUN/host_genome_url.tsv", "r") as f:
        HOST_GENOME_URL = [line.strip() for line in f]
    BATCH = {args.batch}

def run_preprocessing(batch):

    """ Run the preprocessing workflow """

    snakemake_command = [
        "/bin/bash", "-c",  # Ensures the module system works properly
        f"module load {config_vars['SNAKEMAKE_MODULE']} && "
        "snakemake "
        f"-s {PACKAGE_DIR / 'workflow' / 'preprocessing.smk'} "
        f"--config", f"codedir={CODEDIR}", f"workdir={WORKDIR}", f"logdir={LOGDIR}", f"host_genome={HOST_GENOME}", f"host_genome_url={HOST_GENOME_URL}", f"batch={BATCH} "
        f"--directory {output_dir} "
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





    args = parser.parse_args()

    if args.command == "preprocessing":
        run_fetch_input_ppr(args.batch)
        run_preprocessing(args.batch)
    elif args.command == "cataloging":
        run_cataloging(args.batch)
    elif args.command == "profiling":
        run_profiling(args.batch)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
