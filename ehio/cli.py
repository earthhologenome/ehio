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

def main():
    parser = argparse.ArgumentParser(
        description="EHio: Input-output of EHI data between ERDA, Mjolnir and Airtable.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    subparsers = parser.add_subparsers(dest="command", help="Available workflows")

    # Define subcommands for each workflow
    subparser_preprocessing = subparsers.add_parser("preprocessing", help="Process preprocessing data")
    subparser_preprocessing.add_argument("-i", "--input", required=False, help="Input mode")
    subparser_preprocessing.add_argument("-o", "--output", required=False, help="Output mode.")
    subparser_preprocessing.add_argument("-b", "--batch", required=False, help="EHI preprocessing batch")

    subparser_cataloging = subparsers.add_parser("cataloging", help="Process cataloging data")
    subparser_cataloging.add_argument("-i", "--input", required=False, help="Input mode")
    subparser_cataloging.add_argument("-o", "--output", required=False, help="Output mode.")
    subparser_cataloging.add_argument("-b", "--batch", required=False, help="EHI assembly+binning batch")

    subparser_profiling = subparsers.add_parser("profiling", help="Process profiling data")
    subparser_profiling.add_argument("-i", "--input", required=False, help="Input mode")
    subparser_profiling.add_argument("-o", "--output", required=False, help="Output mode.")
    subparser_profiling.add_argument("-b", "--batch", required=False, help="EHI assembly+binning batch")

    args = parser.parse_args()

    if args.command == "preprocessing":
        run_preprocessing(args.input, args.output, args.batch)
    elif args.command == "cataloging":
        run_cataloging(args.input, args.output, args.batch)
    elif args.command == "profiling":
        run_profiling(args.input, args.output, args.batch)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
