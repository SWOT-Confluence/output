"""Script to run Output module.

The Output module appends results from each stage of the Confluence workflow
to a new version of the SoS.

Each stage requiring storage has a class and the run type is determined by
the command line argument so that the new version gets uploaded to the 
correct location in the SoS S3 bucket.

Command line arguments:
continent_json: Name of file that contains continent data in JSON format
run_type: values should be "constrained" or "unconstrained". Default is to run unconstrained.
modules_json: Name of file that contains module names in JSON format
config_py: Name of file that contains AWS login information in JSON format.
"""

# Standard imports
import argparse
import logging
import os
from pathlib import Path

# Local imports
from output.Append import Append
from output.Upload import Upload

INPUT = Path("/mnt/data/input")
FLPE = Path("/mnt/data/flpe")
MOI = Path("/mnt/data/moi")
DIAGNOSTICS = Path("/mnt/data/diagnostics")
OFFLINE = Path("/mnt/data/offline")
VALIDATION = Path("/mnt/data/validation")
OUTPUT = Path("/mnt/data/output")

def create_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Append results of Confluence workflow execution to the SoS.")
    arg_parser.add_argument("-i",
                            "--index",
                            type=int,
                            help="Index to specify input data to execute on, value of -235 indicates AWS selection")
    arg_parser.add_argument("-c",
                            "--contjson",
                            type=str,
                            help="Name of the continent JSON file",
                            default="continent.json")
    arg_parser.add_argument("-r",
                            "--runtype",
                            type=str,
                            choices=["constrained", "unconstrained"],
                            help="Current run type of workflow: 'constrained' or 'unconstrained'",
                            default="constrained")
    arg_parser.add_argument("-m",
                            "--modules",
                            nargs="+",
                            help="List of modules executed in current workflow.")
    return arg_parser

def get_logger():
    """Return a formatted logger object."""
    
    # Create a Logger object and set log level
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Create a handler to console and set level
    console_handler = logging.StreamHandler()

    # Create a formatter and add it to the handler
    console_format = logging.Formatter("%(asctime)s - %(module)s - %(levelname)s : %(message)s")
    console_handler.setFormatter(console_format)

    # Add handlers to logger
    logger.addHandler(console_handler)

    # Return logger
    return logger

def main():
    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()
    
    # Logging
    logger = get_logger()

    # AWS Batch index
    index = args.index if args.index != -235 else int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
    logger.info(f"Job index: {index}.")

    # Append SoS data
    append = Append(INPUT / args.contjson, index, INPUT, OUTPUT, args.modules, logger)
    append.create_new_version()
    append.create_modules(args.runtype, INPUT, DIAGNOSTICS, FLPE, MOI, OFFLINE, \
        VALIDATION / "stats")
    append.append_data()
    
    # Upload SoS data
    upload = Upload(None, append.sos_file)
    upload.upload_data(OUTPUT, VALIDATION / "figs", args.runtype)

if __name__ == "__main__":
    from datetime import datetime
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")