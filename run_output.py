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
from datetime import datetime
import logging
import os
from pathlib import Path
import sys

# Third-party imports
import botocore

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
LAKEFLOW = Path("/mnt/data/input/lakeflow")
SSC = Path("/mnt/data/input/ssc")

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
                            default=[],
                            help="List of modules executed in current workflow.")
    arg_parser.add_argument("-j",
                            "--metadatajson",
                            type=Path,
                            default=Path(__file__).parent / "metadata" / "metadata.json",
                            help="Path to JSON file that contains global attribute values")
    arg_parser.add_argument("-u",
                            "--podaacupload",
                            action="store_true",
                            help="Indicate requirement to upload to PO.DAAC S3 Bucket")
    arg_parser.add_argument("-b",
                            "--podaacbucket",
                            type=str,
                            help="Name of PO.DAAC S3 bucket to upload to")
    arg_parser.add_argument("-s",
                            "--sosbucket",
                            type=str,
                            default="confluence-sos",
                            help="Name of SoS S3 bucket to upload to")
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
    start = datetime.now()

    # Logging
    logger = get_logger()

    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()
    for arg in vars(args):
        logger.info("%s: %s", arg, getattr(args, arg))

    # AWS Batch index
    index = args.index if args.index != -235 else int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
    logger.info(f"Job index: {index}.")

    # Append SoS data
    append = Append(INPUT / args.contjson, index, INPUT, OUTPUT, args.modules, \
        logger, args.metadatajson)
    append.create_new_version()
    append.create_modules(args.runtype, INPUT, DIAGNOSTICS, FLPE, MOI, OFFLINE, \
        VALIDATION / "stats", LAKEFLOW, SSC)
    append.append_data()
    append.update_time_coverage()

    # Upload SoS data
    if args.sosbucket != 'local':
        upload = Upload(append.sos_file, args.sosbucket, args.podaacupload, args.podaacbucket, \
            list(append.cont.keys())[0], append.run_date, args.runtype, logger)
        try:
            upload.upload_data(OUTPUT, VALIDATION / "figs", args.runtype, args.modules)
        except botocore.exceptions.ClientError as error:
            logger.error("Error encountered when trying to upload results file and figures.")
            logger.error(error)
            sys.exit(1)
    else:
        logger.info("Local run, skipping uploading to PO.DAAC or S3")
    
    end = datetime.now()
    logger.info(f"Execution time: {end - start}")

if __name__ == "__main__":
    main()
    