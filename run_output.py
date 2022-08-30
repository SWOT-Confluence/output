"""Script to run Output module.

The Output module appends results from each stage of the Confluence workflow
to a new version of the SoS.

Each stage requiring storage has a class and the run type is determined by
the command line argument so that the new version gets uploaded to the 
correct location in the SoS S3 bucket.

Command line arguments:
continent_json: Name of file that contains continent data in JSON format
run_type: values should be "constrained" or "unconstrained"
Default is to run unconstrained.
"""

# Standard imports
import os
from pathlib import Path
import sys

# Local imports
from output.Append import Append
from output.Login import Login
from output.Upload import Upload

INPUT = Path("/mnt/data/input")
FLPE = Path("/mnt/data/flpe")
MOI = Path("/mnt/data/moi")
DIAGNOSTICS = Path("/mnt/data/diagnostics")
OFFLINE = Path("/mnt/data/offline")
VALIDATION = Path("/mnt/data/validation")
OUTPUT = Path("/mnt/data/output")

def main():
    # Command line arguments
    try:
        continent_json = sys.argv[1]
        run_type = sys.argv[2]
        modules_list = sys.argv[3]
    except IndexError:
        continent_json = "continent.json"
        run_type = "unconstrained"
        modules_list = ["neobam", "hivdi", "metroman", "moi", "momma", "offline",
                        "postdiagnostics", "prediagnostics", "priors", "sad", 
                        "sic4dvar", "swot", "validation"]

    # AWS Batch index
    index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))

    # Append SoS data
    append = Append(INPUT / continent_json, index, INPUT, OUTPUT, modules_list)
    append.create_new_version()
    append.create_modules(run_type, INPUT, DIAGNOSTICS, FLPE, MOI, OFFLINE, \
        VALIDATION / "stats")
    append.append_data()

    # Login
    login = Login()
    login.login()
    
    # Upload SoS data
    upload = Upload(login.sos_fs, append.sos_file)
    # upload.upload_data_local(OUTPUT, VALIDATION / "figs", run_type)
    upload.upload_data(OUTPUT, VALIDATION / "figs", run_type)

if __name__ == "__main__":
    from datetime import datetime
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")