"""Script to run Output module.

The Output module appends results from each stage of the Confluence workflow
to a new version of the SoS.

Each stage requiring storage has a class and the run type is determined by
the command line argument so that the new version gets uploaded to the 
correct location in the SoS S3 bucket.

Command line arguments:
run_type: values should be "constrained" or "unconstrained"
Default is to run unconstrained.
"""

# Standard imports
from pathlib import Path
import sys

# Local imports
from output.Append import Append
from output.Login import Login
from output.Upload import Upload 

INPUT = Path("")
FLPE = Path("")
MOI = Path("")
DIAGNOSTICS = Path("")
OFFLINE = Path("")
OUTPUT = Path("")

def main():
    # Command line arguments
    try:
        run_type = sys.argv[1]
    except IndexError:
        run_type = "unconstrained"

    # AWS Batch index
    # index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
    index = 3

    # Append SoS data
    append = Append(INPUT / "continent.json", index, INPUT, OUTPUT)
    append.create_new_version()
    append.append_data(FLPE, MOI, DIAGNOSTICS, OFFLINE)

    # Login
    login = Login()
    login.login()
    
    # Upload SoS data
    upload = Upload(login.sos_fs, append.sos_file)
    upload.upload_data_local(OUTPUT, run_type)
    # upload.upload_data(OUTPUT, run_type)

if __name__ == "__main__":
    from datetime import datetime
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")