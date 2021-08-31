# Standard imports
from pathlib import Path

# Local imports
from src.Append import Append
from src.Login import Login
from src.Upload import Upload  

INPUT = Path("")
FLPE = Path("")
MOI = Path("")
DIAGNOSTICS = Path("")
OFFLINE = Path("")
OUTPUT = Path("")

def main():

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
    # upload.upload_data_local(OUTPUT)
    upload.upload_data(OUTPUT)

if __name__ == "__main__":
    from datetime import datetime
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")