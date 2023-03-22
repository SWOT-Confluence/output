# Standard imports
from os import scandir
from pathlib import Path

# Third-party imports
import boto3
import botocore
from netCDF4 import Dataset

class Upload:
    """Class that uploads results of Confluence workflow to SoS S3 bucket.

    Attributes
    ----------
    sos_fs: S3FileSystem
        references SWORD of Science S3 bucket
    sos_file: Path
        path to new SoS file to upload
    VERS_LENGTH: int
        number of integers in SoS identifier

    Methods
    -------
    upload()
        Transfers SOS data to S3 from EFS
    """
    
    VERS_LENGTH = 4

    def __init__(self, sos_file, logger):
        """
        Parameters
        ----------
        sos_file: Path
            path to new SoS file to upload
        logger: Logger
            logger to use for logging state
        """

        self.sos_file = sos_file
        self.logger = logger

    def upload_data(self, output_dir, val_dir, run_type):
        """Uploads SoS result file to confluence-sos S3 bucket.

        Parameters
        ----------
        output_dir: Path
            path to output directory
        val_dir: Path   
            path to directory that contains validation figures
        run_type: str
            either "constrained" or "unconstrained"
        """

        # Get SoS version
        sos_ds = Dataset(output_dir / self.sos_file, 'r')
        vers = sos_ds.version
        sos_ds.close()
        padding = ['0'] * (self.VERS_LENGTH - len(vers))
        vers = f"{''.join(padding)}{vers}"
        
        try:
            s3 = boto3.client("s3")
            # Upload SoS result file to the S3 bucket
            s3.upload_file(Filename=str(output_dir / self.sos_file),
                           Bucket="confluence-sos",
                           Key=f"{run_type}/{vers}/{self.sos_file.name}")
            self.logger.info(f"Uploaded: {run_type}/{vers}/{self.sos_file.name}.")
            # Upload validation figures to S3 bucket
            with scandir(val_dir) as entries:
                for entry in entries:
                    s3.upload_file(Filename=str(Path(entry)),
                           Bucket="confluence-sos",
                           Key=f"figs/{run_type}/{vers}/{entry.name}")
                    self.logger.info(f"Uploaded: figs/{run_type}/{vers}/{entry.name}.")
        except botocore.exceptions.ClientError as error:
            raise error
        