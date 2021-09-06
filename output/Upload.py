# Standard imports
from os import mkdir
from shutil import copy

# Third-party imports
from netCDF4 import Dataset

class Upload:
    """Class that uploads results of Confluence workflow to SoS S3 bucket.

    Attributes
    ----------
    sos_fs: S3FileSystem
        references SWORD of Science S3 bucket
    sos_file: Path
            path to new SoS file to upload

    Methods
    -------
    upload()
        Transfers SOS data to S3 from EFS
    """

    def __init__(self, sos_fs, sos_file):
        """
        Parameters
        ----------
        sos_fs: S3FileSystem
            references SWORD of Science S3 bucket
        sos_file: Path
            path to new SoS file to upload
        """

        self.instance = {}
        self.sos_fs = sos_fs
        self.sos_file = sos_file

    def upload_data(self, output_dir, run_type):
        """ Transfers SOS data to S3 from EFS via EC2 instance.

        Parameters
        ----------
        output_dir: Path
            path to output directory
        run_type: str
            either "constrained" or "unconstrained"
        """

        # Get new SoS version
        sos_ds = Dataset(output_dir / self.sos_file, 'r')
        vers = sos_ds.version
        sos_ds.close()

        # Upload new SoS file to the S3 bucket
        self.sos_fs.put(str(output_dir / self.sos_file), f"confluence-sos/{run_type}/{vers}/{self.sos_file}")

    def upload_data_local(self, output_dir, run_type):
        """Copy data to local directory and remove temporary directory.

        Parameters
        ----------
        output_dir: Path
            path to output directory
        run_type: str
            either "constrained" or "unconstrained"    
        """

        # Get new SoS version
        sos_ds = Dataset(output_dir / self.sos_file, 'r')
        vers = sos_ds.version
        sos_ds.close()

        # Copy new version in a new directory
        new_dir = output_dir / run_type / vers
        if not new_dir.exists():
            mkdir(new_dir)
        copy(output_dir / self.sos_file, new_dir / self.sos_file)