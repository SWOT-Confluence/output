# Standard imports
from os import makedirs, scandir
from pathlib import Path
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

    def upload_data(self, output_dir, val_dir, run_type):
        """ Transfers SOS data to S3 from EFS via EC2 instance.

        Parameters
        ----------
        output_dir: Path
            path to output directory
        val_dir: Path   
            path to directory that contains validation figures
        run_type: str
            either "constrained" or "unconstrained"
        """

        # Get new SoS version
        sos_ds = Dataset(output_dir / self.sos_file, 'r')
        vers = sos_ds.version
        sos_ds.close()

        # Upload new SoS file to the S3 bucket
        self.sos_fs.put(str(output_dir / self.sos_file), f"confluence-sos/{run_type}/{vers}/{self.sos_file}")

        # Upload validation figures to S3 bucket
        with scandir(val_dir) as entries:
            for entry in entries:
                self.sos_fs.put(str(Path(entry)), f"confluence-sos/figs/{run_type}/{vers}/{entry.name}")

    def upload_data_local(self, output_dir, val_dir, run_type):
        """Copy data to local directory and remove temporary directory.

        Parameters
        ----------
        output_dir: Path
            path to output directory
        val_dir: Path   
            path to directory that contains validation figures
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
            makedirs(new_dir)
        copy(output_dir / self.sos_file, new_dir / self.sos_file.name)

        # Copy figures to a new directory
        figs_dir = output_dir / "figs" / run_type / vers
        if not figs_dir.exists():
            makedirs(figs_dir)
        with scandir(val_dir) as entries:
            for entry in entries:
                copy(Path(entry), figs_dir / entry.name)