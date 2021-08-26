# Standard imports
from os import mkdir, scandir
from shutil import copy

class Upload:
    """Class that uploads results of Confluence workflow to SoS S3 bucket.

    Attributes
    ----------
    NUM_LENGTH: int
        number of integers in SoS identifier
    sos_fs: S3FileSystem
        references SWORD of Science S3 bucket
    sos_file: Path
            path to new SoS file to upload

    Methods
    -------
    upload()
        Transfers SOS data to S3 from EFS
    """

    NUM_LENGTH = 4

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

    def upload_data(self, output_dir):
        """ Transfers SOS data to S3 from EFS via EC2 instance.
        
        ## TODO
        - Implement
        - Create new directory for the new version of the SoS to live in

        Parameters
        ----------
        output_dir: Path
            path to output directory
        """

        # Determine current version and define the next
        dirs = self.sos_fs.ls("confluence-sos")
        if len(dirs) == 0:
            new_dir = "sos_0001"
        else:
            num_length  = 4
            versions = [ int(d.split('_')[1]) for d in dirs ]
            padding = ['0'] * (num_length - len(str(max(versions) + 1)))
            new_dir = f"sos_{''.join(padding)}{max(versions) + 1}"

        # Upload new SoS file to the S3 bucket
        self.sos_fs.put(str(output_dir / self.sos_file), f"confluence-sos/{new_dir}/{self.sos_file}")

    def upload_data_local(self, output_dir):
        """Copy data to local directory and remove temporary directory.

        Parameters
        ----------
        output_dir: Path
            path to output directory        
        """

        # Get directories present
        with scandir(output_dir) as entries:
            dir_list = [ e for e in entries if e.is_dir() ]

        # Determine current version and define next version
        if len(dir_list) == 0:
            new_dir = output_dir / "sos_0001"
        else:
            num_length  = 4
            versions = [ int(d.name.split('_')[1]) for d in dir_list ]
            padding = ['0'] * (num_length - len(str(max(versions) + 1)))
            new_dir = output_dir / f"sos_{''.join(padding)}{max(versions) + 1}"

        # Copy new version
        mkdir(new_dir)
        copy(output_dir / self.sos_file, new_dir / self.sos_file)