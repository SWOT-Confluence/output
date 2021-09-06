# Third-party imports
import s3fs

# Application imports
from output.output_conf import sos_creds

class Login:
    """
    A class that represents login operations.
    
    A user can log into the SoS S3 buckets.

    Attributes
    ----------
    sos_fs: S3FileSystem
        references Confluence S3 buckets
    Methods
    -------
    login()
        logs into SOS_S3 service
    """

    def __init__(self):
        self.sos_fs = None

    def login(self):
        """Logs into SOS S3 bucket.
        
        Sets references to sos_fs attributes.
        """

        # SoS data
        self.sos_fs = s3fs.S3FileSystem(key=sos_creds["key"],
                                        secret=sos_creds["secret"],
                                        client_kwargs={"region_name": sos_creds["region"]})