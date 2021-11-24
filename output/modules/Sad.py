# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Sad(AbstractModule):
    """
    A class that represents the results of running SAD.

    Data and operations append SAD results to the SoS on the appropriate
    dimensions.

    Attributes
    ----------

    Methods
    -------
    append_module_data(data_dict)
        append module data to the new version of the SoS result file.
    create_data_dict(nt=None)
        creates and returns module data dictionary.
    get_module_data(nt=None)
        retrieve module results from NetCDF files.
    get_nc_attrs(nc_file, data_dict)
        get NetCDF attributes for each NetCDF variable.
    """

    def __init__(self, cont_ids, input_dir, sos_new, rids, nrids, nids):
        """
        Parameters
        ----------
        cont_ids: list
            list of continent identifiers
        input_dir: Path
            path to input directory
        sos_new: Path
            path to new SOS file
        rids: nd.array
            array of SoS reach identifiers associated with continent
        nrids: nd.array
            array of SOS reach identifiers on the node-level
        nids: nd.array
            array of SOS node identifiers
        """

        super().__init__(cont_ids, input_dir, sos_new, rids, nrids, nids)

    def get_module_data(self, nt=None):
        """Extract SAD results from NetCDF files.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        # Files and reach identifiers
        sd_dir = self.input_dir / "sad"
        sd_files = [ Path(sd_file) for sd_file in glob.glob(f"{sd_dir}/{self.cont_ids}*.nc") ] 
        sd_rids = [ int(sd_file.name.split('_')[0]) for sd_file in sd_files ]

        # Storage of results data
        sd_dict = self.create_data_dict(nt)
        
        # Storage of variable attributes
        self.get_nc_attrs(sd_dir / sd_files[0], sd_dict)
        
        if len(sd_files) != 0:
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in sd_rids:
                    sd_ds = Dataset(sd_dir / f"{s_rid}_sad.nc", 'r')
                    sd_dict["A0"][index] = sd_ds["A0"][:].filled(np.nan)
                    sd_dict["n"][index] = sd_ds["n"][:].filled(np.nan)
                    sd_dict["Qa"][index, :] = sd_ds["Qa"][:].filled(np.nan)
                    sd_dict["Q_u"][index] = sd_ds["Q_u"][:].filled(np.nan)
                    sd_ds.close()               
                index += 1
        return sd_dict
    
    def create_data_dict(self, nt=None):
        """Creates and returns SAD data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        return {
            "nt" : nt,
            "A0" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "n" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Qa" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "Q_u" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "attrs": {
                "A0" : None,
                "n" : None,
                "Qa" : None,
                "Q_u" : None
            }
        }
        
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of SAD variables
        """
        
        ds = Dataset(nc_file, 'r')
        data_dict["attrs"]["A0"] = ds["A0"].__dict__
        data_dict["attrs"]["n"] = ds["n"].__dict__
        data_dict["attrs"]["Qa"] = ds["Qa"].__dict__
        data_dict["attrs"]["Q_u"] = ds["Q_u"].__dict__
        ds.close()
    
    def append_module_data(self, data_dict):
        """Append SAD data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of SAD variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        sd_grp = sos_ds.createGroup("sad")

        # SAD data
        self.write_var(sd_grp, "A0", "f8", ("num_reaches",), data_dict)
        self.write_var(sd_grp, "n", "f8", ("num_reaches",), data_dict)
        self.write_var(sd_grp, "Qa", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(sd_grp, "Q_u", "f8", ("num_reaches", "time_steps"), data_dict)
        
        sos_ds.close()