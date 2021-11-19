# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Validation(AbstractModule):
    """A class that represent the results of running Validation.
    
    Data and operations append Validation results to the SoS on the appropriate 
    dimenstions.

    Attributes
    ----------
    num_algos: int
            number of algorithms stats were produced for
    nchar: int
        max number of characters in algorithm names
        
    Methods
    -------
    append_module_data(data_dict)
        append module data to the new version of the SoS result file.
    create_data_dict(nt=None)
        creates and returns module data dictionary.
    get_module_data(nt=None)
        retrieve module results from NetCDF files.
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

        self.num_algos = 0
        self.nchar = 0
        super().__init__(cont_ids, input_dir, sos_new, rids, nrids, nids)


    def get_module_data(self, nt=None):
        """Extract Validation results from NetCDF files.
        
        TODO
        - Remove NA netcdf explicit call for num algos and nchar
        """

        # Files and reach identifiers
        val_dir = self.input_dir
        val_files = [ Path(val_file) for val_file in glob.glob(f"{val_dir}/{self.cont_ids}*.nc") ] 
        val_rids = [ int(val_file.name.split('_')[0]) for val_file in val_files ]

        # Retrieve number of algorithms and nchar
        # temp = Dataset(val_files[0], 'r')    ## TODO
        temp = Dataset(f"{val_dir}/77444000031_validation.nc", 'r')    ## TODO remove for NA
        self.num_algos = temp.dimensions["num_algos"].size
        self.nchar = temp.dimensions["nchar"].size
        temp.close()
        val_dict = self.create_data_dict()
        
        # Storage of results data       
        if len(val_files) != 0:
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in val_rids:
                    val_ds = Dataset(val_dir / f"{s_rid}_validation.nc", 'r')
                    val_dict["has_validation"][index] = val_ds.has_validation
                    val_dict["algo_names"][index,:,:] = val_ds["algorithm"][:]
                    val_dict["nse"][index,:] = val_ds["NSE"][:].filled(np.nan)
                    val_dict["rsq"][index,:] = val_ds["Rsq"][:].filled(np.nan)
                    val_dict["kge"][index,:] = val_ds["KGE"][:].filled(np.nan)
                    val_dict["rmse"][index,:] = val_ds["RMSE"][:].filled(np.nan)
                    val_dict["testn"][index,:] = val_ds["testn"][:].filled(np.nan)
                    val_ds.close()
                index += 1
        return val_dict
    
    def create_data_dict(self, nt=None):
        """Creates and returns Validation data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        return {
            "num_algos" : self.num_algos,
            "nchar": self.nchar,
            "algo_names": np.full((self.sos_rids.shape[0], self.num_algos, self.nchar), ''),
            "has_validation": np.full((self.sos_rids.shape[0]), np.nan, dtype=np.float64),
            "nse": np.full((self.sos_rids.shape[0], self.num_algos), np.nan, dtype=np.float64),
            "rsq": np.full((self.sos_rids.shape[0], self.num_algos), np.nan, dtype=np.float64),
            "kge": np.full((self.sos_rids.shape[0], self.num_algos), np.nan, dtype=np.float64),
            "rmse": np.full((self.sos_rids.shape[0], self.num_algos), np.nan, dtype=np.float64),
            "testn": np.full((self.sos_rids.shape[0], self.num_algos), np.nan, dtype=np.float64)
        }

    def append_module_data(self, data_dict):
        """Append Validation data to the new version of the SoS.
        
        Parameters
        ----------
        val_dict: dict
            dictionary of Validation variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        val_grp = sos_ds.createGroup("validation")

        # Dimensions
        val_grp.createDimension("num_algos", data_dict["num_algos"])
        val_grp.createDimension("nchar", data_dict["nchar"])

        # Validation data
        self.write_var(val_grp, "algo_names", "S1", ("num_reaches", "num_algos", "nchar",), data_dict)
        self.write_var(val_grp, "has_validation", "i4", ("num_reaches",), data_dict)
        self.write_var(val_grp, "nse", "f8", ("num_reaches", "num_algos",), data_dict)
        self.write_var(val_grp, "rsq", "f8", ("num_reaches", "num_algos",), data_dict)
        self.write_var(val_grp, "kge", "f8", ("num_reaches", "num_algos",), data_dict)
        self.write_var(val_grp, "rmse", "f8", ("num_reaches", "num_algos",), data_dict)
        self.write_var(val_grp, "testn", "f8", ("num_reaches", "num_algos",), data_dict)

        sos_ds.close()