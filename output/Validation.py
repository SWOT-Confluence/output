# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

class Validation:
    """A class that represent the results of running Validation.
    
    Data and operations append Validation results to the SoS on the appropriate 
    dimenstions.

    Attributes
    ----------
    cont_ids: list
            list of continent identifiers
    FLOAT_FILL: float
        fill value to use for missing float data
    input_dir: Path
        path to input directory
    INT_FILL: int
        fill value to use for missing int data
    sos_nrids: nd.array
        array of SOS reach identifiers on the node-level
    sos_nids: nd.array
        array of SOS node identifiers
    sos_rids: nd.array
        array of SoS reach identifiers associated with continent
        path to the current SoS
    sos_new: Path
            path to new SOS file

    Methods
    -------
    append_val()
        append Validation results to the SoS
    __create_val_data(val_dict)
        create variables and append MetroMan data to the new version of the SoS
    __create_val_dict(nt)
        creates and returns Validation data dictionary
    __get_val_data()
        extract Validation results from NetCDF files
    __insert_val_data(val_dict)
        insert Validation data into existing variables of the new version of the SoS
    __insert_nr(name, index, val_ds, val_dict)
        insert discharge values into dictionary with nr dimension
    __insert_nt(self, name, index, val_ds, val_dict):
        insert discharge values into dictionary with nr by nt dimensions
    __insert_var(grp, name, val_dict)
        insert new Validation data into NetCDF variable
    __write_var(q_grp, name, dims, val_dict)
        create NetCDF variable and write Validation data to it
    """

    FLOAT_FILL = -999999999999
    INT_FILL = -999

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

        self.cont_ids = cont_ids
        self.input_dir = input_dir
        self.sos_new = sos_new
        self.sos_rids = rids
        self.sos_nrids = nrids
        self.sos_nids = nids

    def append_val(self, version):
        """Append Validation results to the SoS.
        
        Parameters
        ----------
        version: int
            unique identifier for SoS version
        """
        
        val_dict = self.__get_val_data()
        if int(version) == 1:
            self.__create_val_data(val_dict)
        else:
            self.__insert_val_data(val_dict)

    def __get_val_data(self):
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
        num_algos = temp.dimensions["num_algos"].size
        nchar = temp.dimensions["nchar"].size
        temp.close()
        val_dict = self.__create_val_dict(num_algos, nchar)
        
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
    
    def __create_val_dict(self, num_algos, nchar):
        """Creates and returns Validation data dictionary.
        
        Parameters
        ----------
        num_algos: int
            number of algorithms stats were produced for
        nchar: int
            max number of characters in algorithm names
        """

        return {
            "num_algos" : num_algos,
            "nchar": nchar,
            "algo_names": np.full((self.sos_rids.shape[0], num_algos, nchar), ''),
            "has_validation": np.full((self.sos_rids.shape[0]), np.nan, dtype=np.float64),
            "nse": np.full((self.sos_rids.shape[0], num_algos), np.nan, dtype=np.float64),
            "rsq": np.full((self.sos_rids.shape[0], num_algos), np.nan, dtype=np.float64),
            "kge": np.full((self.sos_rids.shape[0], num_algos), np.nan, dtype=np.float64),
            "rmse": np.full((self.sos_rids.shape[0], num_algos), np.nan, dtype=np.float64),
            "testn": np.full((self.sos_rids.shape[0], num_algos), np.nan, dtype=np.float64)
        }

    def __create_val_data(self, val_dict):
        """Append Validation data to the new version of the SoS.
        
        Parameters
        ----------
        val_dict: dict
            dictionary of Validation variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        val_grp = sos_ds.createGroup("validation")

        # Dimensions
        val_grp.createDimension("num_algos", val_dict["num_algos"])
        val_grp.createDimension("nchar", val_dict["nchar"])

        # Validation data
        an = val_grp.createVariable("algo_names", "S1", ("num_reaches", "num_algos", "nchar",))
        an[:] = val_dict["algo_names"]

        hv = val_grp.createVariable("has_validation", "i4", ("num_reaches",), fill_value=self.INT_FILL)
        hv[:] = np.nan_to_num(val_dict["has_validation"], copy=True, nan=self.INT_FILL).astype(int)

        nse = val_grp.createVariable("nse", "f8", ("num_reaches", "num_algos",), fill_value=self.FLOAT_FILL)
        nse[:] = np.nan_to_num(val_dict["nse"], copy=True, nan=self.FLOAT_FILL)

        rsq = val_grp.createVariable("rsq", "f8", ("num_reaches", "num_algos",), fill_value=self.FLOAT_FILL)
        rsq[:] = np.nan_to_num(val_dict["rsq"], copy=True, nan=self.FLOAT_FILL)

        kge = val_grp.createVariable("kge", "f8", ("num_reaches", "num_algos",), fill_value=self.FLOAT_FILL)
        kge[:] = np.nan_to_num(val_dict["kge"], copy=True, nan=self.FLOAT_FILL)

        rmse = val_grp.createVariable("rmse", "f8", ("num_reaches", "num_algos",), fill_value=self.FLOAT_FILL)
        rmse[:] = np.nan_to_num(val_dict["rmse"], copy=True, nan=self.FLOAT_FILL)

        testn = val_grp.createVariable("testn", "f8", ("num_reaches", "num_algos",), fill_value=self.FLOAT_FILL)
        testn[:] = np.nan_to_num(val_dict["testn"], copy=True, nan=self.FLOAT_FILL)

        sos_ds.close()

    def __insert_val_data(self, val_dict):
        """Insert Validation data into existing variables of new SoS.
        
        Parameters
        ----------
        val_dict: dict
            dictionary of Validation variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        val_grp = sos_ds["validation"]

        val_grp["algo_names"][:] = val_dict["algo_names"]
        val_grp["has_validation"][:] = np.nan_to_num(val_dict["has_validation"], copy=True, nan=self.INT_FILL).astype(int)
        val_grp["nse"][:] = np.nan_to_num(val_dict["nse"], copy=True, nan=self.FLOAT_FILL)
        val_grp["rsq"][:] = np.nan_to_num(val_dict["rsq"], copy=True, nan=self.FLOAT_FILL)
        val_grp["kge"][:] = np.nan_to_num(val_dict["kge"], copy=True, nan=self.FLOAT_FILL)
        val_grp["rmse"][:] = np.nan_to_num(val_dict["rmse"], copy=True, nan=self.FLOAT_FILL)
        val_grp["testn"][:] = np.nan_to_num(val_dict["testn"], copy=True, nan=self.FLOAT_FILL)
        
        sos_ds.close()