# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset, stringtochar
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Postdiagnostics(AbstractModule):
    """A class that represents the results of running Postdiagnostics module.
    
    Attributes
    ----------
    basin_algo_names: nd.array
        array of string basin-level algorithm names
    basin_algo_num: int
        number of basin-level algorithms
    reach_algo_names: nd.array
        array of string reach-level algorithm names
    reach_algo_num: int
        number of reach-level algorithms
        
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

        self.basin_algo_names = np.array([])
        self.basin_num_algos = 0
        self.reach_algo_names = np.array([])
        self.reach_num_algos = 0
        super().__init__(cont_ids, input_dir, sos_new, rids, nrids, nids)

    def get_module_data(self, nt=None):
        """Extract Postdiagnostics results from NetCDF files."""

        # Files and reach identifiers
        pd_basin_files = [ Path(pd_file) for pd_file in glob.glob(f"{self.input_dir}/basin/{self.cont_ids}*.nc") ]
        pd_rids = [ int(pd_file.name.split('_')[0]) for pd_file in pd_basin_files ]        
        
        if len(pd_basin_files) == 0:
            # Store empty data
            pd_dict = self.create_data_dict(0)
        else:
            # Get names number of algorithms processed
            self.__get_algo_data(pd_basin_files, pd_rids)

            # Storage initialization
            pd_dict = self.create_data_dict()
            
            # Storage of variable attributes
            self.get_nc_attrs(self.input_dir / pd_basin_files[0], pd_dict)
            self.get_nc_attrs(self.input_dir / "reach" / f"{pd_rids[0]}_flpe_diag.nc", pd_dict)

            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in pd_rids:
                    pd_b_ds = Dataset(self.input_dir / "basin" / f"{s_rid}_moi_diag.nc", 'r')
                    pd_r_ds = Dataset(self.input_dir / "reach" / f"{s_rid}_flpe_diag.nc", 'r')
                    pd_dict["basin"]["realism_flags"][index, :] = pd_b_ds["realism_flags"][:].filled(np.nan)
                    pd_dict["basin"]["stability_flags"][index, :] = pd_b_ds["stability_flags"][:].filled(np.nan)
                    pd_dict["basin"]["prepost_flags"][index, :] = pd_b_ds["prepost_flags"][:].filled(np.nan)
                    pd_dict["reach"]["realism_flags"][index, :] = pd_r_ds["realism_flags"][:].filled(np.nan)
                    pd_dict["reach"]["stability_flags"][index, :] = pd_r_ds["stability_flags"][:].filled(np.nan)
                    pd_b_ds.close()
                    pd_r_ds.close()
                index += 1
        return pd_dict
    
    def __get_algo_data(self, basin_files, rids):
        """Store basin and reach algorithn names and number.

        Parameters
        -----------
        basin_files: list
            list of basin file paths
        rids: list
            list of reach identifiers
        """

        pd_b_ds = Dataset(basin_files[0], 'r')
        self.basin_algo_names = pd_b_ds["algo_names"][:]
        self.basin_num_algos = pd_b_ds.dimensions["num_algos"].size
        pd_b_ds.close()
        
        pd_r_ds = Dataset(self.input_dir / "reach" / f"{rids[0]}_flpe_diag.nc", 'r')
        self.reach_algo_names = pd_r_ds["algo_names"][:]
        self.reach_num_algos = pd_r_ds.dimensions["num_algos"].size
        pd_r_ds.close()
    
    def create_data_dict(self, nt=None):
        """Creates and returns Postdiagnostics data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        return {
            "basin_algo_names" : self.basin_algo_names,
            "basin_num_algos" : self.basin_num_algos,
            "reach_algo_names" : self.reach_algo_names,
            "reach_num_algos" : self.reach_num_algos,
            "basin" : {
                "realism_flags" : np.full((self.sos_rids.shape[0], self.basin_num_algos), np.nan, dtype=np.float64),
                "stability_flags" : np.full((self.sos_rids.shape[0], self.basin_num_algos), np.nan, dtype=np.float64),
                "prepost_flags" : np.full((self.sos_rids.shape[0], self.basin_num_algos), np.nan, dtype=np.float64),
                "attrs": {
                    "realism_flags": None,
                    "stability_flags": None,
                    "prepost_flags": None
                }
            },
            "reach" : {
                "realism_flags" : np.full((self.sos_rids.shape[0], self.reach_num_algos), np.nan, dtype=np.float64),
                "stability_flags" : np.full((self.sos_rids.shape[0], self.reach_num_algos), np.nan, dtype=np.float64),
                "attrs": {
                    "realism_flags": None,
                    "stability_flags": None
                }
            }
        }
    
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of MOI variables
        """
        ds = Dataset(nc_file, 'r')
        level = nc_file.name.split('_')[1]
        if level == "moi":
            for key in data_dict["basin"]["attrs"].keys():
                data_dict["basin"]["attrs"][key] = ds[key].__dict__
        if level == "flpe":
            for key in data_dict["reach"]["attrs"].keys():
                data_dict["reach"]["attrs"][key] = ds[key].__dict__
        ds.close()
    
    def append_module_data(self, data_dict):
        """Append Postdiagnostic data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of Postdiagnostic variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        pd_grp = sos_ds.createGroup("postdiagnostics")

        # Postdiagnostic data
        pd_grp.createDimension("nchar", None)      
        
        # Basin
        b_grp = pd_grp.createGroup("basin")
        b_grp.createDimension("basin_num_algos", None)
        bna_v = b_grp.createVariable("basin_num_algos", "i4", ("basin_num_algos",))
        bna_v[:] = range(1, data_dict["basin_num_algos"] + 1)
        ban_v = b_grp.createVariable("basin_algo_names", "S1", ("basin_num_algos", "nchar"))
        ban_v[:] = stringtochar(np.array(data_dict["basin_algo_names"], dtype="S10"))
        self.write_var(b_grp, "realism_flags", "i4", ("num_reaches", "basin_num_algos"), data_dict["basin"])
        self.write_var(b_grp, "stability_flags", "i4", ("num_reaches", "basin_num_algos"), data_dict["basin"])
        self.write_var(b_grp, "prepost_flags", "i4", ("num_reaches", "basin_num_algos"), data_dict["basin"])

        # Reach
        r_grp = pd_grp.createGroup("reach")
        r_grp.createDimension("reach_num_algos", None)
        rna_v = r_grp.createVariable("reach_num_algos", "i4", ("reach_num_algos",))
        rna_v[:] = range(1, data_dict["reach_num_algos"] + 1)
        ran_v = r_grp.createVariable("reach_algo_names", "S1", ("reach_num_algos", "nchar"))
        ran_v[:] = stringtochar(np.array(data_dict["reach_algo_names"], dtype="S10"))
        self.write_var(r_grp, "realism_flags", "i4", ("num_reaches", "reach_num_algos"), data_dict["reach"])
        self.write_var(r_grp, "stability_flags", "i4", ("num_reaches", "reach_num_algos"), data_dict["reach"])

        sos_ds.close()