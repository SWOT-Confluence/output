# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Prediagnostics(AbstractModule):
    """A class that represent the results of running Prediagnostics.
    
    Data and operations append Prediagnostics results to the SoS on the 
    appropriate dimenstions.
    
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
        """Extract Prediagnostics results from NetCDF files.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        # Files and reach identifiers
        pre_dir = self.input_dir
        pre_files = [ Path(pre_file) for pre_file in glob.glob(f"{pre_dir}/{self.cont_ids}*.nc") ] 
        pre_rids = [ int(pre_file.name.split('_')[0]) for pre_file in pre_files ]

        # Storage of results data
        pre_dict = self.create_data_dict(nt)
        
        if len(pre_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(pre_dir / pre_files[0], pre_dict)
            
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in pre_rids:
                    pre_ds = Dataset(pre_dir / f"{s_rid}_prediagnostics.nc", 'r')
                    # Reach
                    pre_dict["reach"]["ice_clim_f"][index, :] = pre_ds["reach"]["ice_clim_f"][:].filled(np.nan)
                    pre_dict["reach"]["ice_dyn_f"][index, :] = pre_ds["reach"]["ice_dyn_f"][:].filled(np.nan)
                    pre_dict["reach"]["dark_frac"][index, :] = pre_ds["reach"]["dark_frac"][:].filled(np.nan)
                    pre_dict["reach"]["n_good_nod"][index, :] = pre_ds["reach"]["n_good_nod"][:].filled(np.nan)
                    pre_dict["reach"]["obs_frac_n"][index, :] = pre_ds["reach"]["obs_frac_n"][:].filled(np.nan)
                    pre_dict["reach"]["width_outliers"][index, :] = pre_ds["reach"]["width_outliers"][:].filled(np.nan)
                    pre_dict["reach"]["wse_outliers"][index, :] = pre_ds["reach"]["wse_outliers"][:].filled(np.nan)
                    pre_dict["reach"]["slope2_outliers"][index, :] = pre_ds["reach"]["slope2_outliers"][:].filled(np.nan)
                    # Node
                    indexes = np.where(self.sos_nrids == s_rid)
                    pre_dict["node"]["ice_clim_f"][indexes, :] = np.transpose(pre_ds["node"]["ice_clim_f"][:]).filled(np.nan)
                    pre_dict["node"]["ice_dyn_f"][indexes, :] = np.transpose(pre_ds["node"]["ice_dyn_f"][:]).filled(np.nan)
                    pre_dict["node"]["dark_frac"][indexes, :] = np.transpose(pre_ds["node"]["dark_frac"][:]).filled(np.nan)
                    pre_dict["node"]["width_outliers"][indexes, :] = np.transpose(pre_ds["node"]["width_outliers"][:]).filled(np.nan)
                    pre_dict["node"]["wse_outliers"][indexes, :] = np.transpose(pre_ds["node"]["wse_outliers"][:]).filled(np.nan)
                    pre_dict["node"]["slope2_outliers"][indexes, :] = np.transpose(pre_ds["node"]["slope2_outliers"][:]).filled(np.nan)                    
                    pre_ds.close()
                index += 1                
        return pre_dict
    
    def create_data_dict(self, nt=None):
        """Creates and returns Prediagnosics data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        return {
            "nt" : nt,
            "reach" : {
                "ice_clim_f": np.full((self.sos_rids.shape[0], nt), self.FILL["i4"], dtype=int), 
                "ice_dyn_f": np.full((self.sos_rids.shape[0], nt), self.FILL["i4"], dtype=int),
                "dark_frac": np.full((self.sos_rids.shape[0], nt), self.FILL["i4"], dtype=int),
                "n_good_nod": np.full((self.sos_rids.shape[0], nt), self.FILL["i4"], dtype=int),
                "obs_frac_n": np.full((self.sos_rids.shape[0], nt), self.FILL["i4"], dtype=int),
                "width_outliers": np.full((self.sos_rids.shape[0], nt), self.FILL["i4"], dtype=int),
                "wse_outliers": np.full((self.sos_rids.shape[0], nt), self.FILL["i4"], dtype=int),
                "slope2_outliers": np.full((self.sos_rids.shape[0], nt), self.FILL["i4"], dtype=int),
                "attrs": {
                    "ice_clim_f": None,
                    "ice_dyn_f": None,
                    "dark_frac": None,
                    "n_good_nod": None,
                    "obs_frac_n": None,
                    "width_outliers": None,
                    "wse_outliers": None,
                    "slope2_outliers": None
                }
            },
            "node": {
                "ice_clim_f": np.full((self.sos_nids.shape[0], nt), self.FILL["i4"], dtype=int),
                "ice_dyn_f": np.full((self.sos_nids.shape[0], nt), self.FILL["i4"], dtype=int),
                "dark_frac": np.full((self.sos_nids.shape[0], nt), self.FILL["i4"], dtype=int),
                "width_outliers": np.full((self.sos_nids.shape[0], nt), self.FILL["i4"], dtype=int),
                "wse_outliers": np.full((self.sos_nids.shape[0], nt), self.FILL["i4"], dtype=int),
                "slope2_outliers": np.full((self.sos_nids.shape[0], nt), self.FILL["i4"], dtype=int),
                "attrs": {
                    "ice_clim_f": None,
                    "ice_dyn_f": None,
                    "dark_frac": None,
                    "width_outliers": None,
                    "wse_outliers": None,
                    "slope2_outliers": None
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
            dictionary of Prediagnostic variables
        """
        
        ds = Dataset(nc_file, 'r')
        # Reach
        data_dict["reach"]["attrs"]["ice_clim_f"] = ds["reach"]["ice_clim_f"].__dict__
        data_dict["reach"]["attrs"]["ice_dyn_f"] = ds["reach"]["ice_dyn_f"].__dict__
        data_dict["reach"]["attrs"]["dark_frac"] = ds["reach"]["dark_frac"].__dict__
        data_dict["reach"]["attrs"]["n_good_nod"] = ds["reach"]["n_good_nod"].__dict__
        data_dict["reach"]["attrs"]["obs_frac_n"] = ds["reach"]["obs_frac_n"].__dict__
        data_dict["reach"]["attrs"]["width_outliers"] = ds["reach"]["width_outliers"].__dict__
        data_dict["reach"]["attrs"]["wse_outliers"] = ds["reach"]["wse_outliers"].__dict__
        data_dict["reach"]["attrs"]["slope2_outliers"] = ds["reach"]["slope2_outliers"].__dict__
        # Node
        data_dict["node"]["attrs"]["ice_clim_f"] = ds["node"]["ice_clim_f"].__dict__
        data_dict["node"]["attrs"]["ice_dyn_f"] = ds["node"]["ice_dyn_f"].__dict__
        data_dict["node"]["attrs"]["dark_frac"] = ds["node"]["dark_frac"].__dict__
        data_dict["node"]["attrs"]["width_outliers"] = ds["node"]["width_outliers"].__dict__
        data_dict["node"]["attrs"]["wse_outliers"] = ds["node"]["wse_outliers"].__dict__
        data_dict["node"]["attrs"]["slope2_outliers"] = ds["node"]["slope2_outliers"].__dict__
        ds.close()
        
    def append_module_data(self, data_dict):
        """Append Prediagnostic data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of Prediagnostic variables
        """
        
        sos_ds = Dataset(self.sos_new, 'a')
        pre_grp = sos_ds.createGroup("prediagnostics")
        r_grp = pre_grp.createGroup("reach")
        n_grp = pre_grp.createGroup("node")

        # Reach
        self.write_var(r_grp, "ice_clim_f", "i4", ("num_reaches", "time_steps"), data_dict["reach"])
        self.write_var(r_grp, "ice_dyn_f", "i4", ("num_reaches", "time_steps"), data_dict["reach"])
        self.write_var(r_grp, "dark_frac", "i4", ("num_reaches", "time_steps"), data_dict["reach"])
        self.write_var(r_grp, "n_good_nod", "i4", ("num_reaches", "time_steps"), data_dict["reach"])
        self.write_var(r_grp, "obs_frac_n", "i4", ("num_reaches", "time_steps"), data_dict["reach"])
        self.write_var(r_grp, "width_outliers", "i4", ("num_reaches", "time_steps"), data_dict["reach"])
        self.write_var(r_grp, "wse_outliers", "i4", ("num_reaches", "time_steps"), data_dict["reach"])
        self.write_var(r_grp, "slope2_outliers", "i4", ("num_reaches", "time_steps"), data_dict["reach"])
        # Node
        self.write_var(n_grp, "ice_clim_f", "i4", ("num_nodes", "time_steps"), data_dict["node"])
        self.write_var(n_grp, "ice_dyn_f", "i4", ("num_nodes", "time_steps"), data_dict["node"])
        self.write_var(n_grp, "dark_frac", "i4", ("num_nodes", "time_steps"), data_dict["node"])
        self.write_var(n_grp, "width_outliers", "i4", ("num_nodes", "time_steps"), data_dict["node"])
        self.write_var(n_grp, "wse_outliers", "i4", ("num_nodes", "time_steps"), data_dict["node"])
        self.write_var(n_grp, "slope2_outliers", "i4", ("num_nodes", "time_steps"), data_dict["node"])
        sos_ds.close()