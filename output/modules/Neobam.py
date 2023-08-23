# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Neobam(AbstractModule):
    """
    A class that represents the results of running neoBAM.

    Data and operations append neoBAM results to the SoS on the appropriate
    dimensions.

    Attributes
    ----------

    Methods
    -------
    append_module_data(data_dict)
        append module data to the new version of the SoS result file.
    create_data_dict()
        creates and returns module data dictionary.
    get_module_data()
        retrieve module results from NetCDF files.
    get_nc_attrs(nc_file, data_dict)
        get NetCDF attributes for each NetCDF variable.
    """

    def __init__(self, cont_ids, input_dir, sos_new, vlen_f, vlen_i, vlen_s,
                 rids, nrids, nids):
        """
        Parameters
        ----------
        cont_ids: list
            list of continent identifiers
        input_dir: Path
            path to input directory
        sos_new: Path
            path to new SOS file
        vlen_f: VLType
            variable length float data type for NetCDF ragged arrays
        vlen_i: VLType
            variable length int data type for NEtCDF ragged arrays
        vlen_s: VLType
            variable length string data type for NEtCDF ragged arrays
        rids: nd.array
            array of SoS reach identifiers associated with continent
        nrids: nd.array
            array of SOS reach identifiers on the node-level
        nids: nd.array
            array of SOS node identifiers
        """

        super().__init__(cont_ids, input_dir, sos_new, vlen_f, vlen_i, vlen_s, \
            rids, nrids, nids)

    def get_module_data(self):
        """Extract HiVDI results from NetCDF files."""

        # Files and reach identifiers
        nb_dir = self.input_dir / "geobam"
        nb_files = [ Path(nb_file) for nb_file in glob.glob(f"{nb_dir}/{self.cont_ids}*.nc") ] 
        nb_rids = [ int(nb_file.name.split('_')[0]) for nb_file in nb_files ]

        # Storage of results data
        nb_dict = self.create_data_dict()
        
        if len(nb_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(nb_dir / nb_files[0], nb_dict)
        
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in nb_rids:
                    nb_ds = Dataset(nb_dir / f"{int(s_rid)}_geobam.nc", 'r')
                    nb_dict["r"]["mean1"][index] = nb_ds["r"]["mean1"][:].filled(np.nan)
                    nb_dict["r"]["mean2"][index] = nb_ds["r"]["mean2"][:].filled(np.nan)
                    nb_dict["r"]["mean3"][index] = nb_ds["r"]["mean3"][:].filled(np.nan)
                    nb_dict["r"]["sd1"][index] = nb_ds["r"]["sd1"][:].filled(np.nan)
                    nb_dict["r"]["sd2"][index] = nb_ds["r"]["sd2"][:].filled(np.nan)
                    nb_dict["r"]["sd3"][index] = nb_ds["r"]["sd3"][:].filled(np.nan)
                    
                    nb_dict["logn"]["mean1"][index] = nb_ds["logn"]["mean1"][:].filled(np.nan)
                    nb_dict["logn"]["mean2"][index] = nb_ds["logn"]["mean2"][:].filled(np.nan)
                    nb_dict["logn"]["mean3"][index] = nb_ds["logn"]["mean3"][:].filled(np.nan)
                    nb_dict["logn"]["sd1"][index] = nb_ds["logn"]["sd1"][:].filled(np.nan)
                    nb_dict["logn"]["sd2"][index] = nb_ds["logn"]["sd2"][:].filled(np.nan)
                    nb_dict["logn"]["sd3"][index] = nb_ds["logn"]["sd3"][:].filled(np.nan)
                    
                    nb_dict["logWb"]["mean1"][index] = nb_ds["logWb"]["mean1"][:].filled(np.nan)
                    nb_dict["logWb"]["mean2"][index] = nb_ds["logWb"]["mean2"][:].filled(np.nan)
                    nb_dict["logWb"]["mean3"][index] = nb_ds["logWb"]["mean3"][:].filled(np.nan)
                    nb_dict["logWb"]["sd1"][index] = nb_ds["logWb"]["sd1"][:].filled(np.nan)
                    nb_dict["logWb"]["sd2"][index] = nb_ds["logWb"]["sd2"][:].filled(np.nan)
                    nb_dict["logWb"]["sd3"][index] = nb_ds["logWb"]["sd3"][:].filled(np.nan)
                    
                    nb_dict["logDb"]["mean1"][index] = nb_ds["logDb"]["mean1"][:].filled(np.nan)
                    nb_dict["logDb"]["mean2"][index] = nb_ds["logDb"]["mean2"][:].filled(np.nan)
                    nb_dict["logDb"]["mean3"][index] = nb_ds["logDb"]["mean3"][:].filled(np.nan)
                    nb_dict["logDb"]["sd1"][index] = nb_ds["logDb"]["sd1"][:].filled(np.nan)
                    nb_dict["logDb"]["sd2"][index] = nb_ds["logDb"]["sd2"][:].filled(np.nan)
                    nb_dict["logDb"]["sd3"][index] = nb_ds["logDb"]["sd3"][:].filled(np.nan)
                    
                    nb_dict["q"]["q1"][index] = nb_ds["q"]["q1"][:].filled(self.FILL["f8"])
                    nb_dict["q"]["q2"][index] = nb_ds["q"]["q2"][:].filled(self.FILL["f8"])
                    nb_dict["q"]["q3"][index] = nb_ds["q"]["q3"][:].filled(self.FILL["f8"])
                    
                    nb_ds.close()
                index += 1
        return nb_dict
    
    def create_data_dict(self):
        """Creates and returns HiVDI data dictionary."""

        data_dict = {
            "r" : {
                "mean1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "mean2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "mean3" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd3" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs" : {
                    "mean1": {},
                    "mean2": {},
                    "mean3": {},
                    "sd1": {},
                    "sd2": {},
                    "sd3": {}                    
                }
            },
            "logn" : {
                "mean1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "mean2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "mean3" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd3" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs" : {
                    "mean1": {},
                    "mean2": {},
                    "mean3": {},
                    "sd1": {},
                    "sd2": {},
                    "sd3": {}                    
                }
            },
            "logWb" : {
                "mean1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "mean2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "mean3" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd3" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs" : {
                    "mean1": {},
                    "mean2": {},
                    "mean3": {},
                    "sd1": {},
                    "sd2": {},
                    "sd3": {}                    
                }
            },
            "logDb" : {
                "mean1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "mean2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "mean3" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd3" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs" : {
                    "mean1": {},
                    "mean2": {},
                    "mean3": {},
                    "sd1": {},
                    "sd2": {},
                    "sd3": {}                    
                }
            },
            "q" : {
                "q1" : np.empty((self.sos_rids.shape[0]), dtype=object),
                "q2" : np.empty((self.sos_rids.shape[0]), dtype=object),
                "q3" : np.empty((self.sos_rids.shape[0]), dtype=object),
                "attrs" : {
                    "q1": {},
                    "q2": {},
                    "q3": {}                  
                }
            }
        }
        # Vlen variables
        data_dict["q"]["q1"].fill(np.array([self.FILL["f8"]]))
        data_dict["q"]["q2"].fill(np.array([self.FILL["f8"]]))
        data_dict["q"]["q3"].fill(np.array([self.FILL["f8"]]))
        
        return data_dict
        
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable and update data_dict.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of H2iVDI variables        
        """
        
        ds = Dataset(nc_file, 'r')
        for key1, value in data_dict.items():
            if key1 == "nt": continue
            for key2 in value["attrs"].keys():
                data_dict[key1]["attrs"][key2] = ds[key1][key2].__dict__
        ds.close()
        
    def append_module_data(self, data_dict, metadata_json):
        """Append HiVDI data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of HiVDI variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        nb_grp = sos_ds.createGroup("neobam")
        
        r_grp = nb_grp.createGroup("r")        
        self.write_var(r_grp, "r", "mean", ("num_reaches"), data_dict, metadata_json)
        self.write_var(r_grp, "r", "sd", ("num_reaches"), data_dict, metadata_json)
        
        logn_grp = nb_grp.createGroup("logn") 
        self.write_var(logn_grp, "logn", "mean", ("num_reaches"), data_dict, metadata_json)
        self.write_var(logn_grp, "logn", "sd", ("num_reaches"), data_dict, metadata_json)
        
        logDb_grp = nb_grp.createGroup("logDb") 
        self.write_var(logDb_grp, "logDb", "mean", ("num_reaches"), data_dict, metadata_json)
        self.write_var(logDb_grp, "logDb", "sd", ("num_reaches"), data_dict, metadata_json)
        
        logWb_grp = nb_grp.createGroup("logWb") 
        self.write_var(logWb_grp, "logWb", "mean", ("num_reaches"), data_dict, metadata_json)
        self.write_var(logWb_grp, "logWb", "sd", ("num_reaches"), data_dict, metadata_json)
        
        q_grp = nb_grp.createGroup("q")
        self.write_var_nt(q_grp, "q", "q", self.vlen_f, ("num_reaches"), data_dict, metadata_json)
        
        sos_ds.close()
        
    def write_var(self, grp, name, chain, dims, data_dict, metadata_json):
        """Create NetCDF variable and write neoBAM data to it.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        chain: str
            mean or standard deviation chain
        dims: tuple
            tuple of NetCDF4 dimensions that matches shape of var dataa
        data_dict: dict
            dictionary of neoBAM result data
        """

        c1 = grp.createVariable(f"{chain}1", "f8", dims, fill_value=self.FILL["f8"])
        data_dict[name]["attrs"][f"{chain}1"].pop("_FillValue", None)
        c1.setncatts(data_dict[name]["attrs"][f"{chain}1"])
        c1[:] = np.nan_to_num(data_dict[name][f"{chain}1"], copy=True, nan=self.FILL["f8"])
        self.set_variable_atts(c1, metadata_json["neobam"][name][f"{chain}1"])
        
        c2 = grp.createVariable(f"{chain}2", "f8", dims, fill_value=self.FILL["f8"])
        data_dict[name]["attrs"][f"{chain}2"].pop("_FillValue", None)
        c2.setncatts(data_dict[name]["attrs"][f"{chain}2"])
        c2[:] = np.nan_to_num(data_dict[name][f"{chain}2"], copy=True, nan=self.FILL["f8"])
        self.set_variable_atts(c2, metadata_json["neobam"][name][f"{chain}2"])
        
        c3 = grp.createVariable(f"{chain}3", "f8", dims, fill_value=self.FILL["f8"])
        data_dict[name]["attrs"][f"{chain}3"].pop("_FillValue", None)
        c3.setncatts(data_dict[name]["attrs"][f"{chain}3"])
        c3[:] = np.nan_to_num(data_dict[name][f"{chain}3"], copy=True, nan=self.FILL["f8"])
        self.set_variable_atts(c3, metadata_json["neobam"][name][f"{chain}3"])
        
    def write_var_nt(self, grp, name, chain, vlen, dims, data_dict, metadata_json):
        """Create NetCDF variable and write neoBAM data to it.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        chain: str
            mean or standard deviation chain
        vlen: netCDF4._netCDF4.VLType
            variable length data type
        dims: tuple
            tuple of NetCDF4 dimensions that matches shape of var dataa
        data_dict: dict
            dictionary of neoBAM result data
        """

        c1 = grp.createVariable(f"{chain}1", vlen, dims)
        data_dict[name]["attrs"][f"{chain}1"].pop("_FillValue", None)
        c1.setncatts(data_dict[name]["attrs"][f"{chain}1"])
        c1[:] = np.nan_to_num(data_dict[name][f"{chain}1"], copy=True, nan=self.FILL["f8"])
        self.set_variable_atts(c1, metadata_json["neobam"][name][f"{chain}1"])
        
        c2 = grp.createVariable(f"{chain}2", vlen, dims)
        data_dict[name]["attrs"][f"{chain}2"].pop("_FillValue", None)
        c2.setncatts(data_dict[name]["attrs"][f"{chain}2"])
        c2[:] = np.nan_to_num(data_dict[name][f"{chain}2"], copy=True, nan=self.FILL["f8"])
        self.set_variable_atts(c2, metadata_json["neobam"][name][f"{chain}2"])
        
        c3 = grp.createVariable(f"{chain}3", vlen, dims)
        data_dict[name]["attrs"][f"{chain}3"].pop("_FillValue", None)
        c3.setncatts(data_dict[name]["attrs"][f"{chain}3"])
        c3[:] = np.nan_to_num(data_dict[name][f"{chain}3"], copy=True, nan=self.FILL["f8"])
        self.set_variable_atts(c3, metadata_json["neobam"][name][f"{chain}3"])