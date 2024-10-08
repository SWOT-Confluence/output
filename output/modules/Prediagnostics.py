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
    create_data_dict()
        creates and returns module data dictionary.
    get_module_data()
        retrieve module results from NetCDF files.
    get_nc_attrs(nc_file, data_dict)
        get NetCDF attributes for each NetCDF variable.
    """
    
    def __init__(self, cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s,
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
        logger: logging.Logger
            logger to log statements with
        vlen_f: VLType
            variable length float data type for NetCDF ragged arrays
        vlen_i: VLType
            variable length int data type for NEtCDF ragged arrays
        vlen_s: VLType
            variable length string data type for NEtCDF ragged arrays
        rids: nd.array
        rids: nd.array
            array of SoS reach identifiers associated with continent
        nrids: nd.array
            array of SOS reach identifiers on the node-level
        nids: nd.array
            array of SOS node identifiers
        """

        super().__init__(cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s, \
            rids, nrids, nids)
        
    def get_module_data(self):
        """Extract Prediagnostics results from NetCDF files."""

        # Files and reach identifiers
        pre_dir = self.input_dir
        pre_files = [ Path(pre_file) for pre_file in glob.glob(f"{pre_dir}/{self.cont_ids}*.nc") ] 
        pre_rids = [ int(pre_file.name.split('_')[0]) for pre_file in pre_files ]

        # Storage of results data
        pre_ds = Dataset(pre_dir / f"{pre_rids[0]}_prediagnostics.nc", 'r')
        pre_dict = self.create_data_dict(pre_ds)
        pre_ds.close()
        
        if len(pre_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(pre_dir / pre_files[0], pre_dict)
            
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in pre_rids:
                    pre_ds = Dataset(pre_dir / f"{int(s_rid)}_prediagnostics.nc", 'r')
                    # Reach
                    for a_variable in pre_ds['reach'].variables.keys():
                        if a_variable != 'attrs':
                            pre_dict['reach'][a_variable][index] = pre_ds['reach'][a_variable][:].filled(self.FILL["i4"])
                    # pre_dict["reach"]["ice_clim_f"][index] = pre_ds["reach"]["ice_clim_f"][:].filled(self.FILL["i4"])
                    # # pre_dict["reach"]["ice_dyn_f"][index] = pre_ds["reach"]["ice_dyn_f"][:].filled(self.FILL["i4"])
                    # pre_dict["reach"]["dark_frac"][index] = pre_ds["reach"]["dark_frac"][:].filled(self.FILL["i4"])
                    # pre_dict["reach"]["obs_frac_n"][index] = pre_ds["reach"]["obs_frac_n"][:].filled(self.FILL["i4"])
                    # pre_dict["reach"]["reach_q"][index] = pre_ds["reach"]["reach_q"][:].filled(self.FILL["i4"])
                    # pre_dict["reach"]["xovr_cal_q"][index] = pre_ds["reach"]["xovr_cal_q"][:].filled(self.FILL["i4"])
                    # pre_dict["reach"]["width_outliers"][index] = pre_ds["reach"]["width_outliers"][:].filled(self.FILL["i4"])
                    # pre_dict["reach"]["wse_outliers"][index] = pre_ds["reach"]["wse_outliers"][:].filled(self.FILL["i4"])
                    # pre_dict["reach"]["slope_outliers"][index] = pre_ds["reach"]["slope_outliers"][:].filled(self.FILL["i4"])
                    # pre_dict["reach"]["slope2_outliers"][index] = pre_ds["reach"]["slope2_outliers"][:].filled(self.FILL["i4"])
                    # pre_dict["reach"]["low_slope_flag"][index] = pre_ds["reach"]["low_slope_flag"][:].filled(self.FILL["i4"])
                    # pre_dict["reach"]["d_x_area_flag"][index] = pre_ds["reach"]["d_x_area_flag"][:].filled(self.FILL["i4"])
                    # Node
                    indexes = np.where(self.sos_nrids == s_rid)
                    self._insert_nx(pre_dict, pre_ds, indexes)
                    pre_ds.close()
                index += 1 
        return pre_dict
    
    def _insert_nx(self, pre_dict, pre_ds, indexes):
        """Insert node flags into prediagnostics dictionary.
        
        Parameters
        ----------
        pre_dict: dict
            dictionary of reach and node flags (insert into node)
        pre_ds: netCDF4.Dataset
            prediagnostics NetCDF dataset reference
        indexes: list
            list of integer indexes to insert node flags at
        """
        
#   Tukey_number = 1.5,

#   ice_max = 0,

#   dark_max = 0.1,

#   xover_cal_q_max = 1,

#   slope_min = 1.7e-5,

#   prior_width_min = 80 ,

#   bitwise_max= 15,

#   cross_track_dist_max_km= 15000,

#   reach_length_min_km=7000,

#   wse_r_u_max = 0.5,

#   slope_r_u_max=10e-5 

# )
                    # for a_variable in pre_ds['reach'].variables.keys():
                    #     pre_dict['reach'][a_variable][index] = pre_ds['reach'][a_variable][:].filled(self.FILL["i4"])

        j = 0
        for i in indexes[0]:
            for a_variable in pre_ds['node'].variables.keys():
                if a_variable != 'attrs':
                    pre_dict['node'][a_variable][i] = pre_ds['node'][a_variable][:,j].filled(self.FILL["i4"])
            # pre_dict["node"]["ice_clim_f"][i] = pre_ds["node"]["ice_clim_f"][:,j].filled(self.FILL["i4"])
            # # pre_dict["node"]["ice_dyn_f"][i] = pre_ds["node"]["ice_dyn_f"][:,j].filled(self.FILL["i4"])
            # pre_dict["node"]["dark_frac"][i] = pre_ds["node"]["dark_frac"][:,j].filled(self.FILL["i4"])
            # pre_dict["node"]["node_q"][i] = pre_ds["node"]["node_q"][:,j].filled(self.FILL["i4"])
            # pre_dict["node"]["xovr_cal_q"][i] = pre_ds["node"]["xovr_cal_q"][:,j].filled(self.FILL["i4"])
            # pre_dict["node"]["width_outliers"][i] = pre_ds["node"]["width_outliers"][:,j].filled(self.FILL["i4"])
            # pre_dict["node"]["wse_outliers"][i] = pre_ds["node"]["wse_outliers"][:,j].filled(self.FILL["i4"])
            # pre_dict["node"]["slope_outliers"][i] = pre_ds["node"]["slope_outliers"][:,j].filled(self.FILL["i4"])
            # pre_dict["node"]["slope2_outliers"][i] = pre_ds["node"]["slope2_outliers"][:,j].filled(self.FILL["i4"])
            # pre_dict["node"]["low_slope_flag"][i] = pre_ds["node"]["low_slope_flag"][:,j].filled(self.FILL["i4"])
            # pre_dict["node"]["d_x_area_flag"][i] = pre_ds["node"]["d_x_area_flag"][:,j].filled(self.FILL["i4"])
            j +=1
    
    def create_data_dict(self, pre_ds):
        """Creates and returns Prediagnosics data dictionary."""
                        #     for a_variable in pre_ds['reach'].variables.keys():
                        # pre_dict['reach'][a_variable][index] = pre_ds['reach'][a_variable][:].filled(self.FILL["i4"])
        reach_variables = pre_ds['reach'].variables.keys()
        node_variables  = pre_ds['node'].variables.keys()
        data_dict = {
            'reach':{'attrs':{}},
            'node':{'attrs':{}}
        }
        for a_variable in reach_variables:
            data_dict['reach'][a_variable] = np.empty((self.sos_rids.shape[0]), dtype=object)
            data_dict['reach']['attrs'][a_variable] = {}
            data_dict["reach"][a_variable].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        
        for a_variable in node_variables:
            data_dict['node'][a_variable] = np.empty((self.sos_nids.shape[0]), dtype=object)
            data_dict['node']['attrs'][a_variable] = {}
            data_dict["node"][a_variable].fill(np.array([self.FILL["i4"]], dtype=np.int32))

        # data_dict = {
        #     "reach" : {
        #         "ice_clim_f": np.empty((self.sos_rids.shape[0]), dtype=object), 
        #         # "ice_dyn_f": np.empty((self.sos_rids.shape[0]), dtype=object),
        #         "dark_frac": np.empty((self.sos_rids.shape[0]), dtype=object),
        #         "obs_frac_n": np.empty((self.sos_rids.shape[0]), dtype=object),
        #         "reach_q": np.empty((self.sos_rids.shape[0]), dtype=object),
        #         "xovr_cal_q": np.empty((self.sos_rids.shape[0]), dtype=object),
        #         "width_outliers": np.empty((self.sos_rids.shape[0]), dtype=object),
        #         "wse_outliers": np.empty((self.sos_rids.shape[0]), dtype=object),
        #         "slope_outliers": np.empty((self.sos_rids.shape[0]), dtype=object),
        #         "slope2_outliers": np.empty((self.sos_rids.shape[0]), dtype=object),
        #         "low_slope_flag": np.empty((self.sos_rids.shape[0]), dtype=object),
        #         "d_x_area_flag": np.empty((self.sos_rids.shape[0]), dtype=object),
        #         "attrs": {
        #             "ice_clim_f": {},
        #             # "ice_dyn_f": {},
        #             "dark_frac": {},
        #             "obs_frac_n": {},
        #             "reach_q": {},
        #             "xovr_cal_q": {},
        #             "width_outliers": {},
        #             "wse_outliers": {},
        #             "slope_outliers": {},
        #             "slope2_outliers": {},
        #             "low_slope_flag": {},
        #             "d_x_area_flag": {}
        #         }
        #     },
        #     "node": {
        #         "ice_clim_f": np.empty((self.sos_nids.shape[0]), dtype=object),
        #         # "ice_dyn_f": np.empty((self.sos_nids.shape[0]), dtype=object),
        #         "dark_frac": np.empty((self.sos_nids.shape[0]), dtype=object),
        #         "node_q": np.empty((self.sos_nids.shape[0]), dtype=object),
        #         "xovr_cal_q": np.empty((self.sos_nids.shape[0]), dtype=object),
        #         "width_outliers": np.empty((self.sos_nids.shape[0]), dtype=object),
        #         "wse_outliers": np.empty((self.sos_nids.shape[0]), dtype=object),
        #         "slope_outliers": np.empty((self.sos_nids.shape[0]), dtype=object),
        #         "slope2_outliers": np.empty((self.sos_nids.shape[0]), dtype=object),
        #         "low_slope_flag": np.empty((self.sos_nids.shape[0]), dtype=object),
        #         "d_x_area_flag": np.empty((self.sos_nids.shape[0]), dtype=object),
        #         "attrs": {
        #             "ice_clim_f": {},
        #             # "ice_dyn_f": {},
        #             "dark_frac": {},
        #             "node_q": {},
        #             "xovr_cal_q": {},
        #             "width_outliers": {},
        #             "wse_outliers": {},
        #             "slope_outliers": {},
        #             "slope2_outliers": {},
        #             "low_slope_flag": {},
        #             "d_x_area_flag": {}
        #         }
        #     }
        # }
        
        # Vlen variables
        # data_dict["reach"]["ice_clim_f"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # # data_dict["reach"]["ice_dyn_f"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["reach"]["dark_frac"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["reach"]["obs_frac_n"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["reach"]["reach_q"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["reach"]["xovr_cal_q"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["reach"]["width_outliers"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["reach"]["wse_outliers"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["reach"]["slope_outliers"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["reach"]["slope2_outliers"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["reach"]["low_slope_flag"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["reach"]["d_x_area_flag"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["node"]["ice_clim_f"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # # data_dict["node"]["ice_dyn_f"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["node"]["dark_frac"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["node"]["node_q"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["node"]["xovr_cal_q"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["node"]["width_outliers"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["node"]["wse_outliers"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["node"]["slope_outliers"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["node"]["slope2_outliers"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["node"]["low_slope_flag"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        # data_dict["node"]["d_x_area_flag"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        return data_dict
        
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
        for a_variable in ds['reach'].variables.keys():
            if a_variable != 'attrs':
                data_dict['reach']['attrs'][a_variable] = ds['reach'][a_variable].__dict__
        # data_dict["reach"]["attrs"]["ice_clim_f"] = ds["reach"]["ice_clim_f"].__dict__
        # # data_dict["reach"]["attrs"]["ice_dyn_f"] = ds["reach"]["ice_dyn_f"].__dict__
        # data_dict["reach"]["attrs"]["dark_frac"] = ds["reach"]["dark_frac"].__dict__
        # data_dict["reach"]["attrs"]["obs_frac_n"] = ds["reach"]["obs_frac_n"].__dict__
        # data_dict["reach"]["attrs"]["reach_q"] = ds["reach"]["reach_q"].__dict__
        # data_dict["reach"]["attrs"]["xovr_cal_q"] = ds["reach"]["xovr_cal_q"].__dict__
        # data_dict["reach"]["attrs"]["width_outliers"] = ds["reach"]["width_outliers"].__dict__
        # data_dict["reach"]["attrs"]["wse_outliers"] = ds["reach"]["wse_outliers"].__dict__
        # data_dict["reach"]["attrs"]["slope_outliers"] = ds["reach"]["slope2_outliers"].__dict__
        # data_dict["reach"]["attrs"]["slope2_outliers"] = ds["reach"]["slope2_outliers"].__dict__
        # data_dict["reach"]["attrs"]["low_slope_flag"] = ds["reach"]["slope2_outliers"].__dict__
        # data_dict["reach"]["attrs"]["d_x_area_flag"] = ds["reach"]["slope2_outliers"].__dict__
        # # Node
        for a_variable in ds['node'].variables.keys():
            if a_variable != 'attrs':
                data_dict['node']['attrs'][a_variable] = ds['node'][a_variable].__dict__
        # data_dict["node"]["attrs"]["ice_clim_f"] = ds["node"]["ice_clim_f"].__dict__
        # # data_dict["node"]["attrs"]["ice_dyn_f"] = ds["node"]["ice_dyn_f"].__dict__
        # data_dict["node"]["attrs"]["dark_frac"] = ds["node"]["dark_frac"].__dict__
        # data_dict["node"]["attrs"]["node_q"] = ds["node"]["node_q"].__dict__
        # data_dict["node"]["attrs"]["xovr_cal_q"] = ds["node"]["xovr_cal_q"].__dict__
        # data_dict["node"]["attrs"]["width_outliers"] = ds["node"]["width_outliers"].__dict__
        # data_dict["node"]["attrs"]["wse_outliers"] = ds["node"]["wse_outliers"].__dict__
        # data_dict["node"]["attrs"]["slope_outliers"] = ds["node"]["slope2_outliers"].__dict__
        # data_dict["node"]["attrs"]["slope2_outliers"] = ds["node"]["slope2_outliers"].__dict__
        # data_dict["node"]["attrs"]["low_slope_flag"] = ds["node"]["slope2_outliers"].__dict__
        # data_dict["node"]["attrs"]["d_x_area_flag"] = ds["node"]["slope2_outliers"].__dict__
        ds.close()
        
    def append_module_data(self, data_dict, metadata_json):
        """Append Prediagnostic data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of Prediagnostic variables
        """
        
        sos_ds = Dataset(self.sos_new, 'a')
        pre_grp = sos_ds.createGroup("prediagnostics")
        
        # Reach
        r_grp = pre_grp.createGroup("reach")
        # for a_variable in ds['reach'].variables.keys():
        #     pre_dict['reach']['attrs'][a_variable] = ds['reach'][a_variable]
        for a_variable in data_dict["reach"].keys():
            if a_variable != 'attrs' and a_variable in metadata_json["prediagnostics"]["reach"].keys():
                var = self.write_var_nt(r_grp, a_variable, self.vlen_i, ("num_reaches"), data_dict["reach"])
                self.set_variable_atts(var, metadata_json["prediagnostics"]["reach"][a_variable])

        # var = self.write_var_nt(r_grp, "ice_clim_f", self.vlen_i, ("num_reaches"), data_dict["reach"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["reach"]["ice_clim_f"])


        # # var = self.write_var_nt(r_grp, "ice_dyn_f", self.vlen_i, ("num_reaches"), data_dict["reach"])
        # # self.set_variable_atts(var, metadata_json["prediagnostics"]["reach"]["ice_dyn_f"])
        # var = self.write_var_nt(r_grp, "dark_frac", self.vlen_i, ("num_reaches"), data_dict["reach"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["reach"]["dark_frac"])
        # var = self.write_var_nt(r_grp, "obs_frac_n", self.vlen_i, ("num_reaches"), data_dict["reach"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["reach"]["obs_frac_n"])
        # var = self.write_var_nt(r_grp, "reach_q", self.vlen_i, ("num_reaches"), data_dict["reach"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["reach"]["reach_q"])
        # var = self.write_var_nt(r_grp, "xovr_cal_q", self.vlen_i, ("num_reaches"), data_dict["reach"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["reach"]["xovr_cal_q"])
        # var = self.write_var_nt(r_grp, "width_outliers", self.vlen_i, ("num_reaches"), data_dict["reach"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["reach"]["width_outliers"])
        # var = self.write_var_nt(r_grp, "wse_outliers", self.vlen_i, ("num_reaches"), data_dict["reach"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["reach"]["wse_outliers"])
        # var = self.write_var_nt(r_grp, "slope_outliers", self.vlen_i, ("num_reaches"), data_dict["reach"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["reach"]["slope_outliers"])
        # var = self.write_var_nt(r_grp, "slope2_outliers", self.vlen_i, ("num_reaches"), data_dict["reach"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["reach"]["slope2_outliers"])
        # var = self.write_var_nt(r_grp, "low_slope_flag", self.vlen_i, ("num_reaches"), data_dict["reach"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["reach"]["low_slope_flag"])
        # var = self.write_var_nt(r_grp, "d_x_area_flag", self.vlen_i, ("num_reaches"), data_dict["reach"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["reach"]["d_x_area_flag"])


        # Node
        n_grp = pre_grp.createGroup("node")


        for a_variable in data_dict["node"].keys():
            if a_variable != 'attrs' and a_variable in metadata_json["prediagnostics"]["node"].keys():
                var = self.write_var_nt(n_grp, a_variable, self.vlen_i, ("num_nodes"), data_dict["node"])
                self.set_variable_atts(var, metadata_json["prediagnostics"]["node"][a_variable])

        # var = self.write_var_nt(n_grp, "ice_clim_f", self.vlen_i, ("num_nodes"), data_dict["node"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["node"]["ice_clim_f"])
        # # var = self.write_var_nt(n_grp, "ice_dyn_f", self.vlen_i, ("num_nodes"), data_dict["node"])
        # # self.set_variable_atts(var, metadata_json["prediagnostics"]["node"]["ice_dyn_f"])
        # var = self.write_var_nt(n_grp, "dark_frac", self.vlen_i, ("num_nodes"), data_dict["node"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["node"]["dark_frac"])
        # var = self.write_var_nt(n_grp, "node_q", self.vlen_i, ("num_nodes"), data_dict["node"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["node"]["node_q"])
        # var = self.write_var_nt(n_grp, "xovr_cal_q", self.vlen_i, ("num_nodes"), data_dict["node"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["node"]["xovr_cal_q"])
        # var = self.write_var_nt(n_grp, "width_outliers", self.vlen_i, ("num_nodes"), data_dict["node"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["node"]["width_outliers"])
        # var = self.write_var_nt(n_grp, "wse_outliers", self.vlen_i, ("num_nodes"), data_dict["node"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["node"]["wse_outliers"])
        # var = self.write_var_nt(n_grp, "slope_outliers", self.vlen_i, ("num_nodes"), data_dict["node"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["node"]["slope_outliers"])
        # var = self.write_var_nt(n_grp, "slope2_outliers", self.vlen_i, ("num_nodes"), data_dict["node"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["node"]["slope2_outliers"])
        # var = self.write_var_nt(n_grp, "low_slope_flag", self.vlen_i, ("num_nodes"), data_dict["node"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["node"]["low_slope_flag"])
        # var = self.write_var_nt(n_grp, "d_x_area_flag", self.vlen_i, ("num_nodes"), data_dict["node"])
        # self.set_variable_atts(var, metadata_json["prediagnostics"]["node"]["d_x_area_flag"])
        sos_ds.close()