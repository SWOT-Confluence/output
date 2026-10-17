# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Momma(AbstractModule):
    """
    A class that represents the results of running MOMMA.

    Data and operations append MOMMA results to the SoS on the appropriate
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
            array of SoS reach identifiers associated with continent
        nrids: nd.array
            array of SOS reach identifiers on the node-level
        nids: nd.array
            array of SOS node identifiers
        """

        super().__init__(cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s, \
            rids, nrids, nids)

    def get_module_data(self):
        """Extract MOMMA results from NetCDF files."""

        # Files and reach identifiers
        mm_dir = self.input_dir / "momma"
        mm_files = [ Path(mm_file) for mm_file in glob.glob(f"{mm_dir}/{self.cont_ids}*.nc") ] 
        mm_rids = [ int(mm_file.name.split('_')[0]) for mm_file in mm_files ]

        # Storage of results data
        mm_dict = self.create_data_dict()
        
        if len(mm_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(mm_dir / mm_files[0], mm_dict)
        
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in mm_rids:
                    try:
                        mm_ds = Dataset(mm_dir / f"{int(s_rid)}_momma.nc", 'r')
                        mm_dict["stage"][index] = mm_ds["stage"][:].filled(self.FILL["f8"])
                        mm_dict["width"][index] = mm_ds["width"][:].filled(self.FILL["f8"])
                        mm_dict["slope"][index] = mm_ds["slope"][:].filled(self.FILL["f8"])
                        mm_dict["Qgage"][index] = mm_ds["Qgage"][:].filled(self.FILL["f8"])
                        mm_dict["seg"][index] = mm_ds["seg"][:].filled(self.FILL["f8"])
                        mm_dict["n"][index] = mm_ds["n"][:].filled(self.FILL["f8"])
                        mm_dict["Y"][index] = mm_ds["Y"][:].filled(self.FILL["f8"])
                        mm_dict["v"][index] = mm_ds["v"][:].filled(self.FILL["f8"])
                        mm_dict["Q"][index] = mm_ds["Q"][:].filled(self.FILL["f8"])
                        mm_dict["Q_constrained"][index] = mm_ds["Q_constrained"][:].filled(self.FILL["f8"])
                        
                        mm_dict["gage_constrained"][index] = mm_ds["gage_constrained"][:].filled(np.nan)
                        # mm_dict["input_MBL_prior"][index] = mm_ds["input_MBL_prior"][:].filled(np.nan)
                        mm_dict["input_Qm_prior"][index] = mm_ds["input_Qm_prior"][:].filled(np.nan)
                        mm_dict["input_Qb_prior"][index] = mm_ds["input_Qb_prior"][:].filled(np.nan)
                        mm_dict["input_Yb_prior"][index] = mm_ds["input_Yb_prior"][:].filled(np.nan)
                        mm_dict["input_known_ezf"][index] = mm_ds["input_known_ezf"][:].filled(np.nan)
                        mm_dict["input_known_bkfl_stage"][index] = mm_ds["input_known_bkfl_stage"][:].filled(np.nan)
                        mm_dict["input_known_nb_seg1"][index] = mm_ds["input_known_nb_seg1"][:].filled(np.nan)
                        mm_dict["input_known_x_seg1"][index] = mm_ds["input_known_x_seg1"][:].filled(np.nan)
                        mm_dict["Qgage_constrained_nb_seg1"][index] = mm_ds["Qgage_constrained_nb_seg1"][:].filled(np.nan)
                        mm_dict["Qgage_constrained_x_seg1"][index] = mm_ds["Qgage_constrained_x_seg1"][:].filled(np.nan)
                        mm_dict["input_known_nb_seg2"][index] = mm_ds["input_known_nb_seg2"][:].filled(np.nan)
                        mm_dict["input_known_x_seg2"][index] = mm_ds["input_known_x_seg2"][:].filled(np.nan)
                        mm_dict["Qgage_constrained_nb_seg2"][index] = mm_ds["Qgage_constrained_nb_seg2"][:].filled(np.nan)
                        mm_dict["Qgage_constrained_x_seg2"][index] = mm_ds["Qgage_constrained_x_seg2"][:].filled(np.nan)
                        mm_dict["n_bkfl_Qb_prior"][index] = mm_ds["n_bkfl_Qb_prior"][:].filled(np.nan)
                        mm_dict["n_bkfl_slope"][index] = mm_ds["n_bkfl_slope"][:].filled(np.nan) # here
                        mm_dict["vel_bkfl_Qb_prior"][index] = mm_ds["vel_bkfl_Qb_prior"][:].filled(np.nan)
                        # mm_dict["vel_bkfl_diag_MBL"][index] = mm_ds["vel_bkfl_diag_MBL"][:].filled(np.nan)
                        mm_dict["Froude_bkfl_diag_Smean"][index] = mm_ds["Froude_bkfl_diag_Smean"][:].filled(np.nan)
                        # mm_dict["width_bkfl_empirical"][index] = mm_ds["width_bkfl_empirical"][:].filled(np.nan)
                        mm_dict["width_bkfl_solved_obs"][index] = mm_ds["width_bkfl_solved_obs"][:].filled(np.nan)
                        mm_dict["depth_bkfl_solved_obs"][index] = mm_ds["depth_bkfl_solved_obs"][:].filled(np.nan)
                        # mm_dict["depth_bkfl_diag_MBL"][index] = mm_ds["depth_bkfl_diag_MBL"][:].filled(np.nan)
                        mm_dict["depth_bkfl_diag_Wb_Smean"][index] = mm_ds["depth_bkfl_diag_Wb_Smean"][:].filled(np.nan)
                        mm_dict["zero_flow_stage"][index] = mm_ds["zero_flow_stage"][:].filled(np.nan)
                        mm_dict["bankfull_stage"][index] = mm_ds["bankfull_stage"][:].filled(np.nan)
                        mm_dict["Qmean_prior"][index] = mm_ds["Qmean_prior"][:].filled(np.nan)
                        mm_dict["Qmean_momma"][index] = mm_ds["Qmean_momma"][:].filled(np.nan)
                        mm_dict["Qmean_momma.constrained"][index] = mm_ds["Qmean_momma.constrained"][:].filled(np.nan)
                        mm_dict["width_stage_corr"][index] = mm_ds["width_stage_corr"][:].filled(np.nan)

                        mm_ds.close()
                    except:
                        self.logger.warn(f'Reach {s_rid} failed for Momma ...')

                    
                index += 1
        return mm_dict
    
    def create_data_dict(self):
        """Creates and returns MOMMA data dictionary."""

        data_dict = {
            "stage" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "width" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "slope" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "Qgage" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "seg" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "n" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "Y" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "v" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "Q" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "Q_constrained" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "gage_constrained" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            # "input_MBL_prior" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "input_Qm_prior" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "input_Qb_prior" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "input_Yb_prior" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "input_known_ezf" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "input_known_bkfl_stage" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "input_known_nb_seg1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "input_known_x_seg1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Qgage_constrained_nb_seg1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Qgage_constrained_x_seg1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "input_known_nb_seg2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "input_known_x_seg2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Qgage_constrained_nb_seg2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Qgage_constrained_x_seg2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "n_bkfl_Qb_prior" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "n_bkfl_slope" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "vel_bkfl_Qb_prior" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            # "vel_bkfl_diag_MBL" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Froude_bkfl_diag_Smean" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            # "width_bkfl_empirical" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "width_bkfl_solved_obs" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "depth_bkfl_solved_obs" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            # "depth_bkfl_diag_MBL" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "depth_bkfl_diag_Wb_Smean" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "zero_flow_stage" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "bankfull_stage" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Qmean_prior" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Qmean_momma" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Qmean_momma.constrained" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "width_stage_corr": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "attrs": {
                "stage" : {},
                "width" : {},
                "slope" : {},
                "Qgage" : {},
                "seg" : {},
                "n" : {},
                "Y" : {},
                "v" : {},
                "Q" : {},
                "Q_constrained" : {},
                "gage_constrained" : {},
                # "input_MBL_prior" : {},
                "input_Qm_prior" : {},
                "input_Qb_prior" : {},
                "input_Yb_prior" : {},
                "input_known_ezf" : {},
                "input_known_bkfl_stage" : {},
                "input_known_nb_seg1" : {},
                "input_known_x_seg1" : {},
                "Qgage_constrained_nb_seg1" : {},
                "Qgage_constrained_x_seg1" : {},
                "input_known_nb_seg2" : {},
                "input_known_x_seg2" : {},
                "Qgage_constrained_nb_seg2" : {},
                "Qgage_constrained_x_seg2" : {},
                "n_bkfl_Qb_prior" : {},
                "n_bkfl_slope" : {},
                "vel_bkfl_Qb_prior" : {},
                # "vel_bkfl_diag_MBL" : {},
                "Froude_bkfl_diag_Smean" : {},
                # "width_bkfl_empirical" : {},
                "width_bkfl_solved_obs" : {},
                "depth_bkfl_solved_obs" : {},
                # "depth_bkfl_diag_MBL" : {},
                "depth_bkfl_diag_Wb_Smean" : {},
                "zero_flow_stage" : {},
                "bankfull_stage" : {},
                "Qmean_prior" : {},
                "Qmean_momma" : {},
                "Qmean_momma.constrained" : {},
                "width_stage_corr":{},
                
            }
        }
        
        # Vlen variables
        data_dict["stage"].fill(np.array([self.FILL["f8"]]))
        data_dict["width"].fill(np.array([self.FILL["f8"]]))
        data_dict["slope"].fill(np.array([self.FILL["f8"]]))
        data_dict["Qgage"].fill(np.array([self.FILL["f8"]]))
        data_dict["seg"].fill(np.array([self.FILL["f8"]]))
        data_dict["n"].fill(np.array([self.FILL["f8"]]))
        data_dict["Y"].fill(np.array([self.FILL["f8"]]))
        data_dict["v"].fill(np.array([self.FILL["f8"]]))
        data_dict["Q"].fill(np.array([self.FILL["f8"]]))
        data_dict["Q_constrained"].fill(np.array([self.FILL["f8"]]))
        return data_dict
        
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of MOMMA variables
        """
        
        ds = Dataset(nc_file, 'r')
        for key in data_dict["attrs"].keys():
            data_dict["attrs"][key] = ds[key].__dict__        
        ds.close()
    
    def append_module_data(self, data_dict, metadata_json):
        """Append MOMMA data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of MOMMA variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        mm_grp = sos_ds.createGroup("momma")

        # MOMMA data
        var = self.write_var_nt(mm_grp, "stage", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["stage"])
        var = self.write_var_nt(mm_grp, "width", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["width"])
        var = self.write_var_nt(mm_grp, "slope", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["slope"])
        var = self.write_var_nt(mm_grp, "Qgage", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["Qgage"])
        var = self.write_var_nt(mm_grp, "seg", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["seg"])
        var = self.write_var_nt(mm_grp, "n", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["n"])
        var = self.write_var_nt(mm_grp, "Y", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["Y"])
        var = self.write_var_nt(mm_grp, "v", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["v"])
        var = self.write_var_nt(mm_grp, "Q", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["Q"])
        var = self.write_var_nt(mm_grp, "Q_constrained", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["Q_constrained"])
        var = self.write_var(mm_grp, "gage_constrained", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["gage_constrained"])
        # var = self.write_var(mm_grp, "input_MBL_prior", "f8", ("num_reaches",), data_dict)
        # self.set_variable_atts(var, metadata_json["momma"]["input_MBL_prior"])
        var = self.write_var(mm_grp, "input_Qm_prior", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["input_Qm_prior"])
        var = self.write_var(mm_grp, "input_Qb_prior", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["input_Qb_prior"])
        var = self.write_var(mm_grp, "input_Yb_prior", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["input_Yb_prior"])
        var = self.write_var(mm_grp, "input_known_ezf", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["input_known_ezf"])
        var = self.write_var(mm_grp, "input_known_bkfl_stage", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["input_known_bkfl_stage"])
        var = self.write_var(mm_grp, "input_known_nb_seg1", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["input_known_nb_seg1"])
        var = self.write_var(mm_grp, "input_known_x_seg1", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["input_known_x_seg1"])
        var = self.write_var(mm_grp, "Qgage_constrained_nb_seg1", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["Qgage_constrained_nb_seg1"])
        var = self.write_var(mm_grp, "Qgage_constrained_x_seg1", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["Qgage_constrained_x_seg1"])
        var = self.write_var(mm_grp, "input_known_nb_seg2", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["input_known_nb_seg2"])
        var = self.write_var(mm_grp, "input_known_x_seg2", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["input_known_x_seg2"])
        var = self.write_var(mm_grp, "Qgage_constrained_nb_seg2", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["Qgage_constrained_nb_seg2"])
        var = self.write_var(mm_grp, "Qgage_constrained_x_seg2", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["Qgage_constrained_x_seg2"])
        var = self.write_var(mm_grp, "n_bkfl_Qb_prior", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["n_bkfl_Qb_prior"])
        var = self.write_var(mm_grp, "n_bkfl_slope", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["n_bkfl_slope"])
        var = self.write_var(mm_grp, "vel_bkfl_Qb_prior", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["vel_bkfl_Qb_prior"])
        # var = self.write_var(mm_grp, "vel_bkfl_diag_MBL", "f8", ("num_reaches",), data_dict)
        # self.set_variable_atts(var, metadata_json["momma"]["vel_bkfl_diag_MBL"])
        var = self.write_var(mm_grp, "Froude_bkfl_diag_Smean", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["Froude_bkfl_diag_Smean"])
        # var = self.write_var(mm_grp, "width_bkfl_empirical", "f8", ("num_reaches",), data_dict)
        # self.set_variable_atts(var, metadata_json["momma"]["width_bkfl_empirical"])
        var = self.write_var(mm_grp, "width_bkfl_solved_obs", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["width_bkfl_solved_obs"])
        var = self.write_var(mm_grp, "depth_bkfl_solved_obs", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["depth_bkfl_solved_obs"])
        # var = self.write_var(mm_grp, "depth_bkfl_diag_MBL", "f8", ("num_reaches",), data_dict)
        # self.set_variable_atts(var, metadata_json["momma"]["depth_bkfl_diag_MBL"])
        var = self.write_var(mm_grp, "depth_bkfl_diag_Wb_Smean", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["depth_bkfl_diag_Wb_Smean"])
        var = self.write_var(mm_grp, "zero_flow_stage", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["zero_flow_stage"])
        var = self.write_var(mm_grp, "bankfull_stage", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["bankfull_stage"])
        var = self.write_var(mm_grp, "Qmean_prior", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["Qmean_prior"])
        var = self.write_var(mm_grp, "Qmean_momma", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["Qmean_momma"])
        var = self.write_var(mm_grp, "Qmean_momma.constrained", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["Qmean_momma.constrained"])

        var = self.write_var(mm_grp, "width_stage_corr", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["momma"]["width_stage_corr"])

        # NEW: node-level MOMMA v2 results into /momma/node
        self.append_node_data(sos_ds, mm_grp, metadata_json)
        
        sos_ds.close()


    
    def append_node_data(self, sos_ds, mm_grp, metadata_json):
        """Append MOMMA v2 node results aligned with SoS node IDs.

        All variables have dimension (num_nodes,).
        Each element contains a variable-length time series.
        Nodes without MOMMA results contain empty arrays.
        Existing SoS groups and variables are not modified.
        """

        node_names = ("stage", "width", "slope", "seg", "n", "nb", "x", "Y", "v", "Q")

        mm_dir = self.input_dir / "momma"
        mm_files = sorted(Path(f) for f in glob.glob(f"{mm_dir}/{self.cont_ids}*.nc"))

        # Use existing SoS node IDs and ordering
        sos_node_ids = np.asarray(self.sos_nids, dtype=np.int64)

        num_nodes = len(sos_node_ids)

        if len(sos_ds.dimensions["num_nodes"]) != num_nodes:
            return

        sorted_order = np.argsort(sos_node_ids)
        sorted_ids = sos_node_ids[sorted_order]

        if np.any(sorted_ids[1:] == sorted_ids[:-1]):
            return

        # Create only the new /momma/node group
        node_grp = mm_grp.createGroup("node")

        node_vars = {}

        for name in (*node_names, "nt"):
            var = node_grp.createVariable(name, self.vlen_f,("num_nodes",))

            var.setncattr(
                "description",
                "Aligned with /nodes/node_id. Each element contains a node time series. Empty arrays indicate no MOMMA results."
            )

            node_vars[name] = var

        node_vars["nt"].long_name = ("Source MOMMA node time index")


        # Initialize missing nodes as empty arrays
        chunk_size = 10000

        for start in range(0, num_nodes, chunk_size):
            end = min(start + chunk_size, num_nodes)
            n_chunk = end - start

            empty_chunk = np.empty(n_chunk, dtype=object)

            for i in range(n_chunk):
                empty_chunk[i] = np.array([], dtype=np.float64)

            for var in node_vars.values():
                var[start:end] = empty_chunk

        # Optional metadata
        node_meta = metadata_json.get("momma", {}).get("node", {})

        for name, var in node_vars.items():
            if name in node_meta:
                self.set_variable_atts(var, node_meta[name])

        # Read and store MOMMA node results
        count_files = 0
        count_nodes = 0
        seen_node_ids = set()

        for mm_file in mm_files:
            try:
                with Dataset(mm_file, "r") as src:
    
                    if "node" not in src.groups:
                        continue
    
                    src_node = src.groups["node"]
    
                    src_ids = np.asarray(src_node.variables["node_id"][:], dtype=np.int64)
    
                    positions = np.searchsorted(sorted_ids, src_ids)
    
                    valid = positions < num_nodes
    
                    valid[valid] &= (sorted_ids[positions[valid]] == src_ids[valid])
    
                    nt = np.ma.asarray(src_node.variables["nt"][:])
    
                    nt_values = np.asarray(nt.filled(self.FILL["f8"]), dtype=np.float64)
    
                    # Copy source variable attributes
                    if count_files == 0:
                        for name in node_names:
                            if name in src_node.variables:
                                attrs = {
                                    k: src_node.variables[name].getncattr(k)
                                    for k in src_node.variables[name].ncattrs()
                                    if k != "_FillValue"
                                }
    
                                # Preserve explicit output metadata
                                attrs.pop("description", None)
    
                                node_vars[name].setncatts(attrs)
    
                    reach_nodes = 0
    
                    for j in np.flatnonzero(valid):
    
                        node_id = int(src_ids[j])
    
                        if node_id in seen_node_ids:
                            continue
    
                        target = int(sorted_order[positions[j]])
    
                        # Read and validate before writing
                        values_to_write = {}
                        valid_node = True
    
                        for name in node_names:
    
                            if name not in src_node.variables:
                                continue
    
                            col = np.ma.asarray(src_node.variables[name][:, j])
    
                            values = np.asarray(col.filled(self.FILL["f8"]), dtype=np.float64)
    
                            if len(values) != len(nt_values):
                                valid_node = False
                                break
    
                            values_to_write[name] = values
    
                        if not valid_node:
                            continue
    
                        # Write to matching SoS node index
                        node_vars["nt"][target] = nt_values
    
                        for name, values in values_to_write.items():
                            node_vars[name][target] = values
    
                        seen_node_ids.add(node_id)
                        reach_nodes += 1
                        count_nodes += 1
    
                    count_files += 1
    
                    # # Log only reaches with stored nodes
                    # if reach_nodes > 0:
                    #     self.logger.info(
                    #         "MOMMA reach %s: stored %d nodes",
                    #         mm_file.name.split("_")[0],
                    #         reach_nodes
                    #     )

            except Exception:
                    continue
