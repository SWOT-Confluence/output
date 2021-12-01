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
        """Extract MOMMA results from NetCDF files.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        # Files and reach identifiers
        mm_dir = self.input_dir / "momma"
        mm_files = [ Path(mm_file) for mm_file in glob.glob(f"{mm_dir}/{self.cont_ids}*.nc") ] 
        mm_rids = [ int(mm_file.name.split('_')[0]) for mm_file in mm_files ]

        # Storage of results data
        mm_dict = self.create_data_dict(nt)
        
        if len(mm_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(mm_dir / mm_files[0], mm_dict)
        
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in mm_rids:
                    mm_ds = Dataset(mm_dir / f"{s_rid}_momma.nc", 'r')
                    mm_dict["stage"][index, :] = mm_ds["stage"][:].filled(np.nan)
                    mm_dict["width"][index, :] = mm_ds["width"][:].filled(np.nan)
                    mm_dict["slope"][index, :] = mm_ds["slope"][:].filled(np.nan)
                    mm_dict["Qgage"][index, :] = mm_ds["Qgage"][:].filled(np.nan)
                    mm_dict["seg"][index, :] = mm_ds["seg"][:].filled(np.nan)
                    mm_dict["n"][index, :] = mm_ds["n"][:].filled(np.nan)
                    mm_dict["Y"][index, :] = mm_ds["Y"][:].filled(np.nan)
                    mm_dict["v"][index, :] = mm_ds["v"][:].filled(np.nan)
                    mm_dict["Q"][index, :] = mm_ds["Q"][:].filled(np.nan)
                    mm_dict["Q_constrained"][index, :] = mm_ds["Q_constrained"][:].filled(np.nan)
                    
                    mm_dict["gage_constrained"][index] = mm_ds["gage_constrained"][:].filled(np.nan)
                    mm_dict["input_MBL_prior"][index] = mm_ds["input_MBL_prior"][:].filled(np.nan)
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
                    mm_dict["n_bkfl_final_used"][index] = mm_ds["n_bkfl_final_used"][:].filled(np.nan)
                    mm_dict["vel_bkfl_Qb_prior"][index] = mm_ds["vel_bkfl_Qb_prior"][:].filled(np.nan)
                    mm_dict["vel_bkfl_diag_MBL"][index] = mm_ds["vel_bkfl_diag_MBL"][:].filled(np.nan)
                    mm_dict["Froude_bkfl_diag_Smean"][index] = mm_ds["Froude_bkfl_diag_Smean"][:].filled(np.nan)
                    mm_dict["width_bkfl_empirical"][index] = mm_ds["width_bkfl_empirical"][:].filled(np.nan)
                    mm_dict["width_bkfl_solved_obs"][index] = mm_ds["width_bkfl_solved_obs"][:].filled(np.nan)
                    mm_dict["depth_bkfl_solved_obs"][index] = mm_ds["depth_bkfl_solved_obs"][:].filled(np.nan)
                    mm_dict["depth_bkfl_diag_MBL"][index] = mm_ds["depth_bkfl_diag_MBL"][:].filled(np.nan)
                    mm_dict["depth_bkfl_diag_Wb_Smean"][index] = mm_ds["depth_bkfl_diag_Wb_Smean"][:].filled(np.nan)
                    mm_dict["zero_flow_stage"][index] = mm_ds["zero_flow_stage"][:].filled(np.nan)
                    mm_dict["bankfull_stage"][index] = mm_ds["bankfull_stage"][:].filled(np.nan)
                    mm_dict["Qmean_prior"][index] = mm_ds["Qmean_prior"][:].filled(np.nan)
                    mm_dict["Qmean_momma"][index] = mm_ds["Qmean_momma"][:].filled(np.nan)
                    mm_dict["Qmean_momma.constrained"][index] = mm_ds["Qmean_momma.constrained"][:].filled(np.nan)

                    mm_ds.close()
                index += 1
        return mm_dict
    
    def create_data_dict(self, nt=None):
        """Creates and returns MOMMA data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        return {
            "nt" : nt,
            "stage" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "width" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "slope" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "Qgage" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "seg" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "n" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "Y" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "v" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "Q" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "Q_constrained" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "gage_constrained" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "input_MBL_prior" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
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
            "n_bkfl_final_used" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "vel_bkfl_Qb_prior" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "vel_bkfl_diag_MBL" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Froude_bkfl_diag_Smean" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "width_bkfl_empirical" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "width_bkfl_solved_obs" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "depth_bkfl_solved_obs" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "depth_bkfl_diag_MBL" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "depth_bkfl_diag_Wb_Smean" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "zero_flow_stage" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "bankfull_stage" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Qmean_prior" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Qmean_momma" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Qmean_momma.constrained" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "attrs": {
                "stage" : None,
                "width" : None,
                "slope" : None,
                "Qgage" : None,
                "seg" : None,
                "n" : None,
                "Y" : None,
                "v" : None,
                "Q" : None,
                "Q_constrained" : None,
                "gage_constrained" : None,
                "input_MBL_prior" : None,
                "input_Qm_prior" : None,
                "input_Qb_prior" : None,
                "input_Yb_prior" : None,
                "input_known_ezf" : None,
                "input_known_bkfl_stage" : None,
                "input_known_nb_seg1" : None,
                "input_known_x_seg1" : None,
                "Qgage_constrained_nb_seg1" : None,
                "Qgage_constrained_x_seg1" : None,
                "input_known_nb_seg2" : None,
                "input_known_x_seg2" : None,
                "Qgage_constrained_nb_seg2" : None,
                "Qgage_constrained_x_seg2" : None,
                "n_bkfl_Qb_prior" : None,
                "n_bkfl_final_used" : None,
                "vel_bkfl_Qb_prior" : None,
                "vel_bkfl_diag_MBL" : None,
                "Froude_bkfl_diag_Smean" : None,
                "width_bkfl_empirical" : None,
                "width_bkfl_solved_obs" : None,
                "depth_bkfl_solved_obs" : None,
                "depth_bkfl_diag_MBL" : None,
                "depth_bkfl_diag_Wb_Smean" : None,
                "zero_flow_stage" : None,
                "bankfull_stage" : None,
                "Qmean_prior" : None,
                "Qmean_momma" : None,
                "Qmean_momma.constrained" : None,
            }
        }
        
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
    
    def append_module_data(self, data_dict):
        """Append MOMMA data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of MOMMA variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        mm_grp = sos_ds.createGroup("momma")

        # MOMMA data
        self.write_var(mm_grp, "stage", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(mm_grp, "width", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(mm_grp, "slope", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(mm_grp, "Qgage", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(mm_grp, "seg", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(mm_grp, "n", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(mm_grp, "Y", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(mm_grp, "v", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(mm_grp, "Q", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(mm_grp, "Q_constrained", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(mm_grp, "gage_constrained", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "input_MBL_prior", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "input_Qm_prior", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "input_Qb_prior", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "input_Yb_prior", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "input_known_ezf", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "input_known_bkfl_stage", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "input_known_nb_seg1", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "input_known_x_seg1", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "Qgage_constrained_nb_seg1", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "Qgage_constrained_x_seg1", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "input_known_nb_seg2", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "input_known_x_seg2", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "Qgage_constrained_nb_seg2", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "Qgage_constrained_x_seg2", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "n_bkfl_Qb_prior", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "n_bkfl_final_used", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "vel_bkfl_Qb_prior", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "vel_bkfl_diag_MBL", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "Froude_bkfl_diag_Smean", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "width_bkfl_empirical", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "width_bkfl_solved_obs", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "depth_bkfl_solved_obs", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "depth_bkfl_diag_MBL", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "depth_bkfl_diag_Wb_Smean", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "zero_flow_stage", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "bankfull_stage", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "Qmean_prior", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "Qmean_momma", "f8", ("num_reaches",), data_dict)
        self.write_var(mm_grp, "Qmean_momma.constrained", "f8", ("num_reaches",), data_dict)

        sos_ds.close()