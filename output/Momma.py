# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

class Momma:
    """
    A class that represents the results of running MOMMA.

    Data and operations append MOMMA results to the SoS on the appropriate
    dimensions.

    Attributes
    ----------
    cont_ids: list
            list of continent identifiers
    FILL_VALUE: float
        fill value to use for missing data
    input_dir: Path
        path to input directory
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
    append_mm()
        append MOMMA results to the SoS
    __create_mm_data(mm_dict)
        create variables and append MOMMA data to the new version of the SoS
    __create_mm_dict(nt)
        creates and returns MOMMA data dictionary
    __get_mm_data()
        extract MOMMA results from NetCDF files
    __insert_mm_data(mm_dict)
        insert MOMMA data into existing variables of the new version of the SoS
    __insert_nr(name, index, mm_ds, mm_dict)
        insert discharge values into dictionary with nr dimension
    __insert_nt(self, name, index, mm_ds, mm_dict):
        insert discharge values into dictionary with nr by nt dimensions
    __insert_var(grp, name, mm_dict)
        insert new MOMMA data into NetCDF variable
    __write_var(q_grp, name, dims, mm_dict)
        create NetCDF variable and write MOMMA data to it
    """

    FILL_VALUE = -999999999999

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

    def append_mm(self, nt, version):
        """Append MOMMA results to the SoS.
        
        Parameters
        ----------
        nt: int
            number of time steps
        version: int
            unique identifier for SoS version
        """

        mm_dict = self.__get_mm_data(nt)
        self.__create_mm_data(mm_dict)

    def __get_mm_data(self, nt):
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
        mm_dict = self.__create_mm_dict(nt)
        
        if len(mm_files) != 0:
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in mm_rids:
                    mm_ds = Dataset(mm_dir / f"{s_rid}_momma.nc", 'r')
                    self.__insert_nt("stage", index, mm_ds, mm_dict)
                    self.__insert_nt("width", index, mm_ds, mm_dict)
                    self.__insert_nt("slope", index, mm_ds, mm_dict)
                    self.__insert_nt("Qgage", index, mm_ds, mm_dict)
                    self.__insert_nt("seg", index, mm_ds, mm_dict)
                    self.__insert_nt("n", index, mm_ds, mm_dict)
                    self.__insert_nt("Y", index, mm_ds, mm_dict)
                    self.__insert_nt("v", index, mm_ds, mm_dict)
                    self.__insert_nt("Q", index, mm_ds, mm_dict)
                    self.__insert_nt("Q_constrained", index, mm_ds, mm_dict)
                    self.__insert_nr("gage_constrained", index, mm_ds, mm_dict)
                    self.__insert_nr("input_MBL_prior", index, mm_ds, mm_dict)
                    self.__insert_nr("input_Qm_prior", index, mm_ds, mm_dict)
                    self.__insert_nr("input_Qb_prior", index, mm_ds, mm_dict)
                    self.__insert_nr("input_Yb_prior", index, mm_ds, mm_dict)
                    self.__insert_nr("input_known_ezf", index, mm_ds, mm_dict)
                    self.__insert_nr("input_known_bkfl_stage", index, mm_ds, mm_dict)
                    self.__insert_nr("input_known_nb_seg1", index, mm_ds, mm_dict)
                    self.__insert_nr("input_known_x_seg1", index, mm_ds, mm_dict)
                    self.__insert_nr("Qgage_constrained_nb_seg1", index, mm_ds, mm_dict)
                    self.__insert_nr("Qgage_constrained_x_seg1", index, mm_ds, mm_dict)
                    self.__insert_nr("input_known_nb_seg2", index, mm_ds, mm_dict)
                    self.__insert_nr("input_known_x_seg2", index, mm_ds, mm_dict)
                    self.__insert_nr("Qgage_constrained_nb_seg2", index, mm_ds, mm_dict)
                    self.__insert_nr("Qgage_constrained_x_seg2", index, mm_ds, mm_dict)
                    self.__insert_nr("n_bkfl_Qb_prior", index, mm_ds, mm_dict)
                    self.__insert_nr("n_bkfl_final_used", index, mm_ds, mm_dict)
                    self.__insert_nr("vel_bkfl_Qb_prior", index, mm_ds, mm_dict)
                    self.__insert_nr("vel_bkfl_diag_MBL", index, mm_ds, mm_dict)
                    self.__insert_nr("Froude_bkfl_diag_Smean", index, mm_ds, mm_dict)
                    self.__insert_nr("width_bkfl_empirical", index, mm_ds, mm_dict)
                    self.__insert_nr("width_bkfl_solved_obs", index, mm_ds, mm_dict)
                    self.__insert_nr("depth_bkfl_solved_obs", index, mm_ds, mm_dict)
                    self.__insert_nr("depth_bkfl_diag_MBL", index, mm_ds, mm_dict)
                    self.__insert_nr("depth_bkfl_diag_Wb_Smean", index, mm_ds, mm_dict)
                    self.__insert_nr("zero_flow_stage", index, mm_ds, mm_dict)
                    self.__insert_nr("bankfull_stage", index, mm_ds, mm_dict)
                    self.__insert_nr("Qmean_prior", index, mm_ds, mm_dict)
                    self.__insert_nr("Qmean_momma", index, mm_ds, mm_dict)
                    self.__insert_nr("Qmean_momma.constrained", index, mm_ds, mm_dict)                
                    mm_ds.close()
                index += 1
        return mm_dict
    
    def __create_mm_dict(self, nt):
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
            "Qmean_momma.constrained" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64)
        }

    def __insert_nr(self, name, index, mm_ds, mm_dict):
        """Insert discharge values into dictionary with nr dimension.

        Parameters
        ----------
        name: str
            name of data variable
        index: int
            integer index to insert data at
        mm_ds: netCDF4.Dataset
            MOMMA NetCDF that contains result data
        mm_dict: dict
            dictionary to store MOMMA results in
        """

        mm_dict[name][index] = mm_ds[name][:].filled(np.nan)

    def __insert_nt(self, name, index, mm_ds, mm_dict):
        """Insert discharge values into dictionary with nr by nt dimensions.
        
        Parameters
        ----------
        name: str
            name of data variable
        index: int
            integer index to insert data at
        mm_ds: netCDF4.Dataset
            MOMMA NetCDF that contains result data
        mm_dict: dict
            dictionary to store MOMMA results in
        """

        mm_dict[name][index, :] = mm_ds[name][:].filled(np.nan)
    
    def __create_mm_data(self, mm_dict):
        """Append MOMMA data to the new version of the SoS.
        
        Parameters
        ----------
        mm_dict: dict
            dictionary of MOMMA variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        mm_grp = sos_ds.createGroup("momma")

        # MOMMA data
        self.__write_var(mm_grp, "stage", ("num_reaches", "time_steps"), mm_dict)
        self.__write_var(mm_grp, "width", ("num_reaches", "time_steps"), mm_dict)
        self.__write_var(mm_grp, "slope", ("num_reaches", "time_steps"), mm_dict)
        self.__write_var(mm_grp, "Qgage", ("num_reaches", "time_steps"), mm_dict)
        self.__write_var(mm_grp, "seg", ("num_reaches", "time_steps"), mm_dict)
        self.__write_var(mm_grp, "n", ("num_reaches", "time_steps"), mm_dict)
        self.__write_var(mm_grp, "Y", ("num_reaches", "time_steps"), mm_dict)
        self.__write_var(mm_grp, "v", ("num_reaches", "time_steps"), mm_dict)
        self.__write_var(mm_grp, "Q", ("num_reaches", "time_steps"), mm_dict)
        self.__write_var(mm_grp, "Q_constrained", ("num_reaches", "time_steps"), mm_dict)
        self.__write_var(mm_grp, "gage_constrained", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "input_MBL_prior", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "input_Qm_prior", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "input_Qb_prior", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "input_Yb_prior", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "input_known_ezf", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "input_known_bkfl_stage", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "input_known_nb_seg1", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "input_known_x_seg1", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "Qgage_constrained_nb_seg1", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "Qgage_constrained_x_seg1", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "input_known_nb_seg2", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "input_known_x_seg2", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "Qgage_constrained_nb_seg2", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "Qgage_constrained_x_seg2", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "n_bkfl_Qb_prior", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "n_bkfl_final_used", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "vel_bkfl_Qb_prior", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "vel_bkfl_diag_MBL", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "Froude_bkfl_diag_Smean", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "width_bkfl_empirical", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "width_bkfl_solved_obs", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "depth_bkfl_solved_obs", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "depth_bkfl_diag_MBL", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "depth_bkfl_diag_Wb_Smean", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "zero_flow_stage", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "bankfull_stage", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "Qmean_prior", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "Qmean_momma", ("num_reaches",), mm_dict)
        self.__write_var(mm_grp, "Qmean_momma.constrained", ("num_reaches",), mm_dict)

        sos_ds.close()

    def __write_var(self, grp, name, dims, mm_dict):
        """Create NetCDF variable and write MOMMA data to it.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        dims: tuple
            tuple of NetCDF4 dimensions that matches shape of var dataa
        mm_dict: dict
            dictionary of MOMMA result data
        """

        var = grp.createVariable(name, "f8", dims, fill_value=self.FILL_VALUE)
        var[:] = np.nan_to_num(mm_dict[name], copy=True, nan=self.FILL_VALUE)