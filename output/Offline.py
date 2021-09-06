# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

class Offline:
    """
    A class that represents the results of running the Offline module.
    
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
    append_off()
        append Offline results to the SoS
    __create_off_data(off_dict)
        create variables and append Offline data to the new version of the SoS
    __create_off_dict(nt)
        creates and returns Offline data dictionary
    __get_off_data()
        extract Offline results from NetCDF files
    __insert_off_data(off_dict)
        insert Offline data into existing variables off the new version of the SoS
    __insert_nr(name, index, off_ds, off_dict)
        insert discharge values into dictionary with nr dimension
    __insert_nt(self, name, index, off_ds, off_dict):
        insert discharge values into dictionary with nr by nt dimensions
    __insert_var(grp, name, off_dict)
        insert new Offline data into NetCDF variable
    __write_var(q_grp, name, dims, off_dict)
        create NetCDF variable and write Offline data to it
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

    def append_off(self, nt, version):
        """Append Offline results to the SoS.
        
        Parameters
        ----------
        nt: int
            number of time steps
        version: int
            unique identifier for SoS version
        """

        off_dict = self.__get_off_data(nt)
        if int(version) == 1:
            self.__create_off_data(off_dict)
        else:
            self.__insert_off_data(off_dict)

    def __get_off_data(self, nt):
        """Extract Offline results from NetCDF files.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        # Files and reach identifiers
        off_dir = self.input_dir
        off_files = [ Path(off_file) for off_file in glob.glob(f"{off_dir}/{self.cont_ids}*.nc") ] 
        off_rids = [ off_file.name.split('_')[0].split('-') for off_file in off_files ]
        off_rids = [ int(rid) for rid_list in off_rids for rid in rid_list ]

        # # Storage of results data
        off_dict = self.__create_off_dict(nt)
        
        if len(off_files) != 0:
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in off_rids:
                    off_file = [ f for f in glob.glob(f"{off_dir}/*{s_rid}*.nc") ][0]
                    off_ds = Dataset(off_file, 'r')
                    self.__insert_nt("d_x_area", index, off_ds, off_dict)
                    self.__insert_nt("d_x_area_u", index, off_ds, off_dict)
                    self.__insert_nt("metro_q_c", index, off_ds, off_dict)
                    self.__insert_nt("bam_q_c", index, off_ds, off_dict)
                    self.__insert_nt("hivdi_q_c", index, off_ds, off_dict)
                    self.__insert_nt("momma_q_c", index, off_ds, off_dict)
                    self.__insert_nt("sads_q_c", index, off_ds, off_dict)
                    self.__insert_nt("metro_q_uc", index, off_ds, off_dict)
                    self.__insert_nt("bam_q_uc", index, off_ds, off_dict)
                    self.__insert_nt("hivdi_q_uc", index, off_ds, off_dict)
                    self.__insert_nt("momma_q_uc", index, off_ds, off_dict)
                    self.__insert_nt("sads_q_uc", index, off_ds, off_dict)
                    off_ds.close()
                index += 1
        return off_dict

    def __create_off_dict(self, nt):
        """Creates and returns Offline data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        return {
            "nt" : nt,
            "d_x_area" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "d_x_area_u" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "metro_q_c" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "bam_q_c" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "hivdi_q_c" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "momma_q_c" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "sads_q_c" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "metro_q_uc" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "bam_q_uc" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "hivdi_q_uc" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "momma_q_uc" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "sads_q_uc" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64)
        }

    def __insert_nt(self, name, index, off_ds, off_dict):
        """Insert discharge values into dictionary with nr by nt dimensions.
        
        Parameters
        ----------
        name: str
            name of data variable
        index: int
            integer index to insert data at
        off_ds: netCDF4.Dataset
            Offline NetCDF that contains result data
        off_dict: dict
            dictionary to store Offline results in
        """

        off_dict[name][index, :] = off_ds[name][:].filled(np.nan)

    def __create_off_data(self, off_dict):
        """Append Offline data to the new version of the SoS.
        
        Parameters
        ----------
        off_dict: dict
            dictionary of Offline variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        off_grp = sos_ds.createGroup("offline")

        # Offline data
        self.__write_var(off_grp, "d_x_area", ("num_reaches", "time_steps"), off_dict)
        self.__write_var(off_grp, "d_x_area_u", ("num_reaches", "time_steps"), off_dict)
        self.__write_var(off_grp, "metro_q_c", ("num_reaches", "time_steps"), off_dict)
        self.__write_var(off_grp, "bam_q_c", ("num_reaches", "time_steps"), off_dict)
        self.__write_var(off_grp, "hivdi_q_c", ("num_reaches", "time_steps"), off_dict)
        self.__write_var(off_grp, "momma_q_c", ("num_reaches", "time_steps"), off_dict)
        self.__write_var(off_grp, "sads_q_c", ("num_reaches", "time_steps"), off_dict)
        self.__write_var(off_grp, "metro_q_uc", ("num_reaches", "time_steps"), off_dict)
        self.__write_var(off_grp, "bam_q_uc", ("num_reaches", "time_steps"), off_dict)
        self.__write_var(off_grp, "hivdi_q_uc", ("num_reaches", "time_steps"), off_dict)
        self.__write_var(off_grp, "momma_q_uc", ("num_reaches", "time_steps"), off_dict)
        self.__write_var(off_grp, "sads_q_uc", ("num_reaches", "time_steps"), off_dict)

        sos_ds.close()

    def __write_var(self, grp, name, dims, off_dict):
        """Create NetCDF variable and write Offline data to it.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        dims: tuple
            tuple of NetCDF4 dimensions that matches shape of var dataa
        off_dict: dict
            dictionary of Offline result data
        """

        var = grp.createVariable(name, "f8", dims, fill_value=self.FILL_VALUE)
        var[:] = np.nan_to_num(off_dict[name], copy=True, nan=self.FILL_VALUE)
    
    def __insert_off_data(self, off_dict):
        """Insert Offline data into existing variables of new SoS.
        
        Parameters
        ----------
        off_dict: dict
            dictionary of Offline variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        off_grp = sos_ds["offline"]

        self.__insert_var(off_grp, "d_x_area", off_dict)
        self.__insert_var(off_grp, "d_x_area_u", off_dict)
        self.__insert_var(off_grp, "metro_q_c", off_dict)
        self.__insert_var(off_grp, "bam_q_c", off_dict)
        self.__insert_var(off_grp, "hivdi_q_c", off_dict)
        self.__insert_var(off_grp, "momma_q_c", off_dict)
        self.__insert_var(off_grp, "sads_q_c", off_dict)
        self.__insert_var(off_grp, "metro_q_uc", off_dict)
        self.__insert_var(off_grp, "bam_q_uc", off_dict)
        self.__insert_var(off_grp, "hivdi_q_uc", off_dict)
        self.__insert_var(off_grp, "momma_q_uc", off_dict)
        self.__insert_var(off_grp, "sads_q_uc", off_dict)
        
        sos_ds.close()

    def __insert_var(self, grp, name, off_dict):
        """Insert new Offline data into NetCDF variable.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        off_dict: dict
            dictionary of Offline result data
        """

        grp[name][:] = np.nan_to_num(off_dict[name], copy=True, nan=self.FILL_VALUE)