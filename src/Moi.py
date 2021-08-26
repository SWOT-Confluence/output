# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

class Moi:
    """A class that represent the results of running MOI.
    
    Data and operations append MOI results to the SoS on the appropriate 
    dimenstions.

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
    append_moi()
        append MOI results to the SoS
    __append_moi_data( moi_dict)
        append MOI data to the new version of the SoS
    __create_moi_dict(nt)
        creates and returns MOI data dictionary
    __get_moi_data()
        extract MOI results from NetCDF files
    __insert_nr(name, chain, index, moi_ds, moi_dict)
        insert discharge values into dictionary with nr dimension
     __insert_nt(self, name, chain, index, moi_ds, moi_dict):
        insert discharge values into dictionary with nr by nt dimensions
    __write_var(q_grp, name, chain, dims, moi_dict)
        create NetCDF variable and write MOI data to it
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

    def append_moi(self, nt):
        """Append MOI results to the SoS.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        moi_dict = self.__get_moi_data(nt)
        self.__append_moi_data(moi_dict)

    def __get_moi_data(self, nt):
        """Extract MOI results from NetCDF files.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        # Files and reach identifiers
        moi_dir = self.input_dir
        moi_files = [ Path(moi_file) for moi_file in glob.glob(f"{moi_dir}/{self.cont_ids}*.nc") ] 
        moi_rids = [ int(moi_file.name.split('_')[0]) for moi_file in moi_files ]

        # Storage of results data
        moi_dict = self.__create_moi_dict(nt)
        
        if len(moi_files) != 0:
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in moi_rids:
                    moi_ds = Dataset(moi_dir / f"{s_rid}_integrator.nc", 'r')
                    self.__insert_nt("geobam", "q", index, moi_ds, moi_dict)
                    self.__insert_nr("geobam", "a0", index, moi_ds, moi_dict)
                    self.__insert_nr("geobam", "n", index, moi_ds, moi_dict)
                    self.__insert_nr("geobam", "qbar_reachScale", index, moi_ds, moi_dict)
                    self.__insert_nr("geobam", "qbar_basinScale", index, moi_ds, moi_dict)
                    
                    self.__insert_nt("hivdi", "q", index, moi_ds, moi_dict)
                    self.__insert_nr("hivdi", "Abar", index, moi_ds, moi_dict)
                    self.__insert_nr("hivdi", "alpha", index, moi_ds, moi_dict)
                    self.__insert_nr("hivdi", "beta", index, moi_ds, moi_dict)
                    self.__insert_nr("hivdi", "qbar_reachScale", index, moi_ds, moi_dict)
                    self.__insert_nr("hivdi", "qbar_basinScale", index, moi_ds, moi_dict)
                    
                    self.__insert_nt("metroman", "q", index, moi_ds, moi_dict)
                    self.__insert_nr("metroman", "Abar", index, moi_ds, moi_dict)
                    self.__insert_nr("metroman", "na", index, moi_ds, moi_dict)
                    self.__insert_nr("metroman", "x1", index, moi_ds, moi_dict)
                    self.__insert_nr("metroman", "qbar_reachScale", index, moi_ds, moi_dict)
                    self.__insert_nr("metroman", "qbar_basinScale", index, moi_ds, moi_dict)
                    
                    self.__insert_nt("momma", "q", index, moi_ds, moi_dict)
                    self.__insert_nr("momma", "B", index, moi_ds, moi_dict)
                    self.__insert_nr("momma", "H", index, moi_ds, moi_dict)
                    self.__insert_nr("momma", "Save", index, moi_ds, moi_dict)
                    self.__insert_nr("momma", "qbar_reachScale", index, moi_ds, moi_dict)
                    self.__insert_nr("momma", "qbar_basinScale", index, moi_ds, moi_dict)
                    
                    moi_ds.close()
                index += 1
        return moi_dict
    
    def __create_moi_dict(self, nt):
        """Creates and returns MOI data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        return {
            "nt" : nt,
            "geobam" : {
                "q" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "a0" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "n" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64)
            },
            "hivdi" : {
                "q" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "Abar" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "alpha" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "beta" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64)
            },
            "metroman" : {
                "q" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "Abar" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "na" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "x1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64)
            },
            "momma" : {
                "q" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "B" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "H" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "Save" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64)
            }
        }

    def __insert_nr(self, alg, name, index, moi_ds, moi_dict):
        """Insert discharge values into dictionary with nr dimension.

        Parameters
        ----------
        alg: str
            name of algorithm
        name: str
            name of data variable
        index: int
            integer index to insert data at
        moi_ds: netCDF4.Dataset
            MOI NetCDF that contains result data
        moi_dict: dict
            dictionary to store MOI results in
        """

        moi_dict[alg][name][index] = moi_ds[alg][name][:].filled(np.nan)

    def __insert_nt(self, alg, name, index, moi_ds, moi_dict):
        """Insert discharge values into dictionary with nr by nt dimensions.
        
        Parameters
        ----------
        alg: str
            name of algorithm
        name: str
            name of data variable
        index: int
            integer index to insert data at
        moi_ds: netCDF4.Dataset
            MOI NetCDF that contains result data
        moi_dict: dict
            dictionary to store MOI results in
        """

        moi_dict[alg][name][index, :] = moi_ds[alg][name][:].filled(np.nan)

    def __append_moi_data(self, moi_dict):
        """Append MOI data to the new version of the SoS.
        
        Parameters
        ----------
        moi_dict: dict
            dictionary of MOI variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        moi_grp = sos_ds.createGroup("moi")

        # MOI data
        # geobam
        gb_grp = moi_grp.createGroup("geobam")
        self.__write_var(gb_grp, "q", ("num_reaches", "time_steps"), moi_dict["geobam"])
        self.__write_var(gb_grp, "a0", ("num_reaches",), moi_dict["geobam"])
        self.__write_var(gb_grp, "n", ("num_reaches",), moi_dict["geobam"])
        self.__write_var(gb_grp, "qbar_reachScale", ("num_reaches",), moi_dict["geobam"])
        self.__write_var(gb_grp, "qbar_basinScale", ("num_reaches",), moi_dict["geobam"])

        # hivdi
        hv_grp = moi_grp.createGroup("hivdi")
        self.__write_var(hv_grp, "q", ("num_reaches", "time_steps"), moi_dict["hivdi"])
        self.__write_var(hv_grp, "Abar", ("num_reaches",), moi_dict["hivdi"])
        self.__write_var(hv_grp, "alpha", ("num_reaches",), moi_dict["hivdi"])
        self.__write_var(hv_grp, "beta", ("num_reaches",), moi_dict["hivdi"])
        self.__write_var(hv_grp, "qbar_reachScale", ("num_reaches",), moi_dict["hivdi"])
        self.__write_var(hv_grp, "qbar_basinScale", ("num_reaches",), moi_dict["hivdi"])

        # metroman
        mm_grp = moi_grp.createGroup("metroman")
        self.__write_var(mm_grp, "q", ("num_reaches", "time_steps"), moi_dict["metroman"])
        self.__write_var(mm_grp, "Abar", ("num_reaches",), moi_dict["metroman"])
        self.__write_var(mm_grp, "na", ("num_reaches",), moi_dict["metroman"])
        self.__write_var(mm_grp, "x1", ("num_reaches",), moi_dict["metroman"])
        self.__write_var(mm_grp, "qbar_reachScale", ("num_reaches",), moi_dict["metroman"])
        self.__write_var(mm_grp, "qbar_basinScale", ("num_reaches",), moi_dict["metroman"])

        # momma
        mo_grp = moi_grp.createGroup("momma")
        self.__write_var(mo_grp, "q", ("num_reaches", "time_steps"), moi_dict["momma"])
        self.__write_var(mo_grp, "B", ("num_reaches",), moi_dict["momma"])
        self.__write_var(mo_grp, "H", ("num_reaches",), moi_dict["momma"])
        self.__write_var(mo_grp, "Save", ("num_reaches",), moi_dict["momma"])
        self.__write_var(mo_grp, "qbar_reachScale", ("num_reaches",), moi_dict["momma"])
        self.__write_var(mo_grp, "qbar_basinScale", ("num_reaches",), moi_dict["momma"])

        sos_ds.close()

    def __write_var(self, grp, name, dims, moi_dict):
        """Create NetCDF variable and write MOI data to it.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        dims: tuple
            tuple of NetCDF4 dimensions that matches shape of var dataa
        moi_dict: dict
            dictionary of MOI result data
        """

        var = grp.createVariable(name, "f8", dims, fill_value=self.FILL_VALUE)
        var[:] = np.nan_to_num(moi_dict[name], copy=True, nan=self.FILL_VALUE)