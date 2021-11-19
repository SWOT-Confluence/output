# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

class Hivdi:
    """
    A class that represents the results of running HiVDI.

    Data and operations append HiVDI results to the SoS on the appropriate
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
    append_hv()
        append HiVDI results to the SoS
    __create_hv_data(hv_dict)
        create variables and append HiVDI data to the new version of the SoS
    __create_hv_dict(nt)
        creates and returns HiVDI data dictionary
    __get_hv_data()
        extract HiVDI results from NetCDF files
    __insert_hv_data(hv_dict)
        insert HiVDI data into existing variables of the new version of the SoS
    __insert_nr(name, index, hv_ds, hv_dict)
        insert discharge values into dictionary with nr dimension
    __insert_nt(self, name, index, hv_ds, hv_dict):
        insert discharge values into dictionary with nr by nt dimensions
    __insert_var(grp, name, hv_dict)
        insert new HiVDI data into NetCDF variable
    __write_var(q_grp, name, dims, hv_dict)
        create NetCDF variable and write HiVDI data to it
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

    def append_hv(self, nt):
        """Append HiVDI results to the SoS.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        hv_dict = self.__get_hv_data(nt)
        self.__create_hv_data(hv_dict)

    def __get_hv_data(self, nt):
        """Extract HiVDI results from NetCDF files.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        # Files and reach identifiers
        hv_dir = self.input_dir / "hivdi"
        hv_files = [ Path(hv_file) for hv_file in glob.glob(f"{hv_dir}/{self.cont_ids}*.nc") ] 
        hv_rids = [ int(hv_file.name.split('_')[0]) for hv_file in hv_files ]

        # Storage of results data
        hv_dict = self.__create_hv_dict(nt)
        
        if len(hv_files) != 0:
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in hv_rids:
                    hv_ds = Dataset(hv_dir / f"{s_rid}_hivdi.nc", 'r')
                    hv_grp = hv_ds["reach"]
                    self.__insert_nt("Q", index, hv_grp, hv_dict["reach"])
                    self.__insert_nr("A0", index, hv_grp, hv_dict["reach"])
                    self.__insert_nr("alpha", index, hv_grp, hv_dict["reach"])
                    self.__insert_nr("beta", index, hv_grp, hv_dict["reach"])
                    hv_ds.close()               
                index += 1
        return hv_dict
    
    def __create_hv_dict(self, nt):
        """Creates and returns HiVDI data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        return {
            "nt" : nt,
            "reach" : {
                "Q" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "A0" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "alpha" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "beta" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64)
            }
        }

    def __insert_nr(self, name, index, hv_ds, hv_dict):
        """Insert discharge values into dictionary with nr dimension.

        Parameters
        ----------
        name: str
            name of data variable
        chain: str
            mean or standard deviation chain
        index: int
            integer index to insert data at
        mm_ds: netCDF4.Dataset
            HiVDI NetCDF that contains result data
        mm_dict: dict
            dictionary to store HiVDI results in
        """

        hv_dict[name][index] = hv_ds[name][:].filled(np.nan)

    def __insert_nt(self, name, index, hv_ds, hv_dict):
        """Insert discharge values into dictionary with nr by nt dimensions.
        
        Parameters
        ----------
        name: str
            name of data variable
        index: int
            integer index to insert data at
        mm_ds: netCDF4.Dataset
            HiVDI NetCDF that contains result data
        mm_dict: dict
            dictionary to store HiVDI results in
        """

        hv_dict[name][index, :] = hv_ds[name][:].filled(np.nan)
    
    def __create_hv_data(self, hv_dict):
        """Append HiVDI data to the new version of the SoS.
        
        Parameters
        ----------
        hv_dict: dict
            dictionary of HiVDI variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        hv_grp = sos_ds.createGroup("hivdi")

        # HiVDI data
        self.__write_var(hv_grp, "Q", ("num_reaches", "time_steps"), hv_dict["reach"])
        self.__write_var(hv_grp, "A0", ("num_reaches",), hv_dict["reach"])
        self.__write_var(hv_grp, "beta", ("num_reaches",), hv_dict["reach"])
        self.__write_var(hv_grp, "alpha", ("num_reaches",), hv_dict["reach"])

        sos_ds.close()

    def __write_var(self, grp, name, dims, mm_dict):
        """Create NetCDF variable and write HiVDI data to it.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        dims: tuple
            tuple of NetCDF4 dimensions that matches shape of var dataa
        mm_dict: dict
            dictionary of HiVDI result data
        """

        var = grp.createVariable(name, "f8", dims, fill_value=self.FILL_VALUE)
        var[:] = np.nan_to_num(mm_dict[name], copy=True, nan=self.FILL_VALUE)