# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

class Metroman:
    """
    A class that represents the results of running MetroMan.

    Data and operations append MetroMan results to the SoS on the appropriate
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
    append_mn()
        append MetroMan results to the SoS
    __create_mn_data(mn_dict)
        create variables and append MetroMan data to the new version of the SoS
    __create_mn_dict(nt)
        creates and returns MetroMan data dictionary
    __get_mn_data()
        extract MetroMan results from NetCDF files
    __insert_mn_data(mn_dict)
        insert MetroMan data into existing variables of the new version of the SoS
    __insert_nr(name, index, mn_ds, mn_dict)
        insert discharge values into dictionary with nr dimension
    __insert_nt(self, name, index, mn_ds, mn_dict):
        insert discharge values into dictionary with nr by nt dimensions
    __insert_var(grp, name, mn_dict)
        insert new MetroMan data into NetCDF variable
    __write_var(q_grp, name, dims, mn_dict)
        create NetCDF variable and write MetroMan data to it
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

    def append_mn(self, nt, version):
        """Append MetroMan results to the SoS.
        
        Parameters
        ----------
        nt: int
            number of time steps
        version: int
            unique identifier for SoS version
        """

        mn_dict = self.__get_mn_data(nt)
        self.__create_mn_data(mn_dict)

    def __get_mn_data(self, nt):
        """Extract MetroMan results from NetCDF files.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        # Files and reach identifiers
        mn_dir = self.input_dir / "metroman"
        mn_files = [ Path(mn_file) for mn_file in glob.glob(f"{mn_dir}/{self.cont_ids}*.nc") ] 
        mn_rids = [ mn_file.name.split('_')[0].split('-') for mn_file in mn_files ]
        mn_rids = [ int(rid) for rid_list in mn_rids for rid in rid_list ]

        # Storage of results data
        mn_dict = self.__create_mn_dict(nt)
        
        if len(mn_files) != 0:
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in mn_rids:
                    mn_file = [ f for f in glob.glob(f"{mn_dir}/*{s_rid}*.nc") ][0]
                    mn_ds = Dataset(mn_file, 'r')
                    self.__insert_nt(s_rid, "allq", index, mn_ds, mn_dict)
                    self.__insert_nt(s_rid, "q_u", index, mn_ds, mn_dict)
                    self.__insert_nr(s_rid, "A0hat", index, mn_ds, mn_dict)
                    self.__insert_nr(s_rid, "nahat", index, mn_ds, mn_dict)
                    self.__insert_nr(s_rid, "x1hat", index, mn_ds, mn_dict)
                    mn_ds.close()
                index += 1
        return mn_dict

    def __create_mn_dict(self, nt):
        """Creates and returns MetroMan data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        return {
            "nt" : nt,
            "allq" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "A0hat" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "nahat" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "x1hat" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "q_u" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
        }

    def __insert_nr(self, s_rid, name, index, mn_ds, mn_dict):
        """Insert discharge values into dictionary with nr dimension.

        Parameters
        ----------
        s_rid: int
            unique reach identifier to locate data for
        name: str
            name of data variable
        index: int
            integer index to insert data at
        mn_ds: netCDF4.Dataset
            MetroMan NetCDF that contains result data
        mn_dict: dict
            dictionary to store MetroMan results in
        """

        mn_index = np.where(mn_ds["reach_id"][:] == s_rid)[0][0]
        mn_dict[name][index] = mn_ds[name][mn_index].filled(np.nan)

    def __insert_nt(self, s_rid, name, index, mn_ds, mn_dict):
        """Insert discharge values into dictionary with nr by nt dimensions.
        
        Parameters
        ----------
        s_rid: int
            unique reach identifier to locate data for
        name: str
            name of data variable
        index: int
            integer index to insert data at
        mn_ds: netCDF4.Dataset
            MetroMan NetCDF that contains result data
        mn_dict: dict
            dictionary to store MetroMan results in
        """

        mn_index = np.where(mn_ds["reach_id"][:] == s_rid)[0][0]
        mn_dict[name][index, :] = mn_ds[name][mn_index,:].filled(np.nan)

    def __create_mn_data(self, mn_dict):
        """Append MetroMan data to the new version of the SoS.
        
        Parameters
        ----------
        mn_dict: dict
            dictionary of MetroMan variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        mn_grp = sos_ds.createGroup("metroman")

        # MetroMan data
        self.__write_var(mn_grp, "allq", ("num_reaches", "time_steps"), mn_dict)
        self.__write_var(mn_grp, "A0hat", ("num_reaches",), mn_dict)
        self.__write_var(mn_grp, "nahat", ("num_reaches",), mn_dict)
        self.__write_var(mn_grp, "x1hat", ("num_reaches",), mn_dict)
        self.__write_var(mn_grp, "q_u", ("num_reaches", "time_steps"), mn_dict)

        sos_ds.close()

    def __write_var(self, grp, name, dims, mn_dict):
        """Create NetCDF variable and write MetroMan data to it.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        dims: tuple
            tuple of NetCDF4 dimensions that matches shape of var dataa
        mn_dict: dict
            dictionary of MetroMan result data
        """

        var = grp.createVariable(name, "f8", dims, fill_value=self.FILL_VALUE)
        var[:] = np.nan_to_num(mn_dict[name], copy=True, nan=self.FILL_VALUE)