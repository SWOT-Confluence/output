# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

class Sad:
    """
    A class that represents the results of running SAD.

    Data and operations append SAD results to the SoS on the appropriate
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
    append_sd()
        append SAD results to the SoS
    __create_sd_data(sd_dict)
        create variables and append SAD data to the new version of the SoS
    __create_sd_dict(nt)
        creates and returns SAD data dictionary
    __get_sd_data()
        extract SAD results from NetCDF files
    __insert_sd_data(sd_dict)
        insert SAD data into existing variables of the new version of the SoS
    __insert_nr(name, index, sd_ds, sd_dict)
        insert discharge values into dictionary with nr dimension
    __insert_nt(self, name, index, sd_ds, sd_dict):
        insert discharge values into dictionary with nr by nt dimensions
    __insert_var(grp, name, sd_dict)
        insert new SAD data into NetCDF variable
    __write_var(q_grp, name, dims, sd_dict)
        create NetCDF variable and write SAD data to it
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

    def append_sd(self, nt, version):
        """Append SAD results to the SoS.
        
        Parameters
        ----------
        nt: int
            number of time steps
        version: int
            unique identifier for SoS version
        """

        sd_dict = self.__get_sd_data(nt)
        if int(version) == 1:
            self.__create_sd_data(sd_dict)
        else:
            self.__insert_sd_data(sd_dict)

    def __get_sd_data(self, nt):
        """Extract SAD results from NetCDF files.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        # Files and reach identifiers
        sd_dir = self.input_dir / "sad"
        sd_files = [ Path(sd_file) for sd_file in glob.glob(f"{sd_dir}/{self.cont_ids}*.nc") ] 
        sd_rids = [ int(sd_file.name.split('_')[0]) for sd_file in sd_files ]

        # Storage of results data
        sd_dict = self.__create_sd_dict(nt)
        
        if len(sd_files) != 0:
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in sd_rids:
                    sd_ds = Dataset(sd_dir / f"{s_rid}_sad.nc", 'r')
                    self.__insert_nr("A0", index, sd_ds, sd_dict)
                    self.__insert_nr("n", index, sd_ds, sd_dict)
                    self.__insert_nt("Qa", index, sd_ds, sd_dict)
                    self.__insert_nr("Q_u", index, sd_ds, sd_dict)
                    sd_ds.close()               
                index += 1
        return sd_dict
    
    def __create_sd_dict(self, nt):
        """Creates and returns SAD data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        return {
            "nt" : nt,
            "A0" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "n" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Qa" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "Q_u" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64)
        }

    def __insert_nr(self, name, index, sd_ds, sd_dict):
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
            SAD NetCDF that contains result data
        mm_dict: dict
            dictionary to store SAD results in
        """

        sd_dict[name][index] = sd_ds[name][:].filled(np.nan)

    def __insert_nt(self, name, index, sd_ds, sd_dict):
        """Insert discharge values into dictionary with nr by nt dimensions.
        
        Parameters
        ----------
        name: str
            name of data variable
        index: int
            integer index to insert data at
        mm_ds: netCDF4.Dataset
            SAD NetCDF that contains result data
        mm_dict: dict
            dictionary to store SAD results in
        """

        sd_dict[name][index, :] = sd_ds[name][:].filled(np.nan)
    
    def __create_sd_data(self, sd_dict):
        """Append SAD data to the new version of the SoS.
        
        Parameters
        ----------
        sd_dict: dict
            dictionary of SAD variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        sd_grp = sos_ds.createGroup("sad")

        # SAD data
        self.__write_var(sd_grp, "A0", ("num_reaches",), sd_dict)
        self.__write_var(sd_grp, "n", ("num_reaches",), sd_dict)
        self.__write_var(sd_grp, "Qa", ("num_reaches", "time_steps"), sd_dict)
        self.__write_var(sd_grp, "Q_u", ("num_reaches", "time_steps"), sd_dict)
        
        sos_ds.close()

    def __write_var(self, grp, name, dims, sd_dict):
        """Create NetCDF variable and write SAD data to it.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        dims: tuple
            tuple of NetCDF4 dimensions that matches shape of var dataa
        sd_dict: dict
            dictionary of SAD result data
        """

        var = grp.createVariable(name, "f8", dims, fill_value=self.FILL_VALUE)
        var[:] = np.nan_to_num(sd_dict[name], copy=True, nan=self.FILL_VALUE)

    def __insert_sd_data(self, sd_dict):
        """Insert SAD data into existing variables of new SoS.
        
        Parameters
        ----------
        sd_dict: dict
            dictionary of SAD variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        sd_grp = sos_ds["sad"]

        self.__insert_var(sd_grp, "A0", sd_dict)
        self.__insert_var(sd_grp, "n", sd_dict)
        self.__insert_var(sd_grp, "Qa", sd_dict)
        self.__insert_var(sd_grp, "Q_u", sd_dict)

        sos_ds.close()

    def __insert_var(self, grp, name, sd_dict):
        """Insert new SAD data into NetCDF variable.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        sd_dict: dict
            dictionary of SAD result data
        """

        grp[name][:] = np.nan_to_num(sd_dict[name], copy=True, nan=self.FILL_VALUE)