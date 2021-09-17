# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

class Sic4dvar:
    """
    A class that represents the results of running SIC4DVar.

    Data and operations append SIC4DVar results to the SoS on the appropriate
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
    append_sv()
        append SIC4DVar results to the SoS
    __create_sv_data(sv_dict)
        create variables and append SIC4DVar data to the new version of the SoS
    __create_sv_dict(nt)
        creates and returns SIC4DVar data dictionary
    __get_sv_data()
        extract SIC4DVar results from NetCDF files
    __insert_sv_data(sv_dict)
        insert SIC4DVar data into existing variables of the new version of the SoS
    __insert_nr(name, index, sv_ds, sv_dict)
        insert discharge values into dictionary with nr dimension
    __insert_nt(self, name, index, sv_ds, sv_dict):
        insert discharge values into dictionary with nr by nt dimensions
    __insert_var(grp, name, sv_dict)
        insert new SIC4DVar data into NetCDF variable
    __write_var(q_grp, name, dims, sv_dict)
        create NetCDF variable and write SIC4DVar data to it
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

    def append_sv(self, nt, version):
        """Append SIC4DVar results to the SoS.
        
        Parameters
        ----------
        nt: int
            number of time steps
        version: int
            unique identifier for SoS version
        """

        sv_dict = self.__get_sv_data(nt)
        # if int(version) == 1:
        #     self.__create_sv_data(sv_dict)
        # else:
        #     self.__insert_sv_data(sv_dict)

    def __get_sv_data(self, nt):
        """Extract SIC4DVar results from NetCDF files.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        # Files and reach identifiers
        sv_dir = self.input_dir / "sic4dvar"
        sv_files = [ Path(sv_file) for sv_file in glob.glob(f"{sv_dir}/{self.cont_ids}*.nc") ] 
        sv_rids = [ int(sv_file.name.split('_')[0]) for sv_file in sv_files ]

        # Storage of results data
        sv_dict = self.__create_sv_dict(nt)
        
        if len(sv_files) != 0:
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in sv_rids:
                    sv_ds = Dataset(sv_dir / f"{s_rid}_sic4dvar.nc", 'r')
                    self.__insert_nr("A0", index, sv_ds, sv_dict)
                    self.__insert_nr("n", index, sv_ds, sv_dict)
                    self.__insert_nt("Qalgo5", index, sv_ds, sv_dict)
                    self.__insert_nt("Qalgo31", index, sv_ds, sv_dict)
                    self.__insert_nx(s_rid, "half_width", sv_ds, sv_dict)
                    self.__insert_nx(s_rid, "elevation", sv_ds, sv_dict)
                    # if s_rid == 77449100061:
                    #     print(index)
                    #     print(sv_ds["A0"][:].filled(np.nan)[index])
                    #     print(sv_ds["n"][:].filled(np.nan)[index])
                    #     print(sv_ds["Qalgo5"][:].filled(np.nan)[index])
                    #     print(sv_ds["Qalgo31"][:].filled(np.nan)[index])
                    #     print(sv_ds["half_width"][:].filled(np.nan)[index])
                    #     print(sv_ds["elevation"][:].filled(np.nan)[index])
                    sv_ds.close()               
                index += 1
        return sv_dict
    
    def __create_sv_dict(self, nt):
        """Creates and returns SIC4DVar data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        return {
            "nt" : nt,
            "A0" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "n" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Qalgo5" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "Qalgo31" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "half_width": np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
            "elevation": np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64)
        }

    def __insert_nr(self, name, index, sv_ds, sv_dict):
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
            SIC4DVar NetCDF that contains result data
        mm_dict: dict
            dictionary to store SIC4DVar results in
        """

        sv_dict[name][index] = sv_ds[name][:].filled(np.nan)

    def __insert_nt(self, name, index, sv_ds, sv_dict):
        """Insert discharge values into dictionary with nr by nt dimensions.
        
        Parameters
        ----------
        name: str
            name of data variable
        index: int
            integer index to insert data at
        mm_ds: netCDF4.Dataset
            SIC4DVar NetCDF that contains result data
        mm_dict: dict
            dictionary to store SIC4DVar results in
        """

        sv_dict[name][index, :] = sv_ds[name][:].filled(np.nan)

    def __insert_nx(self, rid, name, sv_ds, sv_dict):
        """Append geoBam result data to dictionary with nx dimension.
        
        Parameters
        ----------
        rid: int
            unique reach identifier
        name: str
            name of data variable
        sv_ds: netCDF4.Dataset
            SIC4DVar NetCDF that contains result data
        sv_dict: dict
            dictionary to store SIC4DVar results in
        """

        print(name)
        # print(sv_ds[name][:])
        # print(sv_ds[name][:].shape)
        print(sv_ds[name][0])
        print(sv_ds[name][0].shape)

        # indexes = np.where(self.sos_nrids == rid)      
        # if rid == 77444000061:
        #     sv_dict[name][indexes] = np.append(sv_ds[name][:].filled(np.nan), [np.nan, np.nan])
        
        # elif rid == 77444000073:
        #     sv_dict[name][indexes] = np.insert(sv_ds[name][:].filled(np.nan), 0, np.nan)
        
        # else:
        #     sv_dict[name][indexes] = sv_ds[name][:].filled(np.nan)
