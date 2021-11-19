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
        self.__create_sv_data(sv_dict)

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
                    indexes = np.where(s_rid == self.sos_nrids)
                    sv_dict["node_id"][indexes] = self.sos_nids[indexes]
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
            "half_width": np.full((self.sos_nids.shape[0],1), np.array([np.nan]), dtype=object),
            "elevation": np.full((self.sos_nids.shape[0],1), np.array([np.nan]), dtype=object),
            "node_id" : np.zeros(self.sos_nids.shape[0], dtype=np.int64)
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
        """Append SIC4DVar result data to dictionary with nx dimension.

        Note that for two reaches certain nodes are missing from topology.
        
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

        indexes = np.where(self.sos_nrids == rid)
        if rid == 77444000061:
            size = sv_ds[name][0].shape[0]
            # Nodes with data
            i = indexes[0][0:-2]
            sv_dict[name][i,0] = sv_ds[name][:]
            # Fill in last two nodes that do not have any data for this reach
            j = indexes[0][-2]
            sv_dict[name][j,0] = np.full(size, np.nan)
            k = indexes[0][-1]
            sv_dict[name][k,0] = np.full(size, np.nan)
        
        elif rid == 77444000073:
            size = sv_ds[name][0].shape[0]
            # Nodes with data
            i = indexes[0][1:]
            sv_dict[name][i,0] = sv_ds[name][:]
            # Fill in first node that does not have data for this reach
            j = indexes[0][0] 
            sv_dict[name][j,0] = np.full(size, np.nan)
        
        else:
            sv_dict[name][indexes, 0] = sv_ds[name][:]

    def __create_sv_data(self, sv_dict):
        """Append SIC4DVar data to the new version of the SoS.
        
        Parameters
        ----------
        sv_dict: dict
            dictionary of SIC4DVar variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        sv_grp = sos_ds.createGroup("sic4dvar")

        # SIC4DVar data
        self.__write_var(sv_grp, "A0", ("num_reaches",), sv_dict)
        self.__write_var(sv_grp, "n", ("num_reaches",), sv_dict)
        self.__write_var(sv_grp, "Qalgo31", ("num_reaches", "time_steps"), sv_dict)
        self.__write_var(sv_grp, "Qalgo5", ("num_reaches", "time_steps"), sv_dict)

        # Variable length data
        indexes = np.where(sv_dict["node_id"] != 0)
        
        sv_grp.createDimension("num_sic4dvar_nodes", None)
        nid = sv_grp.createVariable("sic4dvar_node_id", "i8", ("num_sic4dvar_nodes",))
        nid[:] = sv_dict["node_id"][indexes]

        rid = sv_grp.createVariable("sic4dvar_reach_id", "i8", ("num_sic4dvar_nodes",))
        rid[:] = self.sos_nrids[indexes]

        vlen_t = sv_grp.createVLType(np.float64, "vlen")
        hw = sv_grp.createVariable("half_width", vlen_t, ("num_sic4dvar_nodes"))
        hw[:] = sv_dict["half_width"][indexes]

        e = sv_grp.createVariable("elevation", vlen_t, ("num_sic4dvar_nodes"))
        e[:] = sv_dict["elevation"][indexes]
        
        sos_ds.close()

    def __write_var(self, grp, name, dims, sv_dict):
        """Create NetCDF variable and write SIC4DVar data to it.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        dims: tuple
            tuple of NetCDF4 dimensions that matches shape of var dataa
        sv_dict: dict
            dictionary of SIC4DVar result data
        """

        var = grp.createVariable(name, "f8", dims, fill_value=self.FILL_VALUE)
        var[:] = np.nan_to_num(sv_dict[name], copy=True, nan=self.FILL_VALUE)