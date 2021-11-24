# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Sic4dvar(AbstractModule):
    """
    A class that represents the results of running SIC4DVar.

    Data and operations append SIC4DVar results to the SoS on the appropriate
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
    __insert_nx( rid, name, chain, gb_ds, gb_dict)
        append SIC4DVar result data to dictionary with nx dimension
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
        sv_dict = self.create_data_dict(nt)
        
        # Storage of variable attributes
        self.get_nc_attrs(sv_dir / sv_files[0], sv_dict)
        
        if len(sv_files) != 0:
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in sv_rids:
                    sv_ds = Dataset(sv_dir / f"{s_rid}_sic4dvar.nc", 'r')
                    sv_dict["A0"][index] = sv_ds["A0"][:].filled(np.nan)
                    sv_dict["n"][index] = sv_ds["n"][:].filled(np.nan)                    
                    sv_dict["Qalgo5"][index, :] = sv_ds["Qalgo5"][:].filled(np.nan)
                    sv_dict["Qalgo31"][index, :] = sv_ds["Qalgo31"][:].filled(np.nan)
                    self.__insert_nx(s_rid, "half_width", sv_ds, sv_dict)
                    self.__insert_nx(s_rid, "elevation", sv_ds, sv_dict)
                    indexes = np.where(s_rid == self.sos_nrids)
                    sv_dict["node_id"][indexes] = self.sos_nids[indexes]
                    sv_ds.close()
                index += 1
        return sv_dict
    
    def create_data_dict(self, nt=None):
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
            "node_id" : np.zeros(self.sos_nids.shape[0], dtype=np.int64),
            "attrs": {
                "A0" : None,
                "n" : None,
                "Qalgo5" : None,
                "Qalgo31" : None,
                "half_width": None,
                "elevation": None
            }
        }
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of SIC4DVar variables
        """
        
        ds = Dataset(nc_file, 'r')
        for key in data_dict["attrs"].keys():
            data_dict["attrs"][key] = ds[key].__dict__        
        ds.close()
        
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

    def append_module_data(self, data_dict):
        """Append SIC4DVar data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of SIC4DVar variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        sv_grp = sos_ds.createGroup("sic4dvar")

        # SIC4DVar data
        self.write_var(sv_grp, "A0", "f8", ("num_reaches",), data_dict)
        self.write_var(sv_grp, "n", "f8", ("num_reaches",), data_dict)
        self.write_var(sv_grp, "Qalgo31", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(sv_grp, "Qalgo5", "f8", ("num_reaches", "time_steps"), data_dict)

        # Variable length data
        indexes = np.where(data_dict["node_id"] != 0)
        
        sv_grp.createDimension("num_sic4dvar_nodes", None)
        nid = sv_grp.createVariable("sic4dvar_node_id", "i8", ("num_sic4dvar_nodes",))
        nid[:] = data_dict["node_id"][indexes]

        rid = sv_grp.createVariable("sic4dvar_reach_id", "i8", ("num_sic4dvar_nodes",))
        rid[:] = self.sos_nrids[indexes]

        vlen_t = sv_grp.createVLType(np.float64, "vlen")
        hw = sv_grp.createVariable("half_width", vlen_t, ("num_sic4dvar_nodes"))
        hw[:] = data_dict["half_width"][indexes]

        e = sv_grp.createVariable("elevation", vlen_t, ("num_sic4dvar_nodes"))
        e[:] = data_dict["elevation"][indexes]
        
        sos_ds.close()