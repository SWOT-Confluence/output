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
    create_data_dict()
        creates and returns module data dictionary.
    get_module_data()
        retrieve module results from NetCDF files.
    get_nc_attrs(nc_file, data_dict)
        get NetCDF attributes for each NetCDF variable.
    __insert_nx( rid, name, chain, gb_ds, gb_dict)
        append SIC4DVar result data to dictionary with nx dimension
    """

    def __init__(self, cont_ids, input_dir, sos_new, vlen_f, vlen_i, vlen_s,
                 rids, nrids, nids):
        
        """
        Parameters
        ----------
        cont_ids: list
            list of continent identifiers
        input_dir: Path
            path to input directory
        sos_new: Path
            path to new SOS file
        vlen_f: VLType
            variable length float data type for NetCDF ragged arrays
        vlen_i: VLType
            variable length int data type for NEtCDF ragged arrays
        vlen_s: VLType
            variable length string data type for NEtCDF ragged arrays
        rids: nd.array
            array of SoS reach identifiers associated with continent
        nrids: nd.array
            array of SOS reach identifiers on the node-level
        nids: nd.array
            array of SOS node identifiers
        """

        super().__init__(cont_ids, input_dir, sos_new, vlen_f, vlen_i, vlen_s, \
            rids, nrids, nids)
        
    
    def get_module_data(self):
        """Extract SIC4DVar results from NetCDF files."""

        # Files and reach identifiers
        sv_dir = self.input_dir / "sic4dvar"
        sv_files = [ Path(sv_file) for sv_file in glob.glob(f"{sv_dir}/{self.cont_ids}*.nc") ] 
        sv_rids = [ int(sv_file.name.split('_')[0]) for sv_file in sv_files ]

        # Storage of results data
        sv_dict = self.create_data_dict()
        
        if len(sv_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(sv_dir / sv_files[0], sv_dict)
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in sv_rids:
                    sv_ds = Dataset(sv_dir / f"{int(s_rid)}_sic4dvar.nc", 'r')
                    sv_dict["A0"][index] = sv_ds["A0"][:].filled(np.nan)
                    sv_dict["n"][index] = sv_ds["n"][:].filled(np.nan)                    
                    # sv_dict["Qalgo5"][index] = sv_ds["Qalgo5"][:].filled(self.FILL["f8"])
                    # sv_dict["Qalgo31"][index] = sv_ds["Qalgo31"][:].filled(self.FILL["f8"])
                    sv_dict["Q_mm"][index] = sv_ds["Q_mm"][:].filled(self.FILL["f8"])
                    sv_dict["Q_da"][index] = sv_ds["Q_da"][:].filled(self.FILL["f8"])
                    indexes = np.where(s_rid == self.sos_nrids)
                    sv_dict["node_id"][indexes] = self.sos_nids[indexes]
                    # self.__insert_nx(sv_dict, sv_ds, indexes)
                    sv_ds.close()
                index += 1
        return sv_dict
    
    def create_data_dict(self):
        """Creates and returns SIC4DVar data dictionary."""

        data_dict = {
            "A0" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "n" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "Q_mm" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "Q_da" : np.empty((self.sos_rids.shape[0]), dtype=object),
            # "Qalgo31" : np.empty((self.sos_rids.shape[0]), dtype=object),
            # "half_width": np.empty((self.sos_nids.shape[0]), dtype=object),
            # "elevation": np.empty((self.sos_nids.shape[0]), dtype=object),
            "node_id" : np.zeros(self.sos_nids.shape[0], dtype=np.int64),
            "attrs": {
                "A0" : {},
                "n" : {},
                "Q_mm" : {},
                "Q_da":{},
                # "Qalgo31" : {},
                # "half_width": {},
                # "elevation": {}
            }
        }
        
        # Vlen variables
        data_dict["Q_mm"].fill(np.array([self.FILL["f8"]]))
        data_dict["Q_da"].fill(np.array([self.FILL["f8"]]))
        # data_dict["Qalgo31"].fill(np.array([self.FILL["f8"]]))
        # data_dict["half_width"].fill(np.array([self.FILL["f8"]]))
        # data_dict["elevation"].fill(np.array([self.FILL["f8"]]))
        return data_dict
    
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
        
    def __insert_nx(self, sv_dict, sv_ds, indexes):
        """Append SIC4DVar result data to dictionary with nx dimension.
        
        Parameters
        ----------
        sv_dict: dict
            dictionary of sic4dvar results
        sv_ds: netCDF4.Dataset
            sic4dvar NetCDF dataset reference
        indexes: list
            list of integer indexes to insert node flags at
        """

        j = 0
        for i in indexes[0]:
            sv_dict["half_width"][i] = np.nan_to_num(sv_ds["half_width"][j], copy=True, nan=self.FILL["f8"])
            sv_dict["elevation"][i] = np.nan_to_num(sv_ds["elevation"][j], copy=True, nan=self.FILL["f8"])
            j += 1

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
        self.write_var_nt(sv_grp, "Q_mm", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(sv_grp, "Q_da", self.vlen_f, ("num_reaches"), data_dict)
        # self.write_var_nt(sv_grp, "Qalgo5", self.vlen_f, ("num_reaches"), data_dict)

        # no longer using elevation or half width
        # self.write_var_nt(sv_grp, "elevation", self.vlen_f, ("num_nodes"), data_dict)
        # self.write_var_nt(sv_grp, "half_width", self.vlen_f, ("num_nodes"), data_dict)

        # Variable length data
        # indexes = np.where(data_dict["node_id"] != 0)
        # sv_grp.createDimension("num_sic4dvar_nodes", None)
        # nid = sv_grp.createVariable("sic4dvar_node_id", "i8", ("num_sic4dvar_nodes",))
        # nid[:] = data_dict["node_id"][indexes]
        # rid = sv_grp.createVariable("sic4dvar_reach_id", "i8", ("num_sic4dvar_nodes",))
        # rid[:] = self.sos_nrids[indexes]

        # vlen_t = sv_grp.createVLType(np.float64, "vlen")
        # hw = sv_grp.createVariable("half_width", vlen_t, ("num_sic4dvar_nodes"))
        # hw[:] = data_dict["half_width"][indexes]

        # e = sv_grp.createVariable("elevation", vlen_t, ("num_sic4dvar_nodes"))
        # e[:] = data_dict["elevation"][indexes]
        
        sos_ds.close()