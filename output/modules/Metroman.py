# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Metroman(AbstractModule):
    """
    A class that represents the results of running MetroMan.

    Data and operations append MetroMan results to the SoS on the appropriate
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
    get_nc_attrs(nc_file, data_dict)
        get NetCDF attributes for each NetCDF variable.
    __insert_nr(name, index, mn_ds, mn_dict)
        insert discharge values into dictionary with nr dimension
    __insert_nt(self, name, index, mn_ds, mn_dict):
        insert discharge values into dictionary with nr by nt dimensions
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
        mn_dict = self.create_data_dict(nt)
        
        if len(mn_files) != 0:
             # Storage of variable attributes
            self.get_nc_attrs(mn_dir / mn_files[0], mn_dict)
        
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

    def create_data_dict(self, nt=None):
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
            "attrs" : {
                "allq": None,
                "A0hat": None,
                "nahat": None,
                "x1hat": None,
                "q_u": None
            }
        }
        
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of MetroMan variables
        """
        
        ds = Dataset(nc_file, 'r')
        data_dict["attrs"]["allq"] = ds["allq"].__dict__
        data_dict["attrs"]["A0hat"] = ds["A0hat"].__dict__
        data_dict["attrs"]["nahat"] = ds["nahat"].__dict__
        data_dict["attrs"]["x1hat"] = ds["x1hat"].__dict__
        data_dict["attrs"]["q_u"] = ds["q_u"].__dict__
        ds.close()

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

    def append_module_data(self, data_dict):
        """Append MetroMan data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of MetroMan variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        mn_grp = sos_ds.createGroup("metroman")

        # MetroMan data
        self.write_var(mn_grp, "allq", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(mn_grp, "A0hat", "f8", ("num_reaches",), data_dict)
        self.write_var(mn_grp, "nahat", "f8", ("num_reaches",), data_dict)
        self.write_var(mn_grp, "x1hat", "f8", ("num_reaches",), data_dict)
        self.write_var(mn_grp, "q_u", "f8", ("num_reaches", "time_steps"), data_dict)

        sos_ds.close()