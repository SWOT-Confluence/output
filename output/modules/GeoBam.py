# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class GeoBAM(AbstractModule):
    """
    A class that represents the results of running geoBAM.

    Data and operations append geoBAM results to the SoS on the appropriate
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
    __insert_nr(name, chain, index, gb_ds, gb_dict)
        insert discharge values into dictionary with nr dimension
    __insert_nt(self, name, chain, index, gb_ds, gb_dict):
        insert discharge values into dictionary with nr by nt dimensions
    __insert_nx( rid, name, chain, gb_ds, gb_dict)
        append geoBam result data to dictionary with nx dimension
    write_var(q_grp, name, dims, sv_dict)
        create NetCDF variable and write module data to it
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
        """Extract geoBAM results from NetCDF files.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        # Files and reach identifiers
        gb_dir = self.input_dir / "geobam"
        gb_files = [ Path(gb_file) for gb_file in glob.glob(f"{gb_dir}/{self.cont_ids}*.nc") ]       
        gb_rids = [ int(gb_file.name.split('_')[0]) for gb_file in gb_files ]

        # Storage of results data
        gb_dict = self.create_data_dict(nt)
        
        if len(gb_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(gb_dir / gb_files[0], gb_dict)
        
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in gb_rids:
                    gb_ds = Dataset(gb_dir / f"{s_rid}_geobam.nc", 'r')
                    self.__insert_nt("logQ", "mean", index, gb_ds, gb_dict)
                    self.__insert_nt("logQ", "sd", index, gb_ds, gb_dict)
                    self.__insert_nr("logWc", "mean", index, gb_ds, gb_dict)
                    self.__insert_nr("logWc", "sd", index, gb_ds, gb_dict)
                    self.__insert_nr("logQc", "mean", index, gb_ds, gb_dict)
                    self.__insert_nr("logQc", "sd", index, gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "logn_man", "mean", gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "logn_man", "sd", gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "logn_amhg", "mean", gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "logn_amhg", "sd", gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "A0", "mean", gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "A0", "sd", gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "b", "mean", gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "b", "sd", gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "logr", "mean", gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "logr", "sd", gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "logWb", "mean", gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "logWb", "sd", gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "logDb", "mean", gb_ds, gb_dict)
                    self.__insert_nx(s_rid, "logDb", "sd", gb_ds, gb_dict)
                    gb_ds.close()
                index += 1
        return gb_dict

    def create_data_dict(self, nt=None):
        """Creates and returns geoBAM data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """
                
        return {
            "nt" : nt,
            "logQ" : {
                "mean_chain1" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "mean_chain2" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "mean_chain3" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "sd_chain1" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "sd_chain2" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "sd_chain3" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "attrs": {
                    "mean_chain1": {},
                    "mean_chain2": {},
                    "mean_chain3": {},
                    "sd_chain1": {},
                    "sd_chain2": {},
                    "sd_chain3": {},
                }
            },
            "logWc" : {
                "mean_chain1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "mean_chain2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "mean_chain3" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd_chain1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd_chain2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd_chain3" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "mean_chain1": {},
                    "mean_chain2": {},
                    "mean_chain3": {},
                    "sd_chain1": {},
                    "sd_chain2": {},
                    "sd_chain3": {},
                }
            },
            "logQc" : {
                "mean_chain1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "mean_chain2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "mean_chain3" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd_chain1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd_chain2" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "sd_chain3" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "mean_chain1": {},
                    "mean_chain2": {},
                    "mean_chain3": {},
                    "sd_chain1": {},
                    "sd_chain2": {},
                    "sd_chain3": {},
                }
            },
            "logn_man": {
                "mean_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "mean_chain1": {},
                    "mean_chain2": {},
                    "mean_chain3": {},
                    "sd_chain1": {},
                    "sd_chain2": {},
                    "sd_chain3": {},
                }
            },
            "logn_amhg": {
                "mean_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "mean_chain1": {},
                    "mean_chain2": {},
                    "mean_chain3": {},
                    "sd_chain1": {},
                    "sd_chain2": {},
                    "sd_chain3": {},
                }
            },
            "A0": {
                "mean_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "mean_chain1": {},
                    "mean_chain2": {},
                    "mean_chain3": {},
                    "sd_chain1": {},
                    "sd_chain2": {},
                    "sd_chain3": {},
                }
            },
            "b": {
                "mean_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "mean_chain1": {},
                    "mean_chain2": {},
                    "mean_chain3": {},
                    "sd_chain1": {},
                    "sd_chain2": {},
                    "sd_chain3": {},
                }
            },
            "logr": {
                "mean_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "mean_chain1": {},
                    "mean_chain2": {},
                    "mean_chain3": {},
                    "sd_chain1": {},
                    "sd_chain2": {},
                    "sd_chain3": {},
                }
            },
            "logWb": {
                "mean_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "mean_chain1": {},
                    "mean_chain2": {},
                    "mean_chain3": {},
                    "sd_chain1": {},
                    "sd_chain2": {},
                    "sd_chain3": {},
                }
            },
            "logDb": {
                "mean_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "mean_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain1" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain2" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd_chain3" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "mean_chain1": {},
                    "mean_chain2": {},
                    "mean_chain3": {},
                    "sd_chain1": {},
                    "sd_chain2": {},
                    "sd_chain3": {},
                }
            }
        }
        
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of geoBAM variables
        """
        
        ds = Dataset(nc_file, 'r')
        for key1, value in data_dict.items():
            if key1 == "nt": continue
            for key2 in value["attrs"].keys():
                value["attrs"][key2] = ds[key1][key2].__dict__
        ds.close()
                
    
    def __insert_nr(self, name, chain, index, gb_ds, gb_dict):
        """Insert discharge values into dictionary with nr dimension.
        
        Parameters
        ----------
        name: str
            name of data variable
        chain: str
            mean or standard deviation chain
        index: int
            integer index to insert data at
        gb_ds: netCDF4.Dataset
            geoBAM NetCDF that contains result data
        gb_dict: dict
            dictionary to store geoBAM results in
        """

        gb_dict[name][f"{chain}_chain1"][index] = gb_ds[name][f"{chain}_chain1"][:].filled(np.nan)
        gb_dict[name][f"{chain}_chain2"][index] = gb_ds[name][f"{chain}_chain2"][:].filled(np.nan)
        gb_dict[name][f"{chain}_chain3"][index] = gb_ds[name][f"{chain}_chain3"][:].filled(np.nan)

    def __insert_nt(self, name, chain, index, gb_ds, gb_dict):
        """Insert discharge values into dictionary with nr by nt dimensions.
        
        Parameters
        ----------
        name: str
            name of data variable
        chain: str
            mean or standard deviation chain
        index: int
            integer index to insert data at
        gb_ds: netCDF4.Dataset
            geoBAM NetCDF that contains result data
        gb_dict: dict
            dictionary to store geoBAM results in
        """

        gb_dict[name][f"{chain}_chain1"][index, :] = gb_ds[name][f"{chain}_chain1"][:].filled(np.nan)
        gb_dict[name][f"{chain}_chain2"][index, :] = gb_ds[name][f"{chain}_chain2"][:].filled(np.nan)
        gb_dict[name][f"{chain}_chain3"][index, :] = gb_ds[name][f"{chain}_chain3"][:].filled(np.nan)

    def __insert_nx(self, rid, name, chain, gb_ds, gb_dict):
        """Append geoBam result data to dictionary with nx dimension.
        
        Parameters
        ----------
        rid: int
            unique reach identifier
        name: str
            name of data variable
        chain: str
            mean or standard deviation chain
        gb_ds: netCDF4.Dataset
            geoBAM NetCDF that contains result data
        gb_dict: dict
            dictionary to store geoBAM results in
        """

        indexes = np.where(self.sos_nrids == rid)      
        if rid == 77444000061:
            gb_dict[name][f"{chain}_chain1"][indexes] = np.append(gb_ds[name][f"{chain}_chain1"][:].filled(np.nan), [np.nan, np.nan])
            gb_dict[name][f"{chain}_chain2"][indexes] = np.append(gb_ds[name][f"{chain}_chain2"][:].filled(np.nan), [np.nan, np.nan])
            gb_dict[name][f"{chain}_chain3"][indexes] = np.append(gb_ds[name][f"{chain}_chain3"][:].filled(np.nan), [np.nan, np.nan])
        
        elif rid == 77444000073:
            gb_dict[name][f"{chain}_chain1"][indexes] = np.insert(gb_ds[name][f"{chain}_chain1"][:].filled(np.nan), 0, np.nan)
            gb_dict[name][f"{chain}_chain2"][indexes] = np.insert(gb_ds[name][f"{chain}_chain2"][:].filled(np.nan), 0, np.nan)
            gb_dict[name][f"{chain}_chain3"][indexes] = np.insert(gb_ds[name][f"{chain}_chain3"][:].filled(np.nan), 0, np.nan)
        
        else:
            gb_dict[name][f"{chain}_chain1"][indexes] = gb_ds[name][f"{chain}_chain1"][:].filled(np.nan)
            gb_dict[name][f"{chain}_chain2"][indexes] = gb_ds[name][f"{chain}_chain2"][:].filled(np.nan)
            gb_dict[name][f"{chain}_chain3"][indexes] = gb_ds[name][f"{chain}_chain3"][:].filled(np.nan)

    def append_module_data(self, data_dict):
        """Append geoBAM data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of geoBAM variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        gb_grp = sos_ds.createGroup("geobam")

        # geoBAM data
        q_grp = gb_grp.createGroup("logQ")
        self.write_var(q_grp, "logQ", "mean", ("num_reaches", "time_steps"), data_dict)
        self.write_var(q_grp, "logQ", "sd", ("num_reaches", "time_steps"), data_dict)

        wc_grp = gb_grp.createGroup("logWc")
        self.write_var(wc_grp, "logWc", "mean", ("num_reaches",), data_dict)
        self.write_var(wc_grp, "logWc", "sd", ("num_reaches",), data_dict)

        qc_grp = gb_grp.createGroup("logQc")
        self.write_var(qc_grp, "logQc", "mean", ("num_reaches",), data_dict)
        self.write_var(qc_grp, "logQc", "sd", ("num_reaches",), data_dict)

        nm_grp = gb_grp.createGroup("logn_man")
        self.write_var(nm_grp, "logn_man", "mean", ("num_nodes",), data_dict)
        self.write_var(nm_grp, "logn_man", "sd", ("num_nodes",), data_dict)

        an_grp = gb_grp.createGroup("logn_amhg")
        self.write_var(an_grp, "logn_amhg", "mean", ("num_nodes",), data_dict)
        self.write_var(an_grp, "logn_amhg", "sd", ("num_nodes",), data_dict)

        a0_grp = gb_grp.createGroup("A0")
        self.write_var(a0_grp, "A0", "mean", ("num_nodes",), data_dict)
        self.write_var(a0_grp, "A0", "sd", ("num_nodes",), data_dict)

        b_grp = gb_grp.createGroup("b")
        self.write_var(b_grp, "b", "mean", ("num_nodes",), data_dict)
        self.write_var(b_grp, "b", "sd", ("num_nodes",), data_dict)

        r_grp = gb_grp.createGroup("logr")
        self.write_var(r_grp, "logr", "mean", ("num_nodes",), data_dict)
        self.write_var(r_grp, "logr", "sd", ("num_nodes",), data_dict)

        wb_grp = gb_grp.createGroup("logWb")
        self.write_var(wb_grp, "logWb", "mean", ("num_nodes",), data_dict)
        self.write_var(wb_grp, "logWb", "sd", ("num_nodes",), data_dict)

        db_grp = gb_grp.createGroup("logDb")
        self.write_var(db_grp, "logDb", "mean", ("num_nodes",), data_dict)
        self.write_var(db_grp, "logDb", "sd", ("num_nodes",), data_dict)

        sos_ds.close()

    def write_var(self, grp, name, chain, dims, gb_dict):
        """Create NetCDF variable and write geoBAM data to it.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        chain: str
            mean or standard deviation chain
        dims: tuple
            tuple of NetCDF4 dimensions that matches shape of var dataa
        gb_dict: dict
            dictionary of geoBAM result data
        """

        c1 = grp.createVariable(f"{chain}_chain1", "f8", dims, fill_value=self.FILL["f8"])
        c1.setncatts(gb_dict[name]["attrs"][f"{chain}_chain1"])
        c1[:] = np.nan_to_num(gb_dict[name][f"{chain}_chain1"], copy=True, nan=self.FILL["f8"])
        
        c2 = grp.createVariable(f"{chain}_chain2", "f8", dims, fill_value=self.FILL["f8"])
        c2.setncatts(gb_dict[name]["attrs"][f"{chain}_chain2"])
        c2[:] = np.nan_to_num(gb_dict[name][f"{chain}_chain2"], copy=True, nan=self.FILL["f8"])
        
        c3 = grp.createVariable(f"{chain}_chain3", "f8", dims, fill_value=self.FILL["f8"])
        c3.setncatts(gb_dict[name]["attrs"][f"{chain}_chain3"])
        c3[:] = np.nan_to_num(gb_dict[name][f"{chain}_chain3"], copy=True, nan=self.FILL["f8"])