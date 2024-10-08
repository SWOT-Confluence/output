# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Validation(AbstractModule):
    """A class that represent the results of running Validation.
    
    Data and operations append Validation results to the SoS on the appropriate 
    dimenstions.

    Attributes
    ----------
    num_algos: int
            number of algorithms stats were produced for
    nchar: int
        max number of characters in algorithm names
        
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
    __retrieve_dimensions(val_dir, reach_id)
        retrieve num_algos and nchar dimensions
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

        self.num_algos = 14
        self.num_algos_offline = 14
        self.nchar = 100
        self.out_groups = ['offline', 'moi', 'flpe']
        self.algo_names_offline = [
            "dschg_gb",
            "dschg_gh",
            "dschg_gm",
            "dschg_go",
            "dschg_gs",
            "dschg_gi",
            "dschg_i",
            "dschg_b",
            "dschg_h",
            "dschg_m",
            "dschg_o",
            "dschg_s",
            "dschg_gc",
            "dschg_c"
        ]
        self.algo_names = np.array(["metroman", "neobam", "hivdi", "momma", "sad", "sic4dvar", "consensus"])

        self.suffixes = ['_flpe', '_moi', '_o']
        self.suffix_dict = {
            '_flpe': 'flpe',
            '_moi': 'moi',
            '_o':'offline'
        }

        super().__init__(cont_ids, input_dir, sos_new, rids=rids, nrids=nrids, 
                         nids=nids)


    def get_module_data(self):
        """Extract Validation results from NetCDF files."""



        # Files and reach identifiers
        val_dir = self.input_dir
        val_files = [ Path(val_file) for val_file in glob.glob(f"{val_dir}/{self.cont_ids}*.nc") ] 
        val_rids = [ int(val_file.name.split('_')[0]) for val_file in val_files ]

        # Storage of results data
        if len(val_files) == 0:
            val_dict = self.create_data_dict()
        else:
            # Retrieve dimensions and storage of variable attributes
            # self.__retrieve_dimensions(val_dir, val_rids[0])
            val_dict = self.create_data_dict()
            val_dict = self.get_nc_attrs(val_dir / val_files[0], val_dict)
            
            # Data extraction
            index = 0


            for s_rid in self.sos_rids:
                if s_rid in val_rids:
                    val_ds = Dataset(val_dir / f"{int(s_rid)}_validation.nc", 'r')
                    print('processing ', s_rid)
                    for suffix in self.suffixes :
                        # val_dict[self.suffix_dict[suffix]]["algo_names"][:self.num_algos,:val_ds[f"algorithm{suffix}"][0].shape[0]] = val_ds[f"algorithm{suffix}"][:].filled('')




                        val_dict[self.suffix_dict[suffix]]["has_validation"][index] = getattr(val_ds, f'has_validation{suffix}')
                        val_dict[self.suffix_dict[suffix]]["gageid"][index,:val_ds[f"gageID{suffix}"][0].shape[0]] = val_ds[f"gageID{suffix}"][0].filled('')
                        val_dict[self.suffix_dict[suffix]]["nse"][index,:val_ds[f"NSE{suffix}"].shape[0]] = val_ds[f"NSE{suffix}"][:].filled(np.nan)
                        val_dict[self.suffix_dict[suffix]]["rsq"][index,:val_ds[f"Rsq{suffix}"].shape[0]] = val_ds[f"Rsq{suffix}"][:].filled(np.nan)
                        val_dict[self.suffix_dict[suffix]]["kge"][index,:val_ds[f"KGE{suffix}"].shape[0]] = val_ds[f"KGE{suffix}"][:].filled(np.nan)
                        val_dict[self.suffix_dict[suffix]]["rmse"][index,:val_ds[f"RMSE{suffix}"].shape[0]] = val_ds[f"RMSE{suffix}"][:].filled(np.nan)
                        val_dict[self.suffix_dict[suffix]]["nrmse"][index,:val_ds[f"nRMSE{suffix}"].shape[0]] = val_ds[f"nRMSE{suffix}"][:].filled(np.nan)
                        val_dict[self.suffix_dict[suffix]]["nbias"][index,:val_ds[f"nBIAS{suffix}"].shape[0]] = val_ds[f"nBIAS{suffix}"][:].filled(np.nan)
                        # val_dict[self.suffix_dict[suffix]]["rrmse"][index,:] = val_ds[f"rRMSE{suffix}"][:].filled(np.nan)
                        val_dict[self.suffix_dict[suffix]]["sige"][index,:val_ds[f"SIGe{suffix}"].shape[0]] = val_ds[f"SIGe{suffix}"][:].filled(np.nan)
                        val_dict[self.suffix_dict[suffix]]["spearmanr"][index,:val_ds[f"Spearmanr{suffix}"].shape[0]] = val_ds[f"Spearmanr{suffix}"][:].filled(np.nan)
                        val_dict[self.suffix_dict[suffix]]["testn"][index,:val_ds[f"testn{suffix}"].shape[0]] = val_ds[f"testn{suffix}"][:].filled(np.nan)
                    val_ds.close()
                index += 1
        return val_dict
    
    # def __retrieve_dimensions(self, val_dir, reach_id):
    #     """Retrieve num_algos and nchar dimensions.

    #     Parameters
    #     ----------
    #     val_dir: Path
    #         path to validation directory of results
    #     reach_id: int
    #         unique integer reach identifier
    #     """
        
    #     temp = Dataset(f"{val_dir}/{reach_id}_validation.nc", 'r')
    #     self.num_algos = temp.dimensions["num_algos"].size
    #     self.nchar = temp.dimensions["nchar"].size
    #     temp.close()
    
    def create_data_dict(self):
        """Creates and returns Validation data dictionary."""

        data_dict = {}

        for group in self.out_groups:
            if group != 'offline':
                num_algos_dim = self.num_algos
                algo_names = self.algo_names
            else:
                num_algos_dim = self.num_algos_offline
                algo_names = self.algo_names_offline
            data_dict[group] = {

            "num_algos" : self.num_algos,
            "nchar": self.nchar,
            
            "gageid": np.full((self.sos_rids.shape[0], self.nchar), ''),
            "has_validation": np.full((self.sos_rids.shape[0]), np.nan, dtype=np.float64),
            "nse": np.full((self.sos_rids.shape[0], num_algos_dim), np.nan, dtype=np.float64),
            "rsq": np.full((self.sos_rids.shape[0], num_algos_dim), np.nan, dtype=np.float64),
            "kge": np.full((self.sos_rids.shape[0], num_algos_dim), np.nan, dtype=np.float64),
            "rmse": np.full((self.sos_rids.shape[0], num_algos_dim), np.nan, dtype=np.float64),
            "nrmse": np.full((self.sos_rids.shape[0], num_algos_dim), np.nan, dtype=np.float64),
            "nbias": np.full((self.sos_rids.shape[0], num_algos_dim), np.nan, dtype=np.float64),
            # "rrmse": np.full((self.sos_rids.shape[0], self.num_algos), np.nan, dtype=np.float64),
            "sige": np.full((self.sos_rids.shape[0], num_algos_dim), np.nan, dtype=np.float64),
            "spearmanr": np.full((self.sos_rids.shape[0], num_algos_dim), np.nan, dtype=np.float64),
            "testn": np.full((self.sos_rids.shape[0], num_algos_dim), np.nan, dtype=np.float64),
            "attrs": {
                "algo_names": {},
                "gageid":{},
                "nse": {},
                "rsq": {},
                "kge": {},
                "rmse": {},
                "nbias":{},
                "nrmse": {},
                # "rrmse": {},
                "testn": {},
                "sige":{}, 
                "spearmanr":{}, 
                "has_validation": {},
            }}

            data_dict[group]["algo_names"] = {}
            data_dict[group]["algo_names"] =  np.full((num_algos_dim, self.nchar), '')
            for i, name in enumerate(algo_names):
                    # Fill each row with the characters of the algorithm name
                    data_dict[group]['algo_names'][i, :len(name)] = list(name)
        return data_dict
        
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of Validation variables
        """
        
        ds = Dataset(nc_file, 'r')
        for suffix in self.suffixes:
            # data_dict[self.suffix_dict[suffix]]["attrs"]["algo_names"] = ds[f"algorithm{suffix}"].__dict__     
            data_dict[self.suffix_dict[suffix]]["attrs"]["gageid"] = ds[f"gageID{suffix}"].__dict__          
            data_dict[self.suffix_dict[suffix]]["attrs"]["nse"] = ds[f"NSE{suffix}"].__dict__
            data_dict[self.suffix_dict[suffix]]["attrs"]["rsq"] = ds[f"Rsq{suffix}"].__dict__
            data_dict[self.suffix_dict[suffix]]["attrs"]["kge"] = ds[f"KGE{suffix}"].__dict__
            data_dict[self.suffix_dict[suffix]]["attrs"]["rmse"] = ds[f"RMSE{suffix}"].__dict__
            # data_dict[self.suffix_dict[suffix]]["attrs"]["rrmse"] = ds[f"rRMSE{suffix}"].__dict__
            data_dict[self.suffix_dict[suffix]]["attrs"]["nrmse"] = ds[f"nRMSE{suffix}"].__dict__
            data_dict[self.suffix_dict[suffix]]["attrs"]["nbias"] = ds[f"nBIAS{suffix}"].__dict__
            data_dict[self.suffix_dict[suffix]]["attrs"]["testn"] = ds[f"testn{suffix}"].__dict__
            data_dict[self.suffix_dict[suffix]]["attrs"]["sige"] = ds[f"SIGe{suffix}"].__dict__
            data_dict[self.suffix_dict[suffix]]["attrs"]["spearmanr"] = ds[f"Spearmanr{suffix}"].__dict__


        ds.close()
        return data_dict


    def append_module_data(self, data_dict, metadata_json):
        """Append Validation data to the new version of the SoS.
        
        Parameters
        ----------
        val_dict: dict
            dictionary of Validation variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        val_t_grp = sos_ds.createGroup("validation")

        # Dimensions

        val_t_grp.createDimension("num_algos", self.num_algos)
        val_t_grp.createDimension("nchar", self.nchar)
        val_t_grp.createDimension("num_reaches", self.sos_rids.shape[0])


        for group in self.out_groups:

            val_grp = val_t_grp.createGroup(group)

            # # Validation data
            # var = self.write_var(val_grp, "algo_names", "S1", ("num_algos", "nchar",), data_dict[group])
            # self.set_variable_atts(var, metadata_json["validation"]["algo_names"]) 

            # var = self.write_var(val_grp, "gageid", "S1", ("num_reaches","nchar",), data_dict[group])
            # if 'gageid' in list(metadata_json['validation'].keys):
            #     self.set_variable_atts(var, metadata_json["validation"]["gageid"])
            # else:
            #     print('gageid metadata not found...')
            # var = self.write_var(val_grp, "has_validation", "i4", ("num_reaches",), data_dict[group])
            # self.set_variable_atts(var, metadata_json["validation"]["has_validation"])
            # var = self.write_var(val_grp, "nse", "f8", ("num_reaches", "num_algos",), data_dict[group])
            # self.set_variable_atts(var, metadata_json["validation"]["nse"]) 
            # var = self.write_var(val_grp, "rsq", "f8", ("num_reaches", "num_algos",), data_dict[group])
            # self.set_variable_atts(var, metadata_json["validation"]["rsq"]) 
            # var = self.write_var(val_grp, "kge", "f8", ("num_reaches", "num_algos",), data_dict[group])
            # self.set_variable_atts(var, metadata_json["validation"]["kge"]) 
            # var = self.write_var(val_grp, "rmse", "f8", ("num_reaches", "num_algos",), data_dict[group])
            # self.set_variable_atts(var, metadata_json["validation"]["rmse"]) 
            # var = self.write_var(val_grp, "testn", "f8", ("num_reaches", "num_algos",), data_dict[group])
            # self.set_variable_atts(var, metadata_json["validation"]["testn"]) 
            # var = self.write_var(val_grp, "nrmse", "f8", ("num_reaches", "num_algos",), data_dict[group])
            # self.set_variable_atts(var, metadata_json["validation"]["nrmse"]) 
            # var = self.write_var(val_grp, "nbias", "f8", ("num_reaches", "num_algos",), data_dict[group])
            # self.set_variable_atts(var, metadata_json["validation"]["nbias"]) 
            # # var = self.write_var(val_grp, "rrmse", "f8", ("num_reaches", "num_algos",), data_dict)
            # # self.set_variable_atts(var, metadata_json["validation"]["rrmse"]) 
            # var = self.write_var(val_grp, "spearmanr", "f8", ("num_reaches", "num_algos",), data_dict[group])
            # self.set_variable_atts(var, metadata_json["validation"]["spearmanr"]) 
            # var = self.write_var(val_grp, "sige", "f8", ("num_reaches", "num_algos",), data_dict[group])
            # self.set_variable_atts(var, metadata_json["validation"]["sige"]) 

                # Writing "algo_names" and conditionally setting attributes
            var = self.write_var(val_grp, "algo_names", "S1", ("num_reaches", "num_algos", "nchar",), data_dict[group])
            if "algo_names" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["algo_names"])

            # Writing "gageid" and conditionally setting attributes
            var = self.write_var(val_grp, "gageid", "S1", ("num_reaches", "nchar",), data_dict[group])
            if "gageid" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["gageid"])

            # Writing "has_validation" and conditionally setting attributes
            var = self.write_var(val_grp, "has_validation", "i4", ("num_reaches",), data_dict[group])
            if "has_validation" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["has_validation"])

            # Writing "nse" and conditionally setting attributes
            var = self.write_var(val_grp, "nse", "f8", ("num_reaches", "num_algos",), data_dict[group])
            if "nse" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["nse"])

            # Writing "rsq" and conditionally setting attributes
            var = self.write_var(val_grp, "rsq", "f8", ("num_reaches", "num_algos",), data_dict[group])
            if "rsq" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["rsq"])

            # Writing "kge" and conditionally setting attributes
            var = self.write_var(val_grp, "kge", "f8", ("num_reaches", "num_algos",), data_dict[group])
            if "kge" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["kge"])

            # Writing "rmse" and conditionally setting attributes
            var = self.write_var(val_grp, "rmse", "f8", ("num_reaches", "num_algos",), data_dict[group])
            if "rmse" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["rmse"])

            # Writing "testn" and conditionally setting attributes
            var = self.write_var(val_grp, "testn", "f8", ("num_reaches", "num_algos",), data_dict[group])
            if "testn" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["testn"])

            # Writing "nrmse" and conditionally setting attributes
            var = self.write_var(val_grp, "nrmse", "f8", ("num_reaches", "num_algos",), data_dict[group])
            if "nrmse" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["nrmse"])

            # Writing "nbias" and conditionally setting attributes
            var = self.write_var(val_grp, "nbias", "f8", ("num_reaches", "num_algos",), data_dict[group])
            if "nbias" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["nbias"])

            # Writing "spearmanr" and conditionally setting attributes
            var = self.write_var(val_grp, "spearmanr", "f8", ("num_reaches", "num_algos",), data_dict[group])
            if "spearmanr" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["spearmanr"])

            # Writing "sige" and conditionally setting attributes
            var = self.write_var(val_grp, "sige", "f8", ("num_reaches", "num_algos",), data_dict[group])
            if "sige" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["sige"])
        
        sos_ds.close()