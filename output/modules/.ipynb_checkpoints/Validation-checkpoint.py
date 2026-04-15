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
    dimensions.

    Attributes
    ----------
    num_algos: int
        number of algorithms stats were produced for (flpe/moi)
    num_algos_offline: int
        number of algorithms stats were produced for (offline)
    nchar: int
        max number of characters in algorithm names
    """

    def __init__(self, cont_ids, input_dir, sos_new, logger, rids, nrids, nids):

        # Defaults - will be overwritten dynamically from first validation file
        self.num_algos = 8
        self.num_algos_offline = 16
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
            "dschg_c",
            "dschg_ga",
            "dschg_a"
        ]
        self.algo_names = np.array(["metroman", "neobam", "busboi", "hivdi", "momma", "sad", "sic4dvar", "consensus"])

        self.suffixes = ['_flpe', '_moi', '_o']
        self.suffix_dict = {
            '_flpe': 'flpe',
            '_moi': 'moi',
            '_o': 'offline'
        }

        super().__init__(cont_ids, input_dir, sos_new, logger, rids=rids, nrids=nrids,
                         nids=nids)

    def get_module_data(self):
        """Extract Validation results from NetCDF files."""

        val_dir   = self.input_dir
        val_files = [Path(f) for f in glob.glob(f"{val_dir}/{self.cont_ids}*.nc")]
        val_rids  = [int(f.name.split('_')[0]) for f in val_files]

        if len(val_files) == 0:
            val_dict = self.create_data_dict()
        else:
            # Dynamically read dimensions from first file
            temp_ds = Dataset(val_dir / val_files[0], 'r')
            self.num_algos         = temp_ds.dimensions["num_algos_flpe"].size
            self.num_algos_offline = temp_ds.dimensions["num_algos_offline"].size
            temp_ds.close()

            val_dict = self.create_data_dict()
            val_dict = self.get_nc_attrs(val_dir / val_files[0], val_dict)

            index = 0
            for s_rid in self.sos_rids:
                if s_rid in val_rids:
                    try:
                        val_ds = Dataset(val_dir / f"{int(s_rid)}_validation.nc", 'r')
                        self.logger.info('processing validation reach: %s', s_rid)

                        for suffix in self.suffixes:
                            grp = self.suffix_dict[suffix]
                            val_dict[grp]["has_validation"][index] = getattr(val_ds, f'has_validation{suffix}')
                            val_dict[grp]["gageid"][index, :val_ds[f"gageID{suffix}"][0].shape[0]] = val_ds[f"gageID{suffix}"][0].filled('')
                            val_dict[grp]["nse"][index,      :val_ds[f"NSE{suffix}"].shape[0]]      = val_ds[f"NSE{suffix}"][:].filled(np.nan)
                            val_dict[grp]["rsq"][index,      :val_ds[f"Rsq{suffix}"].shape[0]]      = val_ds[f"Rsq{suffix}"][:].filled(np.nan)
                            val_dict[grp]["kge"][index,      :val_ds[f"KGE{suffix}"].shape[0]]      = val_ds[f"KGE{suffix}"][:].filled(np.nan)
                            val_dict[grp]["rmse"][index,     :val_ds[f"RMSE{suffix}"].shape[0]]     = val_ds[f"RMSE{suffix}"][:].filled(np.nan)
                            val_dict[grp]["nrmse"][index,    :val_ds[f"nRMSE{suffix}"].shape[0]]    = val_ds[f"nRMSE{suffix}"][:].filled(np.nan)
                            val_dict[grp]["nbias"][index,    :val_ds[f"nBIAS{suffix}"].shape[0]]    = val_ds[f"nBIAS{suffix}"][:].filled(np.nan)
                            val_dict[grp]["sige"][index,     :val_ds[f"SIGe{suffix}"].shape[0]]     = val_ds[f"SIGe{suffix}"][:].filled(np.nan)
                            val_dict[grp]["pearsonr"][index, :val_ds[f"pearsonr{suffix}"].shape[0]] = val_ds[f"pearsonr{suffix}"][:].filled(np.nan)
                            val_dict[grp]["testn"][index,    :val_ds[f"testn{suffix}"].shape[0]]    = val_ds[f"testn{suffix}"][:].filled(np.nan)

                        val_ds.close()
                    except Exception as e:
                        self.logger.warning(f'Reach {s_rid} failed for Validation: {e}')

                index += 1

        return val_dict

    def create_data_dict(self):
        """Creates and returns Validation data dictionary."""

        data_dict = {}
        for group in self.out_groups:
            if group != 'offline':
                num_algos_dim = self.num_algos
                algo_names    = self.algo_names
            else:
                num_algos_dim = self.num_algos_offline
                algo_names    = self.algo_names_offline

            n = self.sos_rids.shape[0]

            data_dict[group] = {
                "num_algos"     : num_algos_dim,
                "nchar"         : self.nchar,
                "gageid"        : np.full((n, self.nchar), ''),
                "has_validation": np.full(n, np.nan, dtype=np.float64),
                "nse"           : np.full((n, num_algos_dim), np.nan, dtype=np.float64),
                "rsq"           : np.full((n, num_algos_dim), np.nan, dtype=np.float64),
                "kge"           : np.full((n, num_algos_dim), np.nan, dtype=np.float64),
                "rmse"          : np.full((n, num_algos_dim), np.nan, dtype=np.float64),
                "nrmse"         : np.full((n, num_algos_dim), np.nan, dtype=np.float64),
                "nbias"         : np.full((n, num_algos_dim), np.nan, dtype=np.float64),
                "sige"          : np.full((n, num_algos_dim), np.nan, dtype=np.float64),
                "pearsonr"      : np.full((n, num_algos_dim), np.nan, dtype=np.float64),
                "testn"         : np.full((n, num_algos_dim), np.nan, dtype=np.float64),
                "attrs": {
                    "algo_names"    : {},
                    "gageid"        : {},
                    "nse"           : {},
                    "rsq"           : {},
                    "kge"           : {},
                    "rmse"          : {},
                    "nbias"         : {},
                    "nrmse"         : {},
                    "testn"         : {},
                    "sige"          : {},
                    "pearsonr"      : {},
                    "has_validation": {},
                }
            }

            data_dict[group]["algo_names"] = np.full((num_algos_dim, self.nchar), '')
            for i, name in enumerate(algo_names):
                data_dict[group]['algo_names'][i, :len(name)] = list(name)

        data_dict['flpe']['time'] = np.empty((self.sos_rids.shape[0]), dtype=object)
        data_dict['flpe']['time'].fill(np.array([self.FILL["i4"]]))
        data_dict['flpe']['attrs']['time'] = {}

        return data_dict

    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable."""

        ds = Dataset(nc_file, 'r')
        for suffix in self.suffixes:
            grp = self.suffix_dict[suffix]
            data_dict[grp]["attrs"]["gageid"]   = ds[f"gageID{suffix}"].__dict__
            data_dict[grp]["attrs"]["nse"]       = ds[f"NSE{suffix}"].__dict__
            data_dict[grp]["attrs"]["rsq"]       = ds[f"Rsq{suffix}"].__dict__
            data_dict[grp]["attrs"]["kge"]       = ds[f"KGE{suffix}"].__dict__
            data_dict[grp]["attrs"]["rmse"]      = ds[f"RMSE{suffix}"].__dict__
            data_dict[grp]["attrs"]["nrmse"]     = ds[f"nRMSE{suffix}"].__dict__
            data_dict[grp]["attrs"]["nbias"]     = ds[f"nBIAS{suffix}"].__dict__
            data_dict[grp]["attrs"]["testn"]     = ds[f"testn{suffix}"].__dict__
            data_dict[grp]["attrs"]["sige"]      = ds[f"SIGe{suffix}"].__dict__
            data_dict[grp]["attrs"]["pearsonr"]  = ds[f"pearsonr{suffix}"].__dict__
        ds.close()
        return data_dict

    def append_module_data(self, data_dict, metadata_json):
        """Append Validation data to the new version of the SoS."""

        sos_ds    = Dataset(self.sos_new, 'a')
        val_t_grp = sos_ds.createGroup("validation")

        val_t_grp.createDimension("num_algos", self.num_algos)
        val_t_grp.createDimension("num_algos_offline", self.num_algos_offline)
        val_t_grp.createDimension("nchar", self.nchar)
        val_t_grp.createDimension("num_reaches", self.sos_rids.shape[0])

        for group in self.out_groups:
            val_grp = val_t_grp.createGroup(group)

            if group != 'offline':
                algo_dim = "num_algos"
            else:
                algo_dim = "num_algos_offline"

            var = self.write_var(val_grp, "algo_names", "S1", ("num_reaches", algo_dim, "nchar",), data_dict[group])
            if "algo_names" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["algo_names"])

            var = self.write_var(val_grp, "gageid", "S1", ("num_reaches", "nchar",), data_dict[group])
            if "gageid" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["gageid"])

            var = self.write_var(val_grp, "has_validation", "i4", ("num_reaches",), data_dict[group])
            if "has_validation" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["has_validation"])

            var = self.write_var(val_grp, "nse", "f8", ("num_reaches", algo_dim,), data_dict[group])
            if "nse" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["nse"])

            var = self.write_var(val_grp, "rsq", "f8", ("num_reaches", algo_dim,), data_dict[group])
            if "rsq" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["rsq"])

            var = self.write_var(val_grp, "kge", "f8", ("num_reaches", algo_dim,), data_dict[group])
            if "kge" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["kge"])

            var = self.write_var(val_grp, "rmse", "f8", ("num_reaches", algo_dim,), data_dict[group])
            if "rmse" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["rmse"])

            var = self.write_var(val_grp, "testn", "f8", ("num_reaches", algo_dim,), data_dict[group])
            if "testn" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["testn"])

            var = self.write_var(val_grp, "nrmse", "f8", ("num_reaches", algo_dim,), data_dict[group])
            if "nrmse" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["nrmse"])

            var = self.write_var(val_grp, "nbias", "f8", ("num_reaches", algo_dim,), data_dict[group])
            if "nbias" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["nbias"])

            var = self.write_var(val_grp, "pearsonr", "f8", ("num_reaches", algo_dim,), data_dict[group])
            if "pearsonr" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["pearsonr"])

            var = self.write_var(val_grp, "sige", "f8", ("num_reaches", algo_dim,), data_dict[group])
            if "sige" in metadata_json["validation"]:
                self.set_variable_atts(var, metadata_json["validation"]["sige"])

        sos_ds.close()