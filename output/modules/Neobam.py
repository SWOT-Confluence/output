# Standard imports
import glob
from pathlib import Path
import os

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Neobam(AbstractModule):
    """
    A class that represents the results of running neoBAM.

    Data and operations append neoBAM results to the SoS on the appropriate
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
    """

    def __init__(self, cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s,
                 rids, nrids, nids):
        """
        Parameters
        ---------
        cont_ids: list
            list of continent identifiers
        input_dir: Path
            path to input directory
        sos_new: Path
            path to new SOS file
        logger: logging.Logger
            logger to log statements with
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

        super().__init__(cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s, \
            rids, nrids, nids)


    def get_module_data(self):
        """Extract HiVDI results from NetCDF files."""

        # Files and reach identifiers
        nb_dir = os.path.join(self.input_dir, 'geobam')

        if type(self.cont_ids) == list:
            nb_files = []
            for i in self.cont_ids:
                nb_files_int = [ Path(nb_file) for nb_file in glob.glob(f"{nb_dir}/{i}*.nc") ]
                nb_files.extend(nb_files_int)

        else:
            nb_files = [ Path(nb_file) for nb_file in glob.glob(f"{nb_dir}/{self.cont_ids}*.nc") ]
        nb_rids = [ int(nb_file.name.split('_')[0]) for nb_file in nb_files ]

        # Storage of results data
        nb_dict = self.create_data_dict()
        
        if len(nb_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(nb_dir / nb_files[0], nb_dict)
        
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in nb_rids:
                    try:
                        nb_ds = Dataset(os.path.join(nb_dir , f"{int(s_rid)}_geobam.nc"), 'r')

                        nb_dict["q"]["q"][index] = nb_ds["q"]["q"][:].filled(self.FILL["f8"])
                        nb_dict['q']["attrs"]['q']['_FillValue'] = nb_ds['q']['q']._FillValue
                        nb_dict["q"]["q_sd"][index] = nb_ds["q"]["q_sd"][:].filled(np.nan)
                        nb_dict["q"]["attrs"]['q_sd']['_FillValue'] = nb_ds["q"]['q_sd']._FillValue

                        internal_node_count = 0
                        for a_node_id in abs(nb_ds.node_ids)[:]:
                            node_index = np.where(self.sos_nids == abs(a_node_id))[0][0]

                            nb_dict["r"]["mean"][node_index] = nb_ds["r"]["mean"][internal_node_count].filled(np.nan)
                            nb_dict["r"]["attrs"]['mean']['_FillValue'] = nb_ds["r"]['mean']._FillValue
                            nb_dict["r"]["sd"][index] = nb_ds["r"]["sd"][0].filled(np.nan)
                            nb_dict["r"]["attrs"]['sd']['_FillValue'] = nb_ds["r"]['sd']._FillValue
                            
                            nb_dict["logn"]["mean"][node_index] = nb_ds["logn"]["mean"][internal_node_count].filled(np.nan)
                            nb_dict["logn"]["attrs"]['mean']['_FillValue'] = nb_ds["logn"]['mean']._FillValue
                            nb_dict["logn"]["sd"][index] = nb_ds["logn"]["sd"][0].filled(np.nan)
                            nb_dict["logn"]["attrs"]['sd']['_FillValue'] = nb_ds["logn"]['sd']._FillValue
                            
                            nb_dict["logWb"]["mean"][node_index] = nb_ds["logWb"]["mean"][internal_node_count].filled(np.nan)
                            nb_dict["logWb"]["attrs"]['mean']['_FillValue'] = nb_ds["logWb"]['mean']._FillValue
                            nb_dict["logWb"]["sd"][index] = nb_ds["logWb"]["sd"][0].filled(np.nan)
                            nb_dict["logWb"]["attrs"]['sd']['_FillValue'] = nb_ds["logWb"]['sd']._FillValue
                            
                            nb_dict["logDb"]["mean"][node_index] = nb_ds["logDb"]["mean"][internal_node_count].filled(np.nan)
                            nb_dict["logDb"]["attrs"]['mean']['_FillValue'] = nb_ds["logDb"]['mean']._FillValue
                            nb_dict["logDb"]["sd"][index] = nb_ds["logDb"]["sd"][0].filled(np.nan)
                            nb_dict["logDb"]["attrs"]['sd']['_FillValue'] = nb_ds["logDb"]['sd']._FillValue

                            internal_node_count += 1

                        nb_ds.close()
                    except:
                        self.logger.warn('Reach failed...',s_rid )
                index += 1

        return nb_dict
    
    def create_data_dict(self):
        """Creates and returns HiVDI data dictionary."""

        data_dict = {
            "r" : {
                "mean" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs" : {
                    "mean": {},
                    "sd": {}
                }
            },
            "logn" : {
                "mean" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs" : {
                    "mean": {},
                    "sd": {}
                }
            },
            "logWb" : {
                "mean" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs" : {
                    "mean": {},
                    "sd": {}
                }
            },
            "logDb" : {
                "mean" : np.full(self.sos_nids.shape[0], np.nan, dtype=np.float64),
                "sd" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs" : {
                    "mean": {},
                    "sd": {}
                }
            },
            "q" : {
                "q" : np.empty((self.sos_rids.shape[0]), dtype=object),
                "q_sd": np.empty((self.sos_rids.shape[0]), dtype=object),
                "attrs" : {
                    "q": {},
                    "q_sd":{}
                }
            }
        }
        # Vlen variables
        data_dict["q"]["q"].fill(np.array([self.FILL["f8"]]))
        data_dict["q"]["q_sd"].fill(np.array([self.FILL["f8"]]))
        
        return data_dict
        
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable and update data_dict.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of H2iVDI variables        
        """
        
        ds = Dataset(nc_file, 'r')
        for key1, value in data_dict.items():
            if key1 == "nt": continue
            for key2 in value["attrs"].keys():
                data_dict[key1]["attrs"][key2] = ds[key1][key2].__dict__
        ds.close()
        
    def append_module_data(self, data_dict, metadata_json):
        """Append HiVDI data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of HiVDI variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        nb_grp = sos_ds.createGroup("neobam")
        
        r_grp = nb_grp.createGroup("r")
        var = self.write_var(r_grp, "mean", "f8", ("num_nodes"), data_dict["r"])
        self.set_variable_atts(var, metadata_json["neobam"]["r"]["mean"])
        var = self.write_var(r_grp, "sd", "f8", ("num_reaches"), data_dict["r"])
        self.set_variable_atts(var, metadata_json["neobam"]["r"]["sd"])
        
        logn_grp = nb_grp.createGroup("logn") 
        var = self.write_var(logn_grp, "mean", "f8", ("num_nodes"), data_dict["logn"])
        self.set_variable_atts(var, metadata_json["neobam"]["logn"]["mean"])
        var = self.write_var(logn_grp, "sd", "f8", ("num_reaches"), data_dict["logn"])
        self.set_variable_atts(var, metadata_json["neobam"]["logn"]["sd"])
        
        logDb_grp = nb_grp.createGroup("logDb") 
        var = self.write_var(logDb_grp, "mean", "f8", ("num_nodes"), data_dict["logDb"])
        self.set_variable_atts(var, metadata_json["neobam"]["logDb"]["mean"])
        var = self.write_var(logDb_grp, "sd", "f8", ("num_reaches"), data_dict["logDb"])
        self.set_variable_atts(var, metadata_json["neobam"]["logDb"]["sd"])
        
        logWb_grp = nb_grp.createGroup("logWb") 
        var = self.write_var(logWb_grp, "mean", "f8", ("num_nodes"), data_dict["logWb"])
        self.set_variable_atts(var, metadata_json["neobam"]["logWb"]["mean"])
        var = self.write_var(logWb_grp, "sd", "f8", ("num_reaches"), data_dict["logWb"])
        self.set_variable_atts(var, metadata_json["neobam"]["logWb"]["sd"])
        
        q_grp = nb_grp.createGroup("q")
        var = self.write_var_nt(q_grp, "q", self.vlen_f, ("num_reaches"), data_dict["q"])
        self.set_variable_atts(var, metadata_json["neobam"]["q"]["q"])
        var = self.write_var(q_grp, "q_sd", "f8", ("num_reaches"), data_dict["q"])
        self.set_variable_atts(var, metadata_json["neobam"]["q"]["q_sd"])
        
        sos_ds.close()
