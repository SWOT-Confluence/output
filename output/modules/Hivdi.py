# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Hivdi(AbstractModule):
    """
    A class that represents the results of running HiVDI.

    Data and operations append HiVDI results to the SoS on the appropriate
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
        ----------
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
        hv_dir = self.input_dir / "hivdi"
        hv_files = [ Path(hv_file) for hv_file in glob.glob(f"{hv_dir}/{self.cont_ids}*.nc") ] 
        hv_rids = [ int(hv_file.name.split('_')[0]) for hv_file in hv_files ]

        # Storage of results data
        hv_dict = self.create_data_dict()
        
        if len(hv_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(hv_dir / hv_files[0], hv_dict)
        
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in hv_rids:
                    hv_ds = Dataset(hv_dir / f"{int(s_rid)}_h2ivdi.nc", 'r')
                    hv_dict["reach"]["Q"][index] = hv_ds["reach"]["Q"][:].filled(self.FILL["f8"])
                    hv_dict["reach"]["A0"][index] = hv_ds["reach"]["A0"][:].filled(np.nan)
                    # hv_dict["reach"]["alpha"][index] = hv_ds["reach"]["alpha"][:].filled(np.nan)
                    # hv_dict["reach"]["beta"][index] = hv_ds["reach"]["beta"][:].filled(np.nan)s
                    hv_ds.close()
                index += 1    
        return hv_dict
    
    def create_data_dict(self):
        """Creates and returns HiVDI data dictionary."""

        data_dict = {
            "reach" : {
                "Q" : np.empty((self.sos_rids.shape[0]), dtype=object),
                "A0" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                # "alpha" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                # "beta" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs" : {
                    "Q": {},
                    "A0": {},
                    # "alpha": {},
                    # "beta": {}
                }
            }
        }
        
        # Vlen variables
        data_dict["reach"]["Q"].fill(np.array([self.FILL["f8"]]))
        return data_dict
        
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of H2iVDI variables
        """
        
        ds = Dataset(nc_file, 'r')
        data_dict["reach"]["attrs"]["A0"] = ds["reach"]["A0"].__dict__
        # data_dict["reach"]["attrs"]["alpha"] = ds["reach"]["alpha"].__dict__
        # data_dict["reach"]["attrs"]["beta"] = ds["reach"]["beta"].__dict__
        data_dict["reach"]["attrs"]["Q"] = ds["reach"]["Q"].__dict__
        ds.close()
        
    def append_module_data(self, data_dict, metadata_json):
        """Append HiVDI data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of HiVDI variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        hv_grp = sos_ds.createGroup("hivdi")
        
        var = self.write_var_nt(hv_grp, "Q", self.vlen_f, ("num_reaches"), data_dict["reach"])
        self.set_variable_atts(var, metadata_json["hivdi"]["Q"])
        var = self.write_var(hv_grp, "A0", "f8", ("num_reaches",), data_dict["reach"])
        self.set_variable_atts(var, metadata_json["hivdi"]["A0"])
        # var = self.write_var(hv_grp, "beta", "f8", ("num_reaches",), data_dict["reach"])
        # self.set_variable_atts(var, metadata_json["hivdi"]["beta"])
        # var = self.write_var(hv_grp, "alpha", "f8", ("num_reaches",), data_dict["reach"])
        # self.set_variable_atts(var, metadata_json["hivdi"]["alpha"])
        sos_ds.close()