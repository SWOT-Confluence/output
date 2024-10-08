# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
from netCDF4 import chartostring
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Swot(AbstractModule):
    """
    A class that represents the SWOT NetCDF files.

    Data and operations append SWOT time data to the SoS on the appropriate
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
        """Extract SWOT time data from NetCDF files."""

        # Files and reach identifiers
        swot_dir = self.input_dir / "swot"
        swot_files = [ Path(swot_file) for swot_file in glob.glob(f"{swot_dir}/{self.cont_ids}*.nc") ] 
        swot_rids = [ int(swot_file.name.split('_')[0]) for swot_file in swot_files ]

        # Storage of time data
        swot_dict = self.create_data_dict()
        
        if len(swot_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(swot_dir / swot_files[0], swot_dict)
        
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in swot_rids:
                    swot_ds = Dataset(swot_dir / f"{int(s_rid)}_SWOT.nc", 'r')
                    
                    # Reach
                    swot_dict["reach"]["observations"][index] = ','.join(chartostring(swot_ds["observations"][:]))
                    swot_dict["reach"]["time"][index] = swot_ds["reach"]["time"][:].filled(self.FILL["f8"])
                    
                    # Node
                    indexes = np.where(self.sos_nrids == s_rid)
                    self._insert_nx(swot_dict, swot_ds, indexes)
                    
                    swot_ds.close()
                index += 1
        else:
            raise ValueError('no swot files found')
        return swot_dict
    
    def create_data_dict(self):
        """Creates and returns SWOT time data dictionary."""

        data_dict = {
            "reach": {
                "time": np.empty((self.sos_rids.shape[0]), dtype=object),
                "observations": np.empty((self.sos_rids.shape[0]), dtype=object),
                "attrs": {"time": {}, "observations": {}}
                },
            "node": {
                "time": np.empty((self.sos_nids.shape[0]), dtype=object),
                "observations": np.empty((self.sos_nids.shape[0]), dtype=object),
                "attrs": {"time": {}, "observations": {}}
                }            
        }
        # Vlen variables
        data_dict["reach"]["observations"].fill("xxxxxxxxxx")
        data_dict["reach"]["time"].fill(np.array([self.FILL["f8"]]))
        data_dict["node"]["observations"].fill("xxxxxxxxxx")
        data_dict["node"]["time"].fill(np.array([self.FILL["f8"]]))
        return data_dict
        
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of SWOT time variables
        """
        
        ds = Dataset(nc_file, 'r')
        data_dict["reach"]["attrs"]["observations"] = ds["observations"].__dict__
        data_dict["reach"]["attrs"]["time"] = ds["reach"]["time"].__dict__
        data_dict["node"]["attrs"]["observations"] = ds["observations"].__dict__
        data_dict["node"]["attrs"]["time"] = ds["node"]["time"].__dict__
        ds.close()
        
    def _insert_nx(self, swot_dict, swot_ds, indexes):
        """Insert node flags into prediagnostics dictionary.
        
        Parameters
        ----------
        swot_dict: dict
            dictionary of SWOT data
        swot_ds: netCDF4.Dataset
            SWOT NetCDF dataset reference
        indexes: list
            list of integer indexes to insert node flags at
        """
        
        j = 0

        for i in indexes[0]:
            try:
                swot_dict["node"]["observations"][i] = ','.join(chartostring(swot_ds["observations"][:]))
                swot_dict["node"]["time"][i] = swot_ds["node"]["time"][j,:].filled(self.FILL["f8"])
            except:
                self.logging.warn('time variable filled, reach was partially observed')
                return
            j +=1
        
    def append_module_data(self, data_dict, metadata_json):
        """Append SWOT time data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of SWOT time variables
        """

        sos_ds = Dataset(self.sos_new, 'a')

        # Reach
        var = self.write_var_nt(sos_ds["reaches"], "observations", str, ("num_reaches"), data_dict["reach"], fill=-1)
        self.set_variable_atts(var, metadata_json["reaches"]["observations"])
        var = self.write_var_nt(sos_ds["reaches"], "time", self.vlen_f, ("num_reaches"), data_dict["reach"])
        self.set_variable_atts(var, metadata_json["reaches"]["time"])
        
        # Node
        var = self.write_var_nt(sos_ds["nodes"], "observations", str, ("num_nodes"), data_dict["node"], fill=-1)
        self.set_variable_atts(var, metadata_json["nodes"]["observations"])
        var = self.write_var_nt(sos_ds["nodes"], "time", self.vlen_f, ("num_nodes"), data_dict["node"])
        self.set_variable_atts(var, metadata_json["nodes"]["time"])
        
        sos_ds.close()