# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset, chartostring, stringtochar
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
    create_data_dict(nt=None)
        creates and returns module data dictionary.
    get_module_data(nt=None)
        retrieve module results from NetCDF files.
    get_nc_attrs(nc_file, data_dict)
        get NetCDF attributes for each NetCDF variable.
    """

    def __init__(self, cont_ids, input_dir, sos_new,  vlen_f, vlen_i, vlen_s,
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

    def get_module_data(self, nt=None):
        """Extract SWOT time data from NetCDF files.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        # Files and reach identifiers
        swot_dir = self.input_dir / "swot"
        swot_files = [ Path(swot_file) for swot_file in glob.glob(f"{swot_dir}/{self.cont_ids}*.nc") ] 
        swot_rids = [ int(swot_file.name.split('_')[0]) for swot_file in swot_files ]

        # Storage of time data
        swot_dict = self.create_data_dict(nt)
        
        if len(swot_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(swot_dir / swot_files[0], swot_dict)
        
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in swot_rids:
                    swot_ds = Dataset(swot_dir / f"{s_rid}_SWOT.nc", 'r')
                    swot_dict["observations"][index] = swot_ds["observations"][:].filled(np.nan)
                    swot_dict["reach"]["time"][index] = swot_ds["reach"]["time"][:].filled(np.nan)
                    swot_dict["node"]["time"][index] = swot_ds["node"]["time"][:].filled(np.nan)
                    swot_ds.close()
                index += 1
        return swot_dict
    
    def create_data_dict(self, nt=None):
        """Creates and returns SWOT time data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        data_dict = {
            "observations": np.empty((self.sos_rids.shape[0]), dtype=object),
            "attrs" : {"observations": None},
            "reach": {
                "time": np.empty((self.sos_rids.shape[0]), dtype=object),
                "attrs": {"time": None}
                },
            "node": {
                "time": np.empty((self.sos_rids.shape[0]), dtype=object),
                "attrs": {"time": None}
                }            
        }
        # Vlen variables
        data_dict["observations"].fill(stringtochar(np.array(self.FILL["S1"], dtype="S10")))
        data_dict["reach"]["time"].fill(np.array([self.FILL["f8"]]))
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
        data_dict["attrs"]["observations"] = ds["observations"].__dict__
        data_dict["reach"]["attrs"]["time"] = ds["reach"]["time"].__dict__
        data_dict["node"]["attrs"]["time"] = ds["node"]["time"].__dict__
        ds.close()
        
    def append_module_data(self, data_dict):
        """Append SWOT time data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of SWOT time variables
        """

        sos_ds = Dataset(self.sos_new, 'a')        
        self.write_var_nt(sos_ds, "observations", self.vlen_s, ("num_reaches"), data_dict)
        self.write_var_nt(sos_ds["reaches"], "time", self.vlen_f, ("num_reaches"), data_dict["reach"])       
        self.write_var_nt(sos_ds["nodes"], "time", self.vlen_f, ("num_reaches"), data_dict["node"])       
        sos_ds.close()