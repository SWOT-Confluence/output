"""Append module: Contains a class for appending data to a new version of the
SoS and functions that support the operations to do so.

Class
-----
Append
    A class that creates a new version of the SoS and appends Confluence results

Functions
---------
get_cont_data(cont_json)
    Extract and return the continent data needs to be extracted for
get_continent_sos_data(sos_cur)
    Return a dictionary of continents with associated identifier data
"""

# Standard imports
from datetime import datetime
import json
from os import scandir
from pathlib import Path
from shutil import copy

# Third-party imports
from netCDF4 import Dataset

# Local imports
from output.GeoBam import GeoBAM
from output.Hivdi import Hivdi
from output.Metroman import Metroman
from output.Moi import Moi
from output.Momma import Momma
from output.Offline import Offline
from output.Postdiagnostics import Postdiagnostics
from output.Sad import Sad
from output.Sic4dvar import Sic4dvar
from output.Validation import Validation

class Append:
    """
    A class that creates a new version of the SoS and appends Confluence results.
    
    The results from each stage of a Confluence run are appended.

    Attributes
    ----------
    cont: dict
        continent name key with associated numeric identifier values (list)
    FILL_VALUE: float
        fill value to use for missing data
    input_dir: Path
        path to input directory
    sos_nrids: nd.array
        array of SOS reach identifiers on the node-level
    sos_nids: nd.array
        array of SOS node identifiers
    sos_rids: nd.array
        array of SoS reach identifiers associated with continent
    sos_cur: Path
        path to the current SoS
    sos_new: Path
        path to new SoS directory
    VERS_LENGTH: int
        number of integers in SoS identifier
    version: int
        unique identifier for new version of the SoS
    Methods
    -------
    append_data():
        append data to the SoS
    """

    FILL_VALUE = -999999999999
    VERS_LENGTH = 4

    def __init__(self, cont_json, index, input_dir, output_dir):
        """
        TODO: Remove "temp" from output_dir (self.sos_new)

        Parameters
        ----------
        cont_json: Path
            path to continent JSON file
        index: int
            integer index to select JSON data
        input_dir: Path
            path to input directory
        output_dir: Path
            path to output directory
        """

        self.cont = get_cont_data(cont_json, index)
        self.sos_cur = input_dir / "sos"
        self.sos_file = output_dir / "sos" / f"{list(self.cont.keys())[0]}_sword_v11_SOS_results.nc"
        sos_data = get_continent_sos_data(self.sos_cur, list(self.cont.keys())[0])
        self.sos_rids = sos_data["reaches"]
        self.sos_nrids = sos_data["node_reaches"]
        self.sos_nids = sos_data["nodes"]
        self.nt = get_nt(input_dir)
        self.version = "9999"

    def create_new_version(self):
        """Create new version of the SoS."""
        
        # Create directory and file
        self.sos_file.parent.mkdir(parents=True, exist_ok=True)
        file_name = '_'.join(self.sos_file.name.split('_')[:-1])
        prior_sos = Dataset(self.sos_cur / f"{file_name}_priors.nc")
        result_sos = Dataset(self.sos_file, 'w')
        
        # Global attributes
        result_sos.Name = prior_sos.Name
        result_sos.version = prior_sos.version
        result_sos.production_date = datetime.now().strftime('%d-%b-%Y %H:%M:%S')
        result_sos.run_type = prior_sos.run_type

        # Global dimensions
        result_sos.createDimension("num_reaches", prior_sos["reaches"]["reach_id"][:].shape[0])             
        result_sos.createDimension("num_nodes", prior_sos["nodes"]["node_id"][:].shape[0])
        result_sos.createDimension("time_steps", self.nt.shape[0])

        # Global variables
        time_var = result_sos.createVariable("time", "i8", ("time_steps",))
        time_var[:] = self.nt

        # Node and reach group
        write_reaches(prior_sos, result_sos)
        write_nodes(prior_sos, result_sos)

        prior_sos.close()
        result_sos.close()

    def append_data(self, flpe_dir, moi_dir, postd_dir, off_dir, val_dir):
        """Append data to the SoS.
        
        Parameters
        ----------
        flpe_dir: Path
            path to reach-level FLPE directory
        moi_dir: Path
            path to basin-level MOI directory
        postd_dir: Path
            path to Postdiagnostics directory
        off_dir: Path
            path to Offline directory
        val_dir: Path
            path to Validation directory
        """
        
        gb = GeoBAM(list(self.cont.values())[0], flpe_dir, self.sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        gb.append_gb(self.nt.shape[0], self.version)
        
        mm = Momma(list(self.cont.values())[0], flpe_dir, self.sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        mm.append_mm(self.nt.shape[0], self.version)

        hv = Hivdi(list(self.cont.values())[0], flpe_dir, self.sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        hv.append_hv(self.nt.shape[0], self.version)

        mn = Metroman(list(self.cont.values())[0], flpe_dir, self.sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        mn.append_mn(self.nt.shape[0], self.version)

        sd = Sad(list(self.cont.values())[0], flpe_dir, self.sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        sd.append_sd(self.nt.shape[0], self.version)

        sv = Sic4dvar(list(self.cont.values())[0], flpe_dir, self.sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        sv.append_sv(self.nt.shape[0], self.version)

        moi = Moi(list(self.cont.values())[0], moi_dir, self.sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        moi.append_moi(self.nt.shape[0], self.version)

        pd = Postdiagnostics(list(self.cont.values())[0], postd_dir, self.sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        pd.append_pd(self.version)

        off = Offline(list(self.cont.values())[0], off_dir, self.sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        off.append_off(self.nt.shape[0], self.version)

        val = Validation(list(self.cont.values())[0], val_dir, self.sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        val.append_val(self.version)

def get_cont_data(cont_json, index):
    """Extract and return the continent data needs to be extracted for.
    
    Parameters
    ----------
    cont_json : str
        path to the file that contains the list of continents
    index: int
        integer index value to select json data
    
    Returns
    -------
    list
        List of reaches identifiers
    """

    with open(cont_json) as jsonfile:
        data = json.load(jsonfile)
    return data[index]

def get_continent_sos_data(sos_cur, continent):
    """Return a dictionary of continents with associated reach identifiers.
    
    Parameters
    ----------
    continent: str
        continent letter identifiers
    sos_cur: Path
        path to the current SoS
    """
    
    nc = Dataset(sos_cur / f"{continent}_sword_v11_SOS_priors.nc", 'r')
    rids = nc["reaches"]["reach_id"][:]
    nrids = nc["nodes"]["reach_id"][:]
    nids = nc["nodes"]["node_id"][:]
    nc.close()

    return { "reaches": rids, "node_reaches": nrids, "nodes": nids }

def get_nt(input_dir):
    """Return time steps (nt) from first SWOT NetCDF file.
    
    Parameters
    ----------
    input_dir: Path
        Path to input directory with a SWOT directory and files.
    """

    with scandir(input_dir / "swot") as entries:
        files = [ Path(entry) for entry in entries ]
    
    swot_ds = Dataset(files[0], 'r')
    nt = swot_ds["nt"][:]
    swot_ds.close()
    return nt

def write_reaches(prior_sos, result_sos):
        """Write reach_id variable and associated dimension to the SoS."""
        
        sos_reach = result_sos.createGroup("reaches")
        reach_var = sos_reach.createVariable("reach_id", "i8", ("num_reaches",))
        reach_var.format = prior_sos["reaches"]["reach_id"].format
        reach_var[:] = prior_sos["reaches"]["reach_id"][:]

def write_nodes(prior_sos, result_sos):
    """Write node_id and reach_id variables with associated dimension to the
    SoS."""

    sos_node = result_sos.createGroup("nodes")
    
    node_var = sos_node.createVariable("node_id", "i8", ("num_nodes",))
    node_var.format = prior_sos["nodes"]["node_id"].format
    node_var[:] = prior_sos["nodes"]["node_id"][:]
    
    reach_var = sos_node.createVariable("reach_id", "i8", ("num_nodes",))
    reach_var.format = prior_sos["nodes"]["reach_id"].format
    reach_var[:] = prior_sos["nodes"]["reach_id"][:]