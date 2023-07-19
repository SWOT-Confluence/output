"""Append module: Contains a class for appending data to a new version of the
SoS and functions that support the operations to do so.

Class
-----
Append
    A class that creates a new version of the SoS and appends Confluence results

Functions
---------
get_cont_data(cont_json)
    extract and return the continent data needs to be extracted for
get_continent_sos_data(sos_cur)
    return a dictionary of continents with associated identifier data
write_reaches(prior_sos, result_sos)
    write reach_id variable and associated dimension to the SoS
write_nodes(prior_sos, result_sos)
    write node_id and reach_id variables with associated dimension to the SoS
"""

# Standard imports
from datetime import datetime
import json

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.Hivdi import Hivdi
from output.modules.Metroman import Metroman
from output.modules.Moi import Moi
from output.modules.Momma import Momma
from output.modules.Neobam import Neobam
from output.modules.Offline import Offline
from output.modules.Postdiagnostics import Postdiagnostics
from output.modules.Prediagnostics import Prediagnostics
from output.modules.Priors import Priors
from output.modules.Sad import Sad
from output.modules.Sic4dvar import Sic4dvar
from output.modules.Swot import Swot
from output.modules.Validation import Validation

class Append:
    """
    A class that creates a new version of the SoS and appends Confluence results.
    
    The results from each stage of a Confluence run are appended.

    Attributes
    ----------
    cont: dict
        continent name key with associated numeric identifier values (list)
    input_dir: Path
        path to input directory
    modules: list
        list of AbstractModule objects to execute result storage ops for
    MODULES_LIST: list
        list of string module names to create objects for
    PRIORS_SUFFIX: str
        string suffix for priors file name
    RESULTS_SUFFIX: str
        string suffix for output file name
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
    vlen_f: VLType
        variable length float data type for NetCDF ragged arrays
    vlen_i: VLType
        variable length int data type for NEtCDF ragged arrays
    vlen_s: VLType
        variable length string data type for NEtCDF ragged arrays
        
    Methods
    -------
    append_data():
        append data to the SoS
    create_modules(flpe_dir, moi_dir, postd_dir, off_dir, val_dir)
        create and stores a list of AbstractModule objects
    create_new_version()
        create new version of the SoS
    """


    PRIORS_SUFFIX = "sword_v15_SOS_priors"
    RESULTS_SUFFIX = "sword_v15_SOS_results"
    VERS_LENGTH = 4

    def __init__(self, cont_json, index, input_dir, output_dir, modules, logger,
                 metadata_json):
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
        modules: list
            list of module results to append to the SoS
        logger: Logger
            logger to use for logging state
        """
        
        self.cont = get_cont_data(cont_json, index)
        self.sos_cur = input_dir / "sos"
        self.sos_file = output_dir / "sos" / f"{list(self.cont.keys())[0]}_{self.RESULTS_SUFFIX}.nc"
        sos_data = get_continent_sos_data(self.sos_cur, list(self.cont.keys())[0], self.PRIORS_SUFFIX)
        self.sos_rids = sos_data["reaches"]
        self.sos_nrids = sos_data["node_reaches"]
        self.sos_nids = sos_data["nodes"]
        self.logger = logger
        self.metadata_json = metadata_json
        self.modules_list = modules
        self.modules = []
        self.version = "9999"
        self.vlen_f = None
        self.vlen_i = None
        self.vlen_s = None

    def create_new_version(self):
        """Create new version of the SoS."""
        
        # Create directory and file
        self.sos_file.parent.mkdir(parents=True, exist_ok=True)
        continent = self.sos_file.name.split('_')[0]        
        prior_sos = Dataset(self.sos_cur / f"{continent}_{self.PRIORS_SUFFIX}.nc")
        result_sos = Dataset(self.sos_file, 'w')
        
        # Global attributes
        with open(self.metadata_json) as jf:
            global_atts = json.load(jf)
            
        for name, value in global_atts.items():
            setattr(result_sos, name, value)
            
        result_sos.Name = prior_sos.Name
        result_sos.version = prior_sos.version
        result_sos.production_date = datetime.now().strftime('%d-%b-%Y %H:%M:%S')
        result_sos.run_type = prior_sos.run_type

        # Global dimensions
        result_sos.createDimension("num_reaches", prior_sos["reaches"]["reach_id"][:].shape[0])             
        result_sos.createDimension("num_nodes", prior_sos["nodes"]["node_id"][:].shape[0])

        # Variable length time steps
        self.vlen_f = result_sos.createVLType(np.float64, "vlen_float")
        self.vlen_i = result_sos.createVLType(np.int32, "vlen_int")
        self.vlen_s = result_sos.createVLType("S1", "vlen_str")

        # Node and reach group
        write_reaches(prior_sos, result_sos)
        write_nodes(prior_sos, result_sos)

        prior_sos.close()
        result_sos.close()
        self.logger.info(f"Created new SoS results file: {self.sos_file.name}.")

    def append_data(self):
        """Append data to the SoS by executing module storage operations."""
        
        for module in self.modules:
            module.append_module()
            self.logger.info(f"Appended {module.__class__.__name__} data to {self.sos_file.name}.")
        
    def create_modules(self, run_type, input_dir, diag_dir, flpe_dir, moi_dir, \
                       off_dir, val_dir):
        
        """Create and stores a list of AbstractModule objects.
        
        Parameters
        ----------
        run_type: str
            either "constrained" or "unconstrained"
        input_dir: Path
            path to input directory
        diag_dir: Path
            path to diagnostics directory
        flpe_dir: Path
            path to reach-level FLPE directory
        moi_dir: Path
            path to basin-level MOI directory
        off_dir: Path
            path to Offline directory
        val_dir: Path
            path to Validation directory
        """
        
        for module in self.modules_list:
            if module == "hivdi":
                self.modules.append(Hivdi(list(self.cont.values())[0], \
                    flpe_dir, self.sos_file, self.vlen_f, self.vlen_i, \
                    self.vlen_s, self.sos_rids, self.sos_nrids, self.sos_nids))
            if module == "metroman":
                self.modules.append(Metroman(list(self.cont.values())[0], \
                    flpe_dir, self.sos_file, self.vlen_f, self.vlen_i, \
                    self.vlen_s, self.sos_rids, self.sos_nrids, self.sos_nids))
            if module == "moi":
                self.modules.append(Moi(list(self.cont.values())[0], \
                    moi_dir, self.sos_file, self.vlen_f, self.vlen_i, \
                    self.vlen_s, self.sos_rids, self.sos_nrids, self.sos_nids))
            if module == "momma":
                self.modules.append(Momma(list(self.cont.values())[0], \
                    flpe_dir, self.sos_file, self.vlen_f, self.vlen_i, \
                    self.vlen_s, self.sos_rids, self.sos_nrids, self.sos_nids))
            if module == "neobam":
                self.modules.append(Neobam(list(self.cont.values())[0], \
                    flpe_dir, self.sos_file, self.vlen_f, self.vlen_i, \
                    self.vlen_s, self.sos_rids, self.sos_nrids, self.sos_nids))
            if module == "offline":
                self.modules.append(Offline(list(self.cont.values())[0], \
                    off_dir, self.sos_file, self.vlen_f, self.vlen_i, \
                    self.vlen_s, self.sos_rids, self.sos_nrids, self.sos_nids))
            if module == "postdiagnostics":
                self.modules.append(Postdiagnostics(list(self.cont.values())[0], \
                    diag_dir / "postdiagnostics", self.sos_file, self.sos_rids, \
                    self.sos_nrids, self.sos_nids))
            if module == "prediagnostics":
                self.modules.append(Prediagnostics(list(self.cont.values())[0], \
                    diag_dir / "prediagnostics", self.sos_file, self.vlen_f, \
                    self.vlen_i, self.vlen_s, self.sos_rids, self.sos_nrids, \
                    self.sos_nids)) 
            if module == "priors" and run_type == "constrained":
                self.modules.append(Priors(list(self.cont.values())[0], \
                    self.sos_cur, self.sos_file, self.PRIORS_SUFFIX))
            if module == "sad":
                self.modules.append(Sad(list(self.cont.values())[0], \
                    flpe_dir, self.sos_file, self.vlen_f, self.vlen_i, \
                    self.vlen_s, self.sos_rids, self.sos_nrids, self.sos_nids))
            if module == "sic4dvar":
                self.modules.append(Sic4dvar(list(self.cont.values())[0], \
                    flpe_dir, self.sos_file, self.vlen_f, self.vlen_i, \
                    self.vlen_s, self.sos_rids, self.sos_nrids, self.sos_nids))
            if module == "swot":
                self.modules.append(Swot(list(self.cont.values())[0], \
                    input_dir, self.sos_file, self.vlen_f, self.vlen_i, \
                    self.vlen_s, self.sos_rids, self.sos_nrids, self.sos_nids))
            if module == "validation":
                self.modules.append(Validation(list(self.cont.values())[0], \
                    val_dir, self.sos_file, self.sos_rids, self.sos_nrids, \
                    self.sos_nids))
            

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

def get_modules_list(modules_json_path):
    """Extract and return the list of modules that output should extract data for.
    
    Parameters
    ----------
    cont_json : str
        path to the file that contains the list of modules
    
    Returns
    -------
    list
        List of modules
    """

    with open(modules_json_path) as jsonfile:
        data = json.load(jsonfile)
    return data['modules']

def get_continent_sos_data(sos_cur, continent, priors_suffix):
    """Return a dictionary of continents with associated reach identifiers.
    
    Parameters
    ----------
    continent: str
        continent letter identifiers
    sos_cur: Path
        path to the current SoS
    priors_suffix: str
        string suffix of priors SOS file name
    """
    
    nc = Dataset(sos_cur / f"{continent}_{priors_suffix}.nc", 'r')
    rids = nc["reaches"]["reach_id"][:]
    nrids = nc["nodes"]["reach_id"][:]
    nids = nc["nodes"]["node_id"][:]
    nc.close()

    return { "reaches": rids, "node_reaches": nrids, "nodes": nids }

def write_reaches(prior_sos, result_sos):
    """Write reach_id variable and associated dimension to the SoS."""
    
    sos_reach = result_sos.createGroup("reaches")
    reach_var = sos_reach.createVariable("reach_id", "i8", ("num_reaches",))
    reach_var.setncatts(prior_sos["reaches"]["reach_id"].__dict__)
    reach_var[:] = prior_sos["reaches"]["reach_id"][:]

def write_nodes(prior_sos, result_sos):
    """Write node_id and reach_id variables with associated dimension to the
    SoS."""

    sos_node = result_sos.createGroup("nodes")
    
    node_var = sos_node.createVariable("node_id", "i8", ("num_nodes",))
    node_var.setncatts(prior_sos["nodes"]["node_id"].__dict__)
    node_var[:] = prior_sos["nodes"]["node_id"][:]
    
    reach_var = sos_node.createVariable("reach_id", "i8", ("num_nodes",))
    reach_var.setncatts(prior_sos["nodes"]["reach_id"].__dict__)
    reach_var[:] = prior_sos["nodes"]["reach_id"][:]
