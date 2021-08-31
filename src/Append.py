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
from src.GeoBam import GeoBAM
from src.Hivdi import Hivdi
from src.Metroman import Metroman
from src.Moi import Moi
from src.Momma import Momma
from src.Offline import Offline
from src.Postdiagnostics import Postdiagnostics

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
        self.sos_file = f"{list(self.cont.keys())[0]}_apriori_rivers_v07_SOS.nc"
        self.sos_new = output_dir
        sos_data = get_continent_sos_data(self.sos_cur, list(self.cont.keys())[0])
        self.sos_rids = sos_data["reaches"]
        self.sos_nrids = sos_data["node_reaches"]
        self.sos_nids = sos_data["nodes"]
        self.nt = get_nt(input_dir)
        self.version = "9999"

    def create_new_version(self):
        """Create new version of the SoS."""

        new_file = Path(self.sos_new) / self.sos_file
        copy(self.sos_cur / self.sos_file, new_file)
        sos = Dataset(new_file, 'a')
        
        self.version = str(int(sos.version) + 1)
        padding = ['0'] * (self.VERS_LENGTH - len(self.version))
        sos.version = f"{''.join(padding)}{self.version}"
        
        sos.production_date = datetime.now().strftime('%d-%b-%Y %H:%M:%S')
        sos["time_steps"][:] = self.nt
        sos.close()

    def append_data(self, flpe_dir, moi_dir, postd_dir, off_dir):
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
        """

        sos_file = Path(self.sos_new) / self.sos_file
        
        gb = GeoBAM(list(self.cont.values())[0], flpe_dir, sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        gb.append_gb(self.nt.shape[0], self.version)
        
        mm = Momma(list(self.cont.values())[0], flpe_dir, sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        mm.append_mm(self.nt.shape[0], self.version)

        hv = Hivdi(list(self.cont.values())[0], flpe_dir, sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        hv.append_hv(self.nt.shape[0], self.version)

        mn = Metroman(list(self.cont.values())[0], flpe_dir, sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        mn.append_mn(self.nt.shape[0], self.version)

        moi = Moi(list(self.cont.values())[0], moi_dir, sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        moi.append_moi(self.nt.shape[0], self.version)

        pd = Postdiagnostics(list(self.cont.values())[0], postd_dir, sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        pd.append_pd(self.version)

        off = Offline(list(self.cont.values())[0], off_dir, sos_file, 
            self.sos_rids, self.sos_nrids, self.sos_nids)
        off.append_off(self.nt.shape[0], self.version)

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
    
    nc = Dataset(sos_cur / f"{continent}_apriori_rivers_v07_SOS.nc", 'r')
    rids = nc["num_reaches"][:]
    nrids = nc["nodes"]["reach_id"][:]
    nids = nc["num_nodes"][:]
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