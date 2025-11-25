# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset,chartostring
import numpy as np
import numpy.ma as ma
from datetime import datetime,timedelta

# Local imports
from output.modules.AbstractModule import AbstractModule

class Consensus(AbstractModule):
    """
    A class that represents the results of running Consensus.

    Data and operations append Consensus results to the SoS on the appropriate
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

    def __init__(self, cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s,rids):
        
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

        super().__init__(cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s,rids)

    def get_module_data(self):
        """Extract Consensus results from NetCDF files."""

        # Files and reach identifiers
        sd_dir = self.input_dir / "consensus"
        sd_files = [ Path(sd_file) for sd_file in glob.glob(f"{sd_dir}/{self.cont_ids}*.nc") ] 
        sd_rids = [ int(sd_file.name.split('_')[0]) for sd_file in sd_files ]

        # Storage of results data
        sd_dict = self.create_data_dict()
        
        if len(sd_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(sd_dir / sd_files[0], sd_dict)
 
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in sd_rids:
                    print('extracting',s_rid, 'in sd_dir')
                    try:
                        sd_ds = Dataset(sd_dir / f"{int(s_rid)}_consensus.nc", 'r')
    
                        #first, get the Q
                        sd_dict["consensus_q"][index] = sd_ds["consensus_q"][:].filled(self.FILL["f8"])
    
                        #make it masked array
                        mask = (sd_dict["consensus_q"][index] == self.FILL["f8"])
                        sd_dict["consensus_q"][index]=ma.masked_array(sd_dict["consensus_q"][index], mask=mask)
                       
                        #get the time string
                        #sd_dict["time_str"][index]=sd_ds["time_str"][:]
                        tmp=sd_ds["time_str"][:]
    
                        #mask it
                        sd_dict["time_str"][index]=ma.masked_array(tmp, mask=mask)

                        t_int=[] #seconds since the epoch
                        epoch = datetime(2000,1,1,0,0,0)
                        for t in list(sd_dict["time_str"][index]):
                            if np.ma.is_masked(t):
                                t_int.append(np.nan)
                            else:
                                tdelta=datetime.strptime(t,"%Y-%m-%dT%H:%M:%SZ")-epoch
                                t_int.append(tdelta.total_seconds())
                        sd_dict["time_int"][index]=ma.masked_array(t_int, mask=mask)
                           
                        sd_ds.close()
                    except:
                        self.logger.warn(f'Reach {s_rid} failed for consensus ...')
                index += 1
        return sd_dict
    
    def create_data_dict(self):
        """Creates and returns Consensus data dictionary."""

        data_dict = {
            "consensus_q" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "time_str" : np.empty((self.sos_rids.shape[0]), dtype=object),
            #"time_str" : np.empty((self.sos_rids.shape[0]), dtype='S20'),
            "time_int" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "attrs": {
                "consensus_q" : {},
                "time_str" : {},
                "time_int" : {}
            }
        }

        # Vlen variables
        data_dict["consensus_q"].fill(np.array([self.FILL["f8"]]))
        data_dict["time_str"].fill(np.array([self.FILL["S20"]]))
        data_dict["time_int"].fill(np.array([self.FILL["i8"]]))
        return data_dict
        
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of Consensus variables
        """
        
        ds = Dataset(nc_file, 'r')
        data_dict["attrs"]["consensus_q"] = ds["consensus_q"].__dict__
        data_dict["attrs"]["time_str"] = ds["time_str"].__dict__
        data_dict["attrs"]["time_int"] = {
            'long_name': 'integer time (seconds) for consensus Q',
            'calendar' : data_dict["attrs"]["time_str"]['calendar'],
            'short_name':'time_int',
            'standard_name':'time (seconds)',
            'tag_basic_expert':data_dict["attrs"]["time_str"]['tag_basic_expert'],
            'missing_value': self.FILL["i8"],
            'fill': self.FILL["i8"],
            '_FillValue': self.FILL["i8"],
            'comment':'seconds since beginning of January 1, 2000'
        }
        ds.close()
    
    def append_module_data(self, data_dict, metadata_json):
        """Append Consensus data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of Consensus variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        sd_grp = sos_ds.createGroup("consensus")

        # Consensus data
        var = self.write_var_nt(sd_grp, "consensus_q", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["consensus"]["consensus_q"])
        #var = self.write_var_nt(sd_grp, "time_str", self.vlen_s, ("num_reaches"), data_dict)
        #self.set_variable_atts(var, metadata_json["consensus"]["time_str"])
        var = self.write_var_nt(sd_grp, "time_int", self.vlen_i, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["consensus"]["time_int"])
        sos_ds.close()
