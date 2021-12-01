# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Offline(AbstractModule):
    """
    A class that represents the results of running the Offline module.
    
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

        super().__init__(cont_ids, input_dir, sos_new, rids, nrids, nids)

    def get_module_data(self, nt=None):
        """Extract Offline results from NetCDF files.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        # Files and reach identifiers
        off_dir = self.input_dir
        off_files = [ Path(off_file) for off_file in glob.glob(f"{off_dir}/{self.cont_ids}*.nc") ] 
        off_rids = [ off_file.name.split('_')[0].split('-') for off_file in off_files ]
        off_rids = [ int(rid) for rid_list in off_rids for rid in rid_list ]

        # Storage of results data
        off_dict = self.create_data_dict(nt)
        
        if len(off_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(off_dir / off_files[0], off_dict)
            
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in off_rids:
                    off_file = [ f for f in glob.glob(f"{off_dir}/*{s_rid}*.nc") ][0]
                    off_ds = Dataset(off_file, 'r')
                    off_dict["d_x_area"][index, :] = off_ds["d_x_area"][:].filled(np.nan)
                    if "d_x_area_u" in off_ds.variables.keys(): 
                        off_dict["d_x_area_u"][index, :] = off_ds["d_x_area_u"][:].filled(np.nan)
                    off_dict["metro_q_c"][index, :] = off_ds["metro_q_c"][:].filled(np.nan)
                    off_dict["bam_q_c"][index, :] = off_ds["bam_q_c"][:].filled(np.nan)
                    off_dict["hivdi_q_c"][index, :] = off_ds["hivdi_q_c"][:].filled(np.nan)
                    off_dict["momma_q_c"][index, :] = off_ds["momma_q_c"][:].filled(np.nan)
                    off_dict["sads_q_c"][index, :] = off_ds["sads_q_c"][:].filled(np.nan)
                    off_dict["consensus_q_c"][index, :] = off_ds["consensus_q_c"][:].filled(np.nan)
                    off_dict["metro_q_uc"][index, :] = off_ds["metro_q_uc"][:].filled(np.nan)
                    off_dict["bam_q_uc"][index, :] = off_ds["bam_q_uc"][:].filled(np.nan)
                    off_dict["hivdi_q_uc"][index, :] = off_ds["hivdi_q_uc"][:].filled(np.nan)
                    off_dict["momma_q_uc"][index, :] = off_ds["momma_q_uc"][:].filled(np.nan)
                    off_dict["sads_q_uc"][index, :] = off_ds["sads_q_uc"][:].filled(np.nan)
                    off_dict["consensus_q_uc"][index, :] = off_ds["consensus_q_uc"][:].filled(np.nan)
                    
                    off_ds.close()
                index += 1
        return off_dict

    def create_data_dict(self, nt=None):
        """Creates and returns Offline data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        return {
            "nt" : nt,
            "d_x_area" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "d_x_area_u" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "metro_q_c" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "bam_q_c" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "hivdi_q_c" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "momma_q_c" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "sads_q_c" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "consensus_q_c" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "metro_q_uc" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "bam_q_uc" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "hivdi_q_uc" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "momma_q_uc" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "sads_q_uc" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "consensus_q_uc" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
            "attrs": {
                "d_x_area" : None,
                "d_x_area_u" : None,
                "metro_q_c" : None,
                "bam_q_c" : None,
                "hivdi_q_c" : None,
                "momma_q_c" : None,
                "sads_q_c" : None,
                "consensus_q_c" : None,
                "metro_q_uc" : None,
                "bam_q_uc" : None,
                "hivdi_q_uc" : None,
                "momma_q_uc" : None,
                "sads_q_uc" : None,
                "consensus_q_uc" : None
            }
        }
    
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of Offline variables
        """
        
        ds = Dataset(nc_file, 'r')
        for key in data_dict["attrs"].keys():
            if key == "d_x_area_u" and "d_x_area_u" not in ds.variables.keys(): continue
            data_dict["attrs"][key] = ds[key].__dict__        
        ds.close()

    def append_module_data(self, data_dict):
        """Append Offline data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of Offline variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        off_grp = sos_ds.createGroup("offline")

        # Offline data
        self.write_var(off_grp, "d_x_area", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(off_grp, "d_x_area_u", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(off_grp, "metro_q_c", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(off_grp, "bam_q_c", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(off_grp, "hivdi_q_c", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(off_grp, "momma_q_c", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(off_grp, "sads_q_c", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(off_grp, "consensus_q_c", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(off_grp, "metro_q_uc", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(off_grp, "bam_q_uc", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(off_grp, "hivdi_q_uc", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(off_grp, "momma_q_uc", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(off_grp, "sads_q_uc", "f8", ("num_reaches", "time_steps"), data_dict)
        self.write_var(off_grp, "consensus_q_uc", "f8", ("num_reaches", "time_steps"), data_dict)

        sos_ds.close()