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
    create_data_dict()
        creates and returns module data dictionary.
    get_module_data()
        retrieve module results from NetCDF files.
    get_nc_attrs(nc_file, data_dict)
        get NetCDF attributes for each NetCDF variable.
    """

    def __init__(self, cont_ids, input_dir, sos_new, vlen_f, vlen_i, vlen_s,
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

    def get_module_data(self):
        """Extract Offline results from NetCDF files."""

        # Files and reach identifiers
        off_dir = self.input_dir
        off_files = [ Path(off_file) for off_file in glob.glob(f"{off_dir}/{self.cont_ids}*.nc") ] 
        off_rids = [ int(off_file.name.split('_')[0]) for off_file in off_files ]
        
        # Storage of results data
        off_dict = self.create_data_dict()
        
        if len(off_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(off_dir / off_files[0], off_dict)

            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in off_rids:
                    off_ds = Dataset(off_dir / f"{int(s_rid)}_offline.nc", 'r')
                    off_dict["d_x_area"][index] = off_ds["d_x_area"][:].filled(self.FILL["f8"])
                    if "d_x_area_u" in off_ds.variables.keys(): 
                        off_dict["d_x_area_u"][index] = off_ds["d_x_area_u"][:].filled(self.FILL["f8"])
                    off_dict["metro_q_c"][index] = off_ds["dschg_gm"][:].filled(self.FILL["f8"])
                    off_dict["bam_q_c"][index] = off_ds["dschg_gb"][:].filled(self.FILL["f8"])
                    off_dict["hivdi_q_c"][index] = off_ds["dschg_gh"][:].filled(self.FILL["f8"])
                    off_dict["momma_q_c"][index] = off_ds["dschg_go"][:].filled(self.FILL["f8"])
                    off_dict["sads_q_c"][index] = off_ds["dschg_gs"][:].filled(self.FILL["f8"])
                    off_dict["consensus_q_c"][index] = off_ds["dschg_gc"][:].filled(self.FILL["f8"])
                    off_dict["metro_q_uc"][index] = off_ds["dschg_m"][:].filled(self.FILL["f8"])
                    off_dict["bam_q_uc"][index] = off_ds["dschg_b"][:].filled(self.FILL["f8"])
                    off_dict["hivdi_q_uc"][index] = off_ds["dschg_h"][:].filled(self.FILL["f8"])
                    off_dict["momma_q_uc"][index] = off_ds["dschg_o"][:].filled(self.FILL["f8"])
                    off_dict["sads_q_uc"][index] = off_ds["dschg_s"][:].filled(self.FILL["f8"])
                    # off_dict["sic4dvar_q_uc"][index] = off_ds["dschg_i"][:].filled(self.FILL["f8"])
                    off_dict["consensus_q_uc"][index] = off_ds["dschg_c"][:].filled(self.FILL["f8"])
                    off_ds.close()
                                        # off_ds = Dataset(off_dir / f"{int(s_rid)}_offline.nc", 'r')
                    # off_dict["d_x_area"][index] = off_ds["d_x_area"][:].filled(self.FILL["f8"])
                    # if "d_x_area_u" in off_ds.variables.keys(): 
                    #     off_dict["d_x_area_u"][index] = off_ds["d_x_area_u"][:].filled(self.FILL["f8"])
                    # off_dict["metro_q_c"][index] = off_ds["metro_q_c"][:].filled(self.FILL["f8"])
                    # off_dict["bam_q_c"][index] = off_ds["bam_q_c"][:].filled(self.FILL["f8"])
                    # off_dict["hivdi_q_c"][index] = off_ds["hivdi_q_c"][:].filled(self.FILL["f8"])
                    # off_dict["momma_q_c"][index] = off_ds["momma_q_c"][:].filled(self.FILL["f8"])
                    # off_dict["sads_q_c"][index] = off_ds["sads_q_c"][:].filled(self.FILL["f8"])
                    # off_dict["consensus_q_c"][index] = off_ds["consensus_q_c"][:].filled(self.FILL["f8"])
                    # off_dict["metro_q_uc"][index] = off_ds["metro_q_uc"][:].filled(self.FILL["f8"])
                    # off_dict["bam_q_uc"][index] = off_ds["bam_q_uc"][:].filled(self.FILL["f8"])
                    # off_dict["hivdi_q_uc"][index] = off_ds["hivdi_q_uc"][:].filled(self.FILL["f8"])
                    # off_dict["momma_q_uc"][index] = off_ds["momma_q_uc"][:].filled(self.FILL["f8"])
                    # off_dict["sads_q_uc"][index] = off_ds["sads_q_uc"][:].filled(self.FILL["f8"])
                    # off_dict["consensus_q_uc"][index] = off_ds["consensus_q_uc"][:].filled(self.FILL["f8"])
                    # off_ds.close()
                index += 1
        return off_dict

    def create_data_dict(self):
        """Creates and returns Offline data dictionary."""

        data_dict = {
            "d_x_area" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "d_x_area_u" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "metro_q_c" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "bam_q_c" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "hivdi_q_c" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "momma_q_c" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "sads_q_c" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "consensus_q_c" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "metro_q_uc" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "bam_q_uc" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "hivdi_q_uc" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "momma_q_uc" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "sads_q_uc" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "consensus_q_uc" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "attrs": {
                "d_x_area" : {},
                "d_x_area_u" : {},
                "metro_q_c" : {},
                "bam_q_c" : {},
                "hivdi_q_c" : {},
                "momma_q_c" : {},
                "sads_q_c" : {},
                "consensus_q_c" : {},
                "metro_q_uc" : {},
                "bam_q_uc" : {},
                "hivdi_q_uc" : {},
                "momma_q_uc" : {},
                "sads_q_uc" : {},
                "consensus_q_uc" : {}
            }
        }
        
        # Vlen variables
        data_dict["d_x_area"].fill(np.array([self.FILL["f8"]]))
        data_dict["d_x_area_u"].fill(np.array([self.FILL["f8"]]))
        data_dict["metro_q_c"].fill(np.array([self.FILL["f8"]]))
        data_dict["bam_q_c"].fill(np.array([self.FILL["f8"]]))
        data_dict["hivdi_q_c"].fill(np.array([self.FILL["f8"]]))
        data_dict["momma_q_c"].fill(np.array([self.FILL["f8"]]))
        data_dict["sads_q_c"].fill(np.array([self.FILL["f8"]]))
        data_dict["consensus_q_c"].fill(np.array([self.FILL["f8"]]))
        data_dict["metro_q_uc"].fill(np.array([self.FILL["f8"]]))
        data_dict["bam_q_uc"].fill(np.array([self.FILL["f8"]]))
        data_dict["hivdi_q_uc"].fill(np.array([self.FILL["f8"]]))
        data_dict["momma_q_uc"].fill(np.array([self.FILL["f8"]]))
        data_dict["sads_q_uc"].fill(np.array([self.FILL["f8"]]))
        data_dict["consensus_q_uc"].fill(np.array([self.FILL["f8"]]))
        return data_dict
    
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of Offline variables
        """
        
        convention_dict = {
            "metro_q_c":"dschg_gm",
            "bam_q_c":"dschg_gb",
            "hivdi_q_c":"dschg_gh",
            "momma_q_c":"dschg_go",
            "sads_q_c":"dschg_gs",
            "consensus_q_c":"dschg_gc",
            "metro_q_uc":"dschg_m",
            "bam_q_uc":"dschg_b",
            "hivdi_q_uc":"dschg_h",
            "momma_q_uc":"dschg_o",
            "sads_q_uc":"dschg_s",
            # "sic4dvar_q_uc":"dschg_i",
            "consensus_q_uc":"dschg_c",
            "d_x_area":"d_x_area",
            "d_x_area_u":"d_x_area_u",
        }
        ds = Dataset(nc_file, 'r')
        for key in data_dict["attrs"].keys():
            if key == "d_x_area_u" and "d_x_area_u" not in ds.variables.keys(): continue
            data_dict["attrs"][key] = ds[convention_dict[key]].__dict__        
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
        self.write_var_nt(off_grp, "d_x_area", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(off_grp, "d_x_area_u", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(off_grp, "metro_q_c", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(off_grp, "bam_q_c", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(off_grp, "hivdi_q_c", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(off_grp, "momma_q_c", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(off_grp, "sads_q_c", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(off_grp, "consensus_q_c", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(off_grp, "metro_q_uc", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(off_grp, "bam_q_uc", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(off_grp, "hivdi_q_uc", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(off_grp, "momma_q_uc", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(off_grp, "sads_q_uc", self.vlen_f, ("num_reaches"), data_dict)
        self.write_var_nt(off_grp, "consensus_q_uc", self.vlen_f, ("num_reaches"), data_dict)
        sos_ds.close()