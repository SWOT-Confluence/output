# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Moi(AbstractModule):
    """A class that represent the results of running MOI.
    
    Data and operations append MOI results to the SoS on the appropriate 
    dimenstions.

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

    def __init__(self, cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s, \
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
        """Extract MOI results from NetCDF files."""

        # Files and reach identifiers
        moi_dir = self.input_dir
        moi_files = [ Path(moi_file) for moi_file in glob.glob(f"{moi_dir}/{self.cont_ids}*.nc") ] 
        moi_rids = [ int(moi_file.name.split('_')[0]) for moi_file in moi_files ]

        # Storage of results data
        moi_dict = self.create_data_dict()
        
        if len(moi_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(moi_dir / moi_files[0], moi_dict)
            
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in moi_rids:
                    try:
                        moi_ds = Dataset(moi_dir / f"{int(s_rid)}_integrator.nc", 'r')
                        moi_dict["neobam"]["q"][index] = moi_ds["neobam"]["q"][:].filled(self.FILL["f8"])
                        moi_dict["neobam"]["a0"][index] = moi_ds["neobam"]["a0"][:].filled(np.nan)
                        moi_dict["neobam"]["n"][index] = moi_ds["neobam"]["n"][:].filled(np.nan)
                        moi_dict["neobam"]["qbar_reachScale"][index] = moi_ds["neobam"]["qbar_reachScale"][:].filled(np.nan)
                        moi_dict["neobam"]["qbar_basinScale"][index] = moi_ds["neobam"]["qbar_basinScale"][:].filled(np.nan)
                        
                        moi_dict["hivdi"]["q"][index] = moi_ds["hivdi"]["q"][:].filled(self.FILL["f8"])
                        moi_dict["hivdi"]["Abar"][index] = moi_ds["hivdi"]["Abar"][:].filled(np.nan)
                        moi_dict["hivdi"]["alpha"][index] = moi_ds["hivdi"]["alpha"][:].filled(np.nan)
                        moi_dict["hivdi"]["beta"][index] = moi_ds["hivdi"]["beta"][:].filled(np.nan)
                        moi_dict["hivdi"]["qbar_reachScale"][index] = moi_ds["hivdi"]["qbar_reachScale"][:].filled(np.nan)
                        moi_dict["hivdi"]["qbar_basinScale"][index] = moi_ds["hivdi"]["qbar_basinScale"][:].filled(np.nan)
                        
                        moi_dict["metroman"]["q"][index] = moi_ds["metroman"]["q"][:].filled(self.FILL["f8"])
                        moi_dict["metroman"]["Abar"][index] = moi_ds["metroman"]["Abar"][:].filled(np.nan)
                        moi_dict["metroman"]["na"][index] = moi_ds["metroman"]["na"][:].filled(np.nan)
                        moi_dict["metroman"]["x1"][index] = moi_ds["metroman"]["x1"][:].filled(np.nan)
                        moi_dict["metroman"]["qbar_reachScale"][index] = moi_ds["metroman"]["qbar_reachScale"][:].filled(np.nan)
                        moi_dict["metroman"]["qbar_basinScale"][index] = moi_ds["metroman"]["qbar_basinScale"][:].filled(np.nan)
                        
                        moi_dict["momma"]["q"][index] = moi_ds["momma"]["q"][:].filled(self.FILL["f8"])
                        moi_dict["momma"]["B"][index] = moi_ds["momma"]["B"][:].filled(np.nan)
                        moi_dict["momma"]["H"][index] = moi_ds["momma"]["H"][:].filled(np.nan)
                        moi_dict["momma"]["Save"][index] = moi_ds["momma"]["Save"][:].filled(np.nan)
                        moi_dict["momma"]["qbar_reachScale"][index] = moi_ds["momma"]["qbar_reachScale"][:].filled(np.nan)
                        moi_dict["momma"]["qbar_basinScale"][index] = moi_ds["momma"]["qbar_basinScale"][:].filled(np.nan)

                        moi_dict["sad"]["q"][index] = moi_ds["sad"]["q"][:].filled(self.FILL["f8"])
                        moi_dict["sad"]["a0"][index] = moi_ds["sad"]["a0"][:].filled(np.nan)
                        moi_dict["sad"]["n"][index] = moi_ds["sad"]["n"][:].filled(np.nan)
                        moi_dict["sad"]["qbar_reachScale"][index] = moi_ds["sad"]["qbar_reachScale"][:].filled(np.nan)
                        moi_dict["sad"]["qbar_basinScale"][index] = moi_ds["sad"]["qbar_basinScale"][:].filled(np.nan)

                        moi_dict["sic4dvar"]["q"][index] = moi_ds["sic4dvar"]["q"][:].filled(self.FILL["f8"])
                        moi_dict["sic4dvar"]["a0"][index] = moi_ds["sic4dvar"]["a0"][:].filled(np.nan)
                        moi_dict["sic4dvar"]["n"][index] = moi_ds["sic4dvar"]["n"][:].filled(np.nan)
                        moi_dict["sic4dvar"]["qbar_reachScale"][index] = moi_ds["sic4dvar"]["qbar_reachScale"][:].filled(np.nan)
                        moi_dict["sic4dvar"]["qbar_basinScale"][index] = moi_ds["sic4dvar"]["qbar_basinScale"][:].filled(np.nan)
                        
                        moi_ds.close()
                    except:
                        try:
                            moi_ds.close()
                        except:
                            pass
                        print(s_rid, 'failed in moi...')
                            
                index += 1
        return moi_dict
    
    def create_data_dict(self):
        """Creates and returns MOI data dictionary."""

        data_dict = {
            "neobam" : {
                "q" : np.empty((self.sos_rids.shape[0]), dtype=object),
                "a0" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "n" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": {},
                    "a0": {},
                    "n": {},
                    "qbar_reachScale": {},
                    "qbar_basinScale": {}
                }
            },
            "hivdi" : {
                "q" : np.empty((self.sos_rids.shape[0]), dtype=object),
                "Abar" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "alpha" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "beta" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": {},
                    "Abar": {},
                    "alpha": {},
                    "beta": {},
                    "qbar_reachScale": {},
                    "qbar_basinScale": {}
                }
            },
            "metroman" : {
                "q" : np.empty((self.sos_rids.shape[0]), dtype=object),
                "Abar" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "na" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "x1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": {},
                    "Abar": {},
                    "na": {},
                    "x1": {},
                    "qbar_reachScale": {},
                    "qbar_basinScale": {}
                }
            },
            "momma" : {
                "q" : np.empty((self.sos_rids.shape[0]), dtype=object),
                "B" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "H" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "Save" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": {},
                    "B": {},
                    "H": {},
                    "Save": {},
                    "qbar_reachScale": {},
                    "qbar_basinScale": {}
                }
            },
            "sad" : {
                "q" : np.empty((self.sos_rids.shape[0]), dtype=object),
                "a0" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "n" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": {},
                    "a0": {},
                    "n": {},
                    "qbar_reachScale": {},
                    "qbar_basinScale": {}
                }
            },
            "sic4dvar" : {
                "q" : np.empty((self.sos_rids.shape[0]), dtype=object),
                "a0" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "n" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": {},
                    "a0": {},
                    "n": {},
                    "qbar_reachScale": {},
                    "qbar_basinScale": {}
                }
            }
        }
        # Vlen variables
        data_dict["neobam"]["q"].fill(np.array([self.FILL["f8"]]))
        data_dict["hivdi"]["q"].fill(np.array([self.FILL["f8"]]))
        data_dict["metroman"]["q"].fill(np.array([self.FILL["f8"]]))
        data_dict["momma"]["q"].fill(np.array([self.FILL["f8"]]))
        data_dict["sad"]["q"].fill(np.array([self.FILL["f8"]]))
        data_dict["sic4dvar"]["q"].fill(np.array([self.FILL["f8"]]))
        
        return data_dict
        
    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of MOI variables
        """
        
        ds = Dataset(nc_file, 'r')
        for key1, value in data_dict.items():
            if key1 == "nt": continue
            for key2 in value["attrs"].keys():
                data_dict[key1]["attrs"][key2] = ds[key1][key2].__dict__
        ds.close()

    def append_module_data(self, data_dict, metadata_json):
        """Append MOI data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of MOI variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        moi_grp = sos_ds.createGroup("moi")

        # MOI data
        # neobam
        gb_grp = moi_grp.createGroup("neobam")
        var = self.write_var_nt(gb_grp, "q", self.vlen_f, ("num_reaches"), data_dict["neobam"])
        self.set_variable_atts(var, metadata_json["moi"]["neobam"]["q"])
        var = self.write_var(gb_grp, "a0", "f8", ("num_reaches",), data_dict["neobam"])
        self.set_variable_atts(var, metadata_json["moi"]["neobam"]["a0"])
        var = self.write_var(gb_grp, "n", "f8", ("num_reaches",), data_dict["neobam"])
        self.set_variable_atts(var, metadata_json["moi"]["neobam"]["n"])
        var = self.write_var(gb_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["neobam"])
        self.set_variable_atts(var, metadata_json["moi"]["neobam"]["qbar_reachScale"])
        var = self.write_var(gb_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["neobam"])
        self.set_variable_atts(var, metadata_json["moi"]["neobam"]["qbar_basinScale"])

        # hivdi
        hv_grp = moi_grp.createGroup("hivdi")
        var = self.write_var_nt(hv_grp, "q", self.vlen_f, ("num_reaches"), data_dict["hivdi"])
        self.set_variable_atts(var, metadata_json["moi"]["hivdi"]["q"])
        var = self.write_var(hv_grp, "Abar", "f8", ("num_reaches",), data_dict["hivdi"])
        self.set_variable_atts(var, metadata_json["moi"]["hivdi"]["Abar"])
        var = self.write_var(hv_grp, "alpha", "f8", ("num_reaches",), data_dict["hivdi"])
        self.set_variable_atts(var, metadata_json["moi"]["hivdi"]["alpha"])
        var = self.write_var(hv_grp, "beta", "f8", ("num_reaches",), data_dict["hivdi"])
        self.set_variable_atts(var, metadata_json["moi"]["hivdi"]["beta"])
        var = self.write_var(hv_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["hivdi"])
        self.set_variable_atts(var, metadata_json["moi"]["hivdi"]["qbar_reachScale"])
        var = self.write_var(hv_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["hivdi"])
        self.set_variable_atts(var, metadata_json["moi"]["hivdi"]["qbar_basinScale"])

        # metroman
        mm_grp = moi_grp.createGroup("metroman")
        var = self.write_var_nt(mm_grp, "q", self.vlen_f, ("num_reaches"), data_dict["metroman"])
        self.set_variable_atts(var, metadata_json["moi"]["metroman"]["q"])
        var = self.write_var(mm_grp, "Abar", "f8", ("num_reaches",), data_dict["metroman"])
        self.set_variable_atts(var, metadata_json["moi"]["metroman"]["Abar"])
        var = self.write_var(mm_grp, "na", "f8", ("num_reaches",), data_dict["metroman"])
        self.set_variable_atts(var, metadata_json["moi"]["metroman"]["na"])
        var = self.write_var(mm_grp, "x1", "f8", ("num_reaches",), data_dict["metroman"])
        self.set_variable_atts(var, metadata_json["moi"]["metroman"]["x1"])
        var = self.write_var(mm_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["metroman"])
        self.set_variable_atts(var, metadata_json["moi"]["metroman"]["qbar_reachScale"])
        var = self.write_var(mm_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["metroman"])
        self.set_variable_atts(var, metadata_json["moi"]["metroman"]["qbar_basinScale"])

        # momma
        mo_grp = moi_grp.createGroup("momma")
        var = self.write_var_nt(mo_grp, "q", self.vlen_f, ("num_reaches"), data_dict["momma"])
        self.set_variable_atts(var, metadata_json["moi"]["momma"]["q"])
        var = self.write_var(mo_grp, "B", "f8", ("num_reaches",), data_dict["momma"])
        self.set_variable_atts(var, metadata_json["moi"]["momma"]["B"])
        var = self.write_var(mo_grp, "H", "f8", ("num_reaches",), data_dict["momma"])
        self.set_variable_atts(var, metadata_json["moi"]["momma"]["H"])
        var = self.write_var(mo_grp, "Save", "f8", ("num_reaches",), data_dict["momma"])
        self.set_variable_atts(var, metadata_json["moi"]["momma"]["Save"])
        var = self.write_var(mo_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["momma"])
        self.set_variable_atts(var, metadata_json["moi"]["momma"]["qbar_reachScale"])
        var = self.write_var(mo_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["momma"])
        self.set_variable_atts(var, metadata_json["moi"]["momma"]["qbar_basinScale"])

        # sad
        gb_grp = moi_grp.createGroup("sad")
        var = self.write_var_nt(gb_grp, "q", self.vlen_f, ("num_reaches"), data_dict["sad"])
        self.set_variable_atts(var, metadata_json["moi"]["sad"]["q"])
        var = self.write_var(gb_grp, "a0", "f8", ("num_reaches",), data_dict["sad"])
        self.set_variable_atts(var, metadata_json["moi"]["sad"]["a0"])
        var = self.write_var(gb_grp, "n", "f8", ("num_reaches",), data_dict["sad"])
        self.set_variable_atts(var, metadata_json["moi"]["sad"]["n"])
        var = self.write_var(gb_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["sad"])
        self.set_variable_atts(var, metadata_json["moi"]["sad"]["qbar_reachScale"])
        var = self.write_var(gb_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["sad"])
        self.set_variable_atts(var, metadata_json["moi"]["sad"]["qbar_basinScale"])

        # sic4dvar
        gb_grp = moi_grp.createGroup("sic4dvar")
        var = self.write_var_nt(gb_grp, "q", self.vlen_f, ("num_reaches"), data_dict["sic4dvar"])
        self.set_variable_atts(var, metadata_json["moi"]["sic4dvar"]["q"])
        var = self.write_var(gb_grp, "a0", "f8", ("num_reaches",), data_dict["sic4dvar"])
        self.set_variable_atts(var, metadata_json["moi"]["sic4dvar"]["a0"])
        var = self.write_var(gb_grp, "n", "f8", ("num_reaches",), data_dict["sic4dvar"])
        self.set_variable_atts(var, metadata_json["moi"]["sic4dvar"]["n"])
        var = self.write_var(gb_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["sic4dvar"])
        self.set_variable_atts(var, metadata_json["moi"]["sic4dvar"]["qbar_reachScale"])
        var = self.write_var(gb_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["sic4dvar"])
        self.set_variable_atts(var, metadata_json["moi"]["sic4dvar"]["qbar_basinScale"])

        sos_ds.close()