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
    create_data_dict(nt=None)
        creates and returns module data dictionary.
    get_module_data(nt=None)
        retrieve module results from NetCDF files.
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
        """Extract MOI results from NetCDF files.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        # Files and reach identifiers
        moi_dir = self.input_dir
        moi_files = [ Path(moi_file) for moi_file in glob.glob(f"{moi_dir}/{self.cont_ids}*.nc") ] 
        moi_rids = [ int(moi_file.name.split('_')[0]) for moi_file in moi_files ]

        # Storage of results data
        moi_dict = self.create_data_dict(nt)
        
        # Storage of variable attributes
        self.get_nc_attrs(moi_dir / moi_files[0], moi_dict)
        
        if len(moi_files) != 0:
            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in moi_rids:
                    moi_ds = Dataset(moi_dir / f"{s_rid}_integrator.nc", 'r')
                    moi_dict["geobam"]["q"][index, :] = moi_ds["geobam"]["q"][:].filled(np.nan)
                    moi_dict["geobam"]["a0"][index] = moi_ds["geobam"]["a0"][:].filled(np.nan)
                    moi_dict["geobam"]["n"][index] = moi_ds["geobam"]["n"][:].filled(np.nan)
                    moi_dict["geobam"]["qbar_reachScale"][index] = moi_ds["geobam"]["qbar_reachScale"][:].filled(np.nan)
                    moi_dict["geobam"]["qbar_basinScale"][index] = moi_ds["geobam"]["qbar_basinScale"][:].filled(np.nan)
                    
                    moi_dict["hivdi"]["q"][index, :] = moi_ds["hivdi"]["q"][:].filled(np.nan)
                    moi_dict["hivdi"]["Abar"][index] = moi_ds["hivdi"]["Abar"][:].filled(np.nan)
                    moi_dict["hivdi"]["alpha"][index] = moi_ds["hivdi"]["alpha"][:].filled(np.nan)
                    moi_dict["hivdi"]["beta"][index] = moi_ds["hivdi"]["beta"][:].filled(np.nan)
                    moi_dict["hivdi"]["qbar_reachScale"][index] = moi_ds["hivdi"]["qbar_reachScale"][:].filled(np.nan)
                    moi_dict["hivdi"]["qbar_basinScale"][index] = moi_ds["hivdi"]["qbar_basinScale"][:].filled(np.nan)
                    
                    moi_dict["metroman"]["q"][index, :] = moi_ds["metroman"]["q"][:].filled(np.nan)
                    moi_dict["metroman"]["Abar"][index] = moi_ds["metroman"]["Abar"][:].filled(np.nan)
                    moi_dict["metroman"]["na"][index] = moi_ds["metroman"]["na"][:].filled(np.nan)
                    moi_dict["metroman"]["x1"][index] = moi_ds["metroman"]["x1"][:].filled(np.nan)
                    moi_dict["metroman"]["qbar_reachScale"][index] = moi_ds["metroman"]["qbar_reachScale"][:].filled(np.nan)
                    moi_dict["metroman"]["qbar_basinScale"][index] = moi_ds["metroman"]["qbar_basinScale"][:].filled(np.nan)
                    
                    moi_dict["momma"]["q"][index, :] = moi_ds["momma"]["q"][:].filled(np.nan)
                    moi_dict["momma"]["B"][index] = moi_ds["momma"]["B"][:].filled(np.nan)
                    moi_dict["momma"]["H"][index] = moi_ds["momma"]["H"][:].filled(np.nan)
                    moi_dict["momma"]["Save"][index] = moi_ds["momma"]["Save"][:].filled(np.nan)
                    moi_dict["momma"]["qbar_reachScale"][index] = moi_ds["momma"]["qbar_reachScale"][:].filled(np.nan)
                    moi_dict["momma"]["qbar_basinScale"][index] = moi_ds["momma"]["qbar_basinScale"][:].filled(np.nan)

                    moi_dict["sad"]["q"][index, :] = moi_ds["sad"]["q"][:].filled(np.nan)
                    moi_dict["sad"]["a0"][index] = moi_ds["sad"]["a0"][:].filled(np.nan)
                    moi_dict["sad"]["n"][index] = moi_ds["sad"]["n"][:].filled(np.nan)
                    moi_dict["sad"]["qbar_reachScale"][index] = moi_ds["sad"]["qbar_reachScale"][:].filled(np.nan)
                    moi_dict["sad"]["qbar_basinScale"][index] = moi_ds["sad"]["qbar_basinScale"][:].filled(np.nan)

                    moi_dict["sic4dvar"]["q"][index, :] = moi_ds["sic4dvar"]["q"][:].filled(np.nan)
                    moi_dict["sic4dvar"]["a0"][index] = moi_ds["sic4dvar"]["a0"][:].filled(np.nan)
                    moi_dict["sic4dvar"]["n"][index] = moi_ds["sic4dvar"]["n"][:].filled(np.nan)
                    moi_dict["sic4dvar"]["qbar_reachScale"][index] = moi_ds["sic4dvar"]["qbar_reachScale"][:].filled(np.nan)
                    moi_dict["sic4dvar"]["qbar_basinScale"][index] = moi_ds["sic4dvar"]["qbar_basinScale"][:].filled(np.nan)
                    
                    moi_ds.close()
                index += 1
        return moi_dict
    
    def create_data_dict(self, nt=None):
        """Creates and returns MOI data dictionary.
        
        Parameters
        ----------
        nt: int
            number of time steps
        """

        return {
            "nt" : nt,
            "geobam" : {
                "q" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "a0" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "n" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": None,
                    "a0": None,
                    "n": None,
                    "qbar_reachScale": None,
                    "qbar_basinScale": None
                }
            },
            "hivdi" : {
                "q" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "Abar" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "alpha" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "beta" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": None,
                    "Abar": None,
                    "alpha": None,
                    "beta": None,
                    "qbar_reachScale": None,
                    "qbar_basinScale": None
                }
            },
            "metroman" : {
                "q" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "Abar" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "na" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "x1" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": None,
                    "Abar": None,
                    "na": None,
                    "x1": None,
                    "qbar_reachScale": None,
                    "qbar_basinScale": None
                }
            },
            "momma" : {
                "q" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "B" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "H" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "Save" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": None,
                    "B": None,
                    "H": None,
                    "Save": None,
                    "qbar_reachScale": None,
                    "qbar_basinScale": None
                }
            },
            "sad" : {
                "q" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "a0" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "n" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": None,
                    "a0": None,
                    "n": None,
                    "qbar_reachScale": None,
                    "qbar_basinScale": None
                }
            },
            "sic4dvar" : {
                "q" : np.full((self.sos_rids.shape[0], nt), np.nan, dtype=np.float64),
                "a0" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "n" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale" : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": None,
                    "a0": None,
                    "n": None,
                    "qbar_reachScale": None,
                    "qbar_basinScale": None
                }
            }
        }
        
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
                value["attrs"][key2] = ds[key1][key2].__dict__
        ds.close()

    def append_module_data(self, data_dict):
        """Append MOI data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of MOI variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        moi_grp = sos_ds.createGroup("moi")

        # MOI data
        # geobam
        gb_grp = moi_grp.createGroup("geobam")
        self.write_var(gb_grp, "q", "f8", ("num_reaches", "time_steps"), data_dict["geobam"])
        self.write_var(gb_grp, "a0", "f8", ("num_reaches",), data_dict["geobam"])
        self.write_var(gb_grp, "n", "f8", ("num_reaches",), data_dict["geobam"])
        self.write_var(gb_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["geobam"])
        self.write_var(gb_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["geobam"])

        # hivdi
        hv_grp = moi_grp.createGroup("hivdi")
        self.write_var(hv_grp, "q", "f8", ("num_reaches", "time_steps"), data_dict["hivdi"])
        self.write_var(hv_grp, "Abar", "f8", ("num_reaches",), data_dict["hivdi"])
        self.write_var(hv_grp, "alpha", "f8", ("num_reaches",), data_dict["hivdi"])
        self.write_var(hv_grp, "beta", "f8", ("num_reaches",), data_dict["hivdi"])
        self.write_var(hv_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["hivdi"])
        self.write_var(hv_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["hivdi"])

        # metroman
        mm_grp = moi_grp.createGroup("metroman")
        self.write_var(mm_grp, "q", "f8", ("num_reaches", "time_steps"), data_dict["metroman"])
        self.write_var(mm_grp, "Abar", "f8", ("num_reaches",), data_dict["metroman"])
        self.write_var(mm_grp, "na", "f8", ("num_reaches",), data_dict["metroman"])
        self.write_var(mm_grp, "x1", "f8", ("num_reaches",), data_dict["metroman"])
        self.write_var(mm_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["metroman"])
        self.write_var(mm_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["metroman"])

        # momma
        mo_grp = moi_grp.createGroup("momma")
        self.write_var(mo_grp, "q", "f8", ("num_reaches", "time_steps"), data_dict["momma"])
        self.write_var(mo_grp, "B", "f8", ("num_reaches",), data_dict["momma"])
        self.write_var(mo_grp, "H", "f8", ("num_reaches",), data_dict["momma"])
        self.write_var(mo_grp, "Save", "f8", ("num_reaches",), data_dict["momma"])
        self.write_var(mo_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["momma"])
        self.write_var(mo_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["momma"])

        # sad
        gb_grp = moi_grp.createGroup("sad")
        self.write_var(gb_grp, "q", "f8", ("num_reaches", "time_steps"), data_dict["sad"])
        self.write_var(gb_grp, "a0", "f8", ("num_reaches",), data_dict["sad"])
        self.write_var(gb_grp, "n", "f8", ("num_reaches",), data_dict["sad"])
        self.write_var(gb_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["sad"])
        self.write_var(gb_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["sad"])

        # sic4dvar
        gb_grp = moi_grp.createGroup("sic4dvar")
        self.write_var(gb_grp, "q", "f8", ("num_reaches", "time_steps"), data_dict["sic4dvar"])
        self.write_var(gb_grp, "a0", "f8", ("num_reaches",), data_dict["sic4dvar"])
        self.write_var(gb_grp, "n", "f8", ("num_reaches",), data_dict["sic4dvar"])
        self.write_var(gb_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["sic4dvar"])
        self.write_var(gb_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["sic4dvar"])

        sos_ds.close()