# Standard imports
from pathlib import Path
from shutil import copyfile
import unittest

# Third-party imports
from netCDF4 import Dataset, chartostring
import numpy as np
from numpy.testing import assert_array_equal

# Local imports
from output.modules.Postdiagnostics import Postdiagnostics

class test_Postdiagnostics(unittest.TestCase):
    """Test Postdiagnostics class methods."""
    
    SOS_NEW = Path(__file__).parent / "sos_new" / "na_apriori_rivers_v07_SOS_results.nc"
    PD_DIR = Path(__file__).parent / "postdiags"
    PD_SOS = Path(__file__).parent / "postdiags" / "na_apriori_rivers_v07_SOS_results.nc"
    
    def get_sos_data(self):
        """Retrieve and return dictionary of SoS data."""
        
        ds = Dataset(self.SOS_NEW, 'r')
        rids = ds["reaches"]["reach_id"][:]
        nrids = ds["nodes"]["reach_id"][:]
        nids = ds["nodes"]["node_id"][:]
        ds.close()
        return { "reaches": rids, "node_reaches": nrids, "nodes": nids }
    
    def test_get_module_data(self):
        """Test get_module_data method."""
        
        # File operations to prepare for test
        copyfile(self.SOS_NEW, self.PD_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        pd = Postdiagnostics([7,8,9], self.PD_DIR, self.SOS_NEW, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        pd_dict = pd.get_module_data()
        
        # Assert results
        i = np.where(sos_data["reaches"] == 77449100071)
        # Reach
        e_algo = ["geobam", "hivdi", "momma", "metroman", "sad", "sic4dvar5", "sic4dvar31"]
        assert_array_equal(e_algo, pd_dict["reach_algo_names"])
        e_rf = [[1, 1, 1, 1, 1, 0, 1]]
        assert_array_equal(e_rf, pd_dict["reach"]["realism_flags"][i])
        e_sf = [[1, 1, 1, 1, 1, 0, 1]]
        assert_array_equal(e_sf, pd_dict["reach"]["stability_flags"][i])
        # Basin
        e_algo = ["geobam", "hivdi", "momma", "metroman", "sad", "sic4dvar"]
        assert_array_equal(e_algo, pd_dict["basin_algo_names"])
        e_rf = [[1, 1, 1, 1, 1, 1]]
        assert_array_equal(e_rf, pd_dict["basin"]["realism_flags"][i])
        e_sf = [[1, 1, 1, 1, 1, 0]]
        assert_array_equal(e_sf, pd_dict["basin"]["stability_flags"][i])
        e_pf = [[1, 1, 1, 1, 1, 1]]
        assert_array_equal(e_pf, pd_dict["basin"]["prepost_flags"][i])

        # Clean up
        self.PD_SOS.unlink()
        
    def test_append_module_data(self):
        """Tests append_module_data method."""
        
        # File operations to prepare for test
        copyfile(self.SOS_NEW, self.PD_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        pd = Postdiagnostics([7,8,9], self.PD_DIR, self.PD_SOS, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        pd_dict = pd.get_module_data()
        pd.append_module_data(pd_dict)
        
        # Assert results
        sos = Dataset(self.PD_SOS, 'r')
        i = np.where(sos_data["reaches"] == 77449100071)
        # Reach
        r_grp = sos["postdiagnostics"]["reach"]
        e_algo = ["geobam", "hivdi", "momma", "metroman", "sad", "sic4dvar5", "sic4dvar31"]
        assert_array_equal(e_algo, chartostring(r_grp["reach_algo_names"][:]))
        e_rf = [[1, 1, 1, 1, 1, 0, 1]]
        assert_array_equal(e_rf, r_grp["realism_flags"][i])
        e_sf = [[1, 1, 1, 1, 1, 0, 1]]
        assert_array_equal(e_sf, r_grp["stability_flags"][i])
        # Basin
        b_grp = sos["postdiagnostics"]["basin"]
        e_algo = ["geobam", "hivdi", "momma", "metroman", "sad", "sic4dvar"]
        assert_array_equal(e_algo, chartostring(b_grp["basin_algo_names"][:]))
        e_rf = [[1, 1, 1, 1, 1, 1]]
        assert_array_equal(e_rf, b_grp["realism_flags"][i])
        e_sf = [[1, 1, 1, 1, 1, 0]]
        assert_array_equal(e_sf, b_grp["stability_flags"][i])
        e_pf = [[1, 1, 1, 1, 1, 1]]
        assert_array_equal(e_pf, b_grp["prepost_flags"][i])
        
        # Clean up
        sos.close()
        self.PD_SOS.unlink()