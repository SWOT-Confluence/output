# Standard imports
from pathlib import Path
from shutil import copyfile
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_almost_equal, assert_array_equal

# Local imports
from output.modules.Sic4dvar import Sic4dvar

class test_Sic4dvar(unittest.TestCase):
    """Test Sic4dvar class methods."""
    
    SOS_NEW = Path(__file__).parent / "sos_new" / "na_apriori_rivers_v07_SOS_results.nc"
    SV_DIR = Path(__file__).parent / "flpe"
    SV_SOS = Path(__file__).parent / "flpe" / "sic4dvar" / "na_apriori_rivers_v07_SOS_results.nc"
    
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
        copyfile(self.SOS_NEW, self.SV_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        sv = Sic4dvar([7,8,9], self.SV_DIR, self.SOS_NEW, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        sv_dict = sv.get_module_data(25)
        
        # Assert results
        i = np.where(sos_data["reaches"] == 77449100061)
        self.assertAlmostEqual(90231.1058947975, sv_dict["A0"][i])
        self.assertAlmostEqual(0.25, sv_dict["n"][i])
        e_q5 = [[385952.10125669924, np.nan, 403496.86705278844, 319666.2356575589, np.nan, 343807.49095895234, 274121.15020592505, np.nan, 409241.8461504512, 393503.28548204206, np.nan, 387417.6309227186, 418812.2509156531, np.nan, 366004.743657326, 472687.70990219497, np.nan, 368614.770363391, 425233.29815376777, np.nan, 372139.67612399766, 177704.61846383434, np.nan, 283520.14079561236, 339724.17132314603]]
        assert_array_almost_equal(e_q5, sv_dict["Qalgo5"][i])
        e_q31 = [[391.92812203250406, np.nan, 389.9228820973506, 370.0516080057993, np.nan, 414.2119142988095, 1354.9878490455483, np.nan, 449.6980609874542, 367.2360667015813, np.nan, 372.0149160352811, 386.83988704369506, np.nan, 398.25424738964944, 441.4489054270371, np.nan, 390.3331375268637, 414.55654360148435, np.nan, 379.1886033973585, 171.19507006944136, np.nan, 294.27861222149613, 369.0325804018222]]
        assert_array_almost_equal(e_q31, sv_dict["Qalgo31"][i])
        ixs = np.where(sos_data["node_reaches"] == 77449100061)
        e_hw = [30.668317, 30.668317, 49.556449, 50.442531, 58.72346, 64.368724, 85.893091]
        assert_array_almost_equal(e_hw, sv_dict["half_width"][ixs][0][0])
        e_e = [5884.320674968731, 7.12622, 7.65856, 7.98136, 8.47558, 8.56981, 14.55173]
        assert_array_almost_equal(e_e, sv_dict["elevation"][ixs][0][0])
        e_nids = [77449100060011, 77449100060021, 77449100060031, 77449100060041, 77449100060051, 77449100060061, 77449100060071, 77449100060081, 77449100060091, 77449100060101, 77449100060111, 77449100060121, 77449100060131, 77449100060141, 77449100060151, 77449100060161, 77449100060171, 77449100060181, 77449100060191, 77449100060201, 77449100060211, 77449100060221, 77449100060231, 77449100060241, 77449100060251, 77449100060261, 77449100060271, 77449100060281, 77449100060291, 77449100060301, 77449100060311, 77449100060321, 77449100060331, 77449100060341, 77449100060351, 77449100060361, 77449100060371, 77449100060381, 77449100060391, 77449100060401, 77449100060411, 77449100060421, 77449100060431, 77449100060441, 77449100060451, 77449100060461, 77449100060471, 77449100060481, 77449100060491]
        assert_array_equal(e_nids, sv_dict["node_id"][ixs])

        # Clean up
        self.SV_SOS.unlink()
        
    def test_append_module_data(self):
        """Tests append_module_data method."""
        
        # File operations to prepare for test
        copyfile(self.SOS_NEW, self.SV_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        sv = Sic4dvar([7,8,9], self.SV_DIR, self.SV_SOS, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        sv_dict = sv.get_module_data(25)
        sv.append_module_data(sv_dict)
        
        # Assert results
        sos = Dataset(self.SV_SOS, 'r')
        sv_grp = sos["sic4dvar"]
        i = np.where(sos_data["reaches"] == 77449100061)
        self.assertAlmostEqual(90231.1058947975, sv_grp["A0"][i])
        self.assertAlmostEqual(0.25, sv_grp["n"][i])
        e_q5 = [[385952.10125669924, np.nan, 403496.86705278844, 319666.2356575589, np.nan, 343807.49095895234, 274121.15020592505, np.nan, 409241.8461504512, 393503.28548204206, np.nan, 387417.6309227186, 418812.2509156531, np.nan, 366004.743657326, 472687.70990219497, np.nan, 368614.770363391, 425233.29815376777, np.nan, 372139.67612399766, 177704.61846383434, np.nan, 283520.14079561236, 339724.17132314603]]
        assert_array_almost_equal(e_q5, sv_grp["Qalgo5"][i].filled(np.nan))
        e_q31 = [[391.92812203250406, np.nan, 389.9228820973506, 370.0516080057993, np.nan, 414.2119142988095, 1354.9878490455483, np.nan, 449.6980609874542, 367.2360667015813, np.nan, 372.0149160352811, 386.83988704369506, np.nan, 398.25424738964944, 441.4489054270371, np.nan, 390.3331375268637, 414.55654360148435, np.nan, 379.1886033973585, 171.19507006944136, np.nan, 294.27861222149613, 369.0325804018222]]
        assert_array_almost_equal(e_q31, sv_grp["Qalgo31"][i].filled(np.nan))
        e_hw = [30.668317, 30.668317, 49.556449, 50.442531, 58.72346, 64.368724, 85.893091]
        assert_array_almost_equal(e_hw, sv_grp["half_width"][0])
        e_e = [5884.320674968731, 7.12622, 7.65856, 7.98136, 8.47558, 8.56981, 14.55173]
        assert_array_almost_equal(e_e, sv_grp["elevation"][0])
        e_nids = [77449100060011, 77449100060021, 77449100060031, 77449100060041, 77449100060051, 77449100060061, 77449100060071, 77449100060081, 77449100060091, 77449100060101, 77449100060111, 77449100060121, 77449100060131, 77449100060141, 77449100060151, 77449100060161, 77449100060171, 77449100060181, 77449100060191, 77449100060201, 77449100060211, 77449100060221, 77449100060231, 77449100060241, 77449100060251, 77449100060261, 77449100060271, 77449100060281, 77449100060291, 77449100060301, 77449100060311, 77449100060321, 77449100060331, 77449100060341, 77449100060351, 77449100060361, 77449100060371, 77449100060381, 77449100060391, 77449100060401, 77449100060411, 77449100060421, 77449100060431, 77449100060441, 77449100060451, 77449100060461, 77449100060471, 77449100060481, 77449100060491]
        assert_array_equal(e_nids, sv_grp["sic4dvar_node_id"][:])
        e_rnids = [77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061, 77449100061]
        assert_array_equal(e_rnids, sv_grp["sic4dvar_reach_id"][:])
        
        # Clean up
        sos.close()
        self.SV_SOS.unlink()