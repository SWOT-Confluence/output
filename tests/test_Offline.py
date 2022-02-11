# Standard imports
from pathlib import Path
from shutil import copyfile
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_almost_equal

# Local imports
from output.modules.Offline import Offline

class test_Offline(unittest.TestCase):
    """Test Offline class methods."""
    
    SOS_NEW = Path(__file__).parent / "sos_new" / "na_apriori_rivers_v07_SOS_results.nc"
    OFF_DIR = Path(__file__).parent / "offline"
    OFF_SOS = Path(__file__).parent / "offline" / "na_apriori_rivers_v07_SOS_results.nc"
    FILL = {
        "f8": -999999999999.0,
        "i4": -999,
        "S1": "x"
    }
    
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
        copyfile(self.SOS_NEW, self.OFF_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        off = Offline([7,8,9], self.OFF_DIR, self.SOS_NEW, None, None, None, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        off_dict = off.get_module_data()
        
        # Assert results
        i = np.where(sos_data["reaches"] == 77449100071)
        e_con_c = [89.85850822268253, self.FILL["f8"], 59.08326367903342, 120.0903098274155, self.FILL["f8"], 112.18910879591483, 1058.9932899555693, self.FILL["f8"], 271.3415851648624, self.FILL["f8"], self.FILL["f8"], 176.54989668918142, 130.27503953092707, self.FILL["f8"], 168.04993144445604, 138.6774411441166, self.FILL["f8"], 199.0906316632627, self.FILL["f8"], self.FILL["f8"], 170.57211198788613, 224.38644089860577, self.FILL["f8"], 242.0805597931324, 155.74623377459574]
        assert_array_almost_equal(e_con_c, off_dict["consensus_q_c"][i][0])
        e_con_uc = [self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"]]
        assert_array_almost_equal(e_con_uc, off_dict["consensus_q_uc"][i][0])
        e_hv_c = [89.40380742536559, self.FILL["f8"], 57.46602514463849, 120.0903098274155, self.FILL["f8"], 112.18910879591483, 1058.9932899555693, self.FILL["f8"], 271.34083972004515, self.FILL["f8"], self.FILL["f8"], 176.54975896676285, 129.91339662818254, self.FILL["f8"], 173.6599722752703, 137.61136042610076, self.FILL["f8"], 199.0906316632627, self.FILL["f8"], self.FILL["f8"], 170.57211198788613, 223.60864667938154, self.FILL["f8"], 248.78478653239463, 158.53334042704978]
        assert_array_almost_equal(e_hv_c, off_dict["hivdi_q_c"][i][0])
        e_hv_uc = [self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"]]
        assert_array_almost_equal(e_hv_uc, off_dict["hivdi_q_uc"][i][0])
        e_sd_c = [89.85850822268253, self.FILL["f8"], 59.08326367903342, 115.17987421100219, self.FILL["f8"], 105.15305731500284, 1162.3155583168057, self.FILL["f8"], 272.1655495754087, self.FILL["f8"], self.FILL["f8"], 179.70025601694243, 130.27503953092707, self.FILL["f8"], 166.81683624130045, 138.6774411441166, self.FILL["f8"], 182.2578602730342, self.FILL["f8"], self.FILL["f8"], 174.19714603039236, 225.59086600679103, self.FILL["f8"], 235.10624981326959, 151.76513879027635]
        assert_array_almost_equal(e_sd_c, off_dict["sads_q_c"][i][0])
        e_sd_uc = [self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"]]
        assert_array_almost_equal(e_sd_uc, off_dict["sads_q_uc"][i][0])

        # Clean up
        self.OFF_SOS.unlink()
        
    def test_append_module_data(self):
        """Tests append_module_data method."""
        
        # File operations to prepare for test
        copyfile(self.SOS_NEW, self.OFF_SOS)
        sos_data = self.get_sos_data()
        
        # Create vlen types in SOS
        sos = Dataset(self.OFF_SOS, 'a')
        vlen_f = sos.createVLType(np.float64, "vlen_float")
        vlen_i = sos.createVLType(np.int32, "vlen_int")
        vlen_s = sos.createVLType("S1", "vlen_str")
        sos.close()
        
        # Run method
        off = Offline([7,8,9], self.OFF_DIR, self.OFF_SOS, vlen_f, vlen_i, vlen_s, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        off_dict = off.get_module_data()
        off.append_module_data(off_dict)
        
        # Assert results
        sos = Dataset(self.OFF_SOS, 'r')
        off_grp = sos["offline"]
        i = np.where(sos_data["reaches"] == 77449100071)
        e_con_c = [89.85850822268253, self.FILL["f8"], 59.08326367903342, 120.0903098274155, self.FILL["f8"], 112.18910879591483, 1058.9932899555693, self.FILL["f8"], 271.3415851648624, self.FILL["f8"], self.FILL["f8"], 176.54989668918142, 130.27503953092707, self.FILL["f8"], 168.04993144445604, 138.6774411441166, self.FILL["f8"], 199.0906316632627, self.FILL["f8"], self.FILL["f8"], 170.57211198788613, 224.38644089860577, self.FILL["f8"], 242.0805597931324, 155.74623377459574]
        assert_array_almost_equal(e_con_c, off_grp["consensus_q_c"][i][0])
        e_con_uc = [self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"]]
        assert_array_almost_equal(e_con_uc, off_grp["consensus_q_uc"][i][0])
        e_hv_c = [89.40380742536559, self.FILL["f8"], 57.46602514463849, 120.0903098274155, self.FILL["f8"], 112.18910879591483, 1058.9932899555693, self.FILL["f8"], 271.34083972004515, self.FILL["f8"], self.FILL["f8"], 176.54975896676285, 129.91339662818254, self.FILL["f8"], 173.6599722752703, 137.61136042610076, self.FILL["f8"], 199.0906316632627, self.FILL["f8"], self.FILL["f8"], 170.57211198788613, 223.60864667938154, self.FILL["f8"], 248.78478653239463, 158.53334042704978]
        assert_array_almost_equal(e_hv_c, off_grp["hivdi_q_c"][i][0])
        e_hv_uc = [self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"]]
        assert_array_almost_equal(e_hv_uc, off_grp["hivdi_q_uc"][i][0])
        e_sd_c = [89.85850822268253, self.FILL["f8"], 59.08326367903342, 115.17987421100219, self.FILL["f8"], 105.15305731500284, 1162.3155583168057, self.FILL["f8"], 272.1655495754087, self.FILL["f8"], self.FILL["f8"], 179.70025601694243, 130.27503953092707, self.FILL["f8"], 166.81683624130045, 138.6774411441166, self.FILL["f8"], 182.2578602730342, self.FILL["f8"], self.FILL["f8"], 174.19714603039236, 225.59086600679103, self.FILL["f8"], 235.10624981326959, 151.76513879027635]
        assert_array_almost_equal(e_sd_c, off_grp["sads_q_c"][i][0])
        e_sd_uc = [self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"]]
        assert_array_almost_equal(e_sd_uc, off_grp["sads_q_uc"][i][0])
        
        # Clean up
        sos.close()
        self.OFF_SOS.unlink()