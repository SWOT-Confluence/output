# Standard imports
from pathlib import Path
from shutil import copyfile
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_almost_equal

# Local imports
from output.modules.GeoBam import GeoBAM
from output.modules.Offline import Offline

class test_Geobam(unittest.TestCase):
    """Test GeoBam class methods."""
    
    SOS_NEW = Path(__file__).parent / "sos_new" / "na_apriori_rivers_v07_SOS_results.nc"
    OFF_DIR = Path(__file__).parent / "offline"
    OFF_SOS = Path(__file__).parent / "offline" / "na_apriori_rivers_v07_SOS_results.nc"
    
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
        off = Offline([7,8,9], self.OFF_DIR, self.SOS_NEW, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        off_dict = off.get_module_data(25)
        
        # Assert results
        i = np.where(sos_data["reaches"] == 77449100071)
        e_con_c = [[89.85850822268253, np.nan, 59.08326367903342, 120.0903098274155, np.nan, 112.18910879591483, 1058.9932899555693, np.nan, 271.3415851648624, np.nan, np.nan, 176.54989668918142, 130.27503953092707, np.nan, 168.04993144445604, 138.6774411441166, np.nan, 199.0906316632627, np.nan, np.nan, 170.57211198788613, 224.38644089860577, np.nan, 242.0805597931324, 155.74623377459574]]
        assert_array_almost_equal(e_con_c, off_dict["consensus_q_c"][i])
        e_con_uc = [[np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_con_uc, off_dict["consensus_q_uc"][i])
        e_hv_c = [[89.40380742536559, np.nan, 57.46602514463849, 120.0903098274155, np.nan, 112.18910879591483, 1058.9932899555693, np.nan, 271.34083972004515, np.nan, np.nan, 176.54975896676285, 129.91339662818254, np.nan, 173.6599722752703, 137.61136042610076, np.nan, 199.0906316632627, np.nan, np.nan, 170.57211198788613, 223.60864667938154, np.nan, 248.78478653239463, 158.53334042704978]]
        assert_array_almost_equal(e_hv_c, off_dict["hivdi_q_c"][i])
        e_hv_uc = [[np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_hv_uc, off_dict["hivdi_q_uc"][i])
        e_sd_c = [[89.85850822268253, np.nan, 59.08326367903342, 115.17987421100219, np.nan, 105.15305731500284, 1162.3155583168057, np.nan, 272.1655495754087, np.nan, np.nan, 179.70025601694243, 130.27503953092707, np.nan, 166.81683624130045, 138.6774411441166, np.nan, 182.2578602730342, np.nan, np.nan, 174.19714603039236, 225.59086600679103, np.nan, 235.10624981326959, 151.76513879027635]]
        assert_array_almost_equal(e_sd_c, off_dict["sads_q_c"][i])
        e_sd_uc = [[np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_sd_uc, off_dict["sads_q_uc"][i])

        # Clean up
        self.OFF_SOS.unlink()
        
    def test_append_module_data(self):
        """Tests append_module_data method."""
        
        # File operations to prepare for test
        copyfile(self.SOS_NEW, self.OFF_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        off = Offline([7,8,9], self.OFF_DIR, self.OFF_SOS, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        off_dict = off.get_module_data(25)
        off.append_module_data(off_dict)
        
        # Assert results
        sos = Dataset(self.OFF_SOS, 'r')
        off_grp = sos["offline"]
        i = np.where(sos_data["reaches"] == 77449100071)
        e_con_c = [[89.85850822268253, np.nan, 59.08326367903342, 120.0903098274155, np.nan, 112.18910879591483, 1058.9932899555693, np.nan, 271.3415851648624, np.nan, np.nan, 176.54989668918142, 130.27503953092707, np.nan, 168.04993144445604, 138.6774411441166, np.nan, 199.0906316632627, np.nan, np.nan, 170.57211198788613, 224.38644089860577, np.nan, 242.0805597931324, 155.74623377459574]]
        assert_array_almost_equal(e_con_c, off_grp["consensus_q_c"][i].filled(np.nan))
        e_con_uc = [[np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_con_uc, off_grp["consensus_q_uc"][i].filled(np.nan))
        e_hv_c = [[89.40380742536559, np.nan, 57.46602514463849, 120.0903098274155, np.nan, 112.18910879591483, 1058.9932899555693, np.nan, 271.34083972004515, np.nan, np.nan, 176.54975896676285, 129.91339662818254, np.nan, 173.6599722752703, 137.61136042610076, np.nan, 199.0906316632627, np.nan, np.nan, 170.57211198788613, 223.60864667938154, np.nan, 248.78478653239463, 158.53334042704978]]
        assert_array_almost_equal(e_hv_c, off_grp["hivdi_q_c"][i].filled(np.nan))
        e_hv_uc = [[np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_hv_uc, off_grp["hivdi_q_uc"][i].filled(np.nan))
        e_sd_c = [[89.85850822268253, np.nan, 59.08326367903342, 115.17987421100219, np.nan, 105.15305731500284, 1162.3155583168057, np.nan, 272.1655495754087, np.nan, np.nan, 179.70025601694243, 130.27503953092707, np.nan, 166.81683624130045, 138.6774411441166, np.nan, 182.2578602730342, np.nan, np.nan, 174.19714603039236, 225.59086600679103, np.nan, 235.10624981326959, 151.76513879027635]]
        assert_array_almost_equal(e_sd_c, off_grp["sads_q_c"][i].filled(np.nan))
        e_sd_uc = [[np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_sd_uc, off_grp["sads_q_uc"][i].filled(np.nan))
        
        # Clean up
        sos.close()
        self.OFF_SOS.unlink()