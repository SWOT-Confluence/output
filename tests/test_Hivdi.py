# Standard imports
from pathlib import Path
from shutil import copyfile
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_almost_equal

# Local imports
from output.modules.Hivdi import Hivdi

class test_Hivdi(unittest.TestCase):
    """Test HiVDI class methods."""
    
    SOS_NEW = Path(__file__).parent / "sos_new" / "na_apriori_rivers_v07_SOS_results.nc"
    HV_DIR = Path(__file__).parent / "flpe"
    HV_SOS = Path(__file__).parent / "flpe" / "hivdi" / "na_apriori_rivers_v07_SOS_results.nc"
    
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
        copyfile(self.SOS_NEW, self.HV_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        hv = Hivdi([7,8,9], self.HV_DIR, self.HV_SOS, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        hv_dict = hv.get_module_data(25)
        
        # Assert results
        i = np.where(sos_data["reaches"] == 77449100071)
        self.assertAlmostEqual(38.64107565929329, hv_dict["reach"]["A0"][i])
        self.assertAlmostEqual(27.567006815217233, hv_dict["reach"]["alpha"][i])
        self.assertAlmostEqual(-0.04585919424135709, hv_dict["reach"]["beta"][i])
        e_q = [[[4.610149210991192, np.nan, 3.0102350163109644, 10.037038809042247, np.nan, 6.898011986438361, 598.5156523955044, np.nan, 74.0595372684194, 3.9057200768121683, np.nan, 36.09096551402864, 14.392669128600598, np.nan, 30.198241507903912, 17.49850915878955, np.nan, 34.86955767770718, 2.8460926115966667, np.nan, 27.85604286186665, 31.66680961116777, np.nan, 44.77828670319133, 26.617193049776915]]]
        assert_array_almost_equal(e_q, hv_dict["reach"]["Q"][i,:])
        
        # Clean up
        self.HV_SOS.unlink()
        
    def test_append_module_data(self):
        """Tests append_module_data method."""
        
        # File operations to prepare for test
        copyfile(self.SOS_NEW, self.HV_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        hv = Hivdi([7,8,9], self.HV_DIR, self.HV_SOS, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        hv_dict = hv.get_module_data(25)
        hv.append_module_data(hv_dict)
        
        # Assert results
        sos = Dataset(self.HV_SOS, 'r')
        hv_grp = sos["hivdi"]
        i = np.where(sos_data["reaches"] == 77449100071)
        assert_array_almost_equal([38.64107565929329], hv_grp["A0"][i])
        assert_array_almost_equal([27.567006815217233], hv_grp["alpha"][i])
        assert_array_almost_equal([-0.04585919424135709], hv_grp["beta"][i])
        e_q = [[4.610149210991192, np.nan, 3.0102350163109644, 10.037038809042247, np.nan, 6.898011986438361, 598.5156523955044, np.nan, 74.0595372684194, 3.9057200768121683, np.nan, 36.09096551402864, 14.392669128600598, np.nan, 30.198241507903912, 17.49850915878955, np.nan, 34.86955767770718, 2.8460926115966667, np.nan, 27.85604286186665, 31.66680961116777, np.nan, 44.77828670319133, 26.617193049776915]]
        assert_array_almost_equal(e_q, hv_grp["Q"][i].filled(np.nan))
        
        # Clean up
        sos.close()
        # self.HV_SOS.unlink()