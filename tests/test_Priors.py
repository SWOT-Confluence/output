# Standard imports
from pathlib import Path
from shutil import copyfile, rmtree
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_almost_equal

# Local imports
from output.modules.Priors import Priors

class test_Priors(unittest.TestCase):
    """Test Priors class methods."""
    
    PRIORS_SOS = Path(__file__).parent / "priors" / "na_sword_v11_SOS.nc"
    APPEND_DIR = Path(__file__).parent / "priors" / "append"
    
    def test_get_module_data(self):
        """Test get_module_data method."""
        
        # File operations to prep for test
        if not self.APPEND_DIR.exists(): self.APPEND_DIR.mkdir(parents=True, exist_ok=True)
        copyfile(self.PRIORS_SOS, self.APPEND_DIR / self.PRIORS_SOS.name)
        
        # Run method
        priors = Priors([7,8,9], self.PRIORS_SOS.parent, \
            self.APPEND_DIR / self.PRIORS_SOS.name, "sword_v11_SOS")
        prior_dict = priors.get_module_data()
        
        # Assert results
        p_sos = Dataset(self.PRIORS_SOS, 'r')
        # Attributes
        e_atts = []
        self.assertEqual(e_atts, list(prior_dict["attributes"]))
        # Dimensions
        e_names = [name for name in p_sos["model"].dimensions.keys()]
        e_dims = [dim.size for dim in p_sos["model"].dimensions.values()]
        names = [name for name, dim in prior_dict["dimensions"]]
        dims = [dim.size for name, dim in prior_dict["dimensions"]]
        self.assertEqual(e_names, names)
        self.assertEqual(e_dims, dims)
        # Variables
        e_names = ['num_months', 'probability', 'flow_duration_q', 'max_q', 'monthly_q', 'mean_q', 'min_q', 'two_year_return_q', 'comid', 'overwritten_indexes', 'overwritten_source']
        names = [name for name, var in prior_dict["variables"]]
        
        # Clean up
        p_sos.close()
        rmtree(self.APPEND_DIR)
        
    def test_append_module_data(self):
        """Test append_module_data method."""
        
        # File operations to prep for test
        if not self.APPEND_DIR.exists(): self.APPEND_DIR.mkdir(parents=True, exist_ok=True)
        copyfile(self.PRIORS_SOS, self.APPEND_DIR / self.PRIORS_SOS.name)
        
        # Run method
        priors = Priors([7,8,9], self.PRIORS_SOS.parent, \
            self.APPEND_DIR / self.PRIORS_SOS.name, "sword_v11_SOS")
        prior_dict = priors.get_module_data()
        priors.append_module_data(prior_dict)
        
        # Assert results
        sos = Dataset(self.APPEND_DIR / self.PRIORS_SOS.name, 'r')
        priors_grp = sos["priors"]
        i = 1517    # 71226600081
        e_nm = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        assert_array_almost_equal(e_nm, priors_grp["num_months"])
        e_p = [1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56, 61, 66, 71, 76, 81, 86, 91, 96]
        assert_array_almost_equal(e_p, priors_grp["probability"])
        e_fdq = [283.168, 113.2672, 78.154368, 61.164288, 50.97024, 42.4752, 37.661344, 33.98016, 31.14848, 28.599968, 26.90096, 24.352448, 20.7562144, 17.9528512, 14.1867168, 10.0241472, 7.2774176, 5.4934592, 4.530688, 3.4829664]
        assert_array_almost_equal(e_fdq, priors_grp["flow_duration_q"][i])
        self.assertAlmostEqual(778.712, priors_grp["max_q"][i])
        e_mq = [18.2235206, 18.63194133, 41.45104986, 93.62109862, 57.16953133, 57.34250884, 45.22314753, 30.19629389, 30.13020113, 31.55080125, 30.16005107, 21.58601082]
        assert_array_almost_equal(e_mq, priors_grp["monthly_q"][i])
        self.assertAlmostEqual(39.801048024534104, priors_grp["mean_q"][i])
        self.assertAlmostEqual(1.4668102399999998, priors_grp["min_q"][i])
        self.assertAlmostEqual(261.647232, priors_grp["two_year_return_q"][i])
        self.assertEqual(71046603, priors_grp["comid"][i])
        # Clean up
        sos.close()
        rmtree(self.APPEND_DIR)