# Standard imports
from pathlib import Path
from shutil import copyfile
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_almost_equal

# Local imports
from output.modules.Momma import Momma

class test_Momma(unittest.TestCase):
    """Test MOMMA class methods."""
    
    SOS_NEW = Path(__file__).parent / "sos_new" / "na_apriori_rivers_v07_SOS_results.nc"
    MO_DIR = Path(__file__).parent / "flpe"
    MO_SOS = Path(__file__).parent / "flpe" / "momma" / "na_apriori_rivers_v07_SOS_results.nc"
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
        copyfile(self.SOS_NEW, self.MO_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        mo = Momma([7,8,9], self.MO_DIR, self.SOS_NEW, None, None, None, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        mo_dict = mo.get_module_data()
        
        # Assert results
        i = np.where(sos_data["reaches"] == 77449100071)
        e_n = [0.005498252993947423, self.FILL["f8"], 0.005498598252233039, 0.005481929088067681, self.FILL["f8"], 0.005491070313076587, 0.005193530311804394, self.FILL["f8"], 0.005380913030296641, self.FILL["f8"], self.FILL["f8"], 0.005425523953428798, 0.005471731986576757, self.FILL["f8"], 0.005433790199019248, 0.0054640699875294855, self.FILL["f8"], 0.005427822141241952, self.FILL["f8"], self.FILL["f8"], 0.0054471376417266, 0.005456899513926786, self.FILL["f8"], 0.005428915068967917, 0.005436439298598454]
        assert_array_almost_equal(e_n, mo_dict["n"][i][0])
        e_Q = [2811.362375521349, self.FILL["f8"], 1764.3499498808849, 3512.3150928868863, self.FILL["f8"], 3612.8134353455407, 19256.65117868928, self.FILL["f8"], 4920.7420061550665, self.FILL["f8"], self.FILL["f8"], 3526.2333643633906, 3345.687049307767, self.FILL["f8"], 3892.4115956528162, 3354.8519675101925, self.FILL["f8"], 4671.241181186864, self.FILL["f8"], self.FILL["f8"], 3721.218561181851, 5222.138248946258, self.FILL["f8"], 5601.183826030633, 3609.4803007825017]
        assert_array_almost_equal(e_Q, mo_dict["Q"][i][0])
        e_slope = [5.166821e-05, self.FILL["f8"], 2.181522e-05, 6.699164e-05, self.FILL["f8"], 6.786917e-05, 7.091798e-05, self.FILL["f8"], 6.995916e-05, self.FILL["f8"], self.FILL["f8"], 5.815485e-05, 6.718034e-05, self.FILL["f8"], 6.257962e-05, 6.615798e-05, self.FILL["f8"], 7.328251e-05, self.FILL["f8"], self.FILL["f8"], 7.691517e-05, 0.00015469132, self.FILL["f8"], 0.00011810525, 5.430467e-05]
        assert_array_almost_equal(e_slope, mo_dict["slope"][i][0])
        self.assertAlmostEqual(0.06419964601114536, mo_dict["zero_flow_stage"][i])
        self.assertAlmostEqual(15.67655, mo_dict["bankfull_stage"][i])
        
        # Clean up
        self.MO_SOS.unlink()
        
    def test_append_module_data(self):
        """Tests append_module_data method."""
        
        # File operations to prepare for test
        copyfile(self.SOS_NEW, self.MO_SOS)
        sos_data = self.get_sos_data()
        
        # Create vlen types in SOS
        sos = Dataset(self.MO_SOS, 'a')
        vlen_f = sos.createVLType(np.float64, "vlen_float")
        vlen_i = sos.createVLType(np.int32, "vlen_int")
        vlen_s = sos.createVLType("S1", "vlen_str")
        sos.close()
        
        # Run method
        mo = Momma([7,8,9], self.MO_DIR, self.MO_SOS, vlen_f, vlen_i, vlen_s, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        mo_dict = mo.get_module_data()
        mo.append_module_data(mo_dict)
        
        # Assert results
        sos = Dataset(self.MO_SOS, 'r')
        mo_grp = sos["momma"]
        i = np.where(sos_data["reaches"] == 77449100071)
        e_n = [0.005498252993947423, self.FILL["f8"], 0.005498598252233039, 0.005481929088067681, self.FILL["f8"], 0.005491070313076587, 0.005193530311804394, self.FILL["f8"], 0.005380913030296641, self.FILL["f8"], self.FILL["f8"], 0.005425523953428798, 0.005471731986576757, self.FILL["f8"], 0.005433790199019248, 0.0054640699875294855, self.FILL["f8"], 0.005427822141241952, self.FILL["f8"], self.FILL["f8"], 0.0054471376417266, 0.005456899513926786, self.FILL["f8"], 0.005428915068967917, 0.005436439298598454]
        assert_array_almost_equal(e_n, mo_grp["n"][i][0])
        e_Q = [2811.362375521349, self.FILL["f8"], 1764.3499498808849, 3512.3150928868863, self.FILL["f8"], 3612.8134353455407, 19256.65117868928, self.FILL["f8"], 4920.7420061550665, self.FILL["f8"], self.FILL["f8"], 3526.2333643633906, 3345.687049307767, self.FILL["f8"], 3892.4115956528162, 3354.8519675101925, self.FILL["f8"], 4671.241181186864, self.FILL["f8"], self.FILL["f8"], 3721.218561181851, 5222.138248946258, self.FILL["f8"], 5601.183826030633, 3609.4803007825017]
        assert_array_almost_equal(e_Q, mo_grp["Q"][i][0])
        e_slope = [5.166821e-05, self.FILL["f8"], 2.181522e-05, 6.699164e-05, self.FILL["f8"], 6.786917e-05, 7.091798e-05, self.FILL["f8"], 6.995916e-05, self.FILL["f8"], self.FILL["f8"], 5.815485e-05, 6.718034e-05, self.FILL["f8"], 6.257962e-05, 6.615798e-05, self.FILL["f8"], 7.328251e-05, self.FILL["f8"], self.FILL["f8"], 7.691517e-05, 0.00015469132, self.FILL["f8"], 0.00011810525, 5.430467e-05]
        assert_array_almost_equal(e_slope, mo_grp["slope"][i][0])
        self.assertAlmostEqual(0.06419964601114536, mo_grp["zero_flow_stage"][i])
        self.assertAlmostEqual(15.67655, mo_grp["bankfull_stage"][i])
        
        # Clean up
        sos.close()
        self.MO_SOS.unlink()