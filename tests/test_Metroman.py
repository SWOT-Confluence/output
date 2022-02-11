# Standard imports
from pathlib import Path
from shutil import copyfile
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_almost_equal

# Local imports
from output.modules.Metroman import Metroman

class test_Metroman(unittest.TestCase):
    """Test Metroman class methods."""
    
    SOS_NEW = Path(__file__).parent / "sos_new" / "na_apriori_rivers_v07_SOS_results.nc"
    MM_DIR = Path(__file__).parent / "flpe"
    MM_SOS = Path(__file__).parent / "flpe" / "metroman" / "na_apriori_rivers_v07_SOS_results.nc"
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
        copyfile(self.SOS_NEW, self.MM_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        mm = Metroman([7,8,9], self.MM_DIR, self.SOS_NEW, None, None, None, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        mm_dict = mm.get_module_data()
        
        # Assert results
        i = np.where(sos_data["reaches"] == 77449100071)
        self.assertAlmostEqual(241.3398127386478, mm_dict["A0hat"][i])
        self.assertAlmostEqual(0.029737723605629006, mm_dict["nahat"][i])
        self.assertAlmostEqual(0.9521560174519721, mm_dict["x1hat"][i])
        e_q = [102.1852830612334, self.FILL["f8"], 68.12828215402364, 128.61673027519805, self.FILL["f8"], 114.30703438279646, 1221.0705903729436, self.FILL["f8"], 405.3371587930724, 167.9269925059805, self.FILL["f8"], 309.13328109817803, 254.94492318104042, self.FILL["f8"], 289.9590832570638, 267.1006936167945, self.FILL["f8"], 307.6151261048789, 143.08631696050006, self.FILL["f8"], 322.617602117345, 428.22634827260674, self.FILL["f8"], 402.5505081285311, 266.2111168952513]
        assert_array_almost_equal(e_q, mm_dict["allq"][i][0])
        e_qu = [0.6853514633898696, self.FILL["f8"], 0.773512582837442, 0.6311335932027325, self.FILL["f8"], 0.6535712214002629, 0.18795499488978334, self.FILL["f8"], 0.360095650525664, 0.6452256532115092, self.FILL["f8"], 0.4175464037077399, 0.4678020129930746, self.FILL["f8"], 0.4203827440260781, 0.4578623037370324, self.FILL["f8"], 0.4040775588313698, 0.6534005488594158, self.FILL["f8"], 0.43110220257452453, 0.4322567380912632, self.FILL["f8"], 0.3965874223058428, 0.42992457935100314]
        assert_array_almost_equal(e_qu, mm_dict["q_u"][i][0])
        
        # Clean up
        self.MM_SOS.unlink()
        
    def test_append_module_data(self):
        """Tests append_module_data method."""
        
        # File operations to prepare for test
        copyfile(self.SOS_NEW, self.MM_SOS)
        sos_data = self.get_sos_data()
        
        # Create vlen types in SOS
        sos = Dataset(self.MM_SOS, 'a')
        vlen_f = sos.createVLType(np.float64, "vlen_float")
        vlen_i = sos.createVLType(np.int32, "vlen_int")
        vlen_s = sos.createVLType("S1", "vlen_str")
        sos.close()
        
        # Run method
        mm = Metroman([7,8,9], self.MM_DIR, self.MM_SOS, vlen_f, vlen_i, vlen_s, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        mm_dict = mm.get_module_data()
        mm.append_module_data(mm_dict)
        
        # Assert results
        sos = Dataset(self.MM_SOS, 'r')
        mm_grp = sos["metroman"]
        i = np.where(sos_data["reaches"] == 77449100071)
        self.assertAlmostEqual(241.3398127386478, mm_grp["A0hat"][i])
        self.assertAlmostEqual(0.029737723605629006, mm_grp["nahat"][i])
        self.assertAlmostEqual(0.9521560174519721, mm_grp["x1hat"][i])
        e_q = [102.1852830612334, self.FILL["f8"], 68.12828215402364, 128.61673027519805, self.FILL["f8"], 114.30703438279646, 1221.0705903729436, self.FILL["f8"], 405.3371587930724, 167.9269925059805, self.FILL["f8"], 309.13328109817803, 254.94492318104042, self.FILL["f8"], 289.9590832570638, 267.1006936167945, self.FILL["f8"], 307.6151261048789, 143.08631696050006, self.FILL["f8"], 322.617602117345, 428.22634827260674, self.FILL["f8"], 402.5505081285311, 266.2111168952513]        
        assert_array_almost_equal(e_q, mm_grp["allq"][i][0])
        e_qu = [0.6853514633898696, self.FILL["f8"], 0.773512582837442, 0.6311335932027325, self.FILL["f8"], 0.6535712214002629, 0.18795499488978334, self.FILL["f8"], 0.360095650525664, 0.6452256532115092, self.FILL["f8"], 0.4175464037077399, 0.4678020129930746, self.FILL["f8"], 0.4203827440260781, 0.4578623037370324, self.FILL["f8"], 0.4040775588313698, 0.6534005488594158, self.FILL["f8"], 0.43110220257452453, 0.4322567380912632, self.FILL["f8"], 0.3965874223058428, 0.42992457935100314]
        assert_array_almost_equal(e_qu, mm_grp["q_u"][i][0])
        
        # Clean up
        sos.close()
        self.MM_SOS.unlink()