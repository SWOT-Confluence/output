# Standard imports
from pathlib import Path
from shutil import copyfile
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_almost_equal

# Local imports
from output.modules.Sad import Sad

class test_Sad(unittest.TestCase):
    """Test Sad class methods."""
    
    SOS_NEW = Path(__file__).parent / "sos_new" / "na_apriori_rivers_v07_SOS_results.nc"
    SD_DIR = Path(__file__).parent / "flpe"
    SD_SOS = Path(__file__).parent / "flpe" / "sad" / "na_apriori_rivers_v07_SOS_results.nc"
    
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
        copyfile(self.SOS_NEW, self.SD_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        sd = Sad([7,8,9], self.SD_DIR, self.SOS_NEW, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        sd_dict = sd.get_module_data(25)
        
        # Assert results
        i = np.where(sos_data["reaches"] == 77449100071)
        self.assertAlmostEqual(952.9738292773797, sd_dict["A0"][i])
        self.assertAlmostEqual(0.04842261701270651, sd_dict["n"][i])
        e_Qa = [[4.793766366699731, np.nan, 3.4244741293781424, 2.8551077886725507, np.nan, 2.6306421255921952, 2.7331487063822872, np.nan, 2.52367932796721, 0.0, np.nan, 3.361393396888596, 2.3877426473164745, np.nan, 2.8197259062400186, 2.4805551340520866, np.nan, 2.8119598825432894, 0.0, np.nan, 0.7711405650859661, 0.0, np.nan, 0.2426136333523027, 0.0]]
        assert_array_almost_equal(e_Qa, sd_dict["Qa"][i])
        e_Qu = [[1.1609296701095746, 0.0, 2.3986268148014953, 2.3964787769806364, 0.0, 2.336702181056911, 2.368999388280263, 0.0, 2.3035946030677668, 0.0, 0.0, 2.3985121322328653, 2.2630404348413555, 0.0, 2.391253727097976, 2.2906070680021884, 0.0, 2.3901044577500925, 0.0, 0.0, 1.5442763713096164, 0.0, 0.0, 0.7280937343065627, 0.0]]
        assert_array_almost_equal(e_Qu, sd_dict["Q_u"][i])
        
        # Clean up
        self.SD_SOS.unlink()
        
    def test_append_module_data(self):
        """Tests append_module_data method."""
        
        # File operations to prepare for test
        copyfile(self.SOS_NEW, self.SD_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        sd = Sad([7,8,9], self.SD_DIR, self.SD_SOS, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        sd_dict = sd.get_module_data(25)
        sd.append_module_data(sd_dict)
        
        # Assert results
        sos = Dataset(self.SD_SOS, 'r')
        sd_grp = sos["sad"]
        i = np.where(sos_data["reaches"] == 77449100071)
        self.assertAlmostEqual(952.9738292773797, sd_grp["A0"][i])
        self.assertAlmostEqual(0.04842261701270651, sd_grp["n"][i])
        e_Qa = [[4.793766366699731, np.nan, 3.4244741293781424, 2.8551077886725507, np.nan, 2.6306421255921952, 2.7331487063822872, np.nan, 2.52367932796721, 0.0, np.nan, 3.361393396888596, 2.3877426473164745, np.nan, 2.8197259062400186, 2.4805551340520866, np.nan, 2.8119598825432894, 0.0, np.nan, 0.7711405650859661, 0.0, np.nan, 0.2426136333523027, 0.0]]
        assert_array_almost_equal(e_Qa, sd_grp["Qa"][i].filled(np.nan))
        e_Qu = [[1.1609296701095746, 0.0, 2.3986268148014953, 2.3964787769806364, 0.0, 2.336702181056911, 2.368999388280263, 0.0, 2.3035946030677668, 0.0, 0.0, 2.3985121322328653, 2.2630404348413555, 0.0, 2.391253727097976, 2.2906070680021884, 0.0, 2.3901044577500925, 0.0, 0.0, 1.5442763713096164, 0.0, 0.0, 0.7280937343065627, 0.0]]
        assert_array_almost_equal(e_Qu, sd_grp["Q_u"][i].filled(np.nan))
        
        # Clean up
        sos.close()
        self.SD_SOS.unlink()