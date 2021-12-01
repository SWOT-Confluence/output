# Standard imports
from pathlib import Path
from shutil import copyfile
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.Moi import Moi

class test_Moi(unittest.TestCase):
    """Test Moi class methods."""
    
    SOS_NEW = Path(__file__).parent / "sos_new" / "na_apriori_rivers_v07_SOS_results.nc"
    MOI_DIR = Path(__file__).parent / "moi"
    MOI_SOS = Path(__file__).parent / "moi" / "na_apriori_rivers_v07_SOS_results.nc"
    
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
        copyfile(self.SOS_NEW, self.MOI_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        moi = Moi([7,8,9], self.MOI_DIR, self.SOS_NEW, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        moi_dict = moi.get_module_data(25)
        
        # Assert results
        i = np.where(sos_data["reaches"] == 77449100071)
        self.assertAlmostEqual(5435.2047287015175, moi_dict["geobam"]["qbar_basinScale"][i])
        self.assertAlmostEqual(5950.659870291595, moi_dict["hivdi"]["qbar_basinScale"][i])
        self.assertAlmostEqual(228.54740121146182, moi_dict["metroman"]["qbar_basinScale"][i])
        self.assertAlmostEqual(6651.694798064999, moi_dict["momma"]["qbar_basinScale"][i])
        self.assertAlmostEqual(1.0600754209539536, moi_dict["sad"]["qbar_basinScale"][i])
        self.assertAlmostEqual(532.2125197132377, moi_dict["sic4dvar"]["qbar_basinScale"][i])
        
        # Clean up
        self.MOI_SOS.unlink()
        
    def test_append_module_data(self):
        """Tests append_module_data method."""
        
        # File operations to prepare for test
        copyfile(self.SOS_NEW, self.MOI_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        moi = Moi([7,8,9], self.MOI_DIR, self.MOI_SOS, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        moi_dict = moi.get_module_data(25)
        moi.append_module_data(moi_dict)
        
        # Assert results
        sos = Dataset(self.MOI_SOS, 'r')
        moi_grp = sos["moi"]
        i = np.where(sos_data["reaches"] == 77449100071)
        self.assertAlmostEqual(5435.2047287015175, moi_grp["geobam"]["qbar_basinScale"][i])
        self.assertAlmostEqual(5950.659870291595, moi_grp["hivdi"]["qbar_basinScale"][i])
        self.assertAlmostEqual(228.54740121146182, moi_grp["metroman"]["qbar_basinScale"][i])
        self.assertAlmostEqual(6651.694798064999, moi_grp["momma"]["qbar_basinScale"][i])
        self.assertAlmostEqual(1.0600754209539536, moi_grp["sad"]["qbar_basinScale"][i])
        self.assertAlmostEqual(532.2125197132377, moi_grp["sic4dvar"]["qbar_basinScale"][i])
        
        # Clean up
        sos.close()
        self.MOI_SOS.unlink()