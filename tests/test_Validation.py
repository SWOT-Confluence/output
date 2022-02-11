# Standard imports
from pathlib import Path
from shutil import copyfile
import unittest

# Third-party imports
from netCDF4 import Dataset, chartostring
import numpy as np
from numpy.testing import assert_array_almost_equal, assert_array_equal

# Local imports
from output.modules.Validation import Validation

class test_Validation(unittest.TestCase):
    """Test Validation class methods."""
    
    SOS_NEW = Path(__file__).parent / "sos_new" / "na_apriori_rivers_v07_SOS_results.nc"
    V_DIR = Path(__file__).parent / "validation"
    V_SOS = Path(__file__).parent / "validation" / "na_apriori_rivers_v07_SOS_results.nc"
    
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
        copyfile(self.SOS_NEW, self.V_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        val = Validation([7,8,9], self.V_DIR, self.SOS_NEW, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        val_dict = val.get_module_data()
        
        # Assert results
        i = np.where(sos_data["reaches"] == 77449100071)
        e_an = [[b'g', b'e', b'o', b'b', b'a', b'm', b'_', b'q', b'_', b'c', '' , '' , '' , '' , '' , ''], [b'h', b'i', b'v', b'd', b'i', b'_', b'q', b'_', b'c', '' , '' , '' , '' , '' , '' , ''], [b'm', b'e', b't', b'r', b'o', b'm', b'a', b'n', b'_', b'q', b'_', b'c', '' , '' , '' , ''], [b'm', b'o', b'm', b'm', b'a', b'_', b'q', b'_', b'c', '' , '' , '' , '' , '' , '' , ''], [b's', b'a', b'd', b'_', b'q', b'_', b'c', '' , '' , '' , '' , '' , '' , '' , '' , ''], [b'g', b'e', b'o', b'b', b'a', b'm', b'_', b'q', b'_', b'u', b'c', '' , '' , '' , '' , ''], [b'h', b'i', b'v', b'd', b'i', b'_', b'q', b'_', b'u', b'c', '' , '' , '' , '' , '' , ''], [b'm', b'e', b't', b'r', b'o', b'm', b'a', b'n', b'_', b'q', b'_', b'u', b'c', '' , '', ''], [b'm', b'o', b'm', b'm', b'a', b'_', b'q', b'_', b'u', b'c', '' , '' , '' , '' , '' , ''], [b's', b'a', b'd', b'_', b'q', b'_', b'u', b'c', '' , '' , '' , '' , '' , '' , '' , '']]
        assert_array_equal(e_an, val_dict["algo_names"][i][0])
        e_nse = [[-7.341395799433792, 0.30109605280413876, 0.7087562590807416, 0.9995020863814874, 0.9985680455503363, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_nse, val_dict["nse"][i])
        e_rsq = [[0.5723265807987876, 0.5946270795840615, 0.5881837278140023, 0.8889061325671808, 0.9978906102513161, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_rsq, val_dict["rsq"][i])
        e_kge = [[-0.05453668977219128, 0.10516218520928255, 0.12488475542218291, -0.6941515078588039, 0.17583850716080984, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_kge, val_dict["kge"][i])
        e_rmse = [[58.26190062345029, 49.97583575361182, 24.847434199669003, 29.409156494093402, 86.52746745786328, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_rmse, val_dict["rmse"][i])
        e_testn = [[15.0, 9.0, 6.0, 4.0, 3.0, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_testn, val_dict["testn"][i])
        
        # Clean up
        self.V_SOS.unlink()
        
    def test_append_module_data(self):
        """Tests append_module_data method."""
        
        # File operations to prepare for test
        copyfile(self.SOS_NEW, self.V_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        val = Validation([7,8,9], self.V_DIR, self.V_SOS, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        val_dict = val.get_module_data()
        val.append_module_data(val_dict)
        
        # Assert results
        sos = Dataset(self.V_SOS, 'r')
        val_grp = sos["validation"]
        i = np.where(sos_data["reaches"] == 77449100071)
        e_an = [["geobam_q_c", "hivdi_q_c", "metroman_q_c", "momma_q_c", "sad_q_c", "geobam_q_uc", "hivdi_q_uc", "metroman_q_uc", "momma_q_uc", "sad_q_uc"]]
        assert_array_equal(e_an, chartostring(val_grp["algo_names"][i]))
        e_nse = [[-7.341395799433792, 0.30109605280413876, 0.7087562590807416, 0.9995020863814874, 0.9985680455503363, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_nse, val_grp["nse"][i])
        e_rsq = [[0.5723265807987876, 0.5946270795840615, 0.5881837278140023, 0.8889061325671808, 0.9978906102513161, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_rsq, val_grp["rsq"][i])
        e_kge = [[-0.05453668977219128, 0.10516218520928255, 0.12488475542218291, -0.6941515078588039, 0.17583850716080984, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_kge, val_grp["kge"][i])
        e_rmse = [[58.26190062345029, 49.97583575361182, 24.847434199669003, 29.409156494093402, 86.52746745786328, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_rmse, val_grp["rmse"][i])
        e_testn = [[15.0, 9.0, 6.0, 4.0, 3.0, np.nan, np.nan, np.nan, np.nan, np.nan]]
        assert_array_almost_equal(e_testn, val_grp["testn"][i])
        
        # Clean up
        sos.close()
        self.V_SOS.unlink()