# Standard imports
from pathlib import Path
from shutil import copyfile
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_equal

# Local imports
from output.modules.Prediagnostics import Prediagnostics

class test_Prediagnostics(unittest.TestCase):
    """Test Prediagnostics class methods."""
    
    SOS_NEW = Path(__file__).parent / "sos_new" / "na_apriori_rivers_v07_SOS_results.nc"
    PREDIAGS_DIR = Path(__file__).parent / "prediags"
    PREDIAGS_SOS = Path(__file__).parent / "prediags" / "na_apriori_rivers_v07_SOS_results.nc"
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
        copyfile(self.SOS_NEW, self.PREDIAGS_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        pre = Prediagnostics([7,8,9], self.PREDIAGS_DIR, self.SOS_NEW, None, None, None, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        pre_dict = pre.get_module_data()
        
        # Assert results
        # Reach
        i = np.where(sos_data["reaches"] == 77449100071)
        e_icf = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_icf, pre_dict["reach"]["ice_clim_f"][i][0])
        e_idf = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_idf, pre_dict["reach"]["ice_dyn_f"][i][0])
        e_df = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_df, pre_dict["reach"]["dark_frac"][i][0])
        e_ngn = [0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0]
        assert_array_equal(e_ngn, pre_dict["reach"]["n_good_nod"][i][0])
        e_ofn = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        assert_array_equal(e_ofn, pre_dict["reach"]["obs_frac_n"][i][0])
        e_wo = [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_wo, pre_dict["reach"]["width_outliers"][i][0])
        e_wso = [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_wso, pre_dict["reach"]["wse_outliers"][i][0])
        e_so = [0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0]
        assert_array_equal(e_so, pre_dict["reach"]["slope2_outliers"][i][0])
        e_icf = {'long_name': 'climatological ice cover flag', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_icf, pre_dict["reach"]["attrs"]["ice_clim_f"])
        e_idf = {'long_name': 'dynamical ice cover flag', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_idf, pre_dict["reach"]["attrs"]["ice_dyn_f"])
        e_df = {'long_name': 'Fraction of reach area_total covered by dark water', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_df, pre_dict["reach"]["attrs"]["dark_frac"])
        e_ngn = {'long_name': 'number of nodes in the reach that have a valid WSE', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_ngn, pre_dict["reach"]["attrs"]["n_good_nod"])
        e_ofn = {'long_name': 'fraction of nodes that have a valid WSE', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_ofn, pre_dict["reach"]["attrs"]["obs_frac_n"])
        e_wo = {'long_name': 'Outliers detected in width observations.', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_wo, pre_dict["reach"]["attrs"]["width_outliers"])
        e_wso = {'long_name': 'Outliers detected in wse observations.', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_wso, pre_dict["reach"]["attrs"]["wse_outliers"])
        e_so = {'long_name': 'Outliers detected in slope2 observations.', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_so, pre_dict["reach"]["attrs"]["slope2_outliers"])
        # Node
        ixs = np.where(sos_data["node_reaches"] == 77449100071)        
        e_icf = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_icf, pre_dict["node"]["ice_clim_f"][ixs][3])
        e_idf = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_idf, pre_dict["node"]["ice_dyn_f"][ixs][3])
        e_df = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_df, pre_dict["node"]["dark_frac"][ixs][3])
        e_wo = [1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 1]
        assert_array_equal(e_wo, pre_dict["node"]["width_outliers"][ixs][3])
        e_wso = [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_wso, pre_dict["node"]["wse_outliers"][ixs][3])
        e_s = [0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0]
        assert_array_equal(e_s, pre_dict["node"]["slope2_outliers"][ixs][3])
        e_icf = {'long_name': 'climatological ice cover flag', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_icf, pre_dict["node"]["attrs"]["ice_clim_f"])
        e_idf = {'long_name': 'dynamical ice cover flag', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_idf, pre_dict["node"]["attrs"]["ice_dyn_f"])
        e_df = {'long_name': 'Fraction of reach area_total covered by dark water', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_df, pre_dict["node"]["attrs"]["dark_frac"])
        e_wo = {'long_name': 'Outliers detected in width observations.', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_wo, pre_dict["node"]["attrs"]["width_outliers"])
        e_wso = {'long_name': 'Outliers detected in wse observations.', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_wso, pre_dict["node"]["attrs"]["wse_outliers"])
        e_so = {'long_name': 'Outliers detected in slope2 observations.', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten', '_FillValue': -999}
        self.assertEqual(e_so, pre_dict["node"]["attrs"]["slope2_outliers"])
        
        # Clean up
        self.PREDIAGS_SOS.unlink()
        
    def test_append_module_data(self):
        """Tests append_module_data method."""
        
        # File operations to prepare for test
        copyfile(self.SOS_NEW, self.PREDIAGS_SOS)
        sos_data = self.get_sos_data()
        
        # Create vlen types in SOS
        sos = Dataset(self.PREDIAGS_SOS, 'a')
        vlen_f = sos.createVLType(np.float64, "vlen_float")
        vlen_i = sos.createVLType(np.int32, "vlen_int")
        vlen_s = sos.createVLType("S1", "vlen_str")
        sos.close()
        
        # Run method
        pre = Prediagnostics([7,8,9], self.PREDIAGS_DIR, self.PREDIAGS_SOS, vlen_f, vlen_i, vlen_s, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        pre_dict = pre.get_module_data()
        pre.append_module_data(pre_dict)
        
        # Assert results
        sos = Dataset(self.PREDIAGS_SOS, 'r')
        pre_grp = sos["prediagnostics"]
        i = np.where(sos_data["reaches"] == 77449100071)
        e_icf = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_icf, pre_grp["reach"]["ice_clim_f"][i][0])
        e_idf = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_idf, pre_grp["reach"]["ice_dyn_f"][i][0])
        e_df = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_df, pre_grp["reach"]["dark_frac"][i][0])
        e_ngn = [0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0]
        assert_array_equal(e_ngn, pre_grp["reach"]["n_good_nod"][i][0])
        e_ofn = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        assert_array_equal(e_ofn, pre_grp["reach"]["obs_frac_n"][i][0])
        e_wo = [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_wo, pre_grp["reach"]["width_outliers"][i][0])
        e_wso = [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_wso, pre_grp["reach"]["wse_outliers"][i][0])
        e_so = [0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0]
        assert_array_equal(e_so, pre_grp["reach"]["slope2_outliers"][i][0])
        e_icf = {'long_name': 'climatological ice cover flag', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_icf, pre_grp["reach"]["ice_clim_f"].__dict__)
        e_idf = {'long_name': 'dynamical ice cover flag', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_idf, pre_grp["reach"]["ice_dyn_f"].__dict__)
        e_df = {'long_name': 'Fraction of reach area_total covered by dark water', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_df, pre_grp["reach"]["dark_frac"].__dict__)
        e_ngn = {'long_name': 'number of nodes in the reach that have a valid WSE', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_ngn, pre_grp["reach"]["n_good_nod"].__dict__)
        e_ofn = {'long_name': 'fraction of nodes that have a valid WSE', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_ofn, pre_grp["reach"]["obs_frac_n"].__dict__)
        e_wo = {'long_name': 'Outliers detected in width observations.', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_wo, pre_grp["reach"]["width_outliers"].__dict__)
        e_wso = {'long_name': 'Outliers detected in wse observations.', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_wso, pre_grp["reach"]["wse_outliers"].__dict__)
        e_so = {'long_name': 'Outliers detected in slope2 observations.', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_so, pre_grp["reach"]["slope2_outliers"].__dict__)
        # Node
        ixs = np.where(sos_data["node_reaches"] == 77449100071)
        e_icf = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_icf, pre_grp["node"]["ice_clim_f"][ixs][3])
        e_idf = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_idf, pre_grp["node"]["ice_dyn_f"][ixs][3])
        e_df = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_df, pre_grp["node"]["dark_frac"][ixs][3])
        e_wo = [1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 1]
        assert_array_equal(e_wo, pre_grp["node"]["width_outliers"][ixs][3])
        e_wso = [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert_array_equal(e_wso, pre_grp["node"]["wse_outliers"][ixs][3])
        e_s = [0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0]
        assert_array_equal(e_s, pre_grp["node"]["slope2_outliers"][ixs][3])
        e_icf = {'long_name': 'climatological ice cover flag', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_icf, pre_grp["node"]["ice_clim_f"].__dict__)
        e_idf = {'long_name': 'dynamical ice cover flag', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_idf, pre_grp["node"]["ice_dyn_f"].__dict__)
        e_df = {'long_name': 'Fraction of reach area_total covered by dark water', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_df, pre_grp["node"]["dark_frac"].__dict__)
        e_wo = {'long_name': 'Outliers detected in width observations.', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_wo, pre_grp["node"]["width_outliers"].__dict__)
        e_wso = {'long_name': 'Outliers detected in wse observations.', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_wso, pre_grp["node"]["wse_outliers"].__dict__)
        e_so = {'long_name': 'Outliers detected in slope2 observations.', 'flag_values': '0 1', 'flag_meanings': 'not_overwritten overwritten'}
        self.assertEqual(e_so, pre_grp["node"]["slope2_outliers"].__dict__)
        
        # Clean up
        sos.close()
        self.PREDIAGS_SOS.unlink()