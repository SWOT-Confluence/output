# Standard imports
from pathlib import Path
from shutil import copyfile
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_almost_equal

# Local imports
from output.modules.Neobam import Neobam

class test_Neobam(unittest.TestCase):
    """Test GeoBam class methods."""
    
    SOS_NEW = Path(__file__).parent / "sos_new" / "na_apriori_rivers_v07_SOS_results.nc"
    GB_DIR = Path(__file__).parent / "flpe"
    GB_SOS = Path(__file__).parent / "flpe" / "geobam" / "na_apriori_rivers_v07_SOS_results.nc"
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
        copyfile(self.SOS_NEW, self.GB_SOS)
        sos_data = self.get_sos_data()
        
        # Run method
        gb = Neobam([7,8,9], self.GB_DIR, self.SOS_NEW, None, None, None, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        gb_dict = gb.get_module_data()
        
        # Assert results
        i = np.where(sos_data["reaches"] == 74269800011)
        self.assertAlmostEqual(3.887148177733727, gb_dict["r"]["mean1"][i][0])
        self.assertAlmostEqual(1.409251374452681, gb_dict["r"]["sd1"][i][0])
        self.assertAlmostEqual(-3.6778712453067524, gb_dict["logn"]["mean1"][i][0])
        self.assertAlmostEqual(0.4400529681283637, gb_dict["logn"]["sd1"][i][0])
        self.assertAlmostEqual(5.027009650560428, gb_dict["logWb"]["mean1"][i][0])
        self.assertAlmostEqual(0.3595937888692505, gb_dict["logWb"]["sd1"][i][0])
        self.assertAlmostEqual(-0.6802768041401168, gb_dict["logDb"]["mean1"][i][0])
        self.assertAlmostEqual(0.7876693789435661, gb_dict["logDb"]["sd1"][i][0])
        e_q1 = [self.FILL["f8"], self.FILL["f8"], 68.13078048792626, 72.55916430745914, self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], 84.70445500125689, 79.5869603359064, 65.78753636285317, 77.70408695600862, 121.7397648531864, 131.03379447463428, 25.723895188566082, 20.695470904811646, 180.49337101334845, 168.17889358997616, 202.07158545313592, 210.2425577282917, 176.31435739517056, 181.74338643618972, 133.50250488921105, 97.3985728885922, 76.37119080003663, 73.99828671203153, 62.04411363417681, 60.50422659168552, 64.49598426139862, 74.1794082816781, self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], 202.92828356708816, 181.87890620565435, 47.6360200161486, 39.753475739061976, 59.142129115951874, 68.42718671035675, 129.97042413473153, 131.3606700304658, 115.6502777959655, 110.59545303585445, 154.4854769877039, 139.50888897181636, 106.98284652670102, 100.8502025078784, 109.27999689350764, 120.92960573177336, self.FILL["f8"], self.FILL["f8"], 253.66971861145407, 214.70013663352415, 89.99994732998667, 116.5487345751697, self.FILL["f8"], self.FILL["f8"], 88.70357330956789, 69.15296730682387, self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], 55.21572798685118, 56.897351743640805, 71.27088412035151, 78.62002976376405, 53.36781568440703, 51.35110571543956, 48.92259965545475, 46.07141156779786, 39.38717996277508, 35.34866164825882, 76.03619442817998, 87.12926747455774, 23.176375839643043, 25.984222121526628, self.FILL["f8"], self.FILL["f8"], 115.77921038634399, 115.8431917523183, 111.45565412015077, 114.26449386219971, 108.84863682121514, 109.66275869774968, 108.68472663324307, 115.7817885550203, self.FILL["f8"], self.FILL["f8"], 96.31327205702124, 124.78519766983847, 86.26893859250421, 99.5562696238287, 96.6190347114577, 110.82050268349839, 81.74075952630444, 86.24261813116378, self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], 116.07689249205941, 128.25029148511672, 75.49159922426281, 76.42160268858586, 36.94355566712625, 31.36338757882453, 70.67141392597263, 64.53382025963134, 51.84489758025126, 66.7314649004294, 87.04161243247788, 99.7565116794396, 160.14691163428287, 168.67132724279747, 36.135342416555, 34.671251665275975, 133.39415292822625, 103.74588150845318, 189.9426458669919, 179.59296984513213, 183.23227298082617, 217.29215754668445, 69.3928212932094, 61.6559987779335, 131.22120674902257, 105.12571537229839, 29.145271241878895, 24.95703843206399, self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], 129.24706903112357, 107.40958825647772, 75.57738665179781, 70.99138576197376, self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], 75.1916366095983, 78.17420575408539, self.FILL["f8"], self.FILL["f8"], 164.41476288809255, 136.8012188679669, 37.15461711180752, 37.845596838108996]
        assert_array_almost_equal(e_q1, gb_dict["q"]["q1"][i][0])
        
        # Clean up
        self.GB_SOS.unlink()
        
    def test_append_module_data(self):
        """Tests append_module_data method."""
        
        # File operations to prepare for test
        copyfile(self.SOS_NEW, self.GB_SOS)
        sos_data = self.get_sos_data()
        
        # Create vlen types in SOS
        sos = Dataset(self.GB_SOS, 'a')
        vlen_f = sos.createVLType(np.float64, "vlen_float")
        vlen_i = sos.createVLType(np.int32, "vlen_int")
        vlen_s = sos.createVLType("S1", "vlen_str")
        sos.close()
        
        # Run method
        gb = Neobam([7,8,9], self.GB_DIR, self.GB_SOS, vlen_f, vlen_i, vlen_s, \
            sos_data["reaches"], sos_data["node_reaches"], sos_data["nodes"])
        gb_dict = gb.get_module_data()
        gb.append_module_data(gb_dict)
        
        # Assert results
        sos = Dataset(self.GB_SOS, 'r')
        gb_grp = sos["neobam"]
        i = np.where(sos_data["reaches"] == 74269800011)
        self.assertAlmostEqual(3.887148177733727, gb_grp["r"]["mean1"][i][0])
        self.assertAlmostEqual(1.409251374452681, gb_grp["r"]["sd1"][i][0])
        self.assertAlmostEqual(-3.6778712453067524, gb_grp["logn"]["mean1"][i][0])
        self.assertAlmostEqual(0.4400529681283637, gb_grp["logn"]["sd1"][i][0])
        self.assertAlmostEqual(5.027009650560428, gb_grp["logWb"]["mean1"][i][0])
        self.assertAlmostEqual(0.3595937888692505, gb_grp["logWb"]["sd1"][i][0])
        self.assertAlmostEqual(-0.6802768041401168, gb_grp["logDb"]["mean1"][i][0])
        self.assertAlmostEqual(0.7876693789435661, gb_grp["logDb"]["sd1"][i][0])
        e_q1 = [self.FILL["f8"], self.FILL["f8"], 68.13078048792626, 72.55916430745914, self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], 84.70445500125689, 79.5869603359064, 65.78753636285317, 77.70408695600862, 121.7397648531864, 131.03379447463428, 25.723895188566082, 20.695470904811646, 180.49337101334845, 168.17889358997616, 202.07158545313592, 210.2425577282917, 176.31435739517056, 181.74338643618972, 133.50250488921105, 97.3985728885922, 76.37119080003663, 73.99828671203153, 62.04411363417681, 60.50422659168552, 64.49598426139862, 74.1794082816781, self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], 202.92828356708816, 181.87890620565435, 47.6360200161486, 39.753475739061976, 59.142129115951874, 68.42718671035675, 129.97042413473153, 131.3606700304658, 115.6502777959655, 110.59545303585445, 154.4854769877039, 139.50888897181636, 106.98284652670102, 100.8502025078784, 109.27999689350764, 120.92960573177336, self.FILL["f8"], self.FILL["f8"], 253.66971861145407, 214.70013663352415, 89.99994732998667, 116.5487345751697, self.FILL["f8"], self.FILL["f8"], 88.70357330956789, 69.15296730682387, self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], 55.21572798685118, 56.897351743640805, 71.27088412035151, 78.62002976376405, 53.36781568440703, 51.35110571543956, 48.92259965545475, 46.07141156779786, 39.38717996277508, 35.34866164825882, 76.03619442817998, 87.12926747455774, 23.176375839643043, 25.984222121526628, self.FILL["f8"], self.FILL["f8"], 115.77921038634399, 115.8431917523183, 111.45565412015077, 114.26449386219971, 108.84863682121514, 109.66275869774968, 108.68472663324307, 115.7817885550203, self.FILL["f8"], self.FILL["f8"], 96.31327205702124, 124.78519766983847, 86.26893859250421, 99.5562696238287, 96.6190347114577, 110.82050268349839, 81.74075952630444, 86.24261813116378, self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], 116.07689249205941, 128.25029148511672, 75.49159922426281, 76.42160268858586, 36.94355566712625, 31.36338757882453, 70.67141392597263, 64.53382025963134, 51.84489758025126, 66.7314649004294, 87.04161243247788, 99.7565116794396, 160.14691163428287, 168.67132724279747, 36.135342416555, 34.671251665275975, 133.39415292822625, 103.74588150845318, 189.9426458669919, 179.59296984513213, 183.23227298082617, 217.29215754668445, 69.3928212932094, 61.6559987779335, 131.22120674902257, 105.12571537229839, 29.145271241878895, 24.95703843206399, self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], 129.24706903112357, 107.40958825647772, 75.57738665179781, 70.99138576197376, self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], self.FILL["f8"], 75.1916366095983, 78.17420575408539, self.FILL["f8"], self.FILL["f8"], 164.41476288809255, 136.8012188679669, 37.15461711180752, 37.845596838108996]
        assert_array_almost_equal(e_q1, gb_grp["q"]["q1"][i][0])
        
        # Clean up
        sos.close()
        self.GB_SOS.unlink()