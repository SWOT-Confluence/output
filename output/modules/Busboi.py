# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class Busboi(AbstractModule):
    """
    A class that represents the results of running BUSBOI.

    Data and operations append BUSBOI results to the SoS on the appropriate
    dimensions.

    Attributes
    ----------

    Methods
    -------
    append_module_data(data_dict)
        append module data to the new version of the SoS result file.
    create_data_dict()
        creates and returns module data dictionary.
    get_module_data()
        retrieve module results from NetCDF files.
    get_nc_attrs(nc_file, data_dict)
        get NetCDF attributes for each NetCDF variable.
    """

    def __init__(self, cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s,
                 rids, nrids, nids):
        """
        Parameters
        ----------
        cont_ids: list
            list of continent identifiers
        input_dir: Path
            path to input directory
        sos_new: Path
            path to new SOS file
        logger: logging.Logger
            logger to log statements with
        vlen_f: VLType
            variable length float data type for NetCDF ragged arrays
        vlen_i: VLType
            variable length int data type for NetCDF ragged arrays
        vlen_s: VLType
            variable length string data type for NetCDF ragged arrays
        rids: nd.array
            array of SoS reach identifiers associated with continent
        nrids: nd.array
            array of SOS reach identifiers on the node-level
        nids: nd.array
            array of SOS node identifiers
        """

        super().__init__(cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s, \
            rids, nrids, nids)

    def get_module_data(self):
        """Extract BUSBOI results from NetCDF files."""

        # Files and reach identifiers
        bb_dir = self.input_dir / "busboi"
        bb_files = [ Path(bb_file) for bb_file in glob.glob(f"{bb_dir}/{self.cont_ids}*.nc") ]
        bb_rids = [ int(bb_file.name.split('_')[0]) for bb_file in bb_files ]

        # Storage of results data
        bb_dict = self.create_data_dict()

        if len(bb_files) != 0:
            # Storage of variable attributes
            self.get_nc_attrs(bb_dir / bb_files[0], bb_dict)

            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in bb_rids:
                    try:
                        bb_ds = Dataset(bb_dir / f"{int(s_rid)}_busboi.nc", 'r')

                        # Ragged arrays (nt per reach)
                        bb_dict["q"][index] = bb_ds["q/q"][:].filled(self.FILL["f8"])
                        bb_dict["q_sd"][index] = bb_ds["q/q_sd"][:].filled(self.FILL["f8"])
                        bb_dict["prior_Q"][index] = bb_ds["prior_q/q"][:].filled(self.FILL["f8"])
                        bb_dict["time_str"][index] = np.array(bb_ds["time_str"][:], dtype=object)

                        # Ragged arrays (nx per reach)
                        bb_dict["bed_elevation"][index] = bb_ds["bed/elevation"][:].filled(self.FILL["f8"])
                        bb_dict["chainage"][index] = bb_ds["bed/chainage"][:].filled(self.FILL["f8"])

                        # Scalar per reach
                        bb_dict["r"][index] = float(bb_ds["r/mean"][:].filled(self.FILL["f8"]))
                        bb_dict["is_valid"][index] = float(bb_ds.is_valid)

                        bb_ds.close()
                    except:
                        self.logger.warn(f'Reach {s_rid} failed for Busboi ...')

                index += 1
        return bb_dict

    def create_data_dict(self):
        """Creates and returns BUSBOI data dictionary."""

        data_dict = {
            # Ragged arrays (nt per reach)
            "q"             : np.empty((self.sos_rids.shape[0]), dtype=object),
            "q_sd"          : np.empty((self.sos_rids.shape[0]), dtype=object),
            "prior_Q"       : np.empty((self.sos_rids.shape[0]), dtype=object),
            "time_str"      : np.empty((self.sos_rids.shape[0]), dtype=object),
            # Ragged arrays (nx per reach)
            "bed_elevation" : np.empty((self.sos_rids.shape[0]), dtype=object),
            "chainage"      : np.empty((self.sos_rids.shape[0]), dtype=object),
            # Scalars per reach
            "r"             : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "is_valid"      : np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
            "attrs": {
                "q"             : {},
                "q_sd"          : {},
                "prior_Q"       : {},
                "time_str"      : {},
                "bed_elevation" : {},
                "chainage"      : {},
                "r"             : {},
                "is_valid"      : {},
            }
        }

        # Initialize vlen arrays with fill values
        data_dict["q"].fill(np.array([self.FILL["f8"]]))
        data_dict["q_sd"].fill(np.array([self.FILL["f8"]]))
        data_dict["prior_Q"].fill(np.array([self.FILL["f8"]]))
        data_dict["time_str"].fill(np.array(["NA"], dtype=object))
        data_dict["bed_elevation"].fill(np.array([self.FILL["f8"]]))
        data_dict["chainage"].fill(np.array([self.FILL["f8"]]))

        return data_dict

    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of BUSBOI variables
        """

        ds = Dataset(nc_file, 'r')
        for key in data_dict["attrs"].keys():
            try:
                data_dict["attrs"][key] = ds[key].__dict__
            except:
                pass
        ds.close()

    def append_module_data(self, data_dict, metadata_json):
        """Append BUSBOI data to the new version of the SoS.

        Parameters
        ----------
        data_dict: dict
            dictionary of BUSBOI variables
        metadata_json: dict
            dictionary of metadata attributes
        """

        sos_ds = Dataset(self.sos_new, 'a')
        bb_grp = sos_ds.createGroup("busboi")

        # Ragged arrays (nt per reach)
        var = self.write_var_nt(bb_grp, "q", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["q"])

        var = self.write_var_nt(bb_grp, "q_sd", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["q_sd"])

        var = self.write_var_nt(bb_grp, "prior_Q", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["prior_Q"])

        var = self.write_var_nt(bb_grp, "time_str", self.vlen_s, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["time_str"])

        # Ragged arrays (nx per reach)
        var = self.write_var_nt(bb_grp, "bed_elevation", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["bed_elevation"])

        var = self.write_var_nt(bb_grp, "chainage", self.vlen_f, ("num_reaches"), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["chainage"])

        # Scalars per reach
        var = self.write_var(bb_grp, "r", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["r"])

        var = self.write_var(bb_grp, "is_valid", "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["is_valid"])

        sos_ds.close()
