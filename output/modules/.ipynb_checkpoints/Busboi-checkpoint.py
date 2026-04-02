# Standard imports
import glob
from datetime import datetime
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
        super().__init__(cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s,
                         rids, nrids, nids)

    def _read_fill(self, nc_var):
        """Read a netCDF4 variable and sanitise to a clean float64 array.

        Parameters
        ----------
        nc_var : netCDF4.Variable

        Returns
        -------
        numpy.ndarray  dtype float64, no masked values, no NaN
        """
        arr = nc_var[:].filled(self.FILL["f8"])
        if np.issubdtype(arr.dtype, np.floating):
            arr = np.where(np.isnan(arr), self.FILL["f8"], arr)
        return arr

    def get_module_data(self):
        """Extract BUSBOI results from NetCDF files."""

        bb_dir   = self.input_dir / "busboi"
        bb_files = [Path(f) for f in glob.glob(f"{bb_dir}/*.nc")]
        bb_rids  = {int(f.name.split('_')[0]) for f in bb_files}

        bb_dict = self.create_data_dict()

        if not bb_files:
            return bb_dict

        index = 0
        for s_rid in self.sos_rids:
            if int(s_rid) in bb_rids:
                try:
                    bb_ds = Dataset(bb_dir / f"{int(s_rid)}_busboi.nc", 'r')

                    # Discharge
                    q_vals = self._read_fill(bb_ds["q/q"])
                    bb_dict["q"][index] = q_vals

                    # Prior Q
                    try:
                        bb_dict["prior_Q"][index] = self._read_fill(bb_ds["prior_q/q"])
                    except (KeyError, IndexError):
                        bb_dict["prior_Q"][index] = np.array([self.FILL["f8"]])

                    # Time - seconds since 2000-01-01 as floats
                    bb_dict["time"][index] = bb_ds["time"][:].filled(self.FILL["f8"])

                    # Bed elevation and chainage (nx per reach)
                    bb_dict["bed_elevation"][index] = self._read_fill(bb_ds["bed/elevation"])
                    bb_dict["chainage"][index]       = self._read_fill(bb_ds["bed/chainage"])

                    # Scalar r
                    r_raw = bb_ds["r/mean"][:]
                    r_val = float(r_raw) if not np.ma.is_masked(r_raw) else float("nan")
                    bb_dict["r"][index] = self.FILL["f8"] if np.isnan(r_val) else r_val

                    # is_valid: 1 if any q value is real (non-fill, non-NaN)
                    bb_dict["is_valid"][index] = 1.0 if np.any(
                        (q_vals != self.FILL["f8"]) & ~np.isnan(q_vals)
                    ) else 0.0


                    bb_ds.close()
                    self.logger.info(f'Reach {s_rid} successfully read for Busboi')

                except Exception as e:
                    self.logger.warning(f'Reach {s_rid} failed for Busboi: {e}')

            index += 1

        return bb_dict

    def create_data_dict(self):
        """Creates and returns BUSBOI data dictionary."""

        n = self.sos_rids.shape[0]

        data_dict = {
            "q"             : np.empty(n, dtype=object),
            "prior_Q"       : np.empty(n, dtype=object),
            "time"          : np.empty(n, dtype=object),
            "bed_elevation" : np.empty(n, dtype=object),
            "chainage"      : np.empty(n, dtype=object),
            "r"             : np.full(n, np.nan, dtype=np.float64),
            "is_valid"      : np.full(n, np.nan, dtype=np.float64),
            "attrs": {
                "q"             : {},
                "prior_Q"       : {},
                "time"          : {},
                "bed_elevation" : {},
                "chainage"      : {},
                "r"             : {},
                "is_valid"      : {},
            }
        }

        fill_arr = np.array([self.FILL["f8"]])
        for key in ("q", "prior_Q", "time", "bed_elevation", "chainage"):
            data_dict[key].fill(fill_arr)

        return data_dict

    def get_nc_attrs(self, nc_file, data_dict):
        """BUSBOI uses grouped NetCDF variables — attrs come from metadata JSON."""
        pass

    def append_module_data(self, data_dict, metadata_json):
        """Append BUSBOI data to the new version of the SoS."""

        sos_ds = Dataset(self.sos_new, 'a')
        bb_grp = sos_ds.createGroup("busboi")

        # Ragged arrays (nt per reach) - all floats
        var = self.write_var_nt(bb_grp, "q",             self.vlen_f, ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["q"])

        var = self.write_var_nt(bb_grp, "prior_Q",       self.vlen_f, ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["prior_Q"])

        # time - seconds since 2000-01-01, stored as floats
        var = self.write_var_nt(bb_grp, "time",          self.vlen_f, ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["time"])

        # Ragged arrays (nx per reach)
        var = self.write_var_nt(bb_grp, "bed_elevation", self.vlen_f, ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["bed_elevation"])

        var = self.write_var_nt(bb_grp, "chainage",      self.vlen_f, ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["chainage"])

        # Scalars per reach
        var = self.write_var(bb_grp, "r",        "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["r"])

        var = self.write_var(bb_grp, "is_valid",  "f8", ("num_reaches",), data_dict)
        self.set_variable_atts(var, metadata_json["busboi"]["is_valid"])

        sos_ds.close()