# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule


class QQ(AbstractModule):
    """A class that represents QQ FLPE results."""

    def __init__(self, cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s,
                 rids, nrids, nids):
        super().__init__(cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s,
                         rids, nrids, nids)

    def get_module_data(self):
        """Extract QQ results from NetCDF files."""

        qq_dir = self.input_dir / "qq"
        if not qq_dir.exists():
            qq_dir = self.input_dir

        qq_files = [Path(qq_file) for qq_file in glob.glob(f"{qq_dir}/{self.cont_ids}*_qq.nc")]
        qq_rids = [int(qq_file.name.split('_')[0]) for qq_file in qq_files]

        qq_dict = self.create_data_dict()

        if len(qq_files) != 0:
            self.get_nc_attrs(qq_files[0], qq_dict)

            index = 0
            for s_rid in self.sos_rids:
                if s_rid in qq_rids:
                    try:
                        qq_ds = Dataset(qq_dir / f"{int(s_rid)}_qq.nc", 'r')

                        qq_dict["QQ_time"][index] = qq_ds["QQ_time"][:].filled(self.FILL["f8"])
                        qq_dict["QQ_invalid_reach_detailed_flag"][index] = qq_ds["QQ_invalid_reach_detailed_flag"][...].filled(self.FILL["i4"])
                        qq_dict["QQ_invalid_reach_summary_flag"][index] = qq_ds["QQ_invalid_reach_summary_flag"][...].filled(self.FILL["i4"])

                        qq_dict["QQ_q"][index] = qq_ds["q"]["QQ_q"][:].filled(self.FILL["f8"])
                        qq_dict["QQ_q_status_flag"][index] = qq_ds["q"]["QQ_q_status_flag"][:].filled(self.FILL["i4"])

                        qq_dict["QQ_wse_quant_prob"][index] = qq_ds["wse_quantile"]["QQ_wse_quant_prob"][:].filled(self.FILL["f8"])
                        qq_dict["QQ_wse_quant_wse"][index] = qq_ds["wse_quantile"]["QQ_wse_quant_wse"][:].filled(self.FILL["f8"])
                        qq_dict["QQ_wse_quant_flag"][index] = qq_ds["wse_quantile"]["QQ_wse_quant_flag"][...].filled(self.FILL["i4"])

                        qq_dict["QQ_lookup_table_prob"][index] = qq_ds["lookup_table"]["QQ_lookup_table_prob"][:].filled(self.FILL["f8"])
                        qq_dict["QQ_lookup_table_wse"][index] = qq_ds["lookup_table"]["QQ_lookup_table_wse"][:].filled(self.FILL["f8"])
                        qq_dict["QQ_lookup_table_q"][index] = qq_ds["lookup_table"]["QQ_lookup_table_q"][:].filled(self.FILL["f8"])
                        qq_dict["QQ_lookup_table_flag"][index] = qq_ds["lookup_table"]["QQ_lookup_table_flag"][...].filled(self.FILL["i4"])

                        qq_ds.close()
                    except Exception as error:
                        self.logger.warning(f'Reach {s_rid} failed for QQ: {error}')

                index += 1

        return qq_dict

    def create_data_dict(self):
        """Create and return a dictionary for QQ output variables."""

        n_reaches = self.sos_rids.shape[0]
        data_dict = {
            "QQ_time": np.empty(n_reaches, dtype=object),
            "QQ_invalid_reach_detailed_flag": np.full(n_reaches, self.FILL["i4"], dtype=np.int32),
            "QQ_invalid_reach_summary_flag": np.full(n_reaches, self.FILL["i4"], dtype=np.int32),
            "QQ_q": np.empty(n_reaches, dtype=object),
            "QQ_q_status_flag": np.empty(n_reaches, dtype=object),
            "QQ_wse_quant_prob": np.empty(n_reaches, dtype=object),
            "QQ_wse_quant_wse": np.empty(n_reaches, dtype=object),
            "QQ_wse_quant_flag": np.full(n_reaches, self.FILL["i4"], dtype=np.int32),
            "QQ_lookup_table_prob": np.empty(n_reaches, dtype=object),
            "QQ_lookup_table_wse": np.empty(n_reaches, dtype=object),
            "QQ_lookup_table_q": np.empty(n_reaches, dtype=object),
            "QQ_lookup_table_flag": np.full(n_reaches, self.FILL["i4"], dtype=np.int32),
            "attrs": {
                "QQ_time": {},
                "QQ_invalid_reach_detailed_flag": {},
                "QQ_invalid_reach_summary_flag": {},
                "QQ_q": {},
                "QQ_q_status_flag": {},
                "QQ_wse_quant_prob": {},
                "QQ_wse_quant_wse": {},
                "QQ_wse_quant_flag": {},
                "QQ_lookup_table_prob": {},
                "QQ_lookup_table_wse": {},
                "QQ_lookup_table_q": {},
                "QQ_lookup_table_flag": {},
            }
        }

        for name in [
            "QQ_time",
            "QQ_q",
            "QQ_wse_quant_prob",
            "QQ_wse_quant_wse",
            "QQ_lookup_table_prob",
            "QQ_lookup_table_wse",
            "QQ_lookup_table_q",
        ]:
            data_dict[name].fill(np.array([self.FILL["f8"]]))

        data_dict["QQ_q_status_flag"].fill(np.array([self.FILL["i4"]], dtype=np.int32))
        return data_dict

    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each QQ variable."""

        ds = Dataset(nc_file, 'r')
        attr_sources = {
            "QQ_time": ds["QQ_time"],
            "QQ_invalid_reach_detailed_flag": ds["QQ_invalid_reach_detailed_flag"],
            "QQ_invalid_reach_summary_flag": ds["QQ_invalid_reach_summary_flag"],
            "QQ_q": ds["q"]["QQ_q"],
            "QQ_q_status_flag": ds["q"]["QQ_q_status_flag"],
            "QQ_wse_quant_prob": ds["wse_quantile"]["QQ_wse_quant_prob"],
            "QQ_wse_quant_wse": ds["wse_quantile"]["QQ_wse_quant_wse"],
            "QQ_wse_quant_flag": ds["wse_quantile"]["QQ_wse_quant_flag"],
            "QQ_lookup_table_prob": ds["lookup_table"]["QQ_lookup_table_prob"],
            "QQ_lookup_table_wse": ds["lookup_table"]["QQ_lookup_table_wse"],
            "QQ_lookup_table_q": ds["lookup_table"]["QQ_lookup_table_q"],
            "QQ_lookup_table_flag": ds["lookup_table"]["QQ_lookup_table_flag"],
        }
        vlen_names = {
            "QQ_time",
            "QQ_q",
            "QQ_q_status_flag",
            "QQ_wse_quant_prob",
            "QQ_wse_quant_wse",
            "QQ_lookup_table_prob",
            "QQ_lookup_table_wse",
            "QQ_lookup_table_q",
        }
        for name, variable in attr_sources.items():
            attrs = dict(variable.__dict__)
            if name not in vlen_names:
                attrs.pop("_FillValue", None)
            data_dict["attrs"][name] = attrs
        ds.close()

    def append_module_data(self, data_dict, metadata_json):
        """Append QQ data to the new version of the SoS."""

        sos_ds = Dataset(self.sos_new, 'a')
        qq_grp = sos_ds.createGroup("qq")
        qq_metadata = metadata_json.get("qq", {})

        var = self.write_var_nt(qq_grp, "QQ_time", self.vlen_f, ("num_reaches"), data_dict)
        self._set_atts(var, qq_metadata, "QQ_time")
        var = self.write_var(qq_grp, "QQ_invalid_reach_detailed_flag", "i4", ("num_reaches",), data_dict)
        self._set_atts(var, qq_metadata, "QQ_invalid_reach_detailed_flag")
        var = self.write_var(qq_grp, "QQ_invalid_reach_summary_flag", "i4", ("num_reaches",), data_dict)
        self._set_atts(var, qq_metadata, "QQ_invalid_reach_summary_flag")

        var = self.write_var_nt(qq_grp, "QQ_q", self.vlen_f, ("num_reaches"), data_dict)
        self._set_atts(var, qq_metadata, "QQ_q")
        var = self.write_var_nt(qq_grp, "QQ_q_status_flag", self.vlen_i, ("num_reaches"), data_dict)
        self._set_atts(var, qq_metadata, "QQ_q_status_flag")

        var = self.write_var_nt(qq_grp, "QQ_wse_quant_prob", self.vlen_f, ("num_reaches"), data_dict)
        self._set_atts(var, qq_metadata, "QQ_wse_quant_prob")
        var = self.write_var_nt(qq_grp, "QQ_wse_quant_wse", self.vlen_f, ("num_reaches"), data_dict)
        self._set_atts(var, qq_metadata, "QQ_wse_quant_wse")
        var = self.write_var(qq_grp, "QQ_wse_quant_flag", "i4", ("num_reaches",), data_dict)
        self._set_atts(var, qq_metadata, "QQ_wse_quant_flag")

        var = self.write_var_nt(qq_grp, "QQ_lookup_table_prob", self.vlen_f, ("num_reaches"), data_dict)
        self._set_atts(var, qq_metadata, "QQ_lookup_table_prob")
        var = self.write_var_nt(qq_grp, "QQ_lookup_table_wse", self.vlen_f, ("num_reaches"), data_dict)
        self._set_atts(var, qq_metadata, "QQ_lookup_table_wse")
        var = self.write_var_nt(qq_grp, "QQ_lookup_table_q", self.vlen_f, ("num_reaches"), data_dict)
        self._set_atts(var, qq_metadata, "QQ_lookup_table_q")
        var = self.write_var(qq_grp, "QQ_lookup_table_flag", "i4", ("num_reaches",), data_dict)
        self._set_atts(var, qq_metadata, "QQ_lookup_table_flag")

        sos_ds.close()

    def _set_atts(self, variable, metadata, name):
        if name in metadata:
            self.set_variable_atts(variable, metadata[name])
