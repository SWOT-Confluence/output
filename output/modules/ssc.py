# Standard imports
import glob
from pathlib import Path
import os

# Third-party imports
from netCDF4 import Dataset
import pandas as pd
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule

class ssc(AbstractModule):
    """
    A class that represents the results of the SSC module.
    """

    def __init__(self, cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s,
                 rids, nrids, nids):
        super().__init__(cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s,
                         rids, nrids, nids)

    def get_module_data(self):
        ssc_dir = os.path.join(self.input_dir, 'out')

        # if isinstance(self.cont_ids, list):
        #     ssc_files = []
        #     for cid in self.cont_ids:
        #         ssc_files.extend(Path(f) for f in glob.glob(f"{ssc_dir}/{cid}*.csv"))
        # else:
        #     ssc_files = [Path(f) for f in glob.glob(f"{ssc_dir}/{self.cont_ids}*.csv")]
        ssc_files = glob.glob(os.path.join(ssc_dir, *.csv))

        df = pd.concat((pd.read_csv(f) for f in ssc_files), ignore_index=True)
        self.sorted_dates = sorted(df['date_y'].dropna().map(pd.to_datetime).dt.strftime('%Y-%m-%d').unique())

        ssc_node_ids = df.node_id.unique()
        ssc_dict = self.create_data_dict()
        ssc_dict["date"] = self.sorted_dates

        node_index = 0
        for node_id in self.sos_nids:
            if node_id in ssc_node_ids:
                try:
                    node_df = df[df["node_id"] == node_id]
                    ssc_dict["node_id"][node_index] = node_id
                    ssc_dict["reach_id"][node_index] = node_df["reach_id"].iloc[0]

                    for _, row in node_df.iterrows():
                        if row["date_y"] in self.sorted_dates:
                            col_idx = self.sorted_dates.index(row["date_y"])
                            for col in self.nt_vars:
                                ssc_dict[col][node_index, col_idx] = row[col]

                except Exception as e:
                    self.logger.warning(f"Node failed: {node_id} | Error: {str(e)}")
            node_index += 1

        return ssc_dict

    def create_data_dict(self):
        num_nodes = self.sos_nids.shape[0]
        num_dates = len(self.sorted_dates)
        fill_f8 = self.FILL["f8"]

        # Read the first CSV to extract the variable names
        ssc_dir = os.path.join(self.input_dir, 'out')
        sample_file = next(Path(f) for f in glob.glob(f"{ssc_dir}/*.csv"))
        df = pd.read_csv(sample_file)

        exclude_cols = {"node_id", "reach_id", "date_y", "date_x", "Latitude_trunc", "Longitude_trunc", "MGRS", "LorS"}
        nt_vars = [col for col in df.columns if col not in exclude_cols]
        self.nt_vars = nt_vars

        data_dict = {
            "node_id": np.full(num_nodes, np.nan, dtype=np.int64),
            "reach_id": np.full(num_nodes, np.nan, dtype=np.int64),
            "attrs": {
                "node_id": {},
                "reach_id": {},
                "date": {}
            }
        }

        for var in nt_vars:
            data_dict[var] = np.full((num_nodes, num_dates), fill_f8, dtype=np.float64)
            data_dict["attrs"][var] = {}

        return data_dict

    def append_module_data(self, data_dict, metadata_json):
        sos_ds = Dataset(self.sos_new, 'a')
        ssc_grp = sos_ds.createGroup("ssc")
        ssc_grp.createDimension("ssc_dates", len(self.sorted_dates))

        # Scalars
        for scalar_var in ["node_id", "reach_id"]:
            var = self.write_var(ssc_grp, scalar_var, "i8", ("num_nodes",), data_dict)
            if "ssc" in metadata_json and scalar_var in metadata_json["ssc"]:
                self.set_variable_atts(var, metadata_json["ssc"][scalar_var])

        # Dates
        var = self.write_var(ssc_grp, "date", "S1", ("ssc_dates",), data_dict)
        if "ssc" in metadata_json and "date" in metadata_json["ssc"]:
            self.set_variable_atts(var, metadata_json["ssc"]["date"])

        # Time-varying vars
        for varname in self.nt_vars:
            var = self.write_var(ssc_grp, varname, "f8", ("num_nodes", "ssc_dates"), data_dict)
            if "ssc" in metadata_json and varname in metadata_json["ssc"]:
                self.set_variable_atts(var, metadata_json["ssc"][varname])

        sos_ds.close()