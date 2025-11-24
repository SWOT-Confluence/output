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

    def __init__(self, cont_ids, input_dir, sos_new, logger, 
                 rids, nrids, nids):
        super().__init__(cont_ids, input_dir, sos_new, logger,
                         rids, nrids, nids)
        
        self.nids = nids

    def get_module_data(self):

        ssc_files = glob.glob(os.path.join(self.input_dir, '*.csv'))

        dfs = []
        for f in ssc_files:
            tile_name = os.path.basename(f).removesuffix(".csv")
            df = pd.read_csv(f)
            df["tile_name"] = tile_name
            dfs.append(df)

        self.df = pd.concat(dfs, ignore_index=True)

        # parsed to datetime, handling NaNs
        # was previously date_x instead of 'date'
        self.df['date_parsed'] = pd.to_datetime(self.df['date'], errors='coerce')

        epoch_diff = 946684800  # seconds between 1970-01-01 and 2000-01-01

        # Add column of UNIX timestamps (as integers), skipping NaT
        self.df['unix_timestamp'] = self.df['date_parsed'].astype('int64') // 10**9 - epoch_diff

        self.sorted_dates = sorted(self.df['unix_timestamp'].dropna().unique().astype(int).tolist())



        self.ssc_node_ids = self.df.node_id.unique()
        ssc_dict = self.create_data_dict()

        ssc_dict['ssc_date'] = self.sorted_dates[:]
        ssc_dict['ssc_nodes'] = list(self.ssc_node_ids[:])


        node_index = 0
        for node_id in self.ssc_node_ids:
            # if node_id in self.ssc_node_ids:
            try:
                node_df = self.df[self.df["node_id"] == node_id]

                for _, row in node_df.iterrows():
                    timestamp = int(row["unix_timestamp"])
                    col_idx = self.sorted_dates.index(timestamp)
                    value = row['SSC']
                    if pd.notna(value):
                        ssc_dict['ssc_pred'][node_index, col_idx] = value

                    ssc_dict['tile_name'][node_index, col_idx] = row['tile_name']


            except Exception as e:
                self.logger.warning(f"Node failed: {node_id} | Error: {str(e)}")
            node_index += 1

        return ssc_dict

    def create_data_dict(self):
      
        # num_dates = len(self.sorted_dates)
        self.fill_f8 = self.FILL["f8"]
        self.fill_s48 = self.FILL["S48"]

        self.num_nodes = len(self.ssc_node_ids)
        self.num_dates = len(self.sorted_dates)
        self.num_byte_padding = len(self.fill_s48)



        data_dict = {
            "ssc_pred": np.full((self.num_nodes, self.num_dates), self.fill_f8, dtype=np.float64),
            "ssc_nodes": np.full((self.num_nodes,), self.fill_f8, dtype=np.int64),
            "ssc_date": np.full(self.num_dates, self.fill_f8, dtype=np.int64),
            "tile_name": np.full(
                        (self.num_nodes, self.num_dates),  # or just (num_nodes,) if one string per node
                        fill_value= self.fill_s48,  # or any default
                        dtype="S48"  # fixed-length byte string
                    ),
            "attrs": {
                "ssc_pred": {},
                "ssc_nodes": {},
                "ssc_date": {},
                "tile_name": {}
            }
        }


        return data_dict

    def append_module_data(self, data_dict, metadata_json):
        sos_ds = Dataset(self.sos_new, 'a')
        ssc_grp = sos_ds.createGroup("ssc")
        ssc_grp.createDimension("ssc_dates", len(self.sorted_dates))
        ssc_grp.createDimension("num_ssc_nodes")


        # Dates
        var = self.write_var(ssc_grp, "ssc_date", "f8", ("ssc_dates",), data_dict)
        if "ssc" in metadata_json and "ssc_date" in metadata_json["ssc"]:
            self.set_variable_atts(var, metadata_json["ssc"]["ssc_date"])
        
        var = self.write_var(ssc_grp, "ssc_nodes", "f8", ("num_ssc_nodes",), data_dict)
        if "ssc" in metadata_json and "ssc_nodes" in metadata_json["ssc"]:
            self.set_variable_atts(var, metadata_json["ssc"]["ssc_nodes"])

        # Time-varying vars
        var = self.write_var_nt(ssc_grp, "ssc_pred", "f8", ("num_ssc_nodes", "ssc_dates"), data_dict)
        if "ssc" in metadata_json and "ssc_pred" in metadata_json["ssc"]:
            self.set_variable_atts(var, metadata_json["ssc"]["ssc_pred"])


        var = self.write_var_nt(ssc_grp, "tile_name", "S48", ("num_ssc_nodes", "ssc_dates"), data_dict)
        if "ssc" in metadata_json and "tile_name" in metadata_json["ssc"]:
            self.set_variable_atts(var, metadata_json["ssc"]["tile_name"])


        sos_ds.close()
