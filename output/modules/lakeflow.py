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

class lakeflow(AbstractModule):
    """
    A class that represents the results of running lakeflow.

    Data and operations append lakeflow results to the SoS on the appriate
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
        retrieve module results from csv files.
    """

    def __init__(self, cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s,
                 rids, nrids, nids):
        """
        Parameters
        ---------
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
            variable length int data type for NEtCDF ragged arrays
        vlen_s: VLType
            variable length string data type for NEtCDF ragged arrays
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
        """Extract lakeflow results from csv files."""

        # Files and reach identifiers
        lakeflow_dir = os.path.join(self.input_dir, 'out')

        if type(self.cont_ids) == list:
            lakeflow_files = []
            for i in self.cont_ids:
                lakeflow_files_int = [ Path(lakeflow_file) for lakeflow_file in glob.glob(f"{lakeflow_dir}/{i}*.csv") ]
                lakeflow_files.extend(lakeflow_files_int)

        else:
            lakeflow_files = [ Path(lakeflow_file) for lakeflow_file in glob.glob(f"{lakeflow_dir}/{self.cont_ids}*.csv") ]
        print('processing', len(lakeflow_files), 'lakeflow files for', self.cont_ids)
        
        df = pd.concat((pd.read_csv(f) for f in lakeflow_files), ignore_index=True)
        print('found', len(df), 'entries')
        
        # encode catagorical vars
        df["type"] = df["type"].map({"inflow": 0, "outflow": 1})
        df["prior_fit"] = df["prior_fit"].map({"sos": 0, "geobam": 1})


        self.sorted_dates = sorted(df['date'].dropna().map(pd.to_datetime).dt.strftime('%Y-%m-%d').unique())


        unix_timestamps = pd.to_datetime(self.sorted_dates).astype(int) // 10**9  # integer seconds
        unix_timestamps_list = unix_timestamps.tolist()

        
        lakeflow_rids = df.reach_id.unique()

        
        # Storage of results data
        lakeflow_dict = self.create_data_dict()
        lakeflow_dict["date"] = unix_timestamps_list[:]
        lakeflow_dict["reach_id"] = self.sos_rids[:]
        print('here are some sos ids', self.sos_rids[:10])
        
        
        if len(lakeflow_files) != 0:
            reach_index = 0
            for s_rid in self.sos_rids:
                if s_rid in lakeflow_rids:
                    try:
                        lakeflow_ds = df[df["reach_id"] == s_rid]

                  

                        # Scalars
                        
                        lakeflow_dict["lake_id"][reach_index] = lakeflow_ds["lake_id"].iloc[0]
                        print('here is it being written...', lakeflow_dict["lake_id"][reach_index])
                        lakeflow_dict["prior_fit"][reach_index] = lakeflow_ds["prior_fit"].iloc[0]
                        lakeflow_dict["type"][reach_index] = lakeflow_ds["type"].iloc[0]
                        lakeflow_dict["q_upper"][reach_index] = lakeflow_ds["q_upper"].iloc[0]
                        lakeflow_dict["q_lower"][reach_index] = lakeflow_ds["q_lower"].iloc[0]
                        lakeflow_dict["n_lakeflow_sd"][reach_index] = lakeflow_ds["n_lakeflow_sd"].iloc[0]
                        lakeflow_dict["a0_lakeflow"][reach_index] = lakeflow_ds["a0_lakeflow"].iloc[0]
                        
                        
                        nt_vars = [
                            "width",
                            "slope2",
                            "da",
                            "wse",
                            "storage",
                            "dv",
                            "q_model",
                            "tributary",
                            "et",
                            "bayes_q",
                            "bayes_q_sd",
                            "q_lakeflow",
                            "n_lakeflow"
                        ]

                        
                        for _, row in lakeflow_ds.iterrows():
                            if row["date"] in self.sorted_dates:
                                col_idx = self.sorted_dates.index(row["date"])
                                
                                for a_var in nt_vars:
                                    lakeflow_dict[a_var][reach_index, col_idx] = row[a_var]  # or whatever variable

                        # lakeflow_dict["width"][index] = np.array(lakeflow_ds["width"].dropna().tolist(), dtype='float64') if "width" in lakeflow_ds else np.array([self.FILL["f8"]], dtype='float64')
                        # lakeflow_dict["slope2"][index] = np.array(lakeflow_ds["slope2"].dropna().tolist(), dtype='float64') if "slope2" in lakeflow_ds else np.array([self.FILL["f8"]], dtype='float64')
                        # lakeflow_dict["da"][index] = np.array(lakeflow_ds["da"].dropna().tolist(), dtype='float64') if "da" in lakeflow_ds else np.array([self.FILL["f8"]], dtype='float64')
                        # lakeflow_dict["wse"][index] = np.array(lakeflow_ds["wse"].dropna().tolist(), dtype='float64') if "wse" in lakeflow_ds else np.array([self.FILL["f8"]], dtype='float64')
                        # lakeflow_dict["storage"][index] = np.array(lakeflow_ds["storage"].dropna().tolist(), dtype='float64') if "storage" in lakeflow_ds else np.array([self.FILL["f8"]], dtype='float64')
                        # lakeflow_dict["dv"][index] = np.array(lakeflow_ds["dv"].dropna().tolist(), dtype='float64') if "dv" in lakeflow_ds else np.array([self.FILL["f8"]], dtype='float64')
                        # lakeflow_dict["q_model"][index] = np.array(lakeflow_ds["q_model"].dropna().tolist(), dtype='float64') if "q_model" in lakeflow_ds else np.array([self.FILL["f8"]], dtype='float64')
                        # lakeflow_dict["tributary"][index] = np.array(lakeflow_ds["tributary"].dropna().tolist(), dtype='float64') if "tributary" in lakeflow_ds else np.array([self.FILL["f8"]], dtype='float64')
                        # lakeflow_dict["et"][index] = np.array(lakeflow_ds["et"].dropna().tolist(), dtype='float64') if "et" in lakeflow_ds else np.array([self.FILL["f8"]], dtype='float64')
                        # lakeflow_dict["bayes_q"][index] = np.array(lakeflow_ds["bayes_q"].dropna().tolist(), dtype='float64') if "bayes_q" in lakeflow_ds else np.array([self.FILL["f8"]], dtype='float64')
                        # lakeflow_dict["bayes_q_sd"][index] = np.array(lakeflow_ds["bayes_q_sd"].dropna().tolist(), dtype='float64') if "bayes_q_sd" in lakeflow_ds else np.array([self.FILL["f8"]], dtype='float64')
                        # lakeflow_dict["q_lakeflow"][index] = np.array(lakeflow_ds["q_lakeflow"].dropna().tolist(), dtype='float64') if "q_lakeflow" in lakeflow_ds else np.array([self.FILL["f8"]], dtype='float64')
                        # lakeflow_dict["n_lakeflow"][index] = np.array(lakeflow_ds["n_lakeflow"].dropna().tolist(), dtype='float64') if "n_lakeflow" in lakeflow_ds else np.array([self.FILL["f8"]], dtype='float64')

                    except Exception as e:
                        self.logger.warn(f"Reach failed: {s_rid} | Error: {str(e)}")
                reach_index += 1
        print('pull data reaches', lakeflow_dict["date"][:10])
        return lakeflow_dict
    
    def create_data_dict(self):
        """Creates and returns lakeflow data dictionary."""

        num_reaches = self.sos_rids.shape[0]
        num_dates = len(self.sorted_dates)

        # Fill value for floats
        fill_f8 = self.FILL["f8"]
        fill_i8 = self.FILL["i8"]
        fill_i4 = self.FILL["i4"]

        data_dict = {
            # Scalars (1D arrays, one value per reach)
            "reach_id": np.full(num_reaches, fill_i4, dtype=np.int32),
            "lake_id": np.full(num_reaches, fill_i4, dtype=np.int32),
            "prior_fit": np.full(num_reaches, fill_i4, dtype=np.int32),
            "type": np.full(num_reaches, fill_i4, dtype=np.int32),
            "q_upper": np.full(num_reaches, fill_f8, dtype=np.float64),
            "q_lower": np.full(num_reaches, fill_f8, dtype=np.float64),
            "n_lakeflow_sd": np.full(num_reaches, fill_f8, dtype=np.float64),
            "a0_lakeflow": np.full(num_reaches, fill_f8, dtype=np.float64),
            "date": np.full(num_dates, fill_i8, dtype=np.int64),


            # Time-varying variables (2D arrays: num_reaches x num_dates)
            "width": np.full((num_reaches, num_dates), fill_f8, dtype=np.float64),
            "slope2": np.full((num_reaches, num_dates), fill_f8, dtype=np.float64),
            "da": np.full((num_reaches, num_dates), fill_f8, dtype=np.float64),
            "wse": np.full((num_reaches, num_dates), fill_f8, dtype=np.float64),
            "storage": np.full((num_reaches, num_dates), fill_f8, dtype=np.float64),
            "dv": np.full((num_reaches, num_dates), fill_f8, dtype=np.float64),
            "q_model": np.full((num_reaches, num_dates), fill_f8, dtype=np.float64),
            "tributary": np.full((num_reaches, num_dates), fill_f8, dtype=np.float64),
            "et": np.full((num_reaches, num_dates), fill_f8, dtype=np.float64),
            "bayes_q": np.full((num_reaches, num_dates), fill_f8, dtype=np.float64),
            "bayes_q_sd": np.full((num_reaches, num_dates), fill_f8, dtype=np.float64),
            "q_lakeflow": np.full((num_reaches, num_dates), fill_f8, dtype=np.float64),
            "n_lakeflow": np.full((num_reaches, num_dates), fill_f8, dtype=np.float64),
            
            "attrs": {
                "reach_id": {},
                "lake_id": {},
                "prior_fit": {},
                "type": {},
                "q_upper": {},
                "q_lower": {},
                "n_lakeflow_sd": {},
                "a0_lakeflow": {},
                "date": {},
                "width": {},
                "slope2": {},
                "da": {},
                "wse": {},
                "storage": {},
                "dv": {},
                "q_model": {},
                "tributary": {},
                "et": {},
                "bayes_q": {},
                "bayes_q_sd": {},
                "q_lakeflow": {},
                "n_lakeflow": {},
            }
        }


        # Vlen variables
        # data_dict["date"].fill(np.array([self.FILL["f8"]]))
        # data_dict["date"].fill(np.array([self.FILL["f8"]]))
        
        
        # data_dict["q"]["q_sd"].fill(np.array([self.FILL["f8"]]))
        
        return data_dict
        
    def append_module_data(self, data_dict, metadata_json):
        """Append lakeflow data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of lakeflow variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        lakeflow_grp = sos_ds.createGroup("lakeflow")
        lakeflow_grp.createDimension("lakeflow_dates", len(self.sorted_dates))      
        
        # Scalars
        var = self.write_var(lakeflow_grp, "reach_id", "f8", ("num_reaches",), data_dict)
        if "lakeflow" in metadata_json and "reach_id" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["reach_id"])

        var = self.write_var(lakeflow_grp, "lake_id", "i4", ("num_reaches",), data_dict)
        if "lakeflow" in metadata_json and "lake_id" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["lake_id"])

        var = self.write_var(lakeflow_grp, "prior_fit", "i4", ("num_reaches",), data_dict)
        if "lakeflow" in metadata_json and "prior_fit" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["prior_fit"])

        var = self.write_var(lakeflow_grp, "type", "i4", ("num_reaches",), data_dict)
        if "lakeflow" in metadata_json and "type" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["type"])

        var = self.write_var(lakeflow_grp, "q_upper", "f8", ("num_reaches",), data_dict)
        if "lakeflow" in metadata_json and "q_upper" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["q_upper"])

        var = self.write_var(lakeflow_grp, "q_lower", "f8", ("num_reaches",), data_dict)
        if "lakeflow" in metadata_json and "q_lower" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["q_lower"])

        var = self.write_var(lakeflow_grp, "n_lakeflow_sd", "f8", ("num_reaches",), data_dict)
        if "lakeflow" in metadata_json and "n_lakeflow_sd" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["n_lakeflow_sd"])

        var = self.write_var(lakeflow_grp, "a0_lakeflow", "f8", ("num_reaches",), data_dict)
        if "lakeflow" in metadata_json and "a0_lakeflow" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["a0_lakeflow"])
            
        var = self.write_var(lakeflow_grp, "date", "i8", ("lakeflow_dates",), data_dict)
        if "lakeflow" in metadata_json and "date" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["date"])


        # VLENs


        var = self.write_var(lakeflow_grp, "width", "f8", ("num_reaches", "lakeflow_dates"), data_dict)
        if "lakeflow" in metadata_json and "width" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["width"])

        var = self.write_var(lakeflow_grp, "slope2", "f8", ("num_reaches", "lakeflow_dates"), data_dict)
        if "lakeflow" in metadata_json and "slope2" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["slope2"])

        var = self.write_var(lakeflow_grp, "da", "f8", ("num_reaches", "lakeflow_dates"), data_dict)
        if "lakeflow" in metadata_json and "da" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["da"])

        var = self.write_var(lakeflow_grp, "wse", "f8", ("num_reaches", "lakeflow_dates"), data_dict)
        if "lakeflow" in metadata_json and "wse" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["wse"])

        var = self.write_var(lakeflow_grp, "storage", "f8", ("num_reaches", "lakeflow_dates"), data_dict)
        if "lakeflow" in metadata_json and "storage" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["storage"])

        var = self.write_var(lakeflow_grp, "dv", "f8", ("num_reaches", "lakeflow_dates"), data_dict)
        if "lakeflow" in metadata_json and "dv" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["dv"])

        var = self.write_var(lakeflow_grp, "q_model", "f8", ("num_reaches", "lakeflow_dates"), data_dict)
        if "lakeflow" in metadata_json and "q_model" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["q_model"])

        var = self.write_var(lakeflow_grp, "tributary", "f8", ("num_reaches", "lakeflow_dates"), data_dict)
        if "lakeflow" in metadata_json and "tributary" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["tributary"])

        var = self.write_var(lakeflow_grp, "et", "f8", ("num_reaches", "lakeflow_dates"), data_dict)
        if "lakeflow" in metadata_json and "et" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["et"])

        var = self.write_var(lakeflow_grp, "bayes_q", "f8", ("num_reaches", "lakeflow_dates"), data_dict)
        if "lakeflow" in metadata_json and "bayes_q" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["bayes_q"])

        var = self.write_var(lakeflow_grp, "bayes_q_sd", "f8", ("num_reaches", "lakeflow_dates"), data_dict)
        if "lakeflow" in metadata_json and "bayes_q_sd" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["bayes_q_sd"])

        var = self.write_var(lakeflow_grp, "q_lakeflow", "f8", ("num_reaches", "lakeflow_dates"), data_dict)
        if "lakeflow" in metadata_json and "q_lakeflow" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["q_lakeflow"])

        var = self.write_var(lakeflow_grp, "n_lakeflow", "f8", ("num_reaches", "lakeflow_dates"), data_dict)
        if "lakeflow" in metadata_json and "n_lakeflow" in metadata_json["lakeflow"]:
            self.set_variable_atts(var, metadata_json["lakeflow"]["n_lakeflow"])



        
        sos_ds.close()
