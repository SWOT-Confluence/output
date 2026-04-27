# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule


class Moi(AbstractModule):
    """A class that represent the results of running MOI.

    Data and operations append MOI results to the SoS on the appropriate
    dimensions.
    """

    # MOI writes BUSBOI results under the "busboi" group, but the original
    # output metadata keeps the MOI schema under "moi -> neobam".
    # Keep the original metadata.json unchanged and only translate here.
    METADATA_GROUP_MAP = {
        "busboi": "neobam",
        "hivdi": "hivdi",
        "metroman": "metroman",
        "momma": "momma",
        "sad": "sad",
        "sic4dvar": "sic4dvar",
    }

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
        super().__init__(cont_ids, input_dir, sos_new, logger, vlen_f, vlen_i, vlen_s,
                         rids, nrids, nids)

    def _moi_meta(self, metadata_json, alg, var):
        """Return the metadata block for a MOI variable.

        BUSBOI should use the legacy moi.neobam metadata definition from the
        original metadata.json.
        """
        meta_alg = self.METADATA_GROUP_MAP.get(alg, alg)
        return metadata_json["moi"][meta_alg][var]

    def get_module_data(self):
        """Extract MOI results from NetCDF files."""

        moi_dir = self.input_dir
        moi_files = [Path(moi_file) for moi_file in glob.glob(f"{moi_dir}/{self.cont_ids}*.nc")]
        moi_rids = [int(moi_file.name.split('_')[0]) for moi_file in moi_files]

        moi_dict = self.create_data_dict()

        if len(moi_files) != 0:
            self.get_nc_attrs(moi_files[0], moi_dict)

            index = 0
            for s_rid in self.sos_rids:
                if s_rid in moi_rids:
                    moi_ds = None
                    try:
                        moi_ds = Dataset(moi_dir / f"{int(s_rid)}_integrator.nc", "r")

                        # 1. busboi safe extraction
                        try:
                            if "busboi" in moi_ds.groups:
                                bg = moi_ds["busboi"]
                                if "q" in bg.variables:
                                    moi_dict["busboi"]["q"][index] = bg["q"][:].filled(self.FILL["f8"])
                                if "a0" in bg.variables:
                                    moi_dict["busboi"]["a0"][index] = bg["a0"][:].filled(np.nan)
                                if "n" in bg.variables:
                                    moi_dict["busboi"]["n"][index] = bg["n"][:].filled(np.nan)
                                if "qbar_reachScale" in bg.variables:
                                    moi_dict["busboi"]["qbar_reachScale"][index] = bg["qbar_reachScale"][:].filled(np.nan)
                                if "qbar_basinScale" in bg.variables:
                                    moi_dict["busboi"]["qbar_basinScale"][index] = bg["qbar_basinScale"][:].filled(np.nan)
                        except Exception as e:
                            self.logger.warning(f"Failed extracting busboi for {s_rid}: {e}")

                        # 2. hivdi safe extraction
                        try:
                            if "hivdi" in moi_ds.groups:
                                hv = moi_ds["hivdi"]
                                if "q" in hv.variables:
                                    moi_dict["hivdi"]["q"][index] = hv["q"][:].filled(self.FILL["f8"])
                                if "Abar" in hv.variables:
                                    moi_dict["hivdi"]["Abar"][index] = hv["Abar"][:].filled(np.nan)
                                if "alpha" in hv.variables:
                                    moi_dict["hivdi"]["alpha"][index] = hv["alpha"][:].filled(np.nan)
                                if "beta" in hv.variables:
                                    moi_dict["hivdi"]["beta"][index] = hv["beta"][:].filled(np.nan)
                                if "qbar_reachScale" in hv.variables:
                                    moi_dict["hivdi"]["qbar_reachScale"][index] = hv["qbar_reachScale"][:].filled(np.nan)
                                if "qbar_basinScale" in hv.variables:
                                    moi_dict["hivdi"]["qbar_basinScale"][index] = hv["qbar_basinScale"][:].filled(np.nan)
                        except Exception as e:
                            self.logger.warning(f"Failed extracting hivdi for {s_rid}: {e}")

                        # 3. metroman safe extraction
                        try:
                            if "metroman" in moi_ds.groups:
                                mm = moi_ds["metroman"]
                                if "q" in mm.variables:
                                    moi_dict["metroman"]["q"][index] = mm["q"][:].filled(self.FILL["f8"])
                                if "Abar" in mm.variables:
                                    moi_dict["metroman"]["Abar"][index] = mm["Abar"][:].filled(np.nan)
                                if "na" in mm.variables:
                                    moi_dict["metroman"]["na"][index] = mm["na"][:].filled(np.nan)
                                if "x1" in mm.variables:
                                    moi_dict["metroman"]["x1"][index] = mm["x1"][:].filled(np.nan)
                                if "qbar_reachScale" in mm.variables:
                                    moi_dict["metroman"]["qbar_reachScale"][index] = mm["qbar_reachScale"][:].filled(np.nan)
                                if "qbar_basinScale" in mm.variables:
                                    moi_dict["metroman"]["qbar_basinScale"][index] = mm["qbar_basinScale"][:].filled(np.nan)
                        except Exception as e:
                            self.logger.warning(f"Failed extracting metroman for {s_rid}: {e}")

                        # 4. momma safe extraction
                        try:
                            if "momma" in moi_ds.groups:
                                mo = moi_ds["momma"]
                                if "q" in mo.variables:
                                    moi_dict["momma"]["q"][index] = mo["q"][:].filled(self.FILL["f8"])
                                if "B" in mo.variables:
                                    moi_dict["momma"]["B"][index] = mo["B"][:].filled(np.nan)
                                if "H" in mo.variables:
                                    moi_dict["momma"]["H"][index] = mo["H"][:].filled(np.nan)
                                if "Save" in mo.variables:
                                    moi_dict["momma"]["Save"][index] = mo["Save"][:].filled(np.nan)
                                if "qbar_reachScale" in mo.variables:
                                    moi_dict["momma"]["qbar_reachScale"][index] = mo["qbar_reachScale"][:].filled(np.nan)
                                if "qbar_basinScale" in mo.variables:
                                    moi_dict["momma"]["qbar_basinScale"][index] = mo["qbar_basinScale"][:].filled(np.nan)
                        except Exception as e:
                            self.logger.warning(f"Failed extracting momma for {s_rid}: {e}")

                        # 5. sad safe extraction
                        try:
                            if "sad" in moi_ds.groups:
                                sd = moi_ds["sad"]
                                if "q" in sd.variables:
                                    moi_dict["sad"]["q"][index] = sd["q"][:].filled(self.FILL["f8"])
                                if "a0" in sd.variables:
                                    moi_dict["sad"]["a0"][index] = sd["a0"][:].filled(np.nan)
                                if "n" in sd.variables:
                                    moi_dict["sad"]["n"][index] = sd["n"][:].filled(np.nan)
                                if "qbar_reachScale" in sd.variables:
                                    moi_dict["sad"]["qbar_reachScale"][index] = sd["qbar_reachScale"][:].filled(np.nan)
                                if "qbar_basinScale" in sd.variables:
                                    moi_dict["sad"]["qbar_basinScale"][index] = sd["qbar_basinScale"][:].filled(np.nan)
                        except Exception as e:
                            self.logger.warning(f"Failed extracting sad for {s_rid}: {e}")

                        # 6. sic4dvar safe extraction
                        try:
                            if "sic4dvar" in moi_ds.groups:
                                sic = moi_ds["sic4dvar"]
                                if "q" in sic.variables:
                                    moi_dict["sic4dvar"]["q"][index] = sic["q"][:].filled(self.FILL["f8"])
                                if "a0" in sic.variables:
                                    moi_dict["sic4dvar"]["a0"][index] = sic["a0"][:].filled(np.nan)
                                if "n" in sic.variables:
                                    moi_dict["sic4dvar"]["n"][index] = sic["n"][:].filled(np.nan)
                                if "qbar_reachScale" in sic.variables:
                                    moi_dict["sic4dvar"]["qbar_reachScale"][index] = sic["qbar_reachScale"][:].filled(np.nan)
                                if "qbar_basinScale" in sic.variables:
                                    moi_dict["sic4dvar"]["qbar_basinScale"][index] = sic["qbar_basinScale"][:].filled(np.nan)
                        except Exception as e:
                            self.logger.warning(f"Failed extracting sic4dvar for {s_rid}: {e}")

                    except Exception as e:
                        print(s_rid, "failed completely in moi:", e)
                    finally:
                        if moi_ds is not None:
                            try:
                                moi_ds.close()
                            except Exception:
                                pass

                index += 1

        return moi_dict

    def create_data_dict(self):
        """Creates and returns MOI data dictionary."""

        data_dict = {
            "busboi": {
                "q": np.empty((self.sos_rids.shape[0]), dtype=object),
                "a0": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "n": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": {},
                    "a0": {},
                    "n": {},
                    "qbar_reachScale": {},
                    "qbar_basinScale": {},
                },
            },
            "hivdi": {
                "q": np.empty((self.sos_rids.shape[0]), dtype=object),
                "Abar": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "alpha": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "beta": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": {},
                    "Abar": {},
                    "alpha": {},
                    "beta": {},
                    "qbar_reachScale": {},
                    "qbar_basinScale": {},
                },
            },
            "metroman": {
                "q": np.empty((self.sos_rids.shape[0]), dtype=object),
                "Abar": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "na": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "x1": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": {},
                    "Abar": {},
                    "na": {},
                    "x1": {},
                    "qbar_reachScale": {},
                    "qbar_basinScale": {},
                },
            },
            "momma": {
                "q": np.empty((self.sos_rids.shape[0]), dtype=object),
                "B": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "H": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "Save": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": {},
                    "B": {},
                    "H": {},
                    "Save": {},
                    "qbar_reachScale": {},
                    "qbar_basinScale": {},
                },
            },
            "sad": {
                "q": np.empty((self.sos_rids.shape[0]), dtype=object),
                "a0": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "n": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": {},
                    "a0": {},
                    "n": {},
                    "qbar_reachScale": {},
                    "qbar_basinScale": {},
                },
            },
            "sic4dvar": {
                "q": np.empty((self.sos_rids.shape[0]), dtype=object),
                "a0": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "n": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_reachScale": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "qbar_basinScale": np.full(self.sos_rids.shape[0], np.nan, dtype=np.float64),
                "attrs": {
                    "q": {},
                    "a0": {},
                    "n": {},
                    "qbar_reachScale": {},
                    "qbar_basinScale": {},
                },
            },
        }

        data_dict["busboi"]["q"].fill(np.array([self.FILL["f8"]]))
        data_dict["hivdi"]["q"].fill(np.array([self.FILL["f8"]]))
        data_dict["metroman"]["q"].fill(np.array([self.FILL["f8"]]))
        data_dict["momma"]["q"].fill(np.array([self.FILL["f8"]]))
        data_dict["sad"]["q"].fill(np.array([self.FILL["f8"]]))
        data_dict["sic4dvar"]["q"].fill(np.array([self.FILL["f8"]]))

        return data_dict

    def get_nc_attrs(self, nc_file, data_dict):
        """Get NetCDF attributes for each NetCDF variable.

        Parameters
        ----------
        nc_file: Path
            path to NetCDF file
        data_dict: dict
            dictionary of MOI variables
        """

        ds = Dataset(nc_file, "r")
        try:
            for alg, value in data_dict.items():
                if alg not in ds.groups:
                    continue
                grp = ds[alg]
                for var_name in value["attrs"].keys():
                    if var_name in grp.variables:
                        data_dict[alg]["attrs"][var_name] = grp[var_name].__dict__
        finally:
            ds.close()

    def append_module_data(self, data_dict, metadata_json):
        """Append MOI data to the new version of the SoS.

        Parameters
        ----------
        data_dict: dict
            dictionary of MOI variables
        """

        sos_ds = Dataset(self.sos_new, "a")
        moi_grp = sos_ds.createGroup("moi")

        # busboi -> use original moi.neobam metadata
        bb_grp = moi_grp.createGroup("busboi")
        var = self.write_var_nt(bb_grp, "q", self.vlen_f, ("num_reaches"), data_dict["busboi"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "busboi", "q"))
        var = self.write_var(bb_grp, "a0", "f8", ("num_reaches",), data_dict["busboi"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "busboi", "a0"))
        var = self.write_var(bb_grp, "n", "f8", ("num_reaches",), data_dict["busboi"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "busboi", "n"))
        var = self.write_var(bb_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["busboi"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "busboi", "qbar_reachScale"))
        var = self.write_var(bb_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["busboi"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "busboi", "qbar_basinScale"))

        # hivdi
        hv_grp = moi_grp.createGroup("hivdi")
        var = self.write_var_nt(hv_grp, "q", self.vlen_f, ("num_reaches"), data_dict["hivdi"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "hivdi", "q"))
        var = self.write_var(hv_grp, "Abar", "f8", ("num_reaches",), data_dict["hivdi"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "hivdi", "Abar"))
        var = self.write_var(hv_grp, "alpha", "f8", ("num_reaches",), data_dict["hivdi"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "hivdi", "alpha"))
        var = self.write_var(hv_grp, "beta", "f8", ("num_reaches",), data_dict["hivdi"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "hivdi", "beta"))
        var = self.write_var(hv_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["hivdi"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "hivdi", "qbar_reachScale"))
        var = self.write_var(hv_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["hivdi"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "hivdi", "qbar_basinScale"))

        # metroman
        mm_grp = moi_grp.createGroup("metroman")
        var = self.write_var_nt(mm_grp, "q", self.vlen_f, ("num_reaches"), data_dict["metroman"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "metroman", "q"))
        var = self.write_var(mm_grp, "Abar", "f8", ("num_reaches",), data_dict["metroman"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "metroman", "Abar"))
        var = self.write_var(mm_grp, "na", "f8", ("num_reaches",), data_dict["metroman"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "metroman", "na"))
        var = self.write_var(mm_grp, "x1", "f8", ("num_reaches",), data_dict["metroman"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "metroman", "x1"))
        var = self.write_var(mm_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["metroman"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "metroman", "qbar_reachScale"))
        var = self.write_var(mm_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["metroman"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "metroman", "qbar_basinScale"))

        # momma
        mo_grp = moi_grp.createGroup("momma")
        var = self.write_var_nt(mo_grp, "q", self.vlen_f, ("num_reaches"), data_dict["momma"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "momma", "q"))
        var = self.write_var(mo_grp, "B", "f8", ("num_reaches",), data_dict["momma"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "momma", "B"))
        var = self.write_var(mo_grp, "H", "f8", ("num_reaches",), data_dict["momma"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "momma", "H"))
        var = self.write_var(mo_grp, "Save", "f8", ("num_reaches",), data_dict["momma"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "momma", "Save"))
        var = self.write_var(mo_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["momma"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "momma", "qbar_reachScale"))
        var = self.write_var(mo_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["momma"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "momma", "qbar_basinScale"))

        # sad
        sad_grp = moi_grp.createGroup("sad")
        var = self.write_var_nt(sad_grp, "q", self.vlen_f, ("num_reaches"), data_dict["sad"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "sad", "q"))
        var = self.write_var(sad_grp, "a0", "f8", ("num_reaches",), data_dict["sad"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "sad", "a0"))
        var = self.write_var(sad_grp, "n", "f8", ("num_reaches",), data_dict["sad"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "sad", "n"))
        var = self.write_var(sad_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["sad"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "sad", "qbar_reachScale"))
        var = self.write_var(sad_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["sad"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "sad", "qbar_basinScale"))

        # sic4dvar
        sic_grp = moi_grp.createGroup("sic4dvar")
        var = self.write_var_nt(sic_grp, "q", self.vlen_f, ("num_reaches"), data_dict["sic4dvar"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "sic4dvar", "q"))
        var = self.write_var(sic_grp, "a0", "f8", ("num_reaches",), data_dict["sic4dvar"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "sic4dvar", "a0"))
        var = self.write_var(sic_grp, "n", "f8", ("num_reaches",), data_dict["sic4dvar"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "sic4dvar", "n"))
        var = self.write_var(sic_grp, "qbar_reachScale", "f8", ("num_reaches",), data_dict["sic4dvar"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "sic4dvar", "qbar_reachScale"))
        var = self.write_var(sic_grp, "qbar_basinScale", "f8", ("num_reaches",), data_dict["sic4dvar"])
        self.set_variable_atts(var, self._moi_meta(metadata_json, "sic4dvar", "qbar_basinScale"))

        sos_ds.close()
