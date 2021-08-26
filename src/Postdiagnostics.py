# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset, stringtochar
import numpy as np

class Postdiagnostics:
    """A class that represents the results of running Postdiagnostics module.
    
    
    Attributes
    ----------
    cont_ids: list
            list of continent identifiers
    FILL_VALUE: int
        fill value to use for missing data
    input_dir: Path
        path to input directory
    sos_nrids: nd.array
        array of SOS reach identifiers on the node-level
    sos_nids: nd.array
        array of SOS node identifiers
    sos_rids: nd.array
        array of SoS reach identifiers associated with continent
        path to the current SoS
    sos_new: Path
            path to new SOS file

    Methods
    -------
    append_pd()
        append Postdiagnostics results to the SoS
    __append_pd_data( pd_dict)
        append Postdiagnostics data to the new version of the SoS
    __create_pd_dict(nt)
        creates and returns Postdiagnostics data dictionary
    __get_pd_data()
        extract Postdiagnostics results from NetCDF files
    __insert_nr(name, chain, index, pd_ds, pd_dict)
        insert discharge values into dictionary with nr dimension
     __insert_nt(self, name, chain, index, pd_ds, pd_dict):
        insert discharge values into dictionary with nr by nt dimensions
    __write_var(q_grp, name, chain, dims, pd_dict)
        create NetCDF variable and write Postdiagnostics data to it
    """

    FILL_VALUE = -999

    def __init__(self, cont_ids, input_dir, sos_new, rids, nrids, nids):
        """
        Parameters
        ----------
        cont_ids: list
            list of continent identifiers
        input_dir: Path
            path to input directory
        sos_new: Path
            path to new SOS file
        rids: nd.array
            array of SoS reach identifiers associated with continent
        nrids: nd.array
            array of SOS reach identifiers on the node-level
        nids: nd.array
            array of SOS node identifiers
        """

        self.cont_ids = cont_ids
        self.input_dir = input_dir
        self.sos_new = sos_new
        self.sos_rids = rids
        self.sos_nrids = nrids
        self.sos_nids = nids

    def append_pd(self):
        """Append Postdiagnostic results to the SoS."""

        pd_dict = self.__get_pd_data()
        self.__append_pd_data(pd_dict)

    def __get_pd_data(self):
        """Extract Postdiagnostics results from NetCDF files."""

        # Files and reach identifiers
        pd_basin_files = [ Path(pd_file) for pd_file in glob.glob(f"{self.input_dir}/basin/{self.cont_ids}*.nc") ]
        pd_rids = [ int(pd_file.name.split('_')[0]) for pd_file in pd_basin_files ]        
        
        if len(pd_basin_files) == 0:
            # Store empty data
            pd_dict = self.__create_pd_dict([], 0)
        else:
            # Get names number of algorithms processed
            pd_ds = Dataset(pd_basin_files[0], 'r')
            algo_names = pd_ds["algo_names"][:]
            num_algos = pd_ds.dimensions["num_algos"].size
            pd_ds.close()

            # Storage initialization
            pd_dict = self.__create_pd_dict(algo_names, num_algos)

            # Data extraction
            index = 0
            for s_rid in self.sos_rids:
                if s_rid in pd_rids:
                    pd_b_ds = Dataset(self.input_dir / "basin" / f"{s_rid}_moi_diag.nc", 'r')
                    pd_r_ds = Dataset(self.input_dir / "reach" / f"{s_rid}_flpe_diag.nc", 'r')
                    self.__insert_na("basin", "realism_flags", index, pd_b_ds, pd_dict)
                    self.__insert_na("basin", "stability_flags", index, pd_b_ds, pd_dict)
                    self.__insert_na("basin", "prepost_flags", index, pd_b_ds, pd_dict)
                    self.__insert_na("reach", "realism_flags", index, pd_r_ds, pd_dict)
                    self.__insert_na("reach", "stability_flags", index, pd_r_ds, pd_dict)
                    pd_b_ds.close()
                    pd_r_ds.close()
                index += 1
        return pd_dict
    
    def __create_pd_dict(self, algo_names, num_algos):
        """Creates and returns Postdiagnostics data dictionary.
        
        Parameters
        ----------
        algo_names: nd.array
            array of string algorithm names
        num_algos : int
            number of time steps
        """

        return {
            "algo_names" : algo_names,
            "num_algos" : num_algos,
            "basin" : {
                "realism_flags" : np.full((self.sos_rids.shape[0], num_algos), np.nan, dtype=np.float64),
                "stability_flags" : np.full((self.sos_rids.shape[0], num_algos), np.nan, dtype=np.float64),
                "prepost_flags" : np.full((self.sos_rids.shape[0], num_algos), np.nan, dtype=np.float64)
            },
            "reach" : {
                "realism_flags" : np.full((self.sos_rids.shape[0], num_algos), np.nan, dtype=np.float64),
                "stability_flags" : np.full((self.sos_rids.shape[0], num_algos), np.nan, dtype=np.float64)
            }
        }

    def __insert_na(self, alg, name, index, pd_ds, pd_dict):
        """Insert discharge values into dictionary with nr by na dimensions.

        (na equals number of algorithms)
        
        Parameters
        ----------
        alg: str
            name of algorithm
        name: str
            name of data variable
        index: int
            integer index to insert data at
        pd_ds: netCDF4.Dataset
            Postdiagnostic NetCDF that contains result data
        pd_dict: dict
            dictionary to store Postdiagnostics results in
        """

        pd_dict[alg][name][index, :] = pd_ds[name][:].filled(np.nan)

    def __append_pd_data(self, pd_dict):
        """Append Postdiagnostic data to the new version of the SoS.
        
        Parameters
        ----------
        pd_dict: dict
            dictionary of Postdiagnostic variables
        """

        sos_ds = Dataset(self.sos_new, 'a')
        pd_grp = sos_ds.createGroup("postdiagnostics")

        # Postdiagnostic data
        pd_grp.createDimension("num_algos", pd_dict["num_algos"])
        pd_grp.createDimension("nchar", None)
        na_v = pd_grp.createVariable("num_algos", "i4", ("num_algos",))
        na_v[:] = range(1, pd_dict["num_algos"] + 1)
        an_v = pd_grp.createVariable("algo_names", "S1", ("num_algos", "nchar"))
        an_v[:] = stringtochar(np.array(pd_dict["algo_names"], dtype="S8"))

        # Basin
        b_grp = pd_grp.createGroup("basin")
        self.__write_var(b_grp, "realism_flags", ("num_reaches", "num_algos"), pd_dict["basin"])
        self.__write_var(b_grp, "stability_flags", ("num_reaches", "num_algos"), pd_dict["basin"])
        self.__write_var(b_grp, "prepost_flags", ("num_reaches", "num_algos"), pd_dict["basin"])

        # Reach
        r_grp = pd_grp.createGroup("reach")
        self.__write_var(r_grp, "realism_flags", ("num_reaches", "num_algos"), pd_dict["reach"])
        self.__write_var(r_grp, "stability_flags", ("num_reaches", "num_algos"), pd_dict["reach"])

        sos_ds.close()

    def __write_var(self, grp, name, dims, pd_dict):
        """Create NetCDF variable and write Postdiagnostic data to it.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        dims: tuple
            tuple of NetCDF4 dimensions that matches shape of var dataa
        pd_dict: dict
            dictionary of Postdiagnostic result data
        """

        var = grp.createVariable(name, "f8", dims, fill_value=self.FILL_VALUE)
        var[:] = np.nan_to_num(pd_dict[name], copy=True, nan=self.FILL_VALUE)