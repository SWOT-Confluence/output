# Third-party imports
from netCDF4 import Dataset

# Local imports
from output.modules.AbstractModule import AbstractModule

class Priors(AbstractModule):
    """
    A class that represents the results of running the Priors module.
    
    Output stores priors "model" group to track GRADES data that has been 
    overwritten by gage priors (applicable only to constrained runs).
    
    Attributes
    ----------
    sos: netcdf4 Dataset
        NetCDF4 Dataset for current SoS priors file
    suffix: str
        string suffix of priors file
        
    Methods
    -------
    append_module_data(data_dict)
        append module data to the new version of the SoS result file.
    create_data_dict()
        creates and returns module data dictionary.
    get_module_data()
        retrieve module results from NetCDF files.
    open_sos(self)
        open current SoS dataset for reading.
    close_sos(self)
        closes current SoS dataset.
    """
    
    def __init__(self, cont_ids, input_dir, sos_new, suffix):
        """
        Parameters
        ----------
        cont_ids: list
            list of continent identifiers
        input_dir: Path
            path to input directory
        sos_new: Path
            path to new SOS file
        suffix: str
            string suffix of priors file
        """

        self.suffix = suffix
        super().__init__(cont_ids, input_dir, sos_new)
        
    def get_module_data(self):
        """Extract and return model group from priors SoS file."""
        
        continent = self.sos_new.stem.split('_')[0]
        sos_cur = Dataset(self.input_dir / f"{continent}_{self.suffix}.nc", 'r')
        pri_dict = self.create_data_dict(sos_cur)
        sos_cur.close()
        return pri_dict        
        
    def create_data_dict(self, sos):
        """Creates and returns Priors NetCDF 'model' group data dictionary."""

        model = sos["model"]

        # Dimensions
        dims = {}
        for name, dimension in model.dimensions.items():
            dims[name] = dimension.size if not dimension.isunlimited() else None
        
        # Variables
        vars = {}
        for name, variable in model.variables.items():
            vars[name] = {
                "data_type": variable.datatype,
                "dimensions": variable.dimensions,
                "attributes": variable.__dict__,
                "data": variable[:]
            }
        
        return {
            "dimensions": dims,
            "variables": vars                   
        }
    
    def append_module_data(self, data_dict, metadata_json):
        """Append Priors data to the new version of the SoS.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of Priors "model" group variables
        """
        
        sos_ds = Dataset(self.sos_new, 'a')
        pri_grp = sos_ds.createGroup("priors")

        # Dimensions
        for name, size in data_dict["dimensions"].items():
            pri_grp.createDimension(name, size)
        
        # Variables
        for name, variable in data_dict["variables"].items():
            v = pri_grp.createVariable(name, variable["data_type"], variable["dimensions"], compression="zlib")
            v.setncatts(variable["attributes"])
            v[:] = variable["data"]
            self.set_variable_atts(v, metadata_json["priors"][name])
        sos_ds.close()
