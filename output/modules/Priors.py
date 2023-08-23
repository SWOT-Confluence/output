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

        self.sos = None
        self.suffix = suffix
        super().__init__(cont_ids, input_dir, sos_new)
        
    def get_module_data(self):
        """Extract and return model group from priors SoS file."""
        
        self.open_sos()
        pri_dict = self.create_data_dict()
        return pri_dict        
        
    def create_data_dict(self):
        """Creates and returns Priors NetCDF 'model' group data dictionary."""

        model = self.sos["model"]
        return { 
            "attributes": model.__dict__,
            "dimensions": model.dimensions.items(),
            "variables": model.variables.items()                   
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
        # Attributes
        pri_grp.setncatts(data_dict["attributes"])
        # Dimensions
        for name, dimension in data_dict["dimensions"]:
            pri_grp.createDimension(name, dimension.size if not dimension.isunlimited() else None)
        # Variables
        for name, variable in data_dict["variables"]:
            v = pri_grp.createVariable(name, variable.datatype, variable.dimensions)
            v.setncatts(self.sos["model"][name].__dict__)
            v[:] = self.sos["model"][name][:]
            self.set_variable_atts(v, metadata_json["priors"][name])
        self.close_sos()
        
    def open_sos(self):
        """Open current SoS dataset for reading."""
        
        continent = self.sos_new.stem.split('_')[0]
        self.sos = Dataset(self.input_dir / f"{continent}_{self.suffix}.nc", 'r')
        
    def close_sos(self):
        """Closes current SoS dataset."""
        
        self.sos.close()