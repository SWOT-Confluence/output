# Standard imports
from abc import ABCMeta, abstractmethod

# Third-party imports
import numpy as np

class AbstractModule(metaclass=ABCMeta):
    """Class that represents a Confluence Module that has result data to store.
    
    Attributes
    ----------
    cont_ids: list
        list of continent identifiers
    FILL: dict
        dictionary of various NetCDF variable fill values
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
    vlen_f: VLType
        variable length float data type for NetCDF ragged arrays
    vlen_i: VLType
        variable length int data type for NEtCDF ragged arrays
    
    Methods
    -------
    append_module(nt)
        append module results to the SoS.
    append_module_data(data_dict)
        append module data to the new version of the SoS result file.
    create_data_dict(nt=None)
        creates and returns module data dictionary.
    get_module_data(nt=None)
        retrieve module results from NetCDF files.
    write_var(q_grp, name, dims, sv_dict)
        create NetCDF variable and write module data to it
    """
    
    FILL = {
        "f8": -999999999999.0,
        "i4": -999,
        "S1": "x"
    }
    
    def __init__(self, cont_ids, input_dir, sos_new, vlen_f=None, vlen_i=None, 
                 vlen_s=None, rids=None, nrids=None, nids=None):
        
        """
        Parameters
        ----------
        cont_ids: list
            list of continent identifiers
        input_dir: Path
            path to input directory
        sos_new: Path
            path to new SOS file
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

        self.cont_ids = cont_ids
        self.input_dir = input_dir
        self.sos_new = sos_new
        self.vlen_f = vlen_f
        self.vlen_i = vlen_i
        self.vlen_s = vlen_s
        self.sos_rids = rids
        self.sos_nrids = nrids
        self.sos_nids = nids
    
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'get_module_data') and 
                callable(subclass.get_module_data) and 
                hasattr(subclass, 'create_data_dict') and 
                callable(subclass.create_data_dict) and
                hasattr(subclass, 'append_module_data') and 
                callable(subclass.append_module_data) or
                NotImplemented)
        
    def append_module(self, metadata_json):
        """Append module results to the SoS."""
        
        data_dict = self.get_module_data()
        self.append_module_data(data_dict, metadata_json)
        
    @abstractmethod
    def get_module_data(self):
        """Retrieve module results from NetCDF files."""
        
        raise NotImplementedError
    
    @abstractmethod
    def create_data_dict(self):
        """Creates and returns module data dictionary."""
        
        raise NotImplementedError
    
    @abstractmethod
    def append_module_data(self, data_dict, metadata_json):
        """Append module data to the new version of the SoS result file.
        
        Parameters
        ----------
        data_dict: dict
            dictionary of module data
        """
        
        raise NotImplementedError
    
    def write_var(self, grp, name, type, dims, data_dict):
            """Create NetCDF variable and write module data to it.
    
            Parameters
            ----------
            grp: netCDF4._netCDF4.Group
                dicharge NetCDF4 group to write data to
            name: str
                name of variable
            type: str
                string type of NetCDF variable
            dims: tuple
                tuple of NetCDF4 dimensions that matches shape of var data
            data_dict: dict
                dictionary of result data
            """

            var = grp.createVariable(name, type, dims, fill_value=self.FILL[type], compression="zlib")
            if data_dict["attrs"][name]: var.setncatts(data_dict["attrs"][name])
            if type == "f8" or type == "i4": 
                var[:] = np.nan_to_num(data_dict[name], copy=True, nan=self.FILL[type])
            else:
                var[:] = data_dict[name]
            return var
    
    def write_var_nt(self, grp, name, vlen, dims, data_dict):
        """Create NetCDF variable length data variable and write module data.
        
        Parameters
        ----------
        grp: netCDF4._netCDF4.Group
            dicharge NetCDF4 group to write data to
        name: str
            name of variable
        vlen: netCDF4._netCDF4.VLType
            variable length data type
        dims: tuple
            tuple of NetCDF4 dimensions that matches shape of var data
        data_dict: dict
            dictionary of result data
        """
        
        var = grp.createVariable(name, vlen, dims)
        if data_dict["attrs"][name]: 
            data_dict["attrs"][name].pop("_FillValue", None)
            var.setncatts(data_dict["attrs"][name])
        var[:] = data_dict[name]
        return var
        
    def set_variable_atts(self, variable, variable_dict):
        """Set the variable attribute metdata."""
        
        for name, value in variable_dict.items():
            setattr(variable, name, value)