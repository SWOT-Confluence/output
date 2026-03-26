# Standard imports
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from output.modules.AbstractModule import AbstractModule


def detect_continent(inlet_reaches):
    """
    Detect continent code from inlet_reaches variable.

    The first digit of the first inlet reach ID corresponds to the continent code.

    Parameters
    ----------
    inlet_reaches : np.ndarray
        Array of inlet reach IDs

    Returns
    -------
    int
        Single digit continent code
    """
    first_inlet = int(inlet_reaches[0]) if isinstance(inlet_reaches, (list, np.ndarray)) else int(inlet_reaches)
    continent_code = int(str(first_inlet)[0])
    return continent_code


class CoastalQ(AbstractModule):
    """
    A class that represents the results of running CoastalQ delta discharge partitioning.

    Appends delta discharge data to continent files under nested 'coastal/delta_name' groups.
    """

    def __init__(self, cont_ids, input_dir, sos_new, logger):
        """
        Parameters
        ----------
        cont_ids: list
            list of continent identifiers
        input_dir: Path
            path to input directory containing coastalq subdirectory
        sos_new: Path
            path to new SOS file
        logger: logging.Logger
            logger to use for logging state
        """
        # Initialize parent with minimal required attributes
        self.cont_ids = cont_ids
        self.input_dir = Path(input_dir)
        self.sos_new = sos_new
        self.logger = logger

    def create_data_dict(self):
        """Creates and returns an empty dictionary to hold file mappings."""
        return {}

    def get_module_data(self):
        """Queue CoastalQ delta NetCDF files for the active continent."""
        cq_dir = self.input_dir / "coastalq"
        cq_files = list(cq_dir.glob("*.nc"))
        data_dict = self.create_data_dict()

        if not cq_files:
            self.logger.info("No CoastalQ delta files found")
            return data_dict

        for cq_file in cq_files:
            try:
                with Dataset(cq_file, 'r') as ds:
                    continent_code = detect_continent(ds['sword_inlets'][:])

                    # Ensure we only append deltas inside of the currently processing continent(s)
                    if continent_code not in self.cont_ids:
                        continue

                    delta_name = cq_file.stem
                    data_dict[delta_name] = cq_file

                    self.logger.info(f"Queued delta {delta_name} for continent {continent_code}")

            except Exception as e:
                self.logger.warning(f'Failed to read {cq_file}: {e}')

        return data_dict

    def append_module_data(self, data_dict, metadata_json):
        """Dynamically copy all contents of the CoastalQ files to the SoS output.

        Parameters
        ----------
        data_dict : dict
            Dictionary mapping delta_name to its source file Path.
        metadata_json : dict
            JSON object containing SoS metadata.
        """
        if not data_dict:
            self.logger.info("No CoastalQ data to append for this continent.")
            return

        # Open the main output file to append coastal data
        with Dataset(self.sos_new, 'a') as sos_ds:

            # Create or access the 'coastal' root group
            if 'coastal' not in sos_ds.groups:
                coastal_group = sos_ds.createGroup('coastal')
            else:
                coastal_group = sos_ds.groups['coastal']

            for delta_name, cq_file in data_dict.items():
                try:
                    with Dataset(cq_file, 'r') as src_ds:
                        # Create or access subgroup for this specific delta
                        if delta_name not in coastal_group.groups:
                            delta_group = coastal_group.createGroup(delta_name)
                        else:
                            delta_group = coastal_group.groups[delta_name]

                        # 1. Copy Group-level Attributes
                        delta_group.setncatts(src_ds.__dict__)

                        # 2. Copy Dimensions (handling unlimited dimensions safely)
                        for dim_name, dimension in src_ds.dimensions.items():
                            if dim_name not in delta_group.dimensions:
                                delta_group.createDimension(
                                    dim_name, 
                                    len(dimension) if not dimension.isunlimited() else None
                                )

                        # 3. Copy Variables, Variable Attributes, and Data dynamically
                        for var_name, variable in src_ds.variables.items():
                            if var_name not in delta_group.variables:

                                # Safely extract _FillValue if it exists to pass to createVariable
                                fill_value = variable.getncattr('_FillValue') if '_FillValue' in variable.ncattrs() else None

                                out_var = delta_group.createVariable(
                                    var_name,
                                    variable.datatype,
                                    variable.dimensions,
                                    fill_value=fill_value,
                                    compression="zlib"
                                )

                                # Copy all other attributes (excluding _FillValue which is already set)
                                attrs = {k: variable.getncattr(k) for k in variable.ncattrs() if k != '_FillValue'}
                                if attrs:
                                    out_var.setncatts(attrs)

                                # Copy the raw data payload
                                out_var[:] = variable[:]

                    self.logger.info(f"Successfully appended delta {delta_name} to coastal group")

                except Exception as e:
                    self.logger.warning(f'Failed to append delta {delta_name}: {e}')
        return