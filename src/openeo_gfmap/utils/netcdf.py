"""Utilities to edit and update netCDF files.
"""

from pathlib import Path
from typing import Union

from netCDF4 import Dataset


def update_nc_attributes(path: Union[str, Path], attributes: dict):
    """
    Update attributes of a NetCDF file.

    Parameters:
        path (str): Path to the NetCDF file.
        attributes (dict): Dictionary containing attributes to update.
                                Keys are attribute names, values are attribute values.
    """

    with Dataset(path, "r+") as nc:
        for name, value in attributes.items():
            if name in nc.ncattrs():
                setattr(nc, name, value)
            else:
                nc.setncattr(name, value)
