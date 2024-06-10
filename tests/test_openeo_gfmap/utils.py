"""Utilitiaries used in tests, such as download test resources."""

from tempfile import NamedTemporaryFile

import requests
import xarray as xr


def load_dataset_url(url: str) -> NamedTemporaryFile:
    """Download a NetCDF file from the internet and return a Xarray Dataset."""
    with NamedTemporaryFile(suffix=".nc", delete=True) as tmpfile:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        tmpfile.write(response.content)

        inds = xr.open_dataset(tmpfile.name)

        return inds


def load_dataarray_url(url: str) -> NamedTemporaryFile:
    """Download a NetCDF file from the internet and return a Xarray Dataset."""
    with NamedTemporaryFile(suffix=".nc", delete=True) as tmpfile:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        tmpfile.write(response.content)

        inds = xr.open_dataarray(tmpfile.name)

        return inds
