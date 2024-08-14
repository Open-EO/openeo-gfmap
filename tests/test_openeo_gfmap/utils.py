"""Utilitiaries used in tests, such as download test resources."""

from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock

import numpy as np
import openeo
import requests
import xarray as xr
from openeo.metadata import Band, BandDimension, CollectionMetadata

from openeo_gfmap.fetching.s2 import BASE_SENTINEL2_L2A_MAPPING


def create_test_datacube(bands=None):
    """Create a test DataCube with predefined bands or specified bands."""

    if bands is None:
        bands = list(BASE_SENTINEL2_L2A_MAPPING.keys())

    # Create a simple xarray DataArray with the given bands, all set to 1
    data = np.ones((100, 100, len(bands)))  # 100x100 grid with 'len(bands)' bands
    coords = {"x": np.arange(100), "y": np.arange(100), "bands": bands}
    dataarray = xr.DataArray(data, coords=coords, dims=["x", "y", "bands"])

    # Create a mock connection
    connection = MagicMock(spec=openeo.Connection)

    # Create new metadata to reflect the current bands
    band_objects = [Band(name=band_name) for band_name in bands]
    band_dimension = BandDimension(name="bands", bands=band_objects)
    metadata = CollectionMetadata(
        metadata={"id": "sentinel2_l2a", "title": "Sentinel-2 L2A"},
        dimensions=[band_dimension],
    )

    # Wrap this DataArray into an OpenEO DataCube
    cube = openeo.DataCube(graph=None, connection=connection, metadata=metadata)
    cube.dataarray = dataarray  # Add the dataarray to the cube

    return cube


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
