""" Tests for data extractors for Sentinel2 data. """
import pytest
from unittest.mock import MagicMock
from openeo_gfmap import BackendContext, Backend
from openeo_gfmap.fetching import CollectionFetcher
from openeo_gfmap.spatial import SpatialContext
from openeo_gfmap.temporal import TemporalContext

import xarray as xr
import numpy as np
from openeo_gfmap.utils import (
    normalize_array,
    select_optical_bands,
    select_sar_bands,
    array_bounds,
    arrays_cosine_similarity
    )


def test_normalize_array():
    data = np.ones([5, 5, 5]) * 10
    inarr = xr.DataArray(data, dims=["x", "y", "t"])
    
    normalized = normalize_array(inarr)
    
    assert normalized is not None
    assert (normalized >= 0).all()
    assert (normalized <= 1).all()


def test_select_optical_bands():
    data = xr.DataArray(
        np.ones([5, 5, 3]),
        dims=["x", "y", "bands"],
        coords={"bands": ["S2-L2A-B01", "S2-L2A-B02", "S1-SIGMA0-VV"]}
    )
    
    optical_bands = select_optical_bands(data)
    
    assert "S2-L2A-B01" in optical_bands.coords["bands"].values
    assert "S2-L2A-B02" in optical_bands.coords["bands"].values
    assert "S1-SIGMA0-VV" not in optical_bands.coords

def test_array_bounds():
    data = xr.DataArray(
        np.ones([5, 5]),
        dims=["x", "y"],
        coords={"x": np.linspace(0, 4, 5), "y": np.linspace(0, 4, 5)}
    )
    
    bounds = array_bounds(data)
    
    assert bounds == (0.0, 0.0, 4.0, 4.0)


def test_select_sar_bands():
    data = xr.DataArray(
        np.ones([5, 5, 4]),
        dims=["x", "y", "bands"],
        coords={"bands": ["S1-SIGMA0-VV", "S1-SIGMA0-VH", "S2-L2A-B01", "S2-L2A-B02"]}
    )
    
    sar_bands = select_sar_bands(data)
    
    assert "S1-SIGMA0-VV" in sar_bands.coords["bands"].values
    assert "S1-SIGMA0-VH" in sar_bands.coords["bands"].values
    assert "S2-L2A-B01" not in sar_bands.coords["bands"].values
    assert "S2-L2A-B02" not in sar_bands.coords["bands"].values

def test_arrays_cosine_similarity():
    array1 = xr.DataArray(np.array([1, 2, 3]), dims=["x"])
    array2 = xr.DataArray(np.array([1, 2, 3]), dims=["x"])
    
    similarity = arrays_cosine_similarity(array1, array2)
    
    assert similarity == 1.0
    
    array3 = xr.DataArray(np.array([1, 0, 0]), dims=["x"])
    similarity_diff = arrays_cosine_similarity(array1, array3)
    
    assert similarity_diff < 1.0


def test_collection_fetcher_get_cube():
    backend_context = BackendContext(Backend.CDSE)
    bands = ["S2-L2A-B01", "S2-L2A-B04"]
    collection_fetch = MagicMock() # mocked to control behavior and verify interaction
    collection_preprocessing = MagicMock() # mocked to control behavior and verify interaction
    fetcher = CollectionFetcher(backend_context, bands, collection_fetch, collection_preprocessing)
    
    connection = MagicMock()
    spatial_context = MagicMock(spec=SpatialContext)
    temporal_context = MagicMock(spec=TemporalContext)
    
    # Simulate fetched data
    fetched_data = MagicMock()
    collection_fetch.return_value = fetched_data
    
    # Simulate preprocessed data
    preprocessed_data = MagicMock()
    collection_preprocessing.return_value = preprocessed_data
    
    result = fetcher.get_cube(connection, spatial_context, temporal_context)
    
    collection_fetch.assert_called_once_with(connection, spatial_context, temporal_context, bands) #exactly called once with expected arguments
    collection_preprocessing.assert_called_once_with(fetched_data)
    assert result == preprocessed_data 