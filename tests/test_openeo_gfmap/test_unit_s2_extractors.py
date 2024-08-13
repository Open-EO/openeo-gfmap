from unittest.mock import MagicMock, patch

import openeo
import pytest

from openeo_gfmap import BoundingBoxExtent, TemporalContext
from openeo_gfmap.backend import Backend, BackendContext
from openeo_gfmap.fetching import (
    CollectionFetcher,
    FetchType,
    build_sentinel2_l2a_extractor,
)
from openeo_gfmap.fetching.s2 import (
    BASE_SENTINEL2_L2A_MAPPING,
    _get_s2_l2a_default_fetcher,
)

# Mock constants for the tests
BANDS = ["S2-L2A-B01", "S2-L2A-B02"]
COLLECTION_NAME = "SENTINEL2_L2A"
FETCH_TYPE = FetchType.TILE


@pytest.fixture
def mock_backend_context():
    """Fixture to create a mock backend context."""
    backend_context = MagicMock(spec=BackendContext)
    backend_context.backend = Backend.CDSE
    return backend_context


@pytest.fixture
def mock_connection():
    """Fixture to create a mock connection."""
    return MagicMock()


@pytest.fixture
def mock_spatial_extent():
    """Fixture for spatial extent."""
    return BoundingBoxExtent(
        west=5.0515130512706845,
        south=51.215806593713,
        east=5.060320484557499,
        north=51.22149744530769,
        epsg=4326,
    )


@pytest.fixture
def mock_temporal_extent():
    """Fixture for temporal context."""
    return TemporalContext(start_date="2023-04-01", end_date="2023-05-01")


# test the fetcher
def test_get_s2_l2a_default_fetcher_returns_callable():
    """Test that the fetcher function is callable."""
    fetcher = _get_s2_l2a_default_fetcher(COLLECTION_NAME, FETCH_TYPE)
    assert callable(fetcher)


@patch("openeo_gfmap.fetching.s2.convert_band_names")
@patch("openeo_gfmap.fetching.s2._load_collection")
def test_fetch_function_calls_convert_and_load(
    mock_load_collection,
    mock_convert_band_names,
    mock_connection,
    mock_spatial_extent,
    mock_temporal_extent,
):
    """Test that the fetch function calls convert_band_names and _load_collection."""
    fetcher = _get_s2_l2a_default_fetcher(COLLECTION_NAME, FETCH_TYPE)

    # Set up the mock return value for band conversion
    mock_convert_band_names.return_value = BANDS

    # Call the fetch function
    result = fetcher(mock_connection, mock_spatial_extent, mock_temporal_extent, BANDS)

    # Assert that convert_band_names was called with correct bands
    mock_convert_band_names.assert_called_once_with(BANDS, BASE_SENTINEL2_L2A_MAPPING)

    # Assert that _load_collection was called with correct parameters
    mock_load_collection.assert_called_once_with(
        mock_connection,
        BANDS,
        COLLECTION_NAME,
        mock_spatial_extent,
        mock_temporal_extent,
        FETCH_TYPE,
        **{},
    )


@patch("openeo_gfmap.fetching.s2._load_collection")
def test_fetch_function_returns_datacube(
    mock_load_collection, mock_connection, mock_spatial_extent, mock_temporal_extent
):
    """Test that the fetch function returns a DataCube."""
    fetcher = _get_s2_l2a_default_fetcher(COLLECTION_NAME, FETCH_TYPE)

    # Mock return value for _load_collection
    mock_load_collection.return_value = MagicMock(spec=openeo.DataCube)

    result = fetcher(mock_connection, mock_spatial_extent, mock_temporal_extent, BANDS)

    # Assert that the result is an instance of DataCube
    assert isinstance(result, openeo.DataCube)


# test the extractor


def test_build_sentinel2_l2a_extractor(mock_backend_context):
    """Test that build_sentinel2_l2a_extractor returns a CollectionFetcher."""
    bands = ["S2-L2A-B01", "S2-L2A-B02"]
    extractor = build_sentinel2_l2a_extractor(
        backend_context=mock_backend_context, bands=bands, fetch_type=FetchType.TILE
    )

    assert isinstance(extractor, CollectionFetcher)
    assert extractor.bands == bands
