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
    _get_s2_l2a_default_processor,
)

from .utils import create_test_datacube

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


@pytest.fixture
def mock_datacube():
    """Fixture to create a mock DataCube using the create_test_datacube function."""
    return create_test_datacube()


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


# test the processor output
def test_get_s2_l2a_default_processor_returns_callable():
    """Test that the processor function is callable."""
    processor = _get_s2_l2a_default_processor(COLLECTION_NAME, FETCH_TYPE)
    assert callable(processor)


# test the processor behavior if resample and rename
@patch("openeo_gfmap.fetching.s2.resample_reproject")
@patch("openeo_gfmap.fetching.s2.rename_bands")
def test_processor_calls_resample_reproject_and_rename_bands(
    mock_rename_bands, mock_resample_reproject, mock_datacube
):
    """Test that the processor function calls resample_reproject and rename_bands."""
    processor = _get_s2_l2a_default_processor(COLLECTION_NAME, FETCH_TYPE)

    # Mock the return value of resample_reproject
    mock_resample_reproject.return_value = mock_datacube

    # Mock the return value of rename_bands
    mock_rename_bands.return_value = mock_datacube

    # Call the processor function
    result = processor(mock_datacube, target_resolution=10.0, target_crs="EPSG:4326")

    # Assert that resample_reproject was called with correct parameters
    mock_resample_reproject.assert_called_once_with(
        mock_datacube, 10.0, "EPSG:4326", method="near"
    )

    # Assert that rename_bands was called with the correct cube and mapping
    mock_rename_bands.assert_called_once_with(mock_datacube, BASE_SENTINEL2_L2A_MAPPING)

    # Assert that the result is a DataCube
    assert isinstance(result, openeo.DataCube)


# test error in case only target crs is given and not target resolution
@patch("openeo_gfmap.fetching.s2.resample_reproject")
def test_processor_raises_valueerror_for_missing_resolution(
    mock_resample_reproject, mock_datacube
):
    """Test that the processor raises a ValueError if target_crs is specified without target_resolution."""
    processor = _get_s2_l2a_default_processor(COLLECTION_NAME, FETCH_TYPE)

    # Prepare the parameters with target_crs set and target_resolution missing (i.e., set to None)
    params = {"target_crs": "4326", "target_resolution": None}

    # Expect ValueError when target_crs is specified without target_resolution
    with pytest.raises(
        ValueError,
        match="In fetching parameters: `target_crs` specified but not `target_resolution`, which is required to perform reprojection.",
    ):
        processor(mock_datacube, **params)


# test target resolution explicit None
@patch("openeo_gfmap.fetching.s2.rename_bands")
def test_processor_skips_reprojection_if_disabled(mock_rename_bands, mock_datacube):
    """Test that the processor skips reprojection if target_resolution is None."""
    processor = _get_s2_l2a_default_processor(COLLECTION_NAME, FETCH_TYPE)

    # Mock the return value of rename_bands
    mock_rename_bands.return_value = mock_datacube

    # Call the processor function with target_resolution=None
    result = processor(mock_datacube, target_resolution=None, target_crs=None)

    # Assert that resample_reproject was not called
    mock_rename_bands.assert_called_once()

    # Assert that the result is a DataCube
    assert isinstance(result, openeo.DataCube)


# test wheter rescaling is used
@patch("openeo_gfmap.fetching.s2.rename_bands")
def test_processor_changes_datatype_to_uint16(mock_rename_bands, mock_datacube):
    """Test that the processor changes the datatype to uint16."""
    processor = _get_s2_l2a_default_processor(COLLECTION_NAME, FETCH_TYPE)

    # Mock the return value of rename_bands
    mock_rename_bands.return_value = mock_datacube

    # Mock the linear_scale_range method to simulate the datatype conversion
    mock_datacube.linear_scale_range = MagicMock(return_value=mock_datacube)

    # Call the processor function
    result_cube = processor(mock_datacube)

    # Check that linear_scale_range was called with the correct parameters
    mock_datacube.linear_scale_range.assert_called_once_with(0, 65534, 0, 65534)


# test the extractor
def test_build_sentinel2_l2a_extractor(mock_backend_context):
    """Test that build_sentinel2_l2a_extractor returns a CollectionFetcher."""
    bands = ["S2-L2A-B01", "S2-L2A-B02"]
    extractor = build_sentinel2_l2a_extractor(
        backend_context=mock_backend_context, bands=bands, fetch_type=FetchType.TILE
    )

    assert isinstance(extractor, CollectionFetcher)
    assert extractor.bands == bands
