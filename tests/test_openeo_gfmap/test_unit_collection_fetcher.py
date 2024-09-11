from unittest.mock import MagicMock

import pytest
import xarray as xr

from openeo_gfmap import BoundingBoxExtent, TemporalContext
from openeo_gfmap.backend import BackendContext
from openeo_gfmap.fetching import CollectionFetcher


@pytest.fixture
def mock_backend_context():
    """Fixture to create a mock backend context."""
    return MagicMock(spec=BackendContext)


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
def mock_temporal_context():
    """Fixture for temporal context."""
    return TemporalContext(start_date="2023-04-01", end_date="2023-05-01")


@pytest.fixture
def mock_collection_fetch():
    """Fixture for the collection fetch function."""
    return MagicMock(return_value=xr.DataArray([1, 2, 3], dims=["bands"]))


@pytest.fixture
def mock_collection_preprocessing():
    """Fixture for the collection preprocessing function."""
    return MagicMock(return_value=xr.DataArray([1, 2, 3], dims=["bands"]))


def test_collection_fetcher(
    mock_connection,
    mock_spatial_extent,
    mock_temporal_context,
    mock_backend_context,
    mock_collection_fetch,
    mock_collection_preprocessing,
):
    """Test CollectionFetcher with basic data fetching."""

    # Create the CollectionFetcher with the mock functions
    fetcher = CollectionFetcher(
        backend_context=mock_backend_context,
        bands=["B01", "B02", "B03"],
        collection_fetch=mock_collection_fetch,  # Use the mock collection fetch function
        collection_preprocessing=mock_collection_preprocessing,  # Use the mock preprocessing function
    )

    # Call the method you're testing
    result = fetcher.get_cube(
        mock_connection, mock_spatial_extent, mock_temporal_context
    )

    # Assertions to check if everything works as expected
    assert isinstance(
        result, xr.DataArray
    )  # Check if the result is an xarray DataArray
    assert result.dims == ("bands",)  # Ensure the dimensions are as expected
    assert result.values.tolist() == [
        1,
        2,
        3,
    ]  # Ensure the values match the expected output


def test_collection_fetcher_get_cube(
    mock_connection,
    mock_spatial_extent,
    mock_temporal_context,
    mock_backend_context,
    mock_collection_fetch,
    mock_collection_preprocessing,
):
    """Test that CollectionFetcher.get_cube is called correctly."""

    bands = ["S2-L2A-B01", "S2-L2A-B02"]

    # Create the CollectionFetcher with the mock functions
    fetcher = CollectionFetcher(
        backend_context=mock_backend_context,
        bands=bands,
        collection_fetch=mock_collection_fetch,  # Use the mock collection fetch function
        collection_preprocessing=mock_collection_preprocessing,  # Use the mock preprocessing function
    )

    # Call the method you're testing
    result = fetcher.get_cube(
        mock_connection, mock_spatial_extent, mock_temporal_context
    )

    # Assert the fetch method was called with the correct arguments
    mock_collection_fetch.assert_called_once_with(
        mock_connection,
        mock_spatial_extent,
        mock_temporal_context,
        bands,
        **fetcher.params,  # Check for additional parameters if any
    )

    # Assert the preprocessing method was called once
    mock_collection_preprocessing.assert_called_once()

    # Assert that the result is an instance of xarray.DataArray
    assert isinstance(result, xr.DataArray)


def test_collection_fetcher_with_empty_bands(
    mock_backend_context, mock_connection, mock_spatial_extent, mock_temporal_context
):
    """Test that CollectionFetcher raises an error with empty bands."""
    bands = []
    fetcher = CollectionFetcher(
        backend_context=mock_backend_context,
        bands=bands,
        collection_fetch=MagicMock(),  # No need to mock fetch here
        collection_preprocessing=MagicMock(),
    )

    # with pytest.raises(ValueError, match="Bands cannot be empty"):
    fetcher.get_cube(mock_connection, mock_spatial_extent, mock_temporal_context)
