from unittest.mock import MagicMock

import openeo
import pytest

from openeo_gfmap.fetching.commons import (
    convert_band_names,
    rename_bands,
    resample_reproject,
)
from openeo_gfmap.fetching.s2 import (
    BASE_SENTINEL2_L2A_MAPPING,
    ELEMENT84_SENTINEL2_L2A_MAPPING,
)
from tests.utils.helpers import create_test_datacube

# band names


def test_convert_band_names_base_mapping():
    """Test conversion with BASE_SENTINEL2_L2A_MAPPING."""
    desired_bands = ["S2-L2A-B01", "S2-L2A-B03", "S2-L2A-B12"]
    result = convert_band_names(desired_bands, BASE_SENTINEL2_L2A_MAPPING)
    assert result == ["B01", "B03", "B12"]


def test_convert_band_names_element84_mapping():
    """Test conversion with ELEMENT84_SENTINEL2_L2A_MAPPING."""
    desired_bands = ["S2-L2A-B01", "S2-L2A-B08", "S2-L2A-B12"]
    result = convert_band_names(desired_bands, ELEMENT84_SENTINEL2_L2A_MAPPING)
    assert result == ["coastal", "nir", "swir22"]


def test_convert_band_names_mixed_case():
    """Test conversion with a mix of known and unknown bands in BASE_SENTINEL2_L2A_MAPPING."""
    desired_bands = ["S2-L2A-B01", "S2-L2A-B99"]  # S2-L2A-B99 does not exist
    with pytest.raises(KeyError):
        convert_band_names(desired_bands, BASE_SENTINEL2_L2A_MAPPING)


def test_convert_band_names_empty_base_mapping():
    """Test conversion with an empty desired_bands list in BASE_SENTINEL2_L2A_MAPPING."""
    desired_bands = []
    result = convert_band_names(desired_bands, BASE_SENTINEL2_L2A_MAPPING)
    assert result == []


def test_convert_band_names_empty_element84_mapping():
    """Test conversion with an empty desired_bands list in ELEMENT84_SENTINEL2_L2A_MAPPING."""
    desired_bands = []
    result = convert_band_names(desired_bands, ELEMENT84_SENTINEL2_L2A_MAPPING)
    assert result == []


def test_convert_band_names_with_nonexistent_band_element84():
    """Test conversion where a band is not in ELEMENT84_SENTINEL2_L2A_MAPPING."""
    desired_bands = ["S2-L2A-B01", "S2-L2A-B99"]  # S2-L2A-B99 does not exist
    with pytest.raises(KeyError):
        convert_band_names(desired_bands, ELEMENT84_SENTINEL2_L2A_MAPPING)


# resampling
def test_resample_reproject_valid_epsg():
    """Test resample_reproject with a valid EPSG code."""
    # Create a mock DataCube object
    mock_datacube = MagicMock(spec=openeo.DataCube)

    # Mock the resample_spatial method to return a new mock DataCube
    mock_resampled_datacube = MagicMock(spec=openeo.DataCube)
    mock_datacube.resample_spatial.return_value = mock_resampled_datacube

    # Call the resample_reproject function
    resample_reproject(
        mock_datacube, resolution=10.0, epsg_code="4326", method="bilinear"
    )

    # Ensure resample_spatial was called correctly
    mock_datacube.resample_spatial.assert_called_once_with(
        resolution=10.0, projection="4326", method="bilinear"
    )


# invalid espg
def test_resample_reproject_invalid_epsg():
    """Test resample_reproject with an invalid EPSG code."""
    # Create a mock DataCube object
    mock_datacube = MagicMock(spec=openeo.DataCube)

    # Attempt to call the resample_reproject function with an invalid EPSG code
    with pytest.raises(ValueError, match="is not a valid EPSG code"):
        resample_reproject(
            mock_datacube, resolution=10.0, epsg_code="invalid_epsg", method="bilinear"
        )

    # Ensure resample_spatial was not called
    mock_datacube.resample_spatial.assert_not_called()


# valid resolution
def test_resample_reproject_only_resolution():
    """Test resample_reproject with only resolution provided."""
    # Create a mock DataCube object
    mock_datacube = MagicMock(spec=openeo.DataCube)

    # Mock the resample_spatial method to return a new mock DataCube
    mock_resampled_datacube = MagicMock(spec=openeo.DataCube)
    mock_datacube.resample_spatial.return_value = mock_resampled_datacube

    # Call the resample_reproject function with only resolution provided
    resample_reproject(mock_datacube, resolution=20.0)

    # Ensure resample_spatial was called correctly with the resolution and default method
    mock_datacube.resample_spatial.assert_called_once_with(
        resolution=20.0, method="near"
    )


# default espg
def test_resample_reproject_no_epsg():
    """Test resample_reproject with no EPSG code provided."""
    # Create a mock DataCube object
    mock_datacube = MagicMock(spec=openeo.DataCube)

    # Mock the resample_spatial method to return a new mock DataCube
    mock_resampled_datacube = MagicMock(spec=openeo.DataCube)
    mock_datacube.resample_spatial.return_value = mock_resampled_datacube

    # Call the resample_reproject function without specifying an EPSG code
    resample_reproject(
        mock_datacube, resolution=10.0, epsg_code=None, method="bilinear"
    )

    # Ensure resample_spatial was called correctly without the projection argument
    mock_datacube.resample_spatial.assert_called_once_with(
        resolution=10.0, method="bilinear"
    )


# default resampling
def test_resample_reproject_default_method():
    """Test resample_reproject with a valid EPSG code and default resampling method."""
    # Create a mock DataCube object
    mock_datacube = MagicMock(spec=openeo.DataCube)

    # Mock the resample_spatial method to return a new mock DataCube
    mock_resampled_datacube = MagicMock(spec=openeo.DataCube)
    mock_datacube.resample_spatial.return_value = mock_resampled_datacube

    # Call the resample_reproject function with default method ("near")
    resample_reproject(mock_datacube, resolution=10.0, epsg_code="4326")

    # Ensure resample_spatial was called correctly with the default method
    mock_datacube.resample_spatial.assert_called_once_with(
        resolution=10.0, projection="4326", method="near"
    )


def test_rename_bands_all_present():
    """Test rename_bands when all bands in the mapping are present in the datacube."""

    datacube = create_test_datacube()

    mapping = {"B01": "coastal", "B02": "blue", "B03": "green"}

    # Call the rename_bands function
    result = rename_bands(datacube, mapping)

    # Extract the band names from the result metadata
    result_band_names = [band.name for band in result.metadata._dimensions[0].bands]

    # Check that only the available bands have been renamed
    expected_renamed_bands = [
        "coastal",
        "blue",
        "green",
    ] + datacube.metadata.band_names[
        3:
    ]  # Assuming B04-B12 remain unchanged

    assert result_band_names == expected_renamed_bands


def test_rename_bands_some_missing():
    """Test rename_bands when some bands are not present in the datacube."""

    # Use the fixture with specific bands
    datacube = create_test_datacube(bands=["B01", "B02"])  # Only include B01 and B02

    mapping = {
        "B01": "coastal",
        "B02": "blue",
        "B03": "green",  # B03 is not in the datacube
    }

    # Call the rename_bands function
    result = rename_bands(datacube, mapping)

    # Extract the band names from the result metadata
    result_band_names = [band.name for band in result.metadata._dimensions[0].bands]

    # Check that only the available bands have been renamed
    expected_renamed_bands = ["coastal", "blue"]
    assert result_band_names == expected_renamed_bands


def test_rename_bands_no_bands_present():
    """Test rename_bands when no bands in the mapping are present in the datacube."""

    # Use the fixture with specific bands
    datacube = create_test_datacube(bands=["B04", "B05"])  # Only include B04 and B05

    mapping = {
        "B01": "coastal",
        "B02": "blue",
        "B03": "green",  # None of these bands are present in the datacube
    }

    # Call the rename_bands function
    result = rename_bands(datacube, mapping)

    # Extract the band names from the result metadata
    result_band_names = [band.name for band in result.metadata._dimensions[0].bands]

    # Check that only the available bands have been renamed
    assert result_band_names == []
