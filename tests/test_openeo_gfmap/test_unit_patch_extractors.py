from unittest.mock import MagicMock

import numpy as np
import pytest
import xarray as xr

from openeo_gfmap.features import PatchFeatureExtractor

LAT_HARMONIZED_NAME = "GEO-LAT"
LON_HARMONIZED_NAME = "GEO-LON"
EPSG_HARMONIZED_NAME = "GEO-EPSG"


# Mock class for the patch feature extractor
class DummyPatchFeatureExtractor(PatchFeatureExtractor):
    def output_labels(self):
        return ["label1", "label2"]

    def execute(self, inarr: xr.DataArray) -> xr.DataArray:
        return inarr  # Simplified for testing purposes


@pytest.fixture
def mock_feature_extractor():
    return DummyPatchFeatureExtractor()


@pytest.fixture
def mock_data_array():
    return xr.DataArray(np.array([[1, 2], [3, 4]]), dims=["y", "x"])


def test_get_latlons_epsg_none(mock_feature_extractor, mock_data_array):
    mock_feature_extractor._epsg = None
    with pytest.raises(Exception):
        mock_feature_extractor.get_latlons(mock_data_array)


def test_get_latlons_epsg_4326(mock_feature_extractor, mock_data_array):
    mock_feature_extractor._epsg = 4326
    result = mock_feature_extractor.get_latlons(mock_data_array)
    assert LAT_HARMONIZED_NAME in result.coords["bands"].values
    assert LON_HARMONIZED_NAME in result.coords["bands"].values


def test_get_latlons_reproject(mock_feature_extractor, mock_data_array):
    mock_feature_extractor._epsg = (
        3857  # Set the EPSG code to the desired projection (e.g., Web Mercator)
    )

    # Create mock coordinates matching the 'x' and 'y' dimensions
    x_coords = mock_data_array.coords["x"].values
    y_coords = mock_data_array.coords["y"].values

    xx, yy = np.meshgrid(x_coords, y_coords)
    result = mock_feature_extractor.get_latlons(mock_data_array)

    # Assert the expected behavior (add your specific assertions here)
    assert result is not None
    assert result[0].shape == xx.shape
    assert result[1].shape == yy.shape


# test rescaling
def test_rescale_s1_backscatter_valid(mock_feature_extractor, mock_data_array):
    s1_bands = ["S1-SIGMA0-VV", "S1-SIGMA0-VH"]
    data = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=np.uint16)
    mock_data_array = xr.DataArray(
        data, dims=["bands", "y", "x"], coords={"bands": s1_bands}
    )

    result = mock_feature_extractor._rescale_s1_backscatter(mock_data_array)
    assert result.dtype == np.uint16


# TODO
@pytest.mark.skip(
    reason="Skipping test for since underlying excecutor needs to be changed"
)
def test_execute():
    # Create an instance of the extractor
    extractor = DummyPatchFeatureExtractor()
    extractor._parameters = {"rescale_s1": True}

    # Mock the cube
    data = np.ones((1, 2, 2, 2))
    mock_cube = MagicMock()
    mock_cube.get_array.return_value = xr.DataArray(data, dims=["bands", "t", "y", "x"])

    # Mock the methods
    extractor._common_preparations = MagicMock(return_value=mock_cube.get_array())
    extractor._rescale_s1_backscatter = MagicMock(return_value=mock_cube.get_array())

    # Execute the method
    result = extractor._execute(mock_cube, {})

    # Ensure the result is correctly transposed to have dimensions ["bands", "y", "x"]
    expected_dims = ["bands", "t", "y", "x"]
    assert result.get_array().dims == expected_dims

    # Check that the mock methods were called
    extractor._common_preparations.assert_called()
    extractor._rescale_s1_backscatter.assert_called()
