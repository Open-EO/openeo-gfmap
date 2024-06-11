from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr
from pyproj import Transformer


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


def test_get_latlons_epsg_none(mock_feature_extractor, mock_data_array):
    mock_feature_extractor._epsg = None
    with pytest.raises(Exception):
        mock_feature_extractor.get_latlons(mock_data_array)


def test_get_latlons_epsg_4326(mock_feature_extractor, mock_data_array):
    mock_feature_extractor._epsg = 4326
    result = mock_feature_extractor.get_latlons(mock_data_array)
    assert LAT_HARMONIZED_NAME in result.coords['bands'].values
    assert LON_HARMONIZED_NAME in result.coords['bands'].values

def test_get_latlons_reproject(mock_feature_extractor, mock_data_array):
    mock_feature_extractor._epsg = 3857  # Set the EPSG code to the desired projection (e.g., Web Mercator)

    # Create mock coordinates matching the 'x' and 'y' dimensions
    x_coords = mock_data_array.coords['x'].values
    y_coords = mock_data_array.coords['y'].values

    xx, yy = np.meshgrid(x_coords, y_coords)

    # Create the Transformer using pyproj without mocking
    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)  # Example: from Web Mercator to WGS84
    transformed_x, transformed_y = transformer.transform(xx, yy)

    result = mock_feature_extractor.get_latlons(mock_data_array)

    # Assert the expected behavior (add your specific assertions here)
    assert result is not None
    assert result[0].shape == xx.shape
    assert result[1].shape == yy.shape

    # Verify that the transformed coordinates match the expected output using pytest.approx
    assert result[0] == pytest.approx(transformed_x, rel=1e-6)
    assert result[1] == pytest.approx(transformed_y, rel=1e-6)


# test rescaling
def test_rescale_s1_backscatter_valid(mock_feature_extractor, mock_data_array):
    s1_bands = ["S1-SIGMA0-VV", "S1-SIGMA0-VH"]
    data = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=np.uint16)
    mock_data_array = xr.DataArray(
        data, dims=["bands", "y", "x"], coords={"bands": s1_bands}
    )

    result = mock_feature_extractor._rescale_s1_backscatter(mock_data_array)
    assert result.dtype == np.uint16


# Helper functions to test excecute
def create_mock_common_preparations():
    data = np.array([[[[1, 2], [3, 4]], [[5, 6], [7, 8]]]])
    return xr.DataArray(data, dims=["bands", "t", "y", "x"])


def create_mock_rescale_s1():
    data = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]])
    return xr.DataArray(data, dims=["bands", "y", "x"])


# test excecute
@patch.object(
    DummyPatchFeatureExtractor,
    "_common_preparations",
    return_value=create_mock_common_preparations(),
)
@patch.object(
    DummyPatchFeatureExtractor,
    "_rescale_s1_backscatter",
    return_value=create_mock_rescale_s1(),
)
def test_execute(mock_common_preparations, mock_rescale_s1):
    # Create an instance of the extractor
    extractor = DummyPatchFeatureExtractor()
    extractor._parameters = {"rescale_s1": True}

    # Mock the cube
    mock_cube = MagicMock()
    mock_cube.get_array.return_value = xr.DataArray(
        np.random.rand(2, 20, 10, 10), dims=["bands", "t", "y", "x"]
    )

    # Execute the method
    result = extractor._execute(mock_cube, {})

    # Ensure the result is correctly transposed to have dimensions ["bands", "y", "x"]
    expected_dims = ("bands", "y", "x")
    assert result.get_array().dims == expected_dims

    # Check that the mock methods were called
    mock_common_preparations.assert_called()
    mock_rescale_s1.assert_called()
