from unittest.mock import MagicMock, patch

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


# helper function to test latlons_espg
def create_mock_transformer():
    mock_transform = MagicMock()
    mock_transform.transform.return_value = (np.array([1, 2]), np.array([3, 4]))
    return mock_transform


@pytest.mark.parametrize(
    "epsg, expected_coords",
    [
        (4326, {LAT_HARMONIZED_NAME, LON_HARMONIZED_NAME}),
        (3757, {LAT_HARMONIZED_NAME, LON_HARMONIZED_NAME}),
    ],
)
def test_get_latlons_epsg(
    mock_feature_extractor, mock_data_array, epsg, expected_coords
):
    with patch(
        "pyproj.Transformer.from_crs", return_value=create_mock_transformer()
    ) as mock_from_crs:
        mock_feature_extractor._epsg = epsg

        result = mock_feature_extractor.get_latlons(mock_data_array)

        assert set(result.coords["bands"].values) == expected_coords
        mock_from_crs.return_value.transform.assert_called()


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
