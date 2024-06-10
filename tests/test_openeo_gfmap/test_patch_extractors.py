from openeo_gfmap.features import PatchFeatureExtractor
from openeo.udf import XarrayDataCube

import pytest
import numpy as np
import xarray as xr
from unittest.mock import MagicMock, patch

LAT_HARMONIZED_NAME = "GEO-LAT"
LON_HARMONIZED_NAME = "GEO-LON"
EPSG_HARMONIZED_NAME = "GEO-EPSG"

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
    return xr.DataArray(np.random.rand(10, 10), dims=["y", "x"])

def test_get_latlons_epsg_none(mock_feature_extractor, mock_data_array):
    mock_feature_extractor._epsg = None
    with pytest.raises(Exception):
        mock_feature_extractor.get_latlons(mock_data_array)

def test_get_latlons_epsg_4326(mock_feature_extractor, mock_data_array):
    mock_feature_extractor._epsg = 4326
    result = mock_feature_extractor.get_latlons(mock_data_array)
    assert LAT_HARMONIZED_NAME in result.coords['bands'].values
    assert LON_HARMONIZED_NAME in result.coords['bands'].values

@patch('pyproj.Transformer.from_crs')
def test_get_latlons_reproject(mock_from_crs, mock_feature_extractor, mock_data_array):
    mock_feature_extractor._epsg = 3857

    # Create mock coordinates matching the 'x' and 'y' dimensions
    x_coords = mock_data_array.coords['x'].values
    y_coords = mock_data_array.coords['y'].values

    xx, yy = np.meshgrid(x_coords, y_coords)
    
    # Configure the transformer mock
    mock_transform = MagicMock()
    mock_from_crs.return_value = mock_transform
    mock_transform.transform.return_value = (xx, yy)

    result = mock_feature_extractor.get_latlons(mock_data_array)

    assert result.dims == ('bands', 'y', 'x')
    assert LAT_HARMONIZED_NAME in result.coords['bands'].values
    assert LON_HARMONIZED_NAME in result.coords['bands'].values
    mock_transform.transform.assert_called()

def test_rescale_s1_backscatter_valid(mock_feature_extractor, mock_data_array):
    s1_bands = ["S1-SIGMA0-VV", "S1-SIGMA0-VH"]
    data = np.random.randint(1, 65535, size=(len(s1_bands), 10, 10), dtype=np.uint16)
    mock_data_array = xr.DataArray(data, dims=["bands", "y", "x"], coords={"bands": s1_bands})

    result = mock_feature_extractor._rescale_s1_backscatter(mock_data_array)
    assert result.dtype == np.uint16



@patch.object(DummyPatchFeatureExtractor, '_common_preparations', return_value=xr.DataArray(np.random.rand(2, 20, 10, 10), dims=["bands", "t", "y", "x"]))
@patch.object(DummyPatchFeatureExtractor, '_rescale_s1_backscatter', return_value=xr.DataArray(np.random.rand(2, 10, 10), dims=["bands", "y", "x"]))
def test_execute(mock_common_preparations, mock_rescale_s1):
    # Create an instance of the extractor
    extractor = DummyPatchFeatureExtractor()
    extractor._parameters = {"rescale_s1": True}
    
    # Mock the cube
    mock_cube = MagicMock()
    mock_cube.get_array.return_value = xr.DataArray(np.random.rand(2, 20, 10, 10), dims=["bands", "t", "y", "x"])
    
    # Execute the method
    result = extractor._execute(mock_cube, {})
    
    # Ensure the result is correctly transposed to have dimensions ["bands", "y", "x"]
    expected_dims = ("bands", "y", "x")
    assert result.get_array().dims == expected_dims
    
    # Check that the mock methods were called
    mock_common_preparations.assert_called()
    mock_rescale_s1.assert_called()