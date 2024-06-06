from openeo_gfmap.features import PatchFeatureExtractor
import pytest
import numpy as np
import xarray as xr
from unittest.mock import MagicMock, patch

# Constants for test
LAT_HARMONIZED_NAME = "lat"
LON_HARMONIZED_NAME = "lon"
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
    mock_feature_extractor.epsg = None
    with pytest.raises(Exception):
        mock_feature_extractor.get_latlons(mock_data_array)

def test_get_latlons_epsg_4326(mock_feature_extractor, mock_data_array):
    mock_feature_extractor.epsg = 4326
    result = mock_feature_extractor.get_latlons(mock_data_array)
    assert LAT_HARMONIZED_NAME in result.coords['bands'].values
    assert LON_HARMONIZED_NAME in result.coords['bands'].values

@patch('pyproj.Transformer')
def test_get_latlons_reproject(mock_transformer, mock_feature_extractor, mock_data_array):
    mock_feature_extractor.epsg = 3857
    mock_transform = MagicMock()
    mock_transformer.from_crs.return_value = mock_transform
    mock_transform.transform.return_value = (np.zeros_like(mock_data_array.coords['x']), np.zeros_like(mock_data_array.coords['y']))

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

@patch.object(PatchFeatureExtractor, '_common_preparations', return_value=mock_data_array)
@patch.object(PatchFeatureExtractor, '_rescale_s1_backscatter', return_value=mock_data_array)
def test_execute(mock_common_preparations, mock_rescale_s1, mock_feature_extractor):
    mock_feature_extractor._parameters = {"rescale_s1": True}
    mock_cube = MagicMock()
    mock_cube.get_array.return_value = mock_data_array

    result = mock_feature_extractor._execute(mock_cube, {})
    assert result.get_array().dims == mock_data_array.dims
    mock_common_preparations.assert_called()
    mock_rescale_s1.assert_called()