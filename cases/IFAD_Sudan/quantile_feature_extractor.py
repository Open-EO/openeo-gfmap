"""This file provides the example of a basic feature extractor that computes
indices from preprocessed data and extracts quantiles from those indices.

Implementing a function in a source file, and then calling the
`apply_feature_extractor` in OpenEO should be enoug to run the inference.
"""
import openeo
import xarray as xr

from openeo_gfmap.features import PatchFeatureExtractor, PointFeatureExtractor
from openeo_gfmap.features.feature_extractor import apply_feature_extractor_local


class QuantileIndicesExtrctor(PatchFeatureExtractor):
    """Performs feature extraction by returning qunatile indices of the input array."""

    def _import_dependencies(self):
        import xarray as xr
        pass  # No additional dependencies other than xarray and numpy (imported by default)

    def execute(self, inarr: xr.DataArray) -> xr.DataArray:
        # compute indices
        b03 = inarr.sel(bands='S2-B03')
        b04 = inarr.sel(bands='S2-B04')
        b05 = inarr.sel(bands='S2-B05')
        b06 = inarr.sel(bands='S2-B06')
        b08 = inarr.sel(bands='S2-B08')
        b11 = inarr.sel(bands='S2-B11')
        b12 = inarr.sel(bands='S2-B12')
        
        ndvi = (b08 - b04) / (b08 + b04)
        ndwi = (b03 - b08) / (b03 + b08)
        ndmi = (b08 - b11) / (b08 + b11)
        ndre = (b05 - b08) / (b05 + b08)
        ndre5 = (b06 - b08) / (b06 + b08)

        indices_names = ['NDVI', 'NDWI', 'NDMI', 'NDRE', 'NDRE5', 'B11', 'B12']
        indices = [ndvi, ndwi, ndmi, ndre, ndre5, b11, b12]

        quantile_arrays = []
        quantile_names = []

        for index_name, data in zip(indices_names, indices):
            q01 = data.quantile(0.1, dim=['t']).drop('quantile')
            q05 = data.quantile(0.5, dim=['t']).drop('quantile')
            q09 = data.quantile(0.9, dim=['t']).drop('quantile')
            iqr = q09 - q01

            quantile_arrays.extend([q01, q05, q09, iqr])
            quantile_names.extend([
                index_name + ':10',
                index_name + ':50',
                index_name + ':90',
                index_name + ':IQR'
            ])
        
        # Pack the quantile arrays into a single array
        quantile_array = xr.concat(
            quantile_arrays, dim='features'
        ).assign_coords({
            'features': quantile_names
        }).transpose('features', 'y', 'x')

        return quantile_array


if __name__ == '__main__':
    from pathlib import Path
    # Loads the raw data
    raw_tile = Path(
        '/data/sigma/GDA/IFAD_Sudan/inference_data/2023.nc'
    )

    inds = xr.open_dataset(
        raw_tile, chunks={'x': 128, 'y': 128, 't': 1}
    )

    selected_bands = [
        band for band in inds.keys() if band != 'crs'
    ]

    inds = inds.isel(
        x=(slice(0, 128)), y=(slice(0, 128))
    )[selected_bands].to_array(dim='bands').compute()

    # Applies the UDF locally
    features = apply_feature_extractor_local(
        QuantileIndicesExtrctor,
        inds,
        parameters={}
    )

    if isinstance(features, openeo.udf.XarrayDataCube):
        features = features.to_array().transpose('features', 'y', 'x')
    else:
        features = features.transpose('features', 'y', 'x')

    # Saves the features
    features.to_netcdf(
        '/data/users/Public/couchard/test_features.nc'
    )
