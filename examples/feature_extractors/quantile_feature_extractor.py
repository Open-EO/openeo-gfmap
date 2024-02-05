"""This file provides the example of a basic feature extractor that computes
indices from preprocessed data and extracts quantiles from those indices.

Implementing a function in a source file, and then calling the
`apply_feature_extractor` in OpenEO should be enoug to run the inference.
"""
import openeo
import xarray as xr

# Spatial and temporal definitions
from openeo_gfmap import BoundingBoxExtent, TemporalContext

# Backend and context
from openeo_gfmap.backend import Backend, BackendContext
from openeo_gfmap.features import PatchFeatureExtractor
from openeo_gfmap.features.feature_extractor import apply_feature_extractor

# Fetching type (either TILE, POLYGON or POINT)
from openeo_gfmap.fetching import FetchType, build_sentinel2_l2a_extractor

# Preprocessing options
from openeo_gfmap.preprocessing import (
    linear_interpolation,
    mask_scl_dilation,
    median_compositing,
)


class QuantileIndicesExtractor(PatchFeatureExtractor):
    """Performs feature extraction by returning qunatile indices of the input
    array."""

    def output_labels(self) -> list:
        indices_names = ["NDVI", "NDWI", "NDMI", "NDRE", "NDRE5", "B11", "B12"]
        quantile_values = ["10", "50", "90", "IQR"]
        quantile_names = [
            index_name + ":" + quantile_value
            for index_name in indices_names
            for quantile_value in quantile_values
        ]
        return quantile_names

    def execute(self, inarr: xr.DataArray) -> xr.DataArray:
        # compute indices
        b03 = inarr.sel(bands="S2-B03")
        b04 = inarr.sel(bands="S2-B04")
        b05 = inarr.sel(bands="S2-B05")
        b06 = inarr.sel(bands="S2-B06")
        b08 = inarr.sel(bands="S2-B08")
        b11 = inarr.sel(bands="S2-B11")
        b12 = inarr.sel(bands="S2-B12")

        ndvi = (b08 - b04) / (b08 + b04)
        ndwi = (b03 - b08) / (b03 + b08)
        ndmi = (b08 - b11) / (b08 + b11)
        ndre = (b05 - b08) / (b05 + b08)
        ndre5 = (b06 - b08) / (b06 + b08)

        indices = [ndvi, ndwi, ndmi, ndre, ndre5, b11, b12]

        quantile_arrays = []

        for data in indices:
            q01 = data.quantile(0.1, dim=["t"]).drop("quantile")
            q05 = data.quantile(0.5, dim=["t"]).drop("quantile")
            q09 = data.quantile(0.9, dim=["t"]).drop("quantile")
            iqr = q09 - q01

            quantile_arrays.extend([q01, q05, q09, iqr])

        # Pack the quantile arrays into a single array
        quantile_array = (
            xr.concat(quantile_arrays, dim="bands")
            .assign_coords({"bands": self.output_labels()})
            .transpose("bands", "y", "x")
        )

        return quantile_array


if __name__ == "__main__":
    connection = openeo.connect("https://openeo.vito.be").authenticate_oidc()

    # Define your spatial and temporal context
    bbox_extent = BoundingBoxExtent(
        west=4.515859656828771,
        south=50.81721602547749,
        east=4.541689831106636,
        north=50.83654859110982,
        epsg=4326,
    )

    # Define your temporal context, summer 2022
    temporal_extent = TemporalContext(start_date="2022-06-21", end_date="2022-09-23")

    # Define your backend context
    backend_context = BackendContext(backend=Backend.TERRASCOPE)

    # Prepare your S2_L2A extractor

    # The bands that you can extract are defined in the code openeo_gfmap.fetching.s2.BASE_SENTINEL2_L2A_MAPPING
    bands = [
        "S2-B03",
        "S2-B04",
        "S2-B05",
        "S2-B06",
        "S2-B08",
        "S2-B11",
        "S2-B12",
        "S2-SCL",
    ]

    # Use the base feching
    fetching_parameters = {}
    fetcher = build_sentinel2_l2a_extractor(
        backend_context, bands, fetch_type=FetchType.TILE, **fetching_parameters
    )

    cube = fetcher.get_cube(
        connection, spatial_context=bbox_extent, temporal_context=temporal_extent
    )

    # Perform pre-processing, compositing & linear interpolation
    cube = mask_scl_dilation(cube)
    cube = median_compositing(cube, period="dekad")
    cube = linear_interpolation(cube)

    # Apply the feature extractor UDF
    features = apply_feature_extractor(
        QuantileIndicesExtractor,
        cube,
        parameters={},  # No additional parameter required by your UDF
        size=[
            {"dimension": "x", "unit": "px", "value": 128},
            {"dimension": "y", "unit": "px", "value": 128},
        ],
    )

    # Start the job
    job = features.create_job(
        title="Quantile indices extraction - Tervuren Park", out_format="NetCDF"
    )

    job.start_and_wait()

    # Download the results
    for asset in job.get_results().get_assets():
        if asset.metadata["type"].startswith("application/x-netcdf"):
            asset.download("/data/users/Public/couchard/test_features.nc")
