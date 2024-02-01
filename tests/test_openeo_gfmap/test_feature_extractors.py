"""Test on feature extractors implementations, both local and remote."""
from pathlib import Path
from typing import Callable

import pytest
import xarray as xr

from openeo_gfmap import BoundingBoxExtent, FetchType, TemporalContext
from openeo_gfmap.backend import (
    BACKEND_CONNECTIONS,
    Backend,
    BackendContext,
    vito_connection,
)
from openeo_gfmap.features import (
    PatchFeatureExtractor,
    apply_feature_extractor,
    apply_feature_extractor_local,
)
from openeo_gfmap.fetching import build_sentinel2_l2a_extractor

SPATIAL_CONTEXT = BoundingBoxExtent(
    west=4.260981398605067,
    south=51.30876935808223,
    east=4.267355900120883,
    north=51.313279365173884,
    epsg=4326,
)
TEMPORAL_EXTENT = TemporalContext("2023-10-01", "2024-01-01")


class DummyPatchExtractor(PatchFeatureExtractor):
    def output_labels(self) -> list:
        return ["red", "green", "blue"]

    def execute(self, inarr: xr.DataArray):
        # Make the imports WITHIN the class
        import xarray as xr
        from scipy.ndimage import gaussian_filter

        # Performs some gaussian filtering to blur the RGB bands
        rgb_bands = inarr.sel(bands=["S2-B04", "S2-B03", "S2-B02"])

        for band in rgb_bands.bands:
            for timestamp in rgb_bands.t:
                rgb_bands.loc[{"bands": band, "t": timestamp}] = gaussian_filter(
                    rgb_bands.loc[{"bands": band, "t": timestamp}], sigma=1.0
                )

        # Compute the median on the time band
        rgb_bands = rgb_bands.median(dim="t").assign_coords(
            {"bands": ["red", "green", "blue"]}
        )

        # Returns the rgb bands only in the feature, y, x order
        return rgb_bands.transpose("bands", "y", "x")


class LatLonExtractor(PatchFeatureExtractor):
    """Sample extractor that compute the latitude and longitude values
    and concatenates them in a new array.
    """

    def output_labels(self) -> list:
        return ["red", "lat", "lon"]

    def execute(self, inarr: xr.DataArray) -> xr.DataArray:
        # Compute the latitude and longitude as bands in the input array
        latlon = self.get_latlons(inarr)

        # Only select the first time for the input array
        inarr = inarr.isel(t=0)

        # Add the bands in the input array
        inarr = xr.concat([inarr, latlon], dim="bands").assign_coords(
            {"bands": ["red", "lat", "lon"]}
        )

        return inarr.transpose("bands", "y", "x")


# TODO remove
BACKEND_CONNECTIONS = {Backend.TERRASCOPE: vito_connection}


@pytest.mark.parametrize("backend, connection_fn", BACKEND_CONNECTIONS.items())
def test_patch_feature_udf(backend: Backend, connection_fn: Callable):
    backend_context = BackendContext(backend=backend)
    connection = connection_fn()
    output_path = Path(__file__).parent / f"results/patch_features_{backend.value}.nc/"

    bands_to_extract = ["S2-B04", "S2-B03", "S2-B02"]

    # Setup the RGB cube extraction
    extractor = build_sentinel2_l2a_extractor(
        backend_context, bands_to_extract, FetchType.TILE
    )

    rgb_cube = extractor.get_cube(connection, SPATIAL_CONTEXT, TEMPORAL_EXTENT)

    # Run the feature extractor
    features = apply_feature_extractor(
        DummyPatchExtractor,
        rgb_cube,
        parameters={},
        size=[
            {"dimension": "x", "unit": "px", "value": 128},
            {"dimension": "y", "unit": "px", "value": 128},
        ],
    )

    job = features.create_job(title="patch_feature_extractor", out_format="NetCDF")
    job.start_and_wait()

    for asset in job.get_results().get_assets():
        if asset.metadata["type"].startswith("application/x-netcdf"):
            asset.download(output_path)
            break

    assert output_path.exists()

    # Read the output path and checks for the expected band names
    output_cube = xr.open_dataset(output_path)

    assert set(output_cube.keys()) == set(["red", "green", "blue", "crs"])


@pytest.mark.parametrize("backend, connection_fn", BACKEND_CONNECTIONS.items())
def test_latlon_extractor(backend: Backend, connection_fn: Callable):
    backend_context = BackendContext(backend=backend)
    connection = connection_fn()
    output_path = Path(__file__).parent / f"results/latlon_features_{backend.value}.nc"

    REDUCED_TEMPORAL_CONTEXT = TemporalContext(
        start_date="2023-06-01", end_date="2023-06-30"
    )

    bands_to_extract = ["S2-B04"]

    extractor = build_sentinel2_l2a_extractor(
        backend_context, bands_to_extract, FetchType.TILE
    )

    cube = extractor.get_cube(connection, SPATIAL_CONTEXT, REDUCED_TEMPORAL_CONTEXT)

    features = apply_feature_extractor(
        LatLonExtractor,
        cube,
        parameters={},
        size=[
            {"dimension": "x", "unit": "px", "value": 128},
            {"dimension": "y", "unit": "px", "value": 128},
        ],
    )

    job = features.create_job(title="latlon_feature_extractor", out_format="NetCDF")
    job.start_and_wait()

    for asset in job.get_results().get_assets():
        if asset.metadata["type"].startswith("application/x-netcdf"):
            asset.download(output_path)
            break

    assert output_path.exists()

    # Read the output path and checks for the expected band names
    output_cube = xr.open_dataset(output_path)

    assert set(output_cube.keys()) == set(["red", "lat", "lon", "crs"])


def test_patch_feature_local():
    input_path = Path(__file__).parent / "resources/test_optical_cube.nc"

    inds = xr.open_dataset(input_path).to_array(dim="bands")

    inds = inds.sel(
        bands=[band for band in inds.bands.to_numpy() if band != "crs"]
    ).transpose("bands", "t", "y", "x")

    features = apply_feature_extractor_local(DummyPatchExtractor, inds, parameters={})

    features.to_netcdf(Path(__file__).parent / "results/patch_features_local.nc")

    assert set(features.bands.values) == set(["red", "green", "blue"])
