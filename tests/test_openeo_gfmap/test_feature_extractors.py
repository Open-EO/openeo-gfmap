"""Test on feature extractors implementations, both local and remote."""

from pathlib import Path

import pytest
import xarray as xr

from openeo_gfmap import BoundingBoxExtent, FetchType, TemporalContext
from openeo_gfmap.backend import BACKEND_CONNECTIONS, Backend, BackendContext
from openeo_gfmap.features import (
    PatchFeatureExtractor,
    apply_feature_extractor,
    apply_feature_extractor_local,
)
from openeo_gfmap.fetching import (
    build_sentinel1_grd_extractor,
    build_sentinel2_l2a_extractor,
)
from openeo_gfmap.preprocessing.sar import compress_backscatter_uint16

SPATIAL_CONTEXT = BoundingBoxExtent(
    west=4.261,
    south=51.309,
    east=4.267,
    north=51.313,
    epsg=4326,
)
TEMPORAL_EXTENT = TemporalContext("2023-10-01", "2024-01-01")

backends = [Backend.CDSE]


class DummyPatchExtractor(PatchFeatureExtractor):
    def output_labels(self) -> list:
        return ["red", "green", "blue"]

    def execute(self, inarr: xr.DataArray):
        # Make the imports WITHIN the class
        import xarray as xr  # noqa: F401
        from scipy.ndimage import gaussian_filter

        # Performs some gaussian filtering to blur the RGB bands
        rgb_bands = inarr.sel(bands=["S2-L2A-B04", "S2-L2A-B03", "S2-L2A-B02"])

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


class DummyS1PassthroughExtractor(PatchFeatureExtractor):
    def output_labels(self) -> list:
        return ["S1-SIGMA0-VH", "S1-SIGMA0-VV"]

    def execute(self, inarr: xr.DataArray):
        return inarr.mean(dim="t")


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


@pytest.mark.parametrize("backend", backends)
def test_patch_feature_udf(backend: Backend):
    connection = BACKEND_CONNECTIONS[backend]()
    backend_context = BackendContext(backend=backend)

    output_path = Path(__file__).parent / f"results/patch_features_{backend.value}.nc/"

    bands_to_extract = ["S2-L2A-B04", "S2-L2A-B03", "S2-L2A-B02"]

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


@pytest.mark.parametrize("backend", backends)
def test_s1_rescale(backend: Backend):
    connection = BACKEND_CONNECTIONS[backend]()
    backend_context = BackendContext(backend=backend)
    output_path = (
        Path(__file__).parent / f"results/s1_rescaled_features_{backend.value}.nc"
    )

    REDUCED_TEMPORAL_CONTEXT = TemporalContext(
        start_date="2023-06-01", end_date="2023-06-30"
    )

    bands_to_extract = ["S1-SIGMA0-VH", "S1-SIGMA0-VV"]

    extractor = build_sentinel1_grd_extractor(
        backend_context, bands_to_extract, FetchType.TILE
    )

    cube = extractor.get_cube(connection, SPATIAL_CONTEXT, REDUCED_TEMPORAL_CONTEXT)

    cube = compress_backscatter_uint16(backend_context, cube)

    features = apply_feature_extractor(
        DummyS1PassthroughExtractor,
        cube,
        parameters={},
        size=[
            {"dimension": "x", "unit": "px", "value": 128},
            {"dimension": "y", "unit": "px", "value": 128},
        ],
    )

    job = features.create_job(title="s1_rescale_feature_extractor", out_format="NetCDF")
    job.start_and_wait()

    for asset in job.get_results().get_assets():
        if asset.metadata["type"].startswith("application/x-netcdf"):
            asset.download(output_path)
            break

    assert output_path.exists()


@pytest.mark.parametrize("backend", backends)
def test_latlon_extractor(backend: Backend):
    connection = BACKEND_CONNECTIONS[backend]()
    backend_context = BackendContext(backend=backend)
    output_path = Path(__file__).parent / f"results/latlon_features_{backend.value}.nc"

    REDUCED_TEMPORAL_CONTEXT = TemporalContext(
        start_date="2023-06-01", end_date="2023-06-30"
    )

    bands_to_extract = ["S2-L2A-B04"]

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

    inds = (
        xr.open_dataset(input_path)
        .to_array(dim="bands")
        .drop_sel(bands="crs")
        .transpose("bands", "t", "y", "x")
        .astype("uint16")
    )

    features = apply_feature_extractor_local(
        DummyPatchExtractor, inds, parameters={"GEO-EPSG": 32631}
    )

    features.to_netcdf(Path(__file__).parent / "results/patch_features_local.nc")

    assert set(features.bands.values) == set(["red", "green", "blue"])
