""" Tests for data extractors for Sentinel2 data. """
from pathlib import Path
from typing import Union

import openeo
import pytest
import rioxarray
import xarray as xr

from openeo_gfmap import SpatialContext, TemporalContext
from openeo_gfmap.backend import BACKEND_CONNECTIONS, Backend, BackendContext
from openeo_gfmap.fetching import CollectionFetcher, build_sentinel2_l2a_extractor

# Fields close to TAP, Belgium
SPATIAL_EXTENT_1 = {
    "west": 5.0516,
    "south": 51.18682,
    "east": 5.08832,
    "north": 51.20527,
    "crs": "EPSG:4326",
    "country": "Belgium",  # Metadata useful for test suite
}

# Recent dates for first extent
TEMPORAL_EXTENT_1 = ["2023-04-01", "2023-05-01"]

# Scene in Argentina
SPATIAL_EXTENT_2 = {
    "west": -65.15344587627536,
    "south": -26.844952920846367,
    "east": -65.09740328344284,
    "north": -26.81363402938031,
    "crs": "EPSG:4326",
    "country": "Argentina",  # Metadata useful for test suite
}

TEMPORAL_EXTENT_2 = ["2023-01-01", "2023-02-01"]

test_backends = [Backend.TERRASCOPE, Backend.CDSE, Backend.EODC]

test_spatio_temporal_extends = [
    (SPATIAL_EXTENT_1, TEMPORAL_EXTENT_1),
    (SPATIAL_EXTENT_2, TEMPORAL_EXTENT_2),
]

test_configurations = [
    (*spatio_temp, backend)
    for spatio_temp in test_spatio_temporal_extends
    for backend in test_backends
]


class TestS2Extractors:
    """Build collection extractor for different S2 collections on different
    backends.
    """

    def sentinel2_l2a(
        spatial_extent: SpatialContext,
        temporal_extent: TemporalContext,
        backend: Backend,
        connection: openeo.Connection,
    ):
        """For a given backend"""
        context = BackendContext(backend)
        # Fetch a variety of spatial resolution and metadata from different
        # providers.
        bands = ["S2-B01", "S2-B04", "S2-B08", "S2-B11", "S2-SCL", "S2-AOT"]
        expected_harmonized_names = [
            "S2-B01",
            "S2-B04",
            "S2-B08",
            "S2-B11",
            "S2-SCL",
            "S2-AOT",
        ]
        fetching_parameters = {"target_crs": 3035}
        extractor: CollectionFetcher = build_sentinel2_l2a_extractor(
            context, bands, **fetching_parameters
        )

        cube = extractor.get_cube(connection, spatial_extent, temporal_extent)

        country = spatial_extent["country"]

        output_file = (
            Path(__file__).parent
            / f"results/{country}_{backend.value}_sentinel2_l2a.nc"
        )

        cube.download(output_file, format="NetCDF")

        # Load the job results
        results = rioxarray.open_rasterio(output_file)

        # Assert the bands are renamed
        assert results.rio.crs is not None  # TODO Check the CRS (not EPSG)

        for harmonized_name in expected_harmonized_names:
            assert harmonized_name in results.keys()

    def compare_sentinel2_tiles():
        """Compare the different tiels gathered from the different backends,
        they should be similar.
        """

        def xarray_bounds(inarr: Union[xr.Dataset, xr.DataArray]) -> tuple:
            return (
                min(inarr.coords["x"]).item(),
                min(inarr.coords["y"]).item(),
                max(inarr.coords["x"]).item(),
                max(inarr.coords["y"]).item(),
            )

        backend_types = set([conf[2] for conf in test_configurations])
        countries = set([conf[0]["country"] for conf in test_configurations])
        for country in countries:
            loaded_tiles = []
            for backend in backend_types:
                if backend == Backend.EODC:  # TODO fix EDOC backend first
                    continue
                tile_path = (
                    Path(__file__).parent
                    / f"results/{country}_{backend.value}_sentinel2_l2a.nc"
                )
                loaded_tiles.append(xr.open_dataset(tile_path, engine="h5netcdf"))

            # Compare the tile variable types all togheter
            dtype = None
            for tile in loaded_tiles:
                for key in tile.keys():
                    if key == "crs":
                        continue  # Skip CRS array
                    array = tile[key]
                    if dtype is None:
                        dtype = array.dtype
                    else:
                        assert dtype == array.dtype

            # Compare the coordiantes of all the tiles and check if it matches
            bounds = None
            for tile in loaded_tiles:
                tile_bounds = xarray_bounds(tile)
                if bounds is None:
                    bounds = tile_bounds
                else:
                    assert tile_bounds == bounds

            # Compare the similarity score of the arrays
            # TODO implement Mean Squarred Error or other metric to evaluate
            # the similarity between the arrays


@pytest.mark.parametrize(
    "spatial_context, temporal_context, backend", test_configurations
)
def test_sentinel2_l2a(
    spatial_context: SpatialContext, temporal_context: TemporalContext, backend: Backend
):
    connection = BACKEND_CONNECTIONS[backend]()
    TestS2Extractors.sentinel2_l2a(
        spatial_context, temporal_context, backend, connection
    )


@pytest.mark.depends(on=["test_sentinel2_l2a"])
def test_compare_sentinel2_tiles():
    TestS2Extractors.compare_sentinel2_tiles()
