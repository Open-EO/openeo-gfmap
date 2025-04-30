""" Test the data extractors for Sentinel1 data. """

import os
from pathlib import Path

import geojson
import geopandas as gpd
import openeo
import pytest
import rioxarray
import xarray as xr

# TODO to centralize
# Retrieve the test parameters from the s2 fetcher tests
from test_s2_fetchers import POINT_EXTRACTION_DF, test_backends, test_configurations

from openeo_gfmap import SpatialContext, TemporalContext
from openeo_gfmap.backend import Backend, get_connection
from openeo_gfmap.fetching import (
    CollectionFetcher,
    FetchType,
    build_sentinel1_grd_extractor,
)
from openeo_gfmap.preprocessing.sar import compress_backscatter_uint16
from openeo_gfmap.utils import (
    array_bounds,
    arrays_cosine_similarity,
    normalize_array,
    select_sar_bands,
)


# integration test checks if the output S1 cube has the correct band names;
class TestS1Extractors:
    """Build collection extractor for different S1 collections on different
    backends.
    """

    def sentinel1_grd(
        spatial_extent: SpatialContext,
        temporal_extent: TemporalContext,
        backend: Backend,
        connection=openeo.Connection,
    ):
        bands = ["S1-SIGMA0-VV", "S1-SIGMA0-VH"]
        expected_harmonized_bands = ["S1-SIGMA0-VV", "S1-SIGMA0-VH"]

        fetching_parameters = {
            "target_resolution": 10.0,
            "elevation_model": "COPERNICUS_30",
            "coefficient": "gamma0-ellipsoid",
            "load_collection": {
                "polarization": lambda polar: polar == "VV&VH",
            },
        }

        extractor: CollectionFetcher = build_sentinel1_grd_extractor(
            backend, bands, FetchType.TILE, **fetching_parameters
        )

        temporal_extent = TemporalContext(
            start_date=temporal_extent[0], end_date=temporal_extent[1]
        )

        cube = extractor.get_cube(connection, spatial_extent, temporal_extent)
        cube = compress_backscatter_uint16(cube)

        output_file = (
            Path(__file__).parent.parent / f"results/{backend.value}_sentinel1_grd.nc"
        )

        job = cube.create_job(
            title="Sentinel1 GRD tile extraction", out_format="NetCDF"
        )

        job.start_and_wait()

        assets = job.get_results().get_assets()

        for asset in assets:
            if asset.metadata["type"].startswith("application/x-netcdf"):
                asset.download(output_file)
                break

        results = rioxarray.open_rasterio(output_file)

        # Load the job results
        assert results.rio.crs is not None

        for harmonierd_name in expected_harmonized_bands:
            assert harmonierd_name in results.keys()

    # TODO; convoluted comparisson; we can use a utility function which calculates a
    # statistic for every band, better to make use of pytest.approx
    def compare_sentinel1_tiles():
        """Compare the different tiles gathered from different backends,
        they should be similar, if they are computed with the same
        backscatter algorithm."""

        backend_types = set([conf[2] for conf in test_configurations])
        loaded_tiles = []
        for backend in backend_types:
            tile_path = (
                Path(__file__).parent.parent
                / f"results/{backend.value}_sentinel1_grd.nc"
            )
            loaded_tiles.append(xr.open_dataset(tile_path))

        # Compare the variable data type
        dtype = None
        for tile in loaded_tiles:
            for key in tile.keys():
                if key == "crs":
                    continue
            array = tile[key]
            if dtype is None:
                dtype = array.dtype
            else:
                assert dtype == array.dtype

        bounds = None
        for tile in loaded_tiles:
            tile_bounds = array_bounds(tile)
            if bounds is None:
                bounds = tile_bounds
            else:
                assert tile_bounds == bounds

        normalized_tiles = [
            normalize_array(select_sar_bands(inarr.to_array(dim="bands")))
            for inarr in loaded_tiles
        ]
        first_tile = normalized_tiles[0]
        for tile_idx in range(1, len(normalized_tiles)):
            tile_to_compare = normalized_tiles[tile_idx]
            similarity_score = arrays_cosine_similarity(first_tile, tile_to_compare)
            assert similarity_score >= 0.95

    # TODO integration test
    def sentinel1_grd_point_based(
        spatial_context: SpatialContext,
        temporal_context: TemporalContext,
        backend: Backend,
        connection: openeo.Connection,
    ):
        """Test the point based extraction from the spatial aggregation of the
        given polygons.
        """
        bands = ["S1-SIGMA0-VV", "S1-SIGMA0-VH"]

        # Because it is tested in malawi, and this is the EPSG code for
        # the UTM projection in that zone
        fetching_parameters = {
            "target_crs": 32736,
            "target_resolution": 10.0,
            "elevation_model": "COPERNICUS_30",
            "coefficient": "gamma0-ellipsoid",
            "load_collection": {
                "polarization": lambda polar: polar == "VV&VH",
            },
        }
        extractor = build_sentinel1_grd_extractor(
            backend=backend,
            bands=bands,
            fetch_type=FetchType.POINT,
            **fetching_parameters,
        )

        cube = extractor.get_cube(connection, spatial_context, temporal_context)
        cube = compress_backscatter_uint16(cube)

        cube = cube.aggregate_spatial(spatial_context, reducer="mean")

        output_file = (
            Path(__file__).parent.parent
            / f"results/points_{backend.value}_sentinel1_grd.parquet"
        )

        job = cube.execute_batch(
            out_format="Parquet",
            title="test_extract_points_s1",
        )
        job.get_results().download_file(target=output_file, name="timeseries.parquet")

        df = gpd.read_parquet(output_file)

        for band in bands:
            exists = False
            for col in df.columns:
                if band in col:
                    exists = True
            assert exists, f"Couldn't find a single column for band {band}"

        # TODO: compare against reference df?

    # TODO integration test
    def sentinel1_grd_polygon_based(
        spatial_context: SpatialContext,
        temporal_context: TemporalContext,
        backend: Backend,
        connection: openeo.Connection,
    ):
        bands = ["S1-SIGMA0-VV", "S1-SIGMA0-VH"]

        fetching_parameters = {
            "target_crs": 3035,  # Location in Europe
            "target_resolution": 10.0,
            "elevation_model": "COPERNICUS_30",
            "coefficient": "gamma0-ellipsoid",
            "load_collection": {
                "polarization": lambda polar: (polar == "VV") or (polar == "VH"),
            },
        }

        extractor = build_sentinel1_grd_extractor(
            backend=backend,
            bands=bands,
            fetch_type=FetchType.POLYGON,
            **fetching_parameters,
        )

        cube = extractor.get_cube(connection, spatial_context, temporal_context)
        cube = compress_backscatter_uint16(cube)

        output_folder = (
            Path(__file__).parent.parent / f"results/polygons_s1_{backend.value}/"
        )
        output_folder.mkdir(exist_ok=True, parents=True)

        job = cube.create_job(
            title="test_extract_polygons_s1",
            out_format="NetCDF",
            sample_by_feature=True,
        )

        job.start_and_wait()

        results = job.get_results()
        results.download_files(output_folder)

        # List all the files available in the folder
        extracted_files = list(
            filter(lambda file: file.suffix == ".nc", output_folder.iterdir())
        )
        # Check if there is one file for each polygon
        assert len(extracted_files) == len(spatial_context["features"])


@pytest.mark.parametrize(
    "spatial_context, temporal_context, backend", test_configurations
)
@pytest.mark.skipif(
    os.environ.get("SKIP_INTEGRATION_TESTS") == "1", reason="Skip integration tests"
)
def test_sentinel1_grd(
    spatial_context: SpatialContext, temporal_context: TemporalContext, backend: Backend
):
    connection = get_connection(backend=backend)
    TestS1Extractors.sentinel1_grd(
        spatial_context, temporal_context, backend, connection
    )


@pytest.mark.skipif(
    os.environ.get("SKIP_INTEGRATION_TESTS") == "1", reason="Skip integration tests"
)
@pytest.mark.depends(on=["test_sentinel1_grd"])
def test_compare_sentinel1_tiles():
    TestS1Extractors.compare_sentinel1_tiles()


@pytest.mark.skipif(
    os.environ.get("SKIP_INTEGRATION_TESTS") == "1", reason="Skip integration tests"
)
@pytest.mark.parametrize("backend", test_backends)
def test_sentinel1_grd_point_based(backend: Backend):
    connection = get_connection(backend=backend)

    extraction_df = gpd.read_file(POINT_EXTRACTION_DF)

    # Convert GeoDataFrame to feature collection to build spatial context
    geojson_features = extraction_df.geometry.__geo_interface__
    spatial_context = geojson.GeoJSON(
        {"type": "FeatureCollection", "features": geojson_features["features"]}
    )

    # Build the temporal context
    temporal_context = TemporalContext(
        start_date=extraction_df.iloc[0]["start_date"],
        end_date=extraction_df.iloc[0]["end_date"],
    )

    TestS1Extractors.sentinel1_grd_point_based(
        spatial_context, temporal_context, backend, connection
    )
