""" Tests for data extractors for Sentinel2 data. """
from pathlib import Path
from typing import Union

import geojson
import geopandas as gpd
import numpy as np
import openeo
import pytest
import rioxarray
import xarray as xr

from openeo_gfmap import SpatialContext, TemporalContext
from openeo_gfmap.backend import BACKEND_CONNECTIONS, Backend, BackendContext
from openeo_gfmap.fetching import (
    CollectionFetcher,
    FetchType,
    build_sentinel2_l2a_extractor,
)
from openeo_gfmap.utils import load_json

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

# Recent dates for second extent
TEMPORAL_EXTENT_2 = ["2023-01-01", "2023-02-01"]

# Dataset of polygons for POINT based extraction
POINT_EXTRACTION_DF = (
    Path(__file__).parent / "resources/malawi_extraction_polygons.gpkg"
)

# Datase of polygons for Polygon based extraction
POLYGON_EXTRACTION_DF = (
    Path(__file__).parent / "resources/puglia_extraction_polygons.gpkg"
)

test_backends = [Backend.TERRASCOPE, Backend.CDSE]

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
            context=context,
            bands=bands,
            fetch_type=FetchType.TILE,
            **fetching_parameters,
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

        def normalize_array(inarr: xr.DataArray) -> xr.DataArray:
            quantile_99 = inarr.quantile(0.99, dim=["x", "y", "t"])
            minimum = inarr.min(dim=["x", "y", "t"])
            inarr = (inarr - minimum) / (quantile_99 - minimum)
            # Clip exceeding values to 1
            return xr.where(inarr > 1.0, 1.0, inarr)

        def select_optical(inarr: xr.DataArray) -> xr.DataArray:
            return inarr.sel(
                bands=[
                    band
                    for band in inarr.coords["bands"].to_numpy()
                    if band.startswith("S2-B")
                ]
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

            # Compare the arrays on the optical values
            normalized_tiles = [
                normalize_array(select_optical(inarr.to_array(dim="bands")))
                for inarr in loaded_tiles
            ]
            first_tile = normalized_tiles[0]
            for tile_idx in range(1, len(normalized_tiles)):
                tile_to_compare = normalized_tiles[tile_idx]
                dot_product = np.sum(first_tile * tile_to_compare)
                first_norm = np.linalg.norm(first_tile)
                second_norm = np.linalg.norm(tile_to_compare)
                similarity_score = (dot_product / (first_norm * second_norm)).item()

                # Assert the similarity score
                print(similarity_score, tile_idx)
                assert similarity_score >= 0.95

    def sentinel2_l2a_point_based(
        spatial_context: SpatialContext,
        temporal_context: TemporalContext,
        backend: Backend,
        connection: openeo.Connection,
    ):
        """Test the point based extractions from the spatial aggregation of
        given polygons.
        """
        context = BackendContext(backend)
        bands = ["S2-B01", "S2-B04", "S2-B08", "S2-B11"]

        # Because it it tested in malawi, and this is the EPSG code for the
        # UTM projection for that zone
        fetching_parameters = {"target_crs": 32736}
        extractor = build_sentinel2_l2a_extractor(
            backend_context=context,
            bands=bands,
            fetch_type=FetchType.POINT,
            **fetching_parameters,
        )

        cube = extractor.get_cube(connection, spatial_context, temporal_context)

        cube = cube.aggregate_spatial(spatial_context, reducer="mean")

        output_file = (
            Path(__file__).parent / f"results/points_{backend.value}_sentinel2_l2a.json"
        )

        cube.download(output_file, format="JSON")

        # Load the results in to a dataframe
        df = load_json(output_file, bands)

        for band in bands:
            exists = False
            for col in df.columns:
                if band in col:
                    exists = True
            assert exists, f"Couldn't find a single column for band {band}"

        assert len(df.columns) % len(bands) == 0, (
            f"The number of columns ({len(df.columns)}) should be a "
            f"multiple of the number of bands ({len(bands)})"
        )

        df.to_parquet(str(output_file).repalce(".json", ".parquet"))

    def sentinel2_l2a_polygon_based(
        spatial_context: SpatialContext,
        temporal_context: TemporalContext,
        backend: Backend,
        connection: openeo.Connection,
    ):
        context = BackendContext(backend)
        bands = ["S2-B02", "S2-B03", "S2-B04"]

        fetching_parameters = {"target_crs": 3035}
        extractor = build_sentinel2_l2a_extractor(
            backend_context=context,
            bands=bands,
            fetch_type=FetchType.POLYGON,
            **fetching_parameters,
        )

        cube = extractor.get_cube(connection, spatial_context, temporal_context)

        output_folder = Path(__file__).parent / f"results/polygons_{backend.value}/"
        output_folder.mkdir(exist_ok=True, parents=True)

        job = cube.create_job(
            title="test_extract_polygons", out_format="NetCDF", sample_by_feature=True
        )

        job.start_and_wait()

        results = job.get_results()
        results.download_files(output_folder)

        # List all the files available in the folder
        extracted_files = list(
            filter(lambda file: file.suffix != ".json", output_folder.iterdir())
        )
        # Check if there is one file for each polygon
        assert len(extracted_files) == len(spatial_context["features"])


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


@pytest.fixture
def extraction_df() -> gpd.GeoDataFrame:
    return gpd.read_file(POINT_EXTRACTION_DF)


@pytest.mark.parametrize("backend", test_backends)
def test_sentinel2_l2a_point_based(backend: Backend):
    connection = BACKEND_CONNECTIONS[backend]()

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

    TestS2Extractors.sentinel2_l2a_point_based(
        spatial_context, temporal_context, backend, connection
    )


@pytest.mark.parametrize("backend", test_backends)
def test_sentinel2_l2a_polygon_based(backend: Backend):
    connection = BACKEND_CONNECTIONS[backend]()

    extraction_df = gpd.read_file(POLYGON_EXTRACTION_DF)

    geojson_features = extraction_df.geometry.__geo_interface__
    spatial_context = geojson.GeoJSON(
        {"type": "FeatureCollection", "features": geojson_features["features"]}
    )

    temporal_context = TemporalContext(
        start_date=extraction_df.iloc[0]["start_date"],
        end_date=extraction_df.iloc[0]["end_date"],
    )

    TestS2Extractors.sentinel2_l2a_polygon_based(
        spatial_context, temporal_context, backend, connection
    )
