""" Spatial and temporal extents that are used by the test on fetchers. """
from pathlib import Path

import pytest
import geojson
import geopandas as gpd

from openeo_gfmap.backend import Backend, BackendContext
from openeo_gfmap import TemporalContext, SpatialContext, BoundingBoxExtent


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

# Scene in Ilha Grande, Brazil
SPATIAL_EXTENT_2 = {
    "west": -44.13569753706443,
    "south": -23.174673799522953,
    "east": -44.12093529131243,
    "north": -23.16291590441855,
    "crs": "EPSG:4326",
    "country": "Brazil",  # Metadata useful for test suite
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


@pytest.fixture(params=[Backend.TERRASCOPE, Backend.CDSE])
def test_backend_contexts(backend: Backend) -> BackendContext:
    """Backends to test."""
    return BackendContext(backend)


@pytest.fixture(
    params=[
        (SPATIAL_EXTENT_1, TEMPORAL_EXTENT_1),
        (SPATIAL_EXTENT_2, TEMPORAL_EXTENT_2)
    ]
)
def test_spatiotemporal_contexts(
    spatial_context: dict,
    temporal_context: list
) -> tuple[SpatialContext, TemporalContext]:
    """Spatial and temporal context for tile fetching."""
    bbox_extent = BoundingBoxExtent(
        **spatial_context
    )
    temporal_extent = TemporalContext(
        start_date=temporal_context[0], end_date=temporal_context[1]
    )
    return bbox_extent, temporal_extent


@pytest.fixture(scope="session")
def test_point_contexts() -> tuple[SpatialContext, TemporalContext]:
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

    return spatial_context, temporal_context


@pytest.fixture(scope="session")
def test_polygon_contexts() -> tuple[SpatialContext, TemporalContext]:
    extraction_df = gpd.read_file(POLYGON_EXTRACTION_DF)

    geojson_features = extraction_df.geometry.__geo_interface__
    spatial_context = geojson.GeoJSON(
        {"type": "FeatureCollection", "features": geojson_features["features"]}
    )

    temporal_context = TemporalContext(
        start_date=extraction_df.iloc[0]["start_date"],
        end_date=extraction_df.iloc[0]["end_date"],
    )

    return spatial_context, temporal_context
