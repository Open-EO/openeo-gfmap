"""Test the job splitters and managers of OpenEO GFMAP."""

import geopandas as gpd
from shapely.geometry import Point, Polygon

from openeo_gfmap.manager.job_splitters import (
    split_job_hex,
    split_job_s2grid,
    split_job_s2sphere,
)


def test_split_job_s2grid():
    # Create a mock GeoDataFrame with points
    # The points are located in two different S2 tiles
    data = {
        "id": [1, 2, 3, 4, 5],
        "geometry": [
            Point(60.02, 4.57),
            Point(59.6, 5.04),
            Point(59.92, 3.37),
            Point(59.07, 4.11),
            Point(58.77, 4.87),
        ],
    }
    polygons = gpd.GeoDataFrame(data, crs="EPSG:4326")

    # Define expected number of split groups
    max_points = 2

    # Call the function
    result = split_job_s2grid(polygons, max_points)

    assert (
        len(result) == 3
    ), "The number of GeoDataFrames returned should match the number of splits needed."

    # Check if the geometries are preserved
    for gdf in result:
        assert (
            "geometry" in gdf.columns
        ), "Each GeoDataFrame should have a geometry column."
        assert gdf.crs == 4326, "The original CRS should be preserved."
        assert all(
            gdf.geometry.geom_type == "Point"
        ), "Original geometries should be preserved."


def test_split_job_hex():
    # Create a mock GeoDataFrame with points
    # The points/polygons are located in three different h3 hexes of size 3
    data = {
        "id": [1, 2, 3, 4, 5, 6],
        "geometry": [
            Point(60.02, 4.57),
            Point(58.34, 5.06),
            Point(59.92, 3.37),
            Point(58.85, 4.90),
            Point(58.77, 4.87),
            Polygon(
                [
                    (58.78, 4.88),
                    (58.78, 4.86),
                    (58.76, 4.86),
                    (58.76, 4.88),
                    (58.78, 4.88),
                ]
            ),
        ],
    }
    polygons = gpd.GeoDataFrame(data, crs="EPSG:4326")

    max_points = 3

    result = split_job_hex(polygons, max_points)

    assert (
        len(result) == 4
    ), "The number of GeoDataFrames returned should match the number of splits needed."

    for idx, gdf in enumerate(result):
        assert (
            "geometry" in gdf.columns
        ), "Each GeoDataFrame should have a geometry column."
        assert gdf.crs == 4326, "The CRS should be preserved."
        if idx == 1:
            assert all(
                gdf.geometry.geom_type == "Polygon"
            ), "Original geometries should be preserved."
        else:
            assert all(
                gdf.geometry.geom_type == "Point"
            ), "Original geometries should be preserved."

        assert (
            len(result[0]) == 3
        ), "The number of geometries in the first split should be 3."


def test_split_job_s2sphere():
    # Create a mock GeoDataFrame with points
    # The points are located in two different S2 tiles
    data = {
        "id": [1, 2, 3, 4, 5, 6, 7],
        "geometry": [
            Point(60.02, 4.57),
            Point(58.34, 5.06),
            Point(59.92, 3.37),
            Point(59.93, 3.37),
            Point(58.85, 4.90),
            Point(58.77, 4.87),
            Polygon(
                [
                    (58.78, 4.88),
                    (58.78, 4.86),
                    (58.76, 4.86),
                    (58.76, 4.88),
                    (58.78, 4.88),
                ]
            ),
        ],
    }
    polygons = gpd.GeoDataFrame(data, crs="EPSG:4326")

    # Define expected number of split groups
    max_points = 3

    # Call the function
    result = split_job_s2sphere(polygons, max_points, start_level=8)

    assert (
        len(result) == 4
    ), "The number of GeoDataFrames returned should match the number of splits needed."

    # Check if the geometries are preserved
    for gdf in result:
        assert (
            "geometry" in gdf.columns
        ), "Each GeoDataFrame should have a geometry column."
        assert gdf.crs == 4326, "The original CRS should be preserved."
        for _, geom in gdf.iterrows():
            geom_type = geom.geometry.geom_type
            original_type = polygons[polygons.id == geom.id].geometry.geom_type.values[
                0
            ]
            assert (
                geom_type == original_type
            ), "Original geometries should be preserved."
