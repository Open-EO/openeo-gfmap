"""Test the job splitters and managers of OpenEO GFMAP."""


import geopandas as gpd
from shapely.geometry import Point

from openeo_gfmap.manager.job_splitters import split_job_s2grid


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
        assert all(
            gdf.geometry.geom_type == "Point"
        ), "All geometries should be of type Point."
