"""Job splitter functionalities, except input points/polygons to extract in the
form of a GeoDataFrames.
"""
from typing import List

import geopandas as gpd
import h3


def _resplit_group(
    polygons: gpd.GeoDataFrame, max_points: int
) -> List[gpd.GeoDataFrame]:
    """Performs re-splitting of a dataset of polygons in a list of datasets"""
    for i in range(0, len(polygons), max_points):
        yield polygons.iloc[i : i + max_points].reset_index(drop=True)


def split_job_hex(
    polygons: gpd.GeoDataFrame, max_points: int = 500, grid_resolution: int = 4
) -> List[gpd.GeoDataFrame]:
    """Split a job into multiple jobs from the position of the polygons/points. The centroid of
    the geometries to extract are used to select a hexagon in the H3 grid. Using the H3 grid
    allows to split jobs in equal areas, which is useful for parallel processing while taking into
    account OpenEO's limitations.

    Parameters
    ----------
    polygons: gpd.GeoDataFrae
        Dataset containing the polygons to split the job by with a `geometry` column.
    max_points: int
        The maximum number of points to be included in each job.
    grid_resolution: int
        The scale to use in the H3 hexagonal grid to split jobs to, default is 4. Changing the
        grid scale will drastically increase/decrease the area on which jobs will work.
        More information on the H3 grid can be found at
        https://h3geo.org/docs/core-library/restable
    Returns:
    --------
    split_polygons: list
        List of jobs, split by the GeoDataFrame.
    """

    if "geometry" not in polygons.columns:
        raise ValueError("The GeoDataFrame must contain a 'geometry' column.")

    if polygons.crs is None:
        raise ValueError("The GeoDataFrame must contain a CRS")

    # Project to lat/lon positions
    polygons = polygons.to_crs(epsg=4326)

    # Split the polygons into multiple jobs
    polygons["h3index"] = polygons.geometry.centroid.apply(
        lambda pt: h3.geo_to_h3(pt.y, pt.x, grid_resolution)
    )

    split_datasets = []
    for _, sub_gdf in polygons.groupby("h3index"):
        if len(sub_gdf) > max_points:
            # Performs another split
            split_datasets.extend(_resplit_group(sub_gdf, max_points))
        else:
            split_datasets.append(sub_gdf.reset_index(drop=True))

    return split_datasets
