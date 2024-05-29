"""Job splitter functionalities, except input points/polygons to extract in the
form of a GeoDataFrames.
"""

from pathlib import Path
from typing import List

import geopandas as gpd
import h3
import requests

from openeo_gfmap.manager import _log


def load_s2_grid() -> gpd.GeoDataFrame:
    """Returns a geo data frame from the S2 grid."""
    # Builds the path where the geodataframe should be
    gdf_path = Path.home() / ".openeo-gfmap" / "s2grid_bounds.geojson"
    if not gdf_path.exists():
        _log.info("S2 grid not found, downloading it from artifactory.")
        # Downloads the file from the artifactory URL
        gdf_path.parent.mkdir(exist_ok=True)
        response = requests.get(
            "https://artifactory.vgt.vito.be/artifactory/auxdata-public/gfmap/s2grid_bounds.geojson",
            timeout=180,  # 3mins
        )
        with open(gdf_path, "wb") as f:
            f.write(response.content)
    return gpd.read_file(gdf_path)


def _resplit_group(
    polygons: gpd.GeoDataFrame, max_points: int
) -> List[gpd.GeoDataFrame]:
    """Performs re-splitting of a dataset of polygons in a list of datasets"""
    for i in range(0, len(polygons), max_points):
        yield polygons.iloc[i : i + max_points].reset_index(drop=True)


def split_job_s2grid(
    polygons: gpd.GeoDataFrame, max_points: int = 500
) -> List[gpd.GeoDataFrame]:
    """Split a job into multiple jobs from the position of the polygons/points. The centroid of
    the geometries to extract are used to select tile in the Sentinel-2 tile grid.

    Parameters
    ----------
    polygons: gpd.GeoDataFrae
        Dataset containing the polygons to split the job by with a `geometry` column.
    max_points: int
        The maximum number of points to be included in each job.
    Returns:
    --------
    split_polygons: list
        List of jobs, split by the GeoDataFrame.
    """
    if "geometry" not in polygons.columns:
        raise ValueError("The GeoDataFrame must contain a 'geometry' column.")

    if polygons.crs is None:
        raise ValueError("The GeoDataFrame must contain a CRS")

    polygons = polygons.to_crs(epsg=4326)
    if polygons.geometry.geom_type[0] != "Point":
        polygons["geometry"] = polygons.geometry.centroid

    # Dataset containing all the S2 tiles, find the nearest S2 tile for each point
    s2_grid = load_s2_grid()
    s2_grid["geometry"] = s2_grid.geometry.centroid

    polygons = gpd.sjoin_nearest(polygons, s2_grid[["tile", "geometry"]]).drop(
        columns=["index_right"]
    )

    split_datasets = []
    for _, sub_gdf in polygons.groupby("tile"):
        if len(sub_gdf) > max_points:
            # Performs another split
            split_datasets.extend(_resplit_group(sub_gdf, max_points))
        else:
            split_datasets.append(sub_gdf.reset_index(drop=True))
    return split_datasets


def append_h3_index(
    polygons: gpd.GeoDataFrame, grid_resolution: int = 3
) -> gpd.GeoDataFrame:
    """Append the H3 index to the polygons."""
    if polygons.geometry.geom_type[0] != "Point":
        geom_col = polygons.geometry.centroid
    else:
        geom_col = polygons.geometry
    polygons["h3index"] = geom_col.apply(
        lambda pt: h3.geo_to_h3(pt.y, pt.x, grid_resolution)
    )
    return polygons


def split_job_hex(
    polygons: gpd.GeoDataFrame, max_points: int = 500, grid_resolution: int = 3
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
    polygons = append_h3_index(polygons, grid_resolution)

    split_datasets = []
    for _, sub_gdf in polygons.groupby("h3index"):
        if len(sub_gdf) > max_points:
            # Performs another split
            split_datasets.extend(_resplit_group(sub_gdf, max_points))
        else:
            split_datasets.append(sub_gdf.reset_index(drop=True))

    return split_datasets
