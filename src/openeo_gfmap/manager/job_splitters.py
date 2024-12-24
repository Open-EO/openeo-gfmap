"""Job splitter functionalities, except input points/polygons to extract in the
form of a GeoDataFrames.
"""

from pathlib import Path
from typing import List

import geopandas as gpd
import h3
import requests
import s2sphere

from openeo_gfmap.manager import _log


def load_s2_grid(web_mercator: bool = False) -> gpd.GeoDataFrame:
    """Returns a geo data frame from the S2 grid."""
    # Builds the path where the geodataframe should be
    if not web_mercator:
        gdf_path = Path.home() / ".openeo-gfmap" / "s2grid_bounds_4326_v2.geoparquet"
        url = "https://artifactory.vgt.vito.be/artifactory/auxdata-public/gfmap/s2grid_bounds_4326_v2.geoparquet"
    else:
        gdf_path = Path.home() / ".openeo-gfmap" / "s2grid_bounds_3857_v2.geoparquet"
        url = "https://artifactory.vgt.vito.be/artifactory/auxdata-public/gfmap/s2grid_bounds_3857_v2.geoparquet"

    if not gdf_path.exists():
        _log.info("S2 grid not found, downloading it from artifactory.")
        # Downloads the file from the artifactory URL
        gdf_path.parent.mkdir(exist_ok=True)
        response = requests.get(
            url,
            timeout=180,  # 3mins
        )
        if response.status_code != 200:
            raise ValueError(
                "Failed to download the S2 grid from the artifactory. "
                f"Status code: {response.status_code}"
            )
        with open(gdf_path, "wb") as f:
            f.write(response.content)
    return gpd.read_parquet(gdf_path)


def load_s2_grid_centroids(web_mercator: bool = False) -> gpd.GeoDataFrame:
    """Returns a geo data frame from the S2 grid centroids."""
    # Builds the path where the geodataframe should be
    if not web_mercator:
        gdf_path = (
            Path.home() / ".openeo-gfmap" / "s2grid_bounds_4326_centroids.geoparquet"
        )
        url = "https://artifactory.vgt.vito.be/artifactory/auxdata-public/gfmap/s2grid_bounds_4326_centroids.geoparquet"
    else:
        gdf_path = (
            Path.home() / ".openeo-gfmap" / "s2grid_bounds_3857_centroids.geoparquet"
        )
        url = "https://artifactory.vgt.vito.be/artifactory/auxdata-public/gfmap/s2grid_bounds_3857_centroids.geoparquet"

    if not gdf_path.exists():
        _log.info("S2 grid centroids not found, downloading it from artifactory.")
        # Downloads the file from the artifactory URL
        gdf_path.parent.mkdir(exist_ok=True)
        response = requests.get(
            url,
            timeout=180,  # 3mins
        )
        if response.status_code != 200:
            raise ValueError(
                "Failed to download the S2 grid centroids from the artifactory. "
                f"Status code: {response.status_code}"
            )
        with open(gdf_path, "wb") as f:
            f.write(response.content)
    return gpd.read_parquet(gdf_path)


def _resplit_group(
    polygons: gpd.GeoDataFrame, max_points: int
) -> List[gpd.GeoDataFrame]:
    """Performs re-splitting of a dataset of polygons in a list of datasets"""
    for i in range(0, len(polygons), max_points):
        yield polygons.iloc[i : i + max_points].reset_index(drop=True)


def split_job_s2grid(
    polygons: gpd.GeoDataFrame, max_points: int = 500, web_mercator: bool = False
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

    epsg = 3857 if web_mercator else 4326

    original_crs = polygons.crs

    polygons = polygons.to_crs(epsg=epsg)

    polygons["centroid"] = polygons.geometry.centroid

    # Dataset containing all the S2 tile centroids, find the nearest S2 tile for each point
    s2_grid = load_s2_grid_centroids(web_mercator)

    s2_grid = s2_grid[s2_grid.cdse_valid]

    polygons = gpd.sjoin_nearest(
        polygons.set_geometry("centroid"), s2_grid[["tile", "geometry"]]
    ).drop(columns=["index_right", "centroid"])

    polygons = polygons.set_geometry("geometry").to_crs(original_crs)

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

    # Project to Web mercator to calculate centroids
    polygons = polygons.to_crs(epsg=3857)
    geom_col = polygons.geometry.centroid
    # Project to lat lon to calculate the h3 index
    geom_col = geom_col.to_crs(epsg=4326)

    polygons["h3index"] = geom_col.apply(
        lambda pt: h3.latlng_to_cell(pt.y, pt.x, grid_resolution)
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

    original_crs = polygons.crs

    # Split the polygons into multiple jobs
    polygons = append_h3_index(polygons, grid_resolution)

    polygons = polygons.to_crs(original_crs)

    split_datasets = []
    for _, sub_gdf in polygons.groupby("h3index"):
        if len(sub_gdf) > max_points:
            # Performs another split
            split_datasets.extend(_resplit_group(sub_gdf, max_points))
        else:
            split_datasets.append(sub_gdf.reset_index(drop=True))

    return split_datasets


def split_job_s2sphere(
    gdf: gpd.GeoDataFrame, max_points=500, start_level=8
) -> List[gpd.GeoDataFrame]:
    """
    EXPERIMENTAL
    Split a GeoDataFrame into multiple groups based on the S2geometry cell ID of each geometry.

    S2geometry is a library that provides a way to index and query spatial data. This function splits
    the GeoDataFrame into groups based on the S2 cell ID of each geometry, based on it's centroid.

    If a cell contains more points than max_points, it will be recursively split into
    smaller cells until each cell contains at most max_points points.

    More information on S2geometry can be found at https://s2geometry.io/
    An overview of the S2 cell hierarchy can be found at https://s2geometry.io/resources/s2cell_statistics.html

    :param gdf: GeoDataFrame containing points to split
    :param max_points: Maximum number of points per group
    :param start_level: Starting S2 cell level
    :return: List of GeoDataFrames containing the split groups
    """

    if "geometry" not in gdf.columns:
        raise ValueError("The GeoDataFrame must contain a 'geometry' column.")

    if gdf.crs is None:
        raise ValueError("The GeoDataFrame must contain a CRS")

    # Store the original CRS of the GeoDataFrame and reproject to EPSG:3857
    original_crs = gdf.crs
    gdf = gdf.to_crs(epsg=3857)

    # Add a centroid column to the GeoDataFrame and convert it to EPSG:4326
    gdf["centroid"] = gdf.geometry.centroid

    # Reproject the GeoDataFrame to its orginial CRS
    gdf = gdf.to_crs(original_crs)

    # Set the GeoDataFrame's geometry to the centroid column and reproject to EPSG:4326
    gdf = gdf.set_geometry("centroid")
    gdf = gdf.to_crs(epsg=4326)

    # Create a dictionary to store points by their S2 cell ID
    cell_dict = {}

    # Iterate over each point in the GeoDataFrame
    for idx, row in gdf.iterrows():
        # Get the S2 cell ID for the point at a given level
        cell_id = _get_s2cell_id(row.centroid, start_level)

        if cell_id not in cell_dict:
            cell_dict[cell_id] = []

        cell_dict[cell_id].append(row)

    result_groups = []

    # Function to recursively split cells if they contain more points than max_points
    def _split_s2cell(cell_id, points, current_level=start_level):
        if len(points) <= max_points:
            if len(points) > 0:
                points = gpd.GeoDataFrame(
                    points, crs=original_crs, geometry="geometry"
                ).drop(columns=["centroid"])
                points["s2sphere_cell_id"] = cell_id
                points["s2sphere_cell_level"] = current_level
                result_groups.append(gpd.GeoDataFrame(points))
        else:
            children = s2sphere.CellId(cell_id).children()
            child_cells = {child.id(): [] for child in children}

            for point in points:
                child_cell_id = _get_s2cell_id(point.centroid, current_level + 1)
                child_cells[child_cell_id].append(point)

            for child_cell_id, child_points in child_cells.items():
                _split_s2cell(child_cell_id, child_points, current_level + 1)

    # Split cells that contain more points than max_points
    for cell_id, points in cell_dict.items():
        _split_s2cell(cell_id, points)

    return result_groups


def _get_s2cell_id(point, level):
    lat, lon = point.y, point.x
    cell_id = s2sphere.CellId.from_lat_lng(
        s2sphere.LatLng.from_degrees(lat, lon)
    ).parent(level)
    return cell_id.id()
