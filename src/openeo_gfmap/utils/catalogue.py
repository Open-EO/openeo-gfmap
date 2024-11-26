"""Functionalities to interract with product catalogues."""

from typing import Optional

import geojson
import pandas as pd
import requests
from pyproj.crs import CRS
from rasterio.warp import transform_bounds
from requests import adapters
from shapely.geometry import Point, box, shape
from shapely.ops import unary_union

from openeo_gfmap import (
    Backend,
    BackendContext,
    BoundingBoxExtent,
    SpatialContext,
    TemporalContext,
)
from openeo_gfmap.utils import _log

request_sessions: Optional[requests.Session] = None


def _request_session() -> requests.Session:
    global request_sessions

    if request_sessions is None:
        request_sessions = requests.Session()
        retries = adapters.Retry(
            total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504]
        )
        request_sessions.mount("https://", adapters.HTTPAdapter(max_retries=retries))
    return request_sessions


class UncoveredS1Exception(Exception):
    """Exception raised when there is no product available to fully cover spatially a given
    spatio-temporal context for the Sentinel-1 collection."""

    pass


def _parse_cdse_products(response: dict):
    """Parses the geometry and timestamps of products from the CDSE catalogue."""
    geometries = []
    timestamps = []
    products = response["features"]

    for product in products:
        if "geometry" in product and "startDate" in product["properties"]:
            geometries.append(shape(product["geometry"]))
            timestamps.append(pd.to_datetime(product["properties"]["startDate"]))
        else:
            _log.warning(
                "Cannot parse product %s does not have a geometry or timestamp.",
                product["properties"]["id"],
            )
    return geometries, timestamps


def _query_cdse_catalogue(
    collection: str,
    bounds: list,
    temporal_extent: TemporalContext,
    **additional_parameters: dict,
) -> dict:
    """
    Queries the CDSE catalogue for a given collection, spatio-temporal context and additional
    parameters.

    Params
    ------

    """
    minx, miny, maxx, maxy = bounds

    # The date format should be YYYY-MM-DD
    start_date = f"{temporal_extent.start_date}T00:00:00Z"
    end_date = f"{temporal_extent.end_date}T00:00:00Z"

    url = (
        f"https://catalogue.dataspace.copernicus.eu/resto/api/collections/"
        f"{collection}/search.json?box={minx},{miny},{maxx},{maxy}"
        f"&sortParam=startDate&maxRecords=1000&dataset=ESA-DATASET"
        f"&startDate={start_date}&completionDate={end_date}"
    )
    for key, value in additional_parameters.items():
        url += f"&{key}={value}"

    session = _request_session()
    response = session.get(url, timeout=60)

    if response.status_code != 200:
        raise Exception(
            f"Cannot check S1 catalogue on CDSE: Request to {url} failed with "
            f"status code {response.status_code}"
        )

    return response.json()


def _check_cdse_catalogue(
    collection: str,
    bounds: list,
    temporal_extent: TemporalContext,
    **additional_parameters: dict,
) -> bool:
    """Checks if there is at least one product available in the
    given spatio-temporal context for a collection in the CDSE catalogue,
    as there might be issues in the API that sometimes returns empty results
    for a valid query.

    Parameters
    ----------
    collection : str
        The collection name to be checked. (For example: Sentinel1 or Sentinel2)
    spatial_extent : SpatialContext
        The spatial extent to be checked, it will check within its bounding box.
    temporal_extent : TemporalContext
        The temporal period to be checked.
    additional_parameters : Optional[dict], optional
        Additional parameters to be passed to the catalogue, by default empty.
        Parameters (key, value) will be passed as "&key=value" in the query,
        for example: {"sortOrder": "ascending"} will be passed as "&ascendingOrder=True"

    Returns
    -------
    True if there is at least one product, False otherwise.
    """
    body = _query_cdse_catalogue(
        collection, bounds, temporal_extent, **additional_parameters
    )

    grd_tiles = list(
        filter(
            lambda feature: feature["properties"]["productType"].contains("GRD"),
            body["features"],
        )
    )

    return len(grd_tiles) > 0


def _compute_max_gap_days(
    temporal_extent: TemporalContext, timestamps: list[pd.DatetimeIndex]
) -> int:
    """Computes the maximum temporal gap in days from the timestamps parsed from the catalogue.
    Requires the start and end date to be included in the timestamps to compute the gap before
    and after the first and last observation.

    Parameters
    ----------
    temporal_extent : TemporalContext
        The temporal extent to be checked. Same as used to query the catalogue.
    timestamps : list[pd.DatetimeIndex]
        The list of timestamps parsed from the catalogue and to compute the gap from.

    Returns
    -------
    days : int
        The maximum temporal gap in days.
    """
    # Computes max temporal gap. Include requested start and end date so we dont miss
    # any start or end gap before first/last observation
    timestamps = pd.DatetimeIndex(
        sorted(
            [pd.to_datetime(temporal_extent.start_date, utc=True)]
            + timestamps
            + [pd.to_datetime(temporal_extent.end_date, utc=True)]
        )
    )
    return timestamps.to_series().diff().max().days


def s1_area_per_orbitstate_vvvh(
    backend: BackendContext,
    spatial_extent: SpatialContext,
    temporal_extent: TemporalContext,
) -> dict:
    """
    Evaluates for both the ascending and descending state orbits the area of interesection and
    maximum temporal gap for the available products with a VV&VH polarisation.

    Parameters
    ----------
    backend : BackendContext
        The backend to be within, as each backend might use different catalogues. Only the CDSE,
        CDSE_STAGING and FED backends are supported.
    spatial_extent : SpatialContext
        The spatial extent to be checked, it will check within its bounding box.
    temporal_extent : TemporalContext
        The temporal period to be checked.

    Returns
    ------
    dict
        Keys containing the orbit state and values containing the total area of intersection and
        in km^2 and maximum temporal gap in days.
    """
    if isinstance(spatial_extent, geojson.FeatureCollection):
        # Transform geojson into shapely geometry and compute bounds
        shapely_geometries = [
            shape(feature["geometry"]) for feature in spatial_extent["features"]
        ]
        if len(shapely_geometries) == 1 and isinstance(shapely_geometries[0], Point):
            point = shapely_geometries[0]
            buffer_size = 0.0001
            buffered_geometry = point.buffer(buffer_size)
            bounds = buffered_geometry.bounds
        else:
            geometry = unary_union(shapely_geometries)
            bounds = geometry.bounds
        epsg = 4326
    elif isinstance(spatial_extent, BoundingBoxExtent):
        bounds = [
            spatial_extent.west,
            spatial_extent.south,
            spatial_extent.east,
            spatial_extent.north,
        ]
        epsg = spatial_extent.epsg
    else:
        raise ValueError(
            "Provided spatial extent is not a valid GeoJSON or SpatialContext object."
        )
    # Warp the bounds if  the epsg is different from 4326
    if epsg != 4326:
        bounds = transform_bounds(CRS.from_epsg(epsg), CRS.from_epsg(4326), *bounds)

    # Queries the products in the catalogues
    if backend.backend in [Backend.CDSE, Backend.CDSE_STAGING, Backend.FED]:
        ascending_products, ascending_timestamps = _parse_cdse_products(
            _query_cdse_catalogue(
                "Sentinel1",
                bounds,
                temporal_extent,
                orbitDirection="ASCENDING",
                polarisation="VV%26VH",
            )
        )
        descending_products, descending_timestamps = _parse_cdse_products(
            _query_cdse_catalogue(
                "Sentinel1",
                bounds,
                temporal_extent,
                orbitDirection="DESCENDING",
                polarisation="VV%26VH",
            )
        )
    else:
        raise NotImplementedError(
            f"This feature is not supported for backend: {backend.backend}."
        )

    # Builds the shape of the spatial extent and computes the area
    spatial_extent = box(*bounds)

    # Computes if there is the full overlap for each of those states
    union_ascending = unary_union(ascending_products)
    union_descending = unary_union(descending_products)

    ascending_covers = union_ascending.contains(spatial_extent)
    descending_covers = union_descending.contains(spatial_extent)

    # Computes the area of intersection
    return {
        "ASCENDING": {
            "full_overlap": ascending_covers,
            "max_temporal_gap": _compute_max_gap_days(
                temporal_extent, ascending_timestamps
            ),
            "area": sum(
                product.intersection(spatial_extent).area
                for product in ascending_products
            ),
        },
        "DESCENDING": {
            "full_overlap": descending_covers,
            "max_temporal_gap": _compute_max_gap_days(
                temporal_extent, descending_timestamps
            ),
            "area": sum(
                product.intersection(spatial_extent).area
                for product in descending_products
            ),
        },
    }


def select_s1_orbitstate_vvvh(
    backend: BackendContext,
    spatial_extent: SpatialContext,
    temporal_extent: TemporalContext,
    max_temporal_gap: int = 60,
) -> str:
    """Selects the orbit state based on some predefined rules that
    are checked in sequential order:
    1. prefer an orbit with full coverage over the requested bounds
    2. prefer an orbit with a maximum temporal gap under a
        predefined threshold
    3. prefer the orbit that covers the most area of intersection
        for the available products

    Parameters
    ----------
    backend : BackendContext
        The backend to be within, as each backend might use different catalogues. Only the CDSE,
        CDSE_STAGING and FED backends are supported.
    spatial_extent : SpatialContext
        The spatial extent to be checked, it will check within its bounding box.
    temporal_extent : TemporalContext
        The temporal period to be checked.
    max_temporal_gap: int, optional, default: 30
        The maximum temporal gap in days to be considered for the orbit state.

    Returns
    ------
    str
        The orbit state that covers the most area of the given spatio-temporal context
    """

    # Queries the products in the catalogues
    areas = s1_area_per_orbitstate_vvvh(backend, spatial_extent, temporal_extent)

    ascending_overlap = areas["ASCENDING"]["full_overlap"]
    descending_overlap = areas["DESCENDING"]["full_overlap"]
    ascending_gap_too_large = areas["ASCENDING"]["max_temporal_gap"] > max_temporal_gap
    descending_gap_too_large = (
        areas["DESCENDING"]["max_temporal_gap"] > max_temporal_gap
    )

    orbit_choice = None

    if not ascending_overlap and not descending_overlap:
        raise UncoveredS1Exception(
            "No product available to fully cover the requested area in both orbit states."
        )

    # Rule 1: Prefer an orbit with full coverage over the requested bounds
    if ascending_overlap and not descending_overlap:
        orbit_choice = "ASCENDING"
        reason = "Only orbit fully covering the requested area."
    elif descending_overlap and not ascending_overlap:
        orbit_choice = "DESCENDING"
        reason = "Only orbit fully covering the requested area."

    # Rule 2: Prefer an orbit with a maximum temporal gap under a predefined threshold
    elif ascending_gap_too_large and not descending_gap_too_large:
        orbit_choice = "DESCENDING"
        reason = (
            "Only orbit with temporal gap under the threshold. "
            f"{areas['DESCENDING']['max_temporal_gap']} days < {max_temporal_gap} days"
        )
    elif descending_gap_too_large and not ascending_gap_too_large:
        orbit_choice = "ASCENDING"
        reason = (
            "Only orbit with temporal gap under the threshold. "
            f"{areas['ASCENDING']['max_temporal_gap']} days < {max_temporal_gap} days"
        )
    # Rule 3: Prefer the orbit that covers the most area of intersection
    # for the available products
    elif ascending_overlap and descending_overlap:
        ascending_cover_area = areas["ASCENDING"]["area"]
        descending_cover_area = areas["DESCENDING"]["area"]

        # Selects the orbit state that covers the most area
        if ascending_cover_area > descending_cover_area:
            orbit_choice = "ASCENDING"
            reason = (
                "Orbit has more cumulative intersected area. "
                f"{ascending_cover_area} > {descending_cover_area}"
            )
        else:
            reason = (
                "Orbit has more cumulative intersected area. "
                f"{descending_cover_area} > {ascending_cover_area}"
            )
            orbit_choice = "DESCENDING"

    if orbit_choice is not None:
        _log.info(f"Selected orbit state: {orbit_choice}. Reason: {reason}")
        return orbit_choice
    raise UncoveredS1Exception("Failed to select suitable Sentinel-1 orbit.")
