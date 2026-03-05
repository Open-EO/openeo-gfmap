"""Functionalities to interract with product catalogues."""

from typing import Iterator

import geojson
import pandas as pd
import pystac
import requests
from pyproj.crs import CRS
from pystac_client import Client
from pystac_client.stac_api_io import StacApiIO
from rasterio.warp import transform_bounds
from requests.adapters import HTTPAdapter
from shapely.geometry import Point, box, shape
from shapely.ops import unary_union
from urllib3.util.retry import Retry

from openeo_gfmap import (
    Backend,
    BackendContext,
    BoundingBoxExtent,
    SpatialContext,
    TemporalContext,
)
from openeo_gfmap.utils import _log

DEFAULT_OPENEO_SENTINEL1_PROPERTY_FILTERS = [
    {
        "op": "in",
        "args": [
            {"property": "properties.product:type"},
            ["IW_GRDH_1S", "IW_GRDH_1S_B", "IW_GRDH_1S_C"],
        ],
    },
    {"op": "=", "args": [{"property": "properties.processing:level"}, "L1"]},
]


def _build_retry_session(
    *,
    total: int = 7,
    backoff_factor: float = 1.0,
    backoff_jitter: float = 1.5,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
    allowed_methods: frozenset[str] = frozenset(["GET", "POST", "HEAD", "OPTIONS"]),
    pool_connections: int = 10,
    pool_maxsize: int = 10,
) -> requests.Session:
    """
    Build a requests session that retries with exponential backoff + jitter.
    """
    retry = Retry(
        total=total,
        connect=total,
        read=total,
        status=total,
        backoff_factor=backoff_factor,
        backoff_jitter=backoff_jitter,
        status_forcelist=status_forcelist,
        allowed_methods=allowed_methods,
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    session = requests.Session()
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class UncoveredS1Exception(Exception):
    """Exception raised when there is no product available to fully cover spatially a given
    spatio-temporal context for the Sentinel-1 collection."""

    pass


def _parse_cdse_products(response: Iterator[pystac.Item]):
    """
    Parses the geometry and timestamps of products from the CDSE catalogue.

    Assumption: `response` is an iterator/iterable of pystac.Item objects
    (e.g. returned by `pystac_client.Client.search(...).items()`).

    Returns
    -------
    geometries : list[shapely.geometry.base.BaseGeometry]
    timestamps : list[pandas.Timestamp]
    """
    geometries = []
    timestamps = []

    for item in response:  # item is a pystac.Item
        geom = item.geometry
        props = item.properties or {}

        dt = props.get("datetime") or props.get("start_datetime")

        if geom is not None and dt is not None:
            geometries.append(shape(geom))
            timestamps.append(pd.to_datetime(dt, utc=True))
        else:
            _log.warning(
                "Cannot parse product %s: missing geometry or timestamp.",
                getattr(item, "id", "<unknown>"),
            )

    return geometries, timestamps


def _query_cdse_catalogue_s1(
    bounds: list,
    temporal_extent: "TemporalContext",
    **additional_parameters: dict,
) -> Iterator[pystac.Item]:
    """
    Queries the sentinel-1-grd CDSE STAC catalogue for a given spatio-temporal context and
    additional parameters, using pystac-client (auto-pagination) with jittered retries.
    """
    collection = "sentinel-1-grd"
    minx, miny, maxx, maxy = bounds

    start_date = f"{temporal_extent.start_date}T00:00:00Z"
    end_date = f"{temporal_extent.end_date}T00:00:00Z"
    datetime_interval = f"{start_date}/{end_date}"

    # Build CQL2 filter list
    filter_args = list(DEFAULT_OPENEO_SENTINEL1_PROPERTY_FILTERS)

    for key, value in additional_parameters.items():
        if value is None:
            continue

        if isinstance(value, (list, tuple, set)):
            for v in value:
                if v is not None:
                    filter_args.append({"op": "=", "args": [{"property": key}, v]})
        else:
            filter_args.append({"op": "=", "args": [{"property": key}, value]})

    cql_filter = None
    if filter_args:
        cql_filter = (
            {"op": "and", "args": filter_args}
            if len(filter_args) > 1
            else filter_args[0]
        )

    session = _build_retry_session()

    stac_io = StacApiIO(timeout=(10.0, 180.0))
    stac_io.session = session

    try:
        client = Client.open(
            "https://stac.opensearch.dataspace.copernicus.eu/v1", stac_io=stac_io
        )

        search_kwargs = {
            "collections": [collection],
            "bbox": [minx, miny, maxx, maxy],
            "datetime": datetime_interval,
            "limit": 200,  # page size
        }

        if cql_filter is not None:
            search_kwargs["filter_lang"] = "cql2-json"
            search_kwargs["filter"] = cql_filter
            search_kwargs["method"] = "POST"
            _log.debug("Querying CDSE catalogue with CQL2 filter: %s", cql_filter)

        search = client.search(**search_kwargs)

        return search.items()

    except requests.RequestException as e:
        raise Exception(
            "Cannot check S1 catalogue on CDSE: request failed after retries "
            f"(bbox={bounds}, datetime={datetime_interval}). Error: {e}"
        ) from e
    except Exception as e:
        raise Exception(
            "Cannot check S1 catalogue on CDSE: unexpected error "
            f"(bbox={bounds}, datetime={datetime_interval}). Error: {e}"
        ) from e


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

    ascending_filters = {
        "properties.sat:orbit_state": "ascending",
        "properties.sar:polarizations": ["VV", "VH"],
    }

    descending_filters = {
        "properties.sat:orbit_state": "descending",
        "properties.sar:polarizations": ["VV", "VH"],
    }

    # Queries the products in the catalogues
    if backend.backend in [Backend.CDSE, Backend.CDSE_STAGING, Backend.FED]:
        ascending_products, ascending_timestamps = _parse_cdse_products(
            _query_cdse_catalogue_s1(bounds, temporal_extent, **ascending_filters)
        )
        descending_products, descending_timestamps = _parse_cdse_products(
            _query_cdse_catalogue_s1(bounds, temporal_extent, **descending_filters)
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
