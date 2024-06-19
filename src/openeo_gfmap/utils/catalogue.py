"""Functionalities to interract with product catalogues."""

import requests
from geojson import GeoJSON
from pyproj.crs import CRS
from rasterio.warp import transform_bounds
from shapely import unary_union
from shapely.geometry import box, shape

from openeo_gfmap import (
    Backend,
    BackendContext,
    BoundingBoxExtent,
    SpatialContext,
    TemporalContext,
)


class UncoveredS1Exception(Exception):
    """Exception raised when there is no product available to fully cover spatially a given
    spatio-temporal context for the Sentinel-1 collection."""

    pass


def _parse_cdse_products(response: dict):
    """Parses the geometry of products from the CDSE catalogue."""
    geoemetries = []
    products = response["features"]

    for product in products:
        geoemetries.append(shape(product["geometry"]))
    return geoemetries


def _query_cdse_catalogue(
    collection: str,
    bounds: list,
    temporal_extent: TemporalContext,
    **additional_parameters: dict,
) -> dict:
    minx, miny, maxx, maxy = bounds

    # The date format should be YYYY-MM-DD
    start_date = f"{temporal_extent.start_date}T00:00:00Z"
    end_date = f"{temporal_extent.end_date}T00:00:00Z"

    url = (
        f"https://catalogue.dataspace.copernicus.eu/resto/api/collections/"
        f"{collection}/search.json?box={minx},{miny},{maxx},{maxy}"
        f"&sortParam=startDate&maxRecords=100"
        f"&dataset=ESA-DATASET&startDate={start_date}&completionDate={end_date}"
    )
    for key, value in additional_parameters.items():
        url += f"&{key}={value}"

    response = requests.get(url)

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


def s1_area_per_orbitstate(
    backend: BackendContext,
    spatial_extent: SpatialContext,
    temporal_extent: TemporalContext,
) -> dict:
    """Evaluates for both the ascending and descending state orbits the area of interesection
    between the given spatio-temporal context and the products available in the backend's
    catalogue.

    Parameters
    ----------
    backend : BackendContext
        The backend to be within, as each backend might use different catalogues.
    spatial_extent : SpatialContext
        The spatial extent to be checked, it will check within its bounding box.
    temporal_extent : TemporalContext
        The temporal period to be checked.

    Returns
    ------
    dict
        Keys containing the orbit state and values containing the total area of intersection in
        km^2
    """
    if isinstance(spatial_extent, GeoJSON):
        # Transform geojson into shapely geometry and compute bounds
        bounds = shape(spatial_extent).bounds
        epsg = 4362
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
        ascending_products = _parse_cdse_products(
            _query_cdse_catalogue(
                "Sentinel1", bounds, temporal_extent, orbitDirection="ASCENDING"
            )
        )
        descending_products = _parse_cdse_products(
            _query_cdse_catalogue(
                "Sentinel1",
                bounds,
                temporal_extent,
                orbitDirection="DESCENDING",
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
            "area": sum(
                product.intersection(spatial_extent).area
                for product in ascending_products
            ),
        },
        "DESCENDING": {
            "full_overlap": descending_covers,
            "area": sum(
                product.intersection(spatial_extent).area
                for product in descending_products
            ),
        },
    }


def select_S1_orbitstate(
    backend: BackendContext,
    spatial_extent: SpatialContext,
    temporal_extent: TemporalContext,
) -> str:
    """Selects the orbit state that covers the most area of the given spatio-temporal context
    for the Sentinel-1 collection.

    Parameters
    ----------
    backend : BackendContext
        The backend to be within, as each backend might use different catalogues.
    spatial_extent : SpatialContext
        The spatial extent to be checked, it will check within its bounding box.
    temporal_extent : TemporalContext
        The temporal period to be checked.

    Returns
    ------
    str
        The orbit state that covers the most area of the given spatio-temporal context
    """

    # Queries the products in the catalogues
    areas = s1_area_per_orbitstate(backend, spatial_extent, temporal_extent)

    ascending_overlap = areas["ASCENDING"]["full_overlap"]
    descending_overlap = areas["DESCENDING"]["full_overlap"]

    if ascending_overlap and not descending_overlap:
        return "ASCENDING"
    elif descending_overlap and not ascending_overlap:
        return "DESCENDING"
    elif ascending_overlap and descending_overlap:
        ascending_cover_area = areas["ASCENDING"]["area"]
        descending_cover_area = areas["DESCENDING"]["area"]

        # Selects the orbit state that covers the most area
        if ascending_cover_area > descending_cover_area:
            return "ASCENDING"
        else:
            return "DESCENDING"

    raise UncoveredS1Exception(
        "No product available to fully cover the given spatio-temporal context."
    )
