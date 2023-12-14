"""Functionalities to interract with product catalogues."""
import requests

from shapely.geometry import shape
from geojson import GeoJSON
from openeo_gfmap import SpatialContext, TemporalContext

def _check_cdse_s1_catalogue(
    spatial_extent: SpatialContext,
    temporal_extent: TemporalContext
) -> bool:
    """Checks if there is at least one Sentinel1-GRDproduct available in the
    given spatio-temporal context, as there might be issues in the API that
    sometimes returns empty results for a valid query.
    """
    if isinstance(spatial_extent, GeoJSON):
        # Transform geojson into shapely geometry and compute bounds
        bounds = shape(spatial_extent).bounds
    elif isinstance(spatial_extent, SpatialContext):
        bounds = [spatial_extent.west, spatial_extent.south, spatial_extent.east, spatial_extent.north]
    else:
        raise ValueError('Provided spatial extent is not a valid GeoJSON or SpatialContext object.')

    minx, miny, maxx, maxy = bounds

    # The date format should be YYYY-MM-DD
    start_date = f'{temporal_extent.start_date}T00:00:00Z'
    end_date = f'{temporal_extent.end_date}T00:00:00Z'

    url = (
        f"https://catalogue.dataspace.copernicus.eu/resto/api/collections/"
        f"Sentinel1/search.json?box={minx},{miny},{maxx},{maxy}"
        f"&sortParam=startDate&sortOrder=ascending&maxRecords=100"
        f"&dataset=ESA-DATASET&startDate={start_date}&completionDate={end_date}"
    )

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(
            f"Cannot check S1 catalogue on EODC: Request to {url} failed with "
            f"status code {response.status_code}"
        )

    body = response.json()
    grd_tiles = list(
        filter(lambda feature: feature["properties"]["productType"].contains("GRD"), body["features"])
    )

    return len(grd_tiles) > 0 
