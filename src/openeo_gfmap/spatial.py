""" Definitions of spatial context, either point-based or spatial"""
from dataclasses import dataclass
from typing import Union

from geojson import GeoJSON


@dataclass
class BoundingBoxExtent:
    """Definition of a bounding box as accepted by OpenEO

    Contains the minx, miny, maxx, maxy coordinates expressed as east, south
    west, north. The EPSG is also defined.
    """

    west: float
    south: float
    east: float
    north: float
    epsg: int = 4326


SpatialContext = Union[GeoJSON, BoundingBoxExtent]
