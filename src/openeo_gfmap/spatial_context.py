""" Definitions of spatial context, either point-based or spatial"""
from dataclasses import dataclass
from typing import Union

from geojson import GeoJSON


@dataclass
class BoundingBoxExtent:
    # TODO: use east, south, west, north to stay closer to openEO conventions
    minx: float
    miny: float
    maxx: float
    maxy: float
    epsg: int = 4326


SpatialContext = Union[GeoJSON, BoundingBoxExtent]
