""" Definitions of spatial context, either point-based or spatial"""
from typing import Union
from dataclasses import dataclass

from geojson import GeoJSON


@dataclass
class BoundingBoxExtent(dict):

    def __init__(
        self,
        minx: float,
        miny: float,
        maxx: float,
        maxy: float,
        epsg: int
    ):
        pass


SpatialContext = Union[GeoJSON, BoundingBoxExtent]
