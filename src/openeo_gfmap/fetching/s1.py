""" Collection fetching of S1 features, supporting different backends.
"""
from typing import Callable

import openeo
from geojson import GeoJSON

from openeo_gfmap.fetching import FetchType
from openeo_gfmap.spatial import SpatialContext
from openeo_gfmap.temporal import TemporalContext

from .commons import (
    convert_band_names,
    load_collection,
    rename_bands,
    resample_reproject,
)

BASE_SENTINEL1_GRD_MAPPING = {
    "VH": "S1-VH",
    "HH": "S1-HH",
    "HV": "S1-HV",
    "VV": "S1-VV",
}


def get_s1_grd_default_fetcher(collection_name: str, fetch_type: FetchType) -> Callable:
    """Return a default fetcher for Sentinel-1 GRD data.

    Parameters
    ----------
    collection_name : str
        The name of the sentinel1 collection to fetch as named in the backend.
    fetch_type : FetchType
        The type of fetching: TILE, POINT and POLYGON.
    """

    def s1_grd_fetch_default(
        connection: openeo.Connection,
        spatial_extent: SpatialContext,
        temporal_extent: TemporalContext,
        bands: list,
        **params,
    ) -> openeo.DataCube:
        """Default collection fetcher for Sentinel-1 GRD collections.
        The collection here is expected to be expressed in power values, and
        that the backscatter is already sigma0 computed. (Example, the
        collection available by SentinelHub from Sinergize)
        Parameters
        ----------
        connection: openeo.Connection
            Connection to a general backend.
        spatial_extent: SpatialContext
            Either a GeoJSON collection or a bounding box of locations.
            Performs spatial filtering if the spatial context is a GeoJSON
            collection, as it implies sparse data.
        temporal_extent: TemporalContexct
            A time range, defined by a start and end date.
        bands: list
            The name of the bands to load from that collection
        Returns
        ------
        openeo.DataCube: a datacube containing the collection raw products.
        """
        bands = convert_band_names(bands, BASE_SENTINEL1_GRD_MAPPING)

        load_collection_parameters = params.get("load_collection", {})

        cube = load_collection(
            connection,
            bands,
            collection_name,
            spatial_extent,
            temporal_extent,
            fetch_type,
            **load_collection_parameters,
        )

        if isinstance(spatial_extent, GeoJSON):
            cube = cube.filter_spatial(spatial_extent)

        return cube


# def get_s1_grd_backscatter_fetcher(
#     collection_name: str,
#     fetch_type: FetchType
# ) -> Callable:
#     """ Return a default fetcher for Sentinel-1 GRD data.

#     Parameters
#     ----------
#     collection_name : str
#         The name of the sentinel1 collection to fetch as named in the backend.
#     fetch_type : FetchType
#         The type of fetching: TILE, POINT and POLYGON.
#     """

#     def s1_grd_fetch_default(
#         connection: openeo.Connection,
#         spatial_extent: SpatialContext,
#         temporal_extent: TemporalContext,
#         bands: list,
#         **params
#     ) -> openeo.DataCube:
#         """ Default collection fetcher for Sentinel-1 GRD collections.
#         The collection here is expected to be expressed in power values, and
#         that the backscatter is already sigma0 computed. (Example, the
#         collection available by SentinelHub from Sinergize)
#         Parameters
#         ----------
#         connection: openeo.Connection
#             Connection to a general backend.
#         spatial_extent: SpatialContext
#             Either a GeoJSON collection or a bounding box of locations.
#             Performs spatial filtering if the spatial context is a GeoJSON
#             collection, as it implies sparse data.
#         temporal_extent: TemporalContexct
#             A time range, defined by a start and end date.
#         bands: list
#             The name of the bands to load from that collection
#         Returns
#         ------
#         openeo.DataCube: a datacube containing the collection raw products.
#         """
#         bands = convert_band_names(bands, BASE_SENTINEL1_GRD_MAPPING)

#         load_collection_parameters = params.get(
#             'load_collection', {}
#         )

#         cube = load_collection(
#             connection, bands, collection_name, spatial_extent, temporal_extent,
#             fetch_type, **load_collection_parameters
#         )

#         if isinstance(spatial_extent, GeoJSON):
#             cube = cube.filter_spatial(spatial_extent)

#         return cube
