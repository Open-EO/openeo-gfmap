""" Collection fetching of S1 features, supporting different backends.
"""
from functools import partial
from typing import Callable

import openeo
from geojson import GeoJSON

from openeo_gfmap.backend import Backend, BackendContext
from openeo_gfmap.spatial import BoundingBoxExtent, SpatialContext
from openeo_gfmap.temporal import TemporalContext

from .commons import (
    convert_band_names,
    load_collection,
    rename_bands,
    resample_reproject,
)
from .fetching import CollectionFetcher, FetchType

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
        """Default collection fetcher for Sentinel-1 GRD collections. The
        collection values are expected to be expressed in power values.
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

        if isinstance(spatial_extent.value, GeoJSON):
            cube = cube.filter_spatial(spatial_extent)

        return cube

    return s1_grd_fetch_default


def get_s1_grd_default_processor(
    collection_name: str, fetch_type: FetchType
) -> Callable:
    """Builds the preprocessing function from the collection name as it is stored
    in the target backend.
    """

    def s1_grd_default_processor(cube: openeo.DataCube, **params):
        """Default collection preprocessing method.
        This method performs:

        * Compute the backscatter of all the S1 products. By default, the
        "sigma0-ellipsoid" method is used with "COPERNICUS_30" DEM, but those
        can be changed by specifying "coefficient" and "elevation_model" in
        params.
        * Resampling to 10m resolution.
        * Reprojection if a "target_crs" key is specified in `params`.
        * Performs value rescaling to uint16.
        """
        elevation_model = params.get("elevation_model", "COPERNICUS_30")
        coefficient = params.get("coefficient", "sigma0-ellipsoid")

        cube = cube.sar_backscatter(
            elevation_model=elevation_model,
            coefficient=coefficient,
            local_incidence_angle=False,
        )

        cube = resample_reproject(
            cube, params.get("target_resolution", 10.0), params.get("target_crs", None)
        )

        cube = rename_bands(cube, BASE_SENTINEL1_GRD_MAPPING)

        return cube

    return s1_grd_default_processor


SENTINEL1_GRD_BACKEND_MAP = {
    Backend.TERRASCOPE: {
        "default": partial(get_s1_grd_default_fetcher, collection_name="SENTINEL1_GRD"),
        "preprocessor": partial(
            get_s1_grd_default_processor, collection_name="SENTINEL1_GRD"
        ),
    },
    Backend.CDSE: {
        "default": partial(get_s1_grd_default_fetcher, collection_name="SENTINEL1_GRD"),
        "preprocessor": partial(
            get_s1_grd_default_processor, collection_name="SENTINEL1_GRD"
        ),
    },
}


def build_sentinel1_grd_extractor(
    backend_context: BackendContext, bands: list, fetch_type: FetchType, **params
) -> CollectionFetcher:
    """Creates a S1 GRD collection extractor for the given backend."""
    backend_functions = SENTINEL1_GRD_BACKEND_MAP.get(backend_context.backend)

    fetcher, preprocessor = (
        backend_functions["default"](fetch_type=fetch_type),
        backend_functions["preprocessor"](fetch_type=fetch_type),
    )

    return CollectionFetcher(backend_context, bands, fetcher, preprocessor, **params)
