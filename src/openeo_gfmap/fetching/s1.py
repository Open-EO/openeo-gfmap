""" Collection fetching of S1 features, supporting different backends.
"""

from functools import partial
from typing import Callable

import openeo
from geojson import GeoJSON

from openeo_gfmap.backend import Backend, BackendContext
from openeo_gfmap.spatial import SpatialContext
from openeo_gfmap.temporal import TemporalContext

from .commons import (
    _load_collection,
    convert_band_names,
    rename_bands,
    resample_reproject,
)
from .fetching import CollectionFetcher, FetchType

BASE_SENTINEL1_GRD_MAPPING = {
    "VH": "S1-SIGMA0-VH",
    "HH": "S1-SIGMA0-HH",
    "HV": "S1-SIGMA0-HV",
    "VV": "S1-SIGMA0-VV",
}


def _get_s1_grd_default_fetcher(
    collection_name: str, fetch_type: FetchType
) -> Callable:
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

        cube = _load_collection(
            connection,
            bands,
            collection_name,
            spatial_extent,
            temporal_extent,
            fetch_type,
            **params,
        )

        if fetch_type is not FetchType.POINT and isinstance(spatial_extent, GeoJSON):
            cube = cube.filter_spatial(spatial_extent)

        return cube

    return s1_grd_fetch_default


def _get_s1_grd_default_processor(
    collection_name: str, fetch_type: FetchType, backend: Backend
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

        # Reproject collection data to target CRS and resolution, if specified so.
        # Can be disabled by setting target_resolution=None in the parameters
        if params.get("target_resolution", True) is not None:
            cube = resample_reproject(
                cube,
                params.get("target_resolution", 10.0),
                params.get("target_crs", None),
                method=params.get("resampling_method", "near"),
            )
        elif params.get("target_crs") is not None:
            raise ValueError(
                "In fetching parameters: `target_crs` specified but not `target_resolution`, which is required to perform reprojection."
            )

        # Harmonizing the collection band names to the default GFMAP band names
        cube = rename_bands(cube, BASE_SENTINEL1_GRD_MAPPING)

        return cube

    return s1_grd_default_processor


SENTINEL1_GRD_BACKEND_MAP = {
    Backend.TERRASCOPE: {
        "default": partial(
            _get_s1_grd_default_fetcher, collection_name="SENTINEL1_GRD"
        ),
        "preprocessor": partial(
            _get_s1_grd_default_processor,
            collection_name="SENTINEL1_GRD",
            backend=Backend.TERRASCOPE,
        ),
    },
    Backend.CDSE: {
        "default": partial(
            _get_s1_grd_default_fetcher, collection_name="SENTINEL1_GRD"
        ),
        "preprocessor": partial(
            _get_s1_grd_default_processor,
            collection_name="SENTINEL1_GRD",
            backend=Backend.CDSE,
        ),
    },
    Backend.CDSE_STAGING: {
        "default": partial(
            _get_s1_grd_default_fetcher, collection_name="SENTINEL1_GRD"
        ),
        "preprocessor": partial(
            _get_s1_grd_default_processor,
            collection_name="SENTINEL1_GRD",
            backend=Backend.CDSE_STAGING,
        ),
    },
    Backend.FED: {
        "default": partial(
            _get_s1_grd_default_fetcher, collection_name="SENTINEL1_GRD"
        ),
        "preprocessor": partial(
            _get_s1_grd_default_processor,
            collection_name="SENTINEL1_GRD",
            backend=Backend.FED,
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
