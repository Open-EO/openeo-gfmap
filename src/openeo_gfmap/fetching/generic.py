""" Generic extraction of features, supporting VITO backend.
"""

from functools import partial
from typing import Callable

import openeo
from geojson import GeoJSON

from openeo_gfmap.backend import Backend, BackendContext
from openeo_gfmap.fetching import CollectionFetcher, FetchType, _log
from openeo_gfmap.fetching.commons import (
    _load_collection,
    convert_band_names,
    rename_bands,
    resample_reproject,
)
from openeo_gfmap.spatial import SpatialContext
from openeo_gfmap.temporal import TemporalContext

BASE_DEM_MAPPING = {"DEM": "COP-DEM"}
BASE_WEATHER_MAPPING = {
    "dewpoint-temperature": "AGERA5-DEWTEMP",
    "precipitation-flux": "AGERA5-PRECIP",
    "solar-radiation-flux": "AGERA5-SOLRAD",
    "temperature-max": "AGERA5-TMAX",
    "temperature-mean": "AGERA5-TMEAN",
    "temperature-min": "AGERA5-TMIN",
    "vapour-pressure": "AGERA5-VAPOUR",
    "wind-speed": "AGERA5-WIND",
}


def _get_generic_fetcher(collection_name: str, fetch_type: FetchType) -> Callable:
    if collection_name == "COPERNICUS_30":
        BASE_MAPPING = BASE_DEM_MAPPING
    elif collection_name == "AGERA5":
        BASE_MAPPING = BASE_WEATHER_MAPPING
    else:
        raise Exception("Please choose a valid collection.")

    def generic_default_fetcher(
        connection: openeo.Connection,
        spatial_extent: SpatialContext,
        temporal_extent: TemporalContext,
        bands: list,
        **params,
    ) -> openeo.DataCube:
        bands = convert_band_names(bands, BASE_MAPPING)

        if (collection_name == "COPERNICUS_30") and (temporal_extent is not None):
            _log.warning(
                "User set-up non None temporal extent for DEM collection. Ignoring it."
            )
            temporal_extent = None

        cube = _load_collection(
            connection,
            bands,
            collection_name,
            spatial_extent,
            temporal_extent,
            fetch_type,
            **params,
        )

        # Apply if the collection is a GeoJSON Feature collection
        if isinstance(spatial_extent, GeoJSON):
            cube = cube.filter_spatial(spatial_extent)

        return cube

    return generic_default_fetcher


def _get_generic_processor(collection_name: str, fetch_type: FetchType) -> Callable:
    """Builds the preprocessing function from the collection name as it stored
    in the target backend.
    """
    if collection_name == "COPERNICUS_30":
        BASE_MAPPING = BASE_DEM_MAPPING
    elif collection_name == "AGERA5":
        BASE_MAPPING = BASE_WEATHER_MAPPING
    else:
        raise Exception("Please choose a valid collection.")

    def generic_default_processor(cube: openeo.DataCube, **params):
        """Default collection preprocessing method for generic datasets.
        This method renames bands and removes the time dimension in case the
        requested dataset is DEM
        """
        if params.get("target_resolution", None) is not None:
            cube = resample_reproject(
                cube,
                params.get("target_resolution", 10.0),
                params.get("target_crs", None),
                method=params.get("resampling_method", "near"),
            )

        if collection_name == "COPERNICUS_30":
            cube = cube.min_time()

        cube = rename_bands(cube, BASE_MAPPING)

        return cube

    return generic_default_processor


OTHER_BACKEND_MAP = {
    "AGERA5": {
        Backend.TERRASCOPE: {
            "fetch": partial(_get_generic_fetcher, collection_name="AGERA5"),
            "preprocessor": partial(_get_generic_processor, collection_name="AGERA5"),
        },
        Backend.CDSE: {
            "fetch": partial(_get_generic_fetcher, collection_name="AGERA5"),
            "preprocessor": partial(_get_generic_processor, collection_name="AGERA5"),
        },
        Backend.FED: {
            "fetch": partial(_get_generic_fetcher, collection_name="AGERA5"),
            "preprocessor": partial(_get_generic_processor, collection_name="AGERA5"),
        },
    },
    "COPERNICUS_30": {
        Backend.TERRASCOPE: {
            "fetch": partial(_get_generic_fetcher, collection_name="COPERNICUS_30"),
            "preprocessor": partial(
                _get_generic_processor, collection_name="COPERNICUS_30"
            ),
        },
        Backend.CDSE: {
            "fetch": partial(_get_generic_fetcher, collection_name="COPERNICUS_30"),
            "preprocessor": partial(
                _get_generic_processor, collection_name="COPERNICUS_30"
            ),
        },
        Backend.FED: {
            "fetch": partial(_get_generic_fetcher, collection_name="COPERNICUS_30"),
            "preprocessor": partial(
                _get_generic_processor, collection_name="COPERNICUS_30"
            ),
        },
    },
}


def build_generic_extractor(
    backend_context: BackendContext,
    bands: list,
    fetch_type: FetchType,
    collection_name: str,
    **params,
) -> CollectionFetcher:
    """Creates a generic extractor adapted to the given backend. Currently only tested with VITO backend"""
    backend_functions = OTHER_BACKEND_MAP.get(collection_name).get(
        backend_context.backend
    )

    fetcher, preprocessor = (
        backend_functions["fetch"](fetch_type=fetch_type),
        backend_functions["preprocessor"](fetch_type=fetch_type),
    )

    return CollectionFetcher(backend_context, bands, fetcher, preprocessor, **params)
