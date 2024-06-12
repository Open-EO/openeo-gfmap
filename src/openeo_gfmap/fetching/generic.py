""" Generic extraction of features, supporting VITO backend.
"""

from functools import partial
from typing import Callable

import openeo
from geojson import GeoJSON

from openeo_gfmap.backend import Backend, BackendContext
from openeo_gfmap.fetching import CollectionFetcher, FetchType, _log
from openeo_gfmap.fetching.commons import (
    convert_band_names,
    load_collection,
    rename_bands,
    resample_reproject,
)
from openeo_gfmap.spatial import SpatialContext
from openeo_gfmap.temporal import TemporalContext

BASE_DEM_MAPPING = {"DEM": "COP-DEM"}


def get_generic_fetcher(
    collection_name: str,
    fetch_type: FetchType,
    band_mapping: dict,
) -> Callable:
    def generic_default_fetcher(
        connection: openeo.Connection,
        spatial_extent: SpatialContext,
        temporal_extent: TemporalContext,
        bands: list,
        **params,
    ) -> openeo.DataCube:
        bands = convert_band_names(bands, band_mapping)

        if (collection_name == "COPERNICUS_30") and (temporal_extent is not None):
            _log.warning(
                "User set-up non None temporal extent for DEM collection. Ignoring it."
            )
            temporal_extent = None

        cube = load_collection(
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

    return partial(generic_default_fetcher, band_mapping=band_mapping)


def get_generic_processor(
    collection_name: str, fetch_type: FetchType, band_mapping: dict
) -> Callable:
    """Builds the preprocessing function from the collection name as it stored
    in the target backend.
    """

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

        cube = rename_bands(cube, band_mapping)

        return cube

    return generic_default_processor


OTHER_BACKEND_MAP = {
    "COPERNICUS_30": {
        Backend.TERRASCOPE: {
            "fetch": partial(
                get_generic_fetcher,
                collection_name="COPERNICUS_30",
                band_mapping=BASE_DEM_MAPPING,
            ),
            "preprocessor": partial(
                get_generic_processor,
                collection_name="COPERNICUS_30",
                band_mapping=BASE_DEM_MAPPING,
            ),
        },
        Backend.CDSE: {
            "fetch": partial(
                get_generic_fetcher,
                collection_name="COPERNICUS_30",
                band_mapping=BASE_DEM_MAPPING,
            ),
            "preprocessor": partial(
                get_generic_processor,
                collection_name="COPERNICUS_30",
                band_mapping=BASE_DEM_MAPPING,
            ),
        },
        Backend.CDSE_STAGING: {
            "fetch": partial(
                get_generic_fetcher,
                collection_name="COPERNICUS_30",
                band_mapping=BASE_DEM_MAPPING,
            ),
            "preprocessor": partial(
                get_generic_processor,
                collection_name="COPERNICUS_30",
                band_mapping=BASE_DEM_MAPPING,
            ),
        },
        Backend.FED: {
            "fetch": partial(
                get_generic_fetcher,
                collection_name="COPERNICUS_30",
                band_mapping=BASE_DEM_MAPPING,
            ),
            "preprocessor": partial(
                get_generic_processor,
                collection_name="COPERNICUS_30",
                band_mapping=BASE_DEM_MAPPING,
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
