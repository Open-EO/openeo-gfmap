""" Extraction of S2 features, supporting different backends.
"""

from functools import partial
from typing import Callable

import openeo
from geojson import GeoJSON

from openeo_gfmap.backend import Backend, BackendContext
from openeo_gfmap.metadata import FakeMetadata
from openeo_gfmap.spatial import BoundingBoxExtent, SpatialContext
from openeo_gfmap.temporal import TemporalContext

from .commons import (
    _load_collection,
    convert_band_names,
    rename_bands,
    resample_reproject,
)
from .fetching import CollectionFetcher, FetchType

BASE_SENTINEL2_L2A_MAPPING = {
    "B01": "S2-L2A-B01",
    "B02": "S2-L2A-B02",
    "B03": "S2-L2A-B03",
    "B04": "S2-L2A-B04",
    "B05": "S2-L2A-B05",
    "B06": "S2-L2A-B06",
    "B07": "S2-L2A-B07",
    "B08": "S2-L2A-B08",
    "B8A": "S2-L2A-B8A",
    "B09": "S2-L2A-B09",
    "B11": "S2-L2A-B11",
    "B12": "S2-L2A-B12",
    "AOT": "S2-L2A-AOT",
    "SCL": "S2-L2A-SCL",
    "SNW": "S2-L2A-SNW",
}

ELEMENT84_SENTINEL2_L2A_MAPPING = {
    "coastal": "S2-L2A-B01",
    "blue": "S2-L2A-B02",
    "green": "S2-L2A-B03",
    "red": "S2-L2A-B04",
    "rededge1": "S2-L2A-B05",
    "rededge2": "S2-L2A-B06",
    "rededge3": "S2-L2A-B07",
    "nir": "S2-L2A-B08",
    "nir08": "S2-L2A-B8A",
    "nir09": "S2-L2A-B09",
    "cirrus": "S2-L2A-B10",
    "swir16": "S2-L2A-B11",
    "swir22": "S2-L2A-B12",
    "scl": "S2-L2A-SCL",
    "aot": "S2-L2A-AOT",
}


def _get_s2_l2a_default_fetcher(
    collection_name: str, fetch_type: FetchType
) -> Callable:
    """Builds the fetch function from the collection name as it stored in the
    target backend.

    Parameters
    ----------
    collection_name: str
        The name of the sentinel2 collection as named in the backend
    point_based: bool
        The type of fetching: TILE, POINT and POLYGON.
    """

    def s2_l2a_fetch_default(
        connection: openeo.Connection,
        spatial_extent: SpatialContext,
        temporal_extent: TemporalContext,
        bands: list,
        **params,
    ) -> openeo.DataCube:
        """Default collection fetcher for Sentinel_L2A collections.
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
        -------
        openeo.DataCube: a datacube containing the collection raw products.
        """
        # Rename the bands to the backend collection names
        bands = convert_band_names(bands, BASE_SENTINEL2_L2A_MAPPING)

        cube = _load_collection(
            connection,
            bands,
            collection_name,
            spatial_extent,
            temporal_extent,
            fetch_type,
            **params,
        )

        return cube

    return s2_l2a_fetch_default


# TODO deprecated?
def _get_s2_l2a_element84_fetcher(
    collection_name: str, fetch_type: FetchType
) -> Callable:
    """Fetches the collections from the Sentinel-2 Cloud-Optimized GeoTIFFs
    bucket provided by Amazon and managed by Element84.
    """

    def s2_l2a_element84_fetcher(
        connection: openeo.Connection,
        spatial_extent: SpatialContext,
        temporal_extent: TemporalContext,
        bands: list,
        **params,
    ) -> openeo.DataCube:
        """Collection fetcher on the element84 collection."""
        bands = convert_band_names(bands, ELEMENT84_SENTINEL2_L2A_MAPPING)

        if isinstance(spatial_extent, BoundingBoxExtent):
            spatial_extent = dict(spatial_extent)
        elif isinstance(spatial_extent, GeoJSON):
            assert (
                spatial_extent.get("crs", None) is not None
            ), "CRS not defined within GeoJSON collection."
            spatial_extent = dict(spatial_extent)

        cube = connection.load_stac(
            "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a",
            spatial_extent,
            temporal_extent,
            bands,
        )

        cube.metadata = FakeMetadata(band_names=bands)

        # Apply if the collection is a GeoJSON Feature collection
        if isinstance(spatial_extent, GeoJSON):
            cube = cube.filter_spatial(spatial_extent)

        return cube

    return s2_l2a_element84_fetcher


def _get_s2_l2a_default_processor(
    collection_name: str, fetch_type: FetchType
) -> Callable:
    """Builds the preprocessing function from the collection name as it stored
    in the target backend.
    """

    def s2_l2a_default_processor(cube: openeo.DataCube, **params):
        """Default collection preprocessing method.
        This method performs reprojection if specified, upsampling of bands
        at 10m resolution as well as band reprojection. Finally, it converts
        the type of the cube values to uint16
        """
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
        cube = rename_bands(cube, BASE_SENTINEL2_L2A_MAPPING)

        # Change the data type to uint16 for optimization purposes
        cube = cube.linear_scale_range(0, 65534, 0, 65534)

        return cube

    return s2_l2a_default_processor


SENTINEL2_L2A_BACKEND_MAP = {
    Backend.TERRASCOPE: {
        "fetch": partial(_get_s2_l2a_default_fetcher, collection_name="SENTINEL2_L2A"),
        "preprocessor": partial(
            _get_s2_l2a_default_processor, collection_name="SENTINEL2_L2A"
        ),
    },
    Backend.CDSE: {
        "fetch": partial(_get_s2_l2a_default_fetcher, collection_name="SENTINEL2_L2A"),
        "preprocessor": partial(
            _get_s2_l2a_default_processor, collection_name="SENTINEL2_L2A"
        ),
    },
    Backend.CDSE_STAGING: {
        "fetch": partial(_get_s2_l2a_default_fetcher, collection_name="SENTINEL2_L2A"),
        "preprocessor": partial(
            _get_s2_l2a_default_processor, collection_name="SENTINEL2_L2A"
        ),
    },
    Backend.FED: {
        "fetch": partial(_get_s2_l2a_default_fetcher, collection_name="SENTINEL2_L2A"),
        "preprocessor": partial(
            _get_s2_l2a_default_processor, collection_name="SENTINEL2_L2A"
        ),
    },
}


def build_sentinel2_l2a_extractor(
    backend_context: BackendContext, bands: list, fetch_type: FetchType, **params
) -> CollectionFetcher:
    """Creates a S2 L2A extractor adapted to the given backend."""
    backend_functions = SENTINEL2_L2A_BACKEND_MAP.get(backend_context.backend)

    fetcher, preprocessor = (
        backend_functions["fetch"](fetch_type=fetch_type),
        backend_functions["preprocessor"](fetch_type=fetch_type),
    )

    return CollectionFetcher(backend_context, bands, fetcher, preprocessor, **params)
