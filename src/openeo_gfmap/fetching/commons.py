"""
Common internal operations within collection extraction logic, such as reprojection.
"""

from functools import partial
from typing import Dict, Optional, Sequence, Union

import openeo
from geojson import GeoJSON
from openeo.api.process import Parameter
from openeo.rest.connection import InputDate
from pyproj.crs import CRS
from pyproj.exceptions import CRSError

from openeo_gfmap.spatial import BoundingBoxExtent, SpatialContext
from openeo_gfmap.temporal import TemporalContext

from .fetching import FetchType


def convert_band_names(desired_bands: list, band_dict: dict) -> list:
    """Renames the desired bands to the band names of the collection specified
    in the backend.

    Parameters
    ----------
    desired_bands: list
        List of bands that are desired by the user, written in the OpenEO-GFMAP
        harmonized names convention.
    band_dict: dict
        Dictionnary mapping for a backend the collection band names to the
        OpenEO-GFMAP harmonized names. This dictionnary will be reversed to be
        used within this function.
    Returns
    -------
    backend_band_list: list
        List of band names within the backend collection names.
    """
    # Reverse the dictionarry
    band_dict = {v: k for k, v in band_dict.items()}
    return [band_dict[band] for band in desired_bands]


def resample_reproject(
    datacube: openeo.DataCube,
    resolution: float,
    epsg_code: Optional[Union[str, int]] = None,
    method: str = "near",
) -> openeo.DataCube:
    """Reprojects the given datacube to the target epsg code, if the provided
    epsg code is not None. Also performs checks on the give code to check
    its validity.
    """
    if epsg_code is not None:
        # Checks that the code is valid
        try:
            CRS.from_epsg(int(epsg_code))
        except (CRSError, ValueError) as exc:
            raise ValueError(
                f"Specified target_crs: {epsg_code} is not a valid " "EPSG code."
            ) from exc
        return datacube.resample_spatial(
            resolution=resolution, projection=epsg_code, method=method
        )
    return datacube.resample_spatial(resolution=resolution, method=method)


def rename_bands(datacube: openeo.DataCube, mapping: dict) -> openeo.DataCube:
    """Rename the bands from the given mapping scheme"""

    # Filter out bands that are not part of the datacube
    def filter_condition(band_name, _):
        return band_name in datacube.metadata.band_names

    mapping = {k: v for k, v in mapping.items() if filter_condition(k, v)}

    return datacube.rename_labels(
        dimension="bands", target=list(mapping.values()), source=list(mapping.keys())
    )


def _load_collection_hybrid(
    connection: openeo.Connection,
    is_stac: bool,
    collection_id_or_url: str,
    bands: list,
    spatial_extent: Union[Dict[str, float], Parameter, None] = None,
    temporal_extent: Union[Sequence[InputDate], Parameter, str, None] = None,
    properties: Optional[dict] = None,
):
    """Wrapper around the load_collection, or load_stac method of the openeo connection."""
    if not is_stac:
        return connection.load_collection(
            collection_id=collection_id_or_url,
            spatial_extent=spatial_extent,
            temporal_extent=temporal_extent,
            bands=bands,
            properties=properties,
        )
    cube = connection.load_stac(
        url=collection_id_or_url,
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent,
        bands=bands,
        properties=properties,
    )
    cube = cube.rename_labels(dimension="bands", target=bands)
    return cube


# TODO; deprecated?
def _load_collection(
    connection: openeo.Connection,
    bands: list,
    collection_name: str,
    spatial_extent: SpatialContext,
    temporal_extent: Optional[TemporalContext],
    fetch_type: FetchType,
    is_stac: bool = False,
    **params,
):
    """Loads a collection from the openeo backend, acting differently depending
    on the fetch type.
    """
    load_collection_parameters = params.get("load_collection", {})
    load_collection_method = partial(
        _load_collection_hybrid, is_stac=is_stac, collection_id_or_url=collection_name
    )

    if (
        temporal_extent is not None
    ):  # Can be ignored for intemporal collections such as DEM
        temporal_extent = [temporal_extent.start_date, temporal_extent.end_date]

    if fetch_type == FetchType.TILE:
        assert isinstance(
            spatial_extent, BoundingBoxExtent
        ), "Please provide only a bounding box for tile based fetching."
        spatial_extent = dict(spatial_extent)
        cube = load_collection_method(
            connection=connection,
            bands=bands,
            spatial_extent=spatial_extent,
            temporal_extent=temporal_extent,
            properties=load_collection_parameters,
        )
    elif fetch_type == FetchType.POINT:
        assert isinstance(
            spatial_extent, GeoJSON
        ), "Please provide only a GeoJSON FeatureCollection for point based fetching."
        assert (
            spatial_extent["type"] == "FeatureCollection"
        ), "Please provide a FeatureCollection type of GeoJSON"
        cube = load_collection_method(
            connection=connection,
            bands=bands,
            spatial_extent=spatial_extent,
            temporal_extent=temporal_extent,
            properties=load_collection_parameters,
        )
    elif fetch_type == FetchType.POLYGON:
        if isinstance(spatial_extent, GeoJSON):
            assert (
                spatial_extent["type"] == "FeatureCollection"
            ), "Please provide a FeatureCollection type of GeoJSON"
        elif isinstance(spatial_extent, str):
            assert spatial_extent.startswith("https://") or spatial_extent.startswith(
                "http://"
            ), "Please provide a valid URL or a path to a GeoJSON file."
        else:
            raise ValueError(
                "Please provide a valid URL to a GeoParquet or GeoJSON file."
            )
        cube = load_collection_method(
            connection=connection,
            bands=bands,
            temporal_extent=temporal_extent,
            properties=load_collection_parameters,
        )

    # Adding the process graph updates for experimental features
    if params.get("update_arguments") is not None:
        cube.result_node().update_arguments(**params["update_arguments"])

    # Peforming pre-mask optimization
    pre_mask = params.get("pre_mask", None)
    if pre_mask is not None:
        assert isinstance(pre_mask, openeo.DataCube), (
            f"The 'pre_mask' parameter must be an openeo datacube, " f"got {pre_mask}."
        )
        cube = cube.mask(pre_mask)

    # Merges additional bands continuing the operations.
    pre_merge_cube = params.get("pre_merge", None)
    if pre_merge_cube is not None:
        assert isinstance(pre_merge_cube, openeo.DataCube), (
            f"The 'pre_merge' parameter value must be an openeo datacube, "
            f"got {pre_merge_cube}."
        )
        if pre_mask is not None:
            pre_merge_cube = pre_merge_cube.mask(pre_mask)
        cube = cube.merge_cubes(pre_merge_cube)

    if fetch_type == FetchType.POLYGON:
        if isinstance(spatial_extent, str):
            geometry = connection.load_url(
                spatial_extent,
                format="Parquet" if ".parquet" in spatial_extent else "GeoJSON",
            )
            cube = cube.filter_spatial(geometry)
        else:
            cube = cube.filter_spatial(spatial_extent)

    return cube
