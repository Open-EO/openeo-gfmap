"""Meteo data fetchers."""

from functools import partial

import openeo
from geojson import GeoJSON

from openeo_gfmap import (
    Backend,
    BackendContext,
    FetchType,
    SpatialContext,
    TemporalContext,
)
from openeo_gfmap.fetching import CollectionFetcher
from openeo_gfmap.fetching.commons import convert_band_names
from openeo_gfmap.fetching.generic import (
    _get_generic_fetcher,
    _get_generic_processor,
    _load_collection,
)

WEATHER_MAPPING_TERRASCOPE = {
    "dewpoint-temperature": "AGERA5-DEWTEMP",
    "precipitation-flux": "AGERA5-PRECIP",
    "solar-radiation-flux": "AGERA5-SOLRAD",
    "temperature-max": "AGERA5-TMAX",
    "temperature-mean": "AGERA5-TMEAN",
    "temperature-min": "AGERA5-TMIN",
    "vapour-pressure": "AGERA5-VAPOUR",
    "wind-speed": "AGERA5-WIND",
}

WEATHER_MAPPING_STAC = {
    "dewpoint_temperature_mean": "AGERA5-DEWTEMP",
    "total_precipitation": "AGERA5-PRECIP",
    "solar_radiataion_flux": "AGERA5-SOLRAD",
    "2m_temperature_max": "AGERA5-TMAX",
    "2m_temperature_mean": "AGERA5-TMEAN",
    "2m_temperature_min": "AGERA5-TMIN",
    "vapour_pressure": "AGERA5-VAPOUR",
    "wind_speed": "AGERA5-WIND",
}


def stac_fetcher(
    connection: openeo.Connection,
    spatial_extent: SpatialContext,
    temporal_extent: TemporalContext,
    bands: list,
    fetch_type: FetchType,
    **params,
) -> openeo.DataCube:
    bands = convert_band_names(bands, WEATHER_MAPPING_STAC)

    cube = _load_collection(
        connection,
        bands,
        "https://stac.openeo.vito.be/collections/agera5_daily",
        spatial_extent,
        temporal_extent,
        fetch_type,
        is_stac=True,
        **params,
    )

    if isinstance(spatial_extent, GeoJSON) and fetch_type == FetchType.POLYGON:
        cube = cube.filter_spatial(spatial_extent)

    return cube


METEO_BACKEND_MAP = {
    Backend.TERRASCOPE: {
        "fetch": partial(
            _get_generic_fetcher,
            collection_name="AGERA5",
            band_mapping=WEATHER_MAPPING_TERRASCOPE,
        ),
        "preprocessor": partial(
            _get_generic_processor,
            collection_name="AGERA5",
            band_mapping=WEATHER_MAPPING_TERRASCOPE,
        ),
    },
    Backend.CDSE: {
        "fetch": stac_fetcher,
        "preprocessor": partial(
            _get_generic_processor,
            collection_name="AGERA5",
            band_mapping=WEATHER_MAPPING_STAC,
        ),
    },
    Backend.CDSE_STAGING: {
        "fetch": stac_fetcher,
        "preprocessor": partial(
            _get_generic_processor,
            collection_name="AGERA5",
            band_mapping=WEATHER_MAPPING_STAC,
        ),
    },
    Backend.FED: {
        "fetch": stac_fetcher,
        "preprocessor": partial(
            _get_generic_processor,
            collection_name="AGERA5",
            band_mapping=WEATHER_MAPPING_STAC,
        ),
    },
}


def build_meteo_extractor(
    backend_context: BackendContext,
    bands: list,
    fetch_type: FetchType,
    **params,
) -> CollectionFetcher:
    backend_functions = METEO_BACKEND_MAP.get(backend_context.backend)

    fetcher, preprocessor = (
        partial(backend_functions["fetch"], fetch_type=fetch_type),
        backend_functions["preprocessor"](fetch_type=fetch_type),
    )

    return CollectionFetcher(backend_context, bands, fetcher, preprocessor, **params)
