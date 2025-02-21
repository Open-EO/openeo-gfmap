"""Backend Contct.

Defines on which backend the pipeline is being currently used.
"""

import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, Optional

import openeo

_log = logging.getLogger(__name__)


class Backend(Enum):
    """Enumerating the backends supported by the Mapping Framework."""

    TERRASCOPE = "terrascope"
    EODC = "eodc"  # Dask implementation. Do not test on this yet.
    CDSE = "cdse"  # Terrascope implementation (pyspark) #URL: openeo.dataspace.copernicus.eu (need to register)
    CDSE_STAGING = "cdse-staging"
    LOCAL = "local"  # Based on the same components of EODc
    FED = "fed"  # Federation backend


@dataclass
class BackendContext:
    """Backend context and information.

    Containing backend related information useful for the framework to
    adapt the process graph.
    """

    backend: Backend


def _create_connection(
    url: str, *, env_var_suffix: str, connect_kwargs: Optional[dict] = None
):
    """
    Generic helper to create an openEO connection
    with support for multiple client credential configurations from environment variables
    """
    connection = openeo.connect(url, **(connect_kwargs or {}))

    if (
        os.environ.get("OPENEO_AUTH_METHOD") == "client_credentials"
        and f"OPENEO_AUTH_CLIENT_ID_{env_var_suffix}" in os.environ
    ):
        # Support for multiple client credentials configs from env vars
        client_id = os.environ[f"OPENEO_AUTH_CLIENT_ID_{env_var_suffix}"]
        client_secret = os.environ[f"OPENEO_AUTH_CLIENT_SECRET_{env_var_suffix}"]
        provider_id = os.environ.get(f"OPENEO_AUTH_PROVIDER_ID_{env_var_suffix}")
        _log.info(
            f"Doing client credentials from env var with {env_var_suffix=} {provider_id} {client_id=} {len(client_secret)=} "
        )

        connection.authenticate_oidc_client_credentials(
            client_id=client_id, client_secret=client_secret, provider_id=provider_id
        )
    else:
        # Standard authenticate_oidc procedure: refresh token, device code or default env var handling
        # See https://open-eo.github.io/openeo-python-client/auth.html#oidc-authentication-dynamic-method-selection

        # Use a shorter max poll time by default to alleviate the default impression that the test seem to hang
        # because of the OIDC device code poll loop.
        max_poll_time = int(
            os.environ.get("OPENEO_OIDC_DEVICE_CODE_MAX_POLL_TIME") or 30
        )
        connection.authenticate_oidc(max_poll_time=max_poll_time)
    return connection


def vito_connection() -> openeo.Connection:
    """Performs a connection to the VITO backend using the oidc authentication."""
    return _create_connection(
        url="openeo.vito.be",
        env_var_suffix="VITO",
    )


def cdse_connection() -> openeo.Connection:
    """Performs a connection to the CDSE backend using oidc authentication."""
    return _create_connection(
        url="openeo.dataspace.copernicus.eu",
        env_var_suffix="CDSE",
    )


def cdse_staging_connection() -> openeo.Connection:
    """Performs a connection to the CDSE backend using oidc authentication."""
    return _create_connection(
        url="openeo-staging.dataspace.copernicus.eu",
        env_var_suffix="CDSE_STAGING",
    )


def eodc_connection() -> openeo.Connection:
    """Perfroms a connection to the EODC backend using the oidc authentication."""
    return _create_connection(
        url="https://openeo.eodc.eu/openeo/1.1.0",
        env_var_suffix="EODC",
    )


def fed_connection() -> openeo.Connection:
    """Performs a connection to the OpenEO federated backend using the oidc
    authentication."""
    return _create_connection(
        url="openeofed.dataspace.copernicus.eu/",
        env_var_suffix="FED",
    )


BACKEND_CONNECTIONS: Dict[Backend, Callable] = {
    Backend.TERRASCOPE: vito_connection,
    Backend.CDSE: cdse_connection,
    Backend.CDSE_STAGING: cdse_staging_connection,
    Backend.FED: fed_connection,
}
