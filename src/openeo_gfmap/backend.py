"""Backend Contct.

Defines on which backend the pipeline is being currently used.
"""
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, Optional

import openeo


class Backend(Enum):
    """Enumerating the backends supported by the Mapping Framework."""

    TERRASCOPE = "terrascope"
    EODC = "eodc"  # Dask implementation. Do not test on this yet.
    CDSE = "cdse"  # Terrascope implementation (pyspark) #URL: openeo.dataspace.copernicus.eu (need to register)
    LOCAL = "local"  # Based on the same components of EODc


@dataclass
class BackendContext:
    """Backend context and information.

    Containing backend related information useful for the framework to
    adapt the process graph.
    """

    backend: Backend


def vito_connection(capfd: Optional = None) -> openeo.Connection:
    """Performs a connection to the VITO backend using the oidc authentication."""
    # Note: this generic `authenticate_oidc()` call allows both:
    # - device code/refresh token based authentication for manual test
    #   suiteruns by a developer
    # - client credentials auth through env vars for automated/Jenkins CI runs
    #
    # See https://open-eo.github.io/openeo-python-client/auth.html#oidc-authentication-dynamic-method-selection  # NOQA
    # and Jenkinsfile, where Jenkins fetches the env vars from VITO TAP Vault.
    connection = openeo.connect("openeo.vito.be")

    if capfd is not None:
        with capfd.disabled():
            # Temporarily disable output capturing, to make sure that OIDC device
            # code instructions (if any) are shown.
            connection.authenticate_oidc()
    else:
        connection.authenticate_oidc()
    return connection


def cdse_connection(capfd: Optional = None) -> openeo.Connection:
    """Performs a connection to the CDSE backend using oidc authentication."""
    connection = openeo.connect(
        "https://openeo.dataspace.copernicus.eu/openeo/1.2"
    )
    if capfd is not None:
        with capfd.disabled():
            # Temporarily disable output capturing, to make sure that OIDC device
            # code instructions (if any) are shown.
            connection.authenticate_oidc()
    else:
        connection.authenticate_oidc()
    return connection


def eodc_connection(capfd: Optional = None) -> openeo.Connection:
    """Perfroms a connection to the EODC backend using the oidc authentication."""
    connection = openeo.connect("https://openeo.eodc.eu/openeo/1.1.0")
    if capfd is not None:
        with capfd.disabled():
            # Temporarily disable output capturing, to make sure that OIDC device
            # code instructions (if any) are shown.
            connection.authenticate_oidc()
    else:
        connection.authenticate_oidc()
    return connection


BACKEND_CONNECTIONS: Dict[Backend, Callable] = {
    Backend.TERRASCOPE: vito_connection,
    Backend.CDSE: cdse_connection,
}
