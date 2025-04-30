"""
This module provides a set of predefined backends and helper functions to create connections to them or add them to a job manager.
"""

import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Union

import openeo
from openeo.extra.job_management import MultiBackendJobManager

_log = logging.getLogger(__name__)


@dataclass
class BackendInfo:
    """
    Dataclass that holds information about a backend
    Can be expanded with new fields if needed.

    This class should only be used internally in this module. Getting the backend name and url should be done through the Backend Enum.
    """

    name: str
    url: str


class Backend(Enum):
    """
    Class that holds the backend names and urls. Can be used to get information about specific backends.
    """

    CDSE = BackendInfo(name="CDSE", url="openeo.dataspace.copernicus.eu")
    CDSE_STAGING = BackendInfo(
        name="CDSE-STAGING", url="openeo-staging.dataspace.copernicus.eu"
    )
    TERRASCOPE = BackendInfo(name="TERRASCOPE", url="openeo.vito.be")
    TERRASCOPE_DEV = BackendInfo(name="TERRASCOPE-DEV", url="openeo-dev.vito.be")
    OPENEO_CLOUD = BackendInfo(name="OPENEO-CLOUD", url="openeo.cloud")
    OTC = BackendInfo(
        name="OTC", url="https://openeo.prod.amsterdam.openeo.dataspace.copernicus.eu/"
    )  # only available on vito VPN

    @property
    def name(self) -> str:
        """Get the name of the backend."""
        return self.value.name

    @property
    def url(self) -> str:
        """Get the URL of the backend."""
        return self.value.url

    @staticmethod
    def from_backend_name(backend_name: str) -> "Backend":
        """
        Get the backend from the backend name.

        :param backend_name: The name of the backend.
        :return: The Backend object.
        """
        for backend in Backend:
            if backend.name == backend_name:
                return backend
        raise ValueError(f"Unknown backend name: {backend_name}")


def add_backend_to_job_manager(
    job_manager: MultiBackendJobManager,
    backend: Union[Backend, str],
    parallel_jobs: Optional[int] = None,
) -> None:
    """
    Add a backend to the job_manager.

    :param job_manager: The job_manager to add the backend to.
    :param backend: The backend to add. Can be a Backend object or a string with the backend name.
    :param parallel_jobs: The number of parallel jobs to allow on this backend.
    :return: None
    """
    if isinstance(backend, str):
        backend = Backend.from_backend_name(backend)
    connection = get_connection(backend)
    if parallel_jobs is not None:
        job_manager.add_backend(
            name=backend.name, connection=connection, parallel_jobs=parallel_jobs
        )
    else:
        job_manager.add_backend(name=backend.name, connection=connection)


def get_connection(backend: Union[Backend, str]) -> openeo.Connection:
    """
    Get the connection to a backend.

    :param backend: The backend to connect to. Can be a Backend object or a string with the backend name.
    :return: The connection to the backend.
    """
    if isinstance(backend, str):
        backend = Backend.from_backend_name(backend)

    url = backend.url
    return _create_connection(url, env_var_suffix=backend)


def _create_connection(
    url: str, *, env_var_suffix: str, connect_kwargs: Optional[dict] = None
) -> openeo.Connection:
    """
    Generic helper to create an openEO connection
    with support for multiple client credential configurations from environment variables

    :param url: The URL of the backend to connect to.
    :param env_var_suffix: The suffix to use for the environment variables.
    :param connect_kwargs: Additional keyword arguments to pass to the connection.
    :return: The connection to the backend.
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
