"""Backend Contct.

Defines on which backend the pipeline is being currently used.
"""

import logging
import os
from typing import Optional

import openeo
from openeo.extra.job_management import MultiBackendJobManager

_log = logging.getLogger(__name__)

BACKEND_URLS = {
    "CDSE": "openeo.dataspace.copernicus.eu",
    "CDSE-STAGING": "openeo-staging.dataspace.copernicus.eu",
    "TERRASCOPE": "openeo.vito.be",
    "TERRASCOPE-DEV": "openeo-dev.vito.be",
    "OPENEO-CLOUD": "openeo.cloud",
    "OTC": "https://openeo.prod.amsterdam.openeo.dataspace.copernicus.eu/",  # only available on vito VPN
}


def get_connection(backend: str) -> openeo.Connection:
    """
    Get the connection to a backend.

    :param backend: The backend to connect to.
    :return: The connection to the backend.
    """
    if backend not in BACKEND_URLS:
        raise ValueError(
            f"Unknown backend: {backend}. Supported backends are: {BACKEND_URLS.keys()}"
        )
    url = BACKEND_URLS[backend]
    connection = _create_connection(url, env_var_suffix=backend)
    return connection


def add_backend_to_jobmanager(
    job_manager: MultiBackendJobManager,
    backend: str,
    parallel_jobs: Optional[int] = None,
) -> None:
    """
    Add a backend to the job_manager.

    :param job_manager: The job_manager to add the backend to.
    :param backend: The backend to add.
    :param parallel_jobs: The number of parallel jobs to allow on this backend.
    :return: None
    """
    connection = get_connection(backend)
    if parallel_jobs is not None:
        job_manager.add_backend(
            name=backend, connection=connection, parallel_jobs=parallel_jobs
        )
    else:
        job_manager.add_backend(name=backend, connection=connection)


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
