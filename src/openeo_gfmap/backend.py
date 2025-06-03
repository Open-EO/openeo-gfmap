"""
This module provides a set of predefined backend Groups.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Union
from urllib.parse import urlparse

import openeo

_log = logging.getLogger(__name__)


@dataclass
class _BackendInfo:
    """
    Dataclass that holds information about a backend
    Can be expanded with new fields if needed.

    This class should only be used internally in this module. Getting the backend name and url should be done through the _BackendGroup Enum.
    """

    name: str
    default_full_url: str
    url_domain: str


class _BackendGroup(Enum):
    """
    Enum class that holds backend groups and their default url's and url domains. Can be used to make behavior dependent on the backend group.
    """

    CDSE = _BackendInfo(
        name="CDSE",
        default_full_url="https://openeo.dataspace.copernicus.eu/",
        url_domain="dataspace.copernicus.eu",
    )
    TERRASCOPE = _BackendInfo(
        name="TERRASCOPE",
        default_full_url="https://openeo.vito.be/",
        url_domain="vito.be",
    )
    OPENEO_CLOUD = _BackendInfo(
        name="OPENEO-CLOUD",
        default_full_url="https://openeo.cloud/",
        url_domain="openeo.cloud",
    )
    TEST = _BackendInfo(
        name="TEST", default_full_url="https://oeo.test/", url_domain="oeo.test"
    )  # only for testing purposes

    @property
    def name(self) -> str:
        """Get the name of the backend group."""
        return self.value.name

    @property
    def default_url(self) -> str:
        """Get the URL of the backend."""
        return self.value.default_full_url

    @classmethod
    def list_backends(cls) -> list[str]:
        """
        Get a list of all backend groups.

        :return: A list of backend groups.
        """
        return [backend.name for backend in cls]

    @classmethod
    def from_backend_name(cls, backend_name: str) -> _BackendGroup:
        """
        Get the backend group from the backend name.

        :param backend_name: The name of the backend group.
        :return: The _BackendGroup object.
        """
        normalized_name = backend_name.replace("_", "-").upper()
        for backend in cls:
            if backend.name == normalized_name:
                return backend
        raise ValueError(f"Unknown backend name: {backend_name}")

    @classmethod
    def from_openeo_connection(cls, connection: openeo.Connection) -> _BackendGroup:
        """
        Get the backend group from an openeo connection.

        :param connection: The openeo connection.
        :return: The _BackendGroup object.
        """
        for backend in cls:
            parsed_url = urlparse(connection._orig_url)
            domain = parsed_url.hostname or ""
            if domain.endswith(backend.value.url_domain):
                return backend
        raise ValueError(f"Unknown backend URL domain: {connection._orig_url}")

    @staticmethod
    def _resolve_backend(backend: Union[str, _BackendGroup]) -> _BackendGroup:
        """
        Resolve the backend from a string or Backend object.

        :param backend: The backend to resolve.
        :return: The resolved _BackendGroup object.
        """
        if isinstance(backend, str):
            return _BackendGroup.from_backend_name(backend)
        return backend
