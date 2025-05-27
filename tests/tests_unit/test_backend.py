import openeo
import pytest
from openeo.rest._testing import build_capabilities

from openeo_gfmap.backend import _BackendType


def test_backend_unique_names():
    """
    Test that all backends have unique names.
    """

    backend_names = [backend.name for backend in _BackendType]
    assert len(backend_names) == len(
        set(backend_names)
    ), "Backend names are not unique."
    for name in backend_names:
        assert name.isupper(), f"Backend name {name} is not uppercase."
        assert "_" not in name, f"Backend name {name} contains underscores."
        assert len(name) > 0, "Backend name is empty."


def test_backend_unique_url_domains():
    """
    Test that all backends have unique URL domains.
    """

    backend_url_domains = [backend.value.url_domain for backend in _BackendType]

    assert len(backend_url_domains) == len(
        set(backend_url_domains)
    ), "Backend URLs are not unique."


def test_backend_default_urls():
    """
    Test that all backends have a valid default URL.
    """

    for url in [backend.default_url for backend in _BackendType]:
        assert url.startswith(
            "https://"
        ), f"Backend URL {url} does not start with https://"
        assert url.endswith(
            "/"
        ), f"Backend URL {url} ends with /, which is not allowed."
        assert url.islower(), f"Backend URL {url} is not lowercase."


def test_backend_list_backends():
    """
    Test that the list of backends is correct.
    """
    backend_names = _BackendType.list_backends()
    assert len(backend_names) == len(
        _BackendType
    ), f"Expected {len(_BackendType)} backends, got {len(backend_names)}"
    assert set(backend_names) == set(
        [backend.name for backend in _BackendType]
    ), f"Expected backends {_BackendType}, got {backend_names}"


@pytest.mark.parametrize(
    "backend_name, expected_backend",
    [
        ("CDSE", _BackendType.CDSE),
        ("cdse", _BackendType.CDSE),
    ],
)
def test_backend_from_backend_name(backend_name, expected_backend):
    """
    Test that the backend can be created from the backend name (case and dash/underscore insensitive).
    """
    backend = _BackendType.from_backend_name(backend_name)

    assert isinstance(backend, _BackendType), f"Expected Backend, got {type(backend)}"
    assert (
        backend == expected_backend
    ), f"Expected backend {expected_backend}, got {backend}"


def test_backend_from_backend_name_invalid():
    """
    Test that an error is raised when the backend name is invalid.
    """
    backend_name = "INVALID_BACKEND_NAME"

    with pytest.raises(ValueError, match=f"Unknown backend name: {backend_name}"):
        _BackendType.from_backend_name(backend_name)


def mock_con(url, requests_mock, api_capabilities):
    """
    Fixture to create a connection to the dummy backend using openeo 1.2.0.

    This is used for testing the job manager with a dummy backend.

    Inspired by the tests in https://github.com/Open-EO/openeo-python-client.
    """
    requests_mock.get(
        url, json=build_capabilities(api_version="1.2.0", **api_capabilities)
    )
    requests_mock.get(
        url + "udf_runtimes",
        json={
            "Python": {
                "type": "language",
                "default": "3",
                "versions": {"3": {"libraries": {}}},
            },
        },
    )
    return openeo.Connection(url)


@pytest.mark.parametrize(
    "url, expected_backend",
    [
        ("https://openeo.dataspace.copernicus.eu/", _BackendType.CDSE),
        ("https://openeo-staging.dataspace.copernicus.eu/", _BackendType.CDSE),
        ("https://openeo-dev.dataspace.copernicus.eu/", _BackendType.CDSE),
        ("https://openeofed.dataspace.copernicus.eu/", _BackendType.CDSE),
        ("https://openeo.vito.be/", _BackendType.TERRASCOPE),
        ("https://openeo-dev.vito.be/", _BackendType.TERRASCOPE),
        ("https://openeo.cloud/", _BackendType.OPENEO_CLOUD),
        ("https://oeo.test/", _BackendType.TEST),
    ],
)
def test_backend_from_openeo_connection(
    url, expected_backend, requests_mock, api_capabilities
):
    """
    Test that the backend can be created from an openeo connection.
    """
    con = mock_con(url, requests_mock, api_capabilities)
    backend = _BackendType.from_openeo_connection(con)

    assert isinstance(backend, _BackendType), f"Expected Backend, got {type(backend)}"
    assert (
        backend == expected_backend
    ), f"Expected backend {expected_backend}, got {backend}"
