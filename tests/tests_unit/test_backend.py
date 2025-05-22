import pytest

from openeo_gfmap.backend import Backend


def test_backend_unique_names():
    """
    Test that all backends have unique names.
    """

    backend_names = [backend.name for backend in Backend]
    assert len(backend_names) == len(
        set(backend_names)
    ), "Backend names are not unique."
    for name in backend_names:
        assert name.isupper(), f"Backend name {name} is not uppercase."
        assert "_" not in name, f"Backend name {name} contains underscores."
        assert len(name) > 0, "Backend name is empty."


def test_backend_unique_urls():
    """
    Test that all backends have unique URLs.
    """

    backend_urls = [backend.url for backend in Backend]

    for url in backend_urls:
        assert url.startswith(
            "https://"
        ), f"Backend URL {url} does not start with https://"
        assert url.endswith(
            "/"
        ), f"Backend URL {url} ends with /, which is not allowed."
        assert url.islower(), f"Backend URL {url} is not lowercase."

    assert len(backend_urls) == len(set(backend_urls)), "Backend URLs are not unique."


def test_backend_list_backends():
    """
    Test that the list of backends is correct.
    """
    backend_names = Backend.list_backends()
    assert len(backend_names) == len(
        Backend
    ), f"Expected {len(Backend)} backends, got {len(backend_names)}"
    assert set(backend_names) == set(
        [backend.name for backend in Backend]
    ), f"Expected backends {Backend}, got {backend_names}"


@pytest.mark.parametrize(
    "backend, expected_name, expected_url",
    [
        (Backend.CDSE, "CDSE", "https://openeo.dataspace.copernicus.eu/"),
        (
            Backend.CDSE_STAGING,
            "CDSE-STAGING",
            "https://openeo-staging.dataspace.copernicus.eu/",
        ),
        (
            Backend.CDSE_OTC,
            "CDSE-OTC",
            "https://openeo.prod.amsterdam.openeo.dataspace.copernicus.eu/",
        ),
        (Backend.TERRASCOPE, "TERRASCOPE", "https://openeo.vito.be/"),
        (Backend.TERRASCOPE_DEV, "TERRASCOPE-DEV", "https://openeo-dev.vito.be/"),
        (Backend.OPENEO_CLOUD, "OPENEO-CLOUD", "https://openeo.cloud/"),
    ],
)
def test_backend_properties(backend, expected_name, expected_url):
    """
    Test that the backend properties are correct.
    """
    assert (
        backend.name == expected_name
    ), f"Expected name {expected_name}, got {backend.name}"
    assert (
        backend.url == expected_url
    ), f"Expected url {expected_url}, got {backend.url}"


@pytest.mark.parametrize(
    "backend_name, expected_backend",
    [
        ("CDSE", Backend.CDSE),
        ("cdse", Backend.CDSE),
        ("CDSE_STAGING", Backend.CDSE_STAGING),
        ("CDSE-staging", Backend.CDSE_STAGING),
    ],
)
def test_backend_from_backend_name(backend_name, expected_backend):
    """
    Test that the backend can be created from the backend name (case and dash/underscore insensitive).
    """
    backend = Backend.from_backend_name(backend_name)

    assert isinstance(backend, Backend), f"Expected Backend, got {type(backend)}"
    assert (
        backend == expected_backend
    ), f"Expected backend {expected_backend}, got {backend}"


def test_backend_from_backend_name_invalid():
    """
    Test that an error is raised when the backend name is invalid.
    """
    backend_name = "INVALID_BACKEND_NAME"

    with pytest.raises(ValueError, match=f"Unknown backend name: {backend_name}"):
        Backend.from_backend_name(backend_name)


def test_backend_from_openeo_connection(con):
    """
    Test that the backend can be created from an openeo connection.
    """
    backend = Backend.from_openeo_connection(con)

    assert isinstance(backend, Backend), f"Expected Backend, got {type(backend)}"
    assert backend == Backend.TEST, f"Expected backend {Backend.CDSE}, got {backend}"
