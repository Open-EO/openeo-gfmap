import pytest
from openeo.extra.job_management import MultiBackendJobManager

from openeo_gfmap.backend import Backend, add_backend_to_job_manager


def test_backend_unique_names():
    """
    Test that all backends have unique names.
    """

    backend_names = [backend.name for backend in Backend]
    assert len(backend_names) == len(
        set(backend_names)
    ), "Backend names are not unique."


@pytest.mark.parametrize(
    "backend, expected_name, expected_url",
    [
        (Backend.CDSE, "CDSE", "openeo.dataspace.copernicus.eu"),
        (
            Backend.CDSE_STAGING,
            "CDSE-STAGING",
            "openeo-staging.dataspace.copernicus.eu",
        ),
        (
            Backend.CDSE_OTC,
            "CDSE-OTC",
            "https://openeo.prod.amsterdam.openeo.dataspace.copernicus.eu/",
        ),
        (Backend.TERRASCOPE, "TERRASCOPE", "openeo.vito.be"),
        (Backend.TERRASCOPE_DEV, "TERRASCOPE-DEV", "openeo-dev.vito.be"),
        (Backend.OPENEO_CLOUD, "OPENEO-CLOUD", "openeo.cloud"),
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


def test_backend_from_backend_name():
    """
    Test that the backend can be created from the backend name.
    """
    backend_name = "CDSE"
    backend = Backend.from_backend_name(backend_name)

    assert isinstance(backend, Backend), f"Expected Backend, got {type(backend)}"
    assert backend == Backend.CDSE, f"Expected backend {Backend.CDSE}, got {backend}"


def test_backend_from_backend_name_invalid():
    """
    Test that an error is raised when the backend name is invalid.
    """
    backend_name = "INVALID_BACKEND_NAME"

    with pytest.raises(ValueError, match=f"Unknown backend name: {backend_name}"):
        Backend.from_backend_name(backend_name)


def test_add_backend_to_job_manager(con_client_creds, tmp_path):
    """
    Test that the backend can be added to a job manager.
    """
    job_manager = MultiBackendJobManager(root_dir=tmp_path)

    add_backend_to_job_manager(job_manager=job_manager, backend=Backend.TEST)

    assert job_manager.backends.keys() == {
        Backend.TEST.name
    }, f"Expected backend {Backend.TEST.name}, got {job_manager.backends.keys()}"
