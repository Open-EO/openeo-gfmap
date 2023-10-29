""" Common operations within collection extraction logic, such as reprojection.
"""
from typing import Optional, Union

import openeo
from rasterio import CRS
from rasterio.errors import CRSError


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
    datacube: openeo.DataCube, resolution: float, epsg_code: Optional[Union[str, int]]
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
        return datacube.resample_spatial(resolution=resolution, projection=epsg_code)
    return datacube.resample_spatial(resolution=resolution)


def rename_bands(datacube: openeo.DataCube, mapping: dict) -> openeo.DataCube:
    """Rename the bands from the given mapping scheme"""
    # Filter out bands that are not part of the datacube
    print(datacube.dimension_labels("bands"))

    def filter_condition(band_name, _):
        return band_name in datacube.metadata.band_names

    mapping = {k: v for k, v in mapping.items() if filter_condition(k, v)}

    return datacube.rename_labels(
        dimension="bands", target=list(mapping.values()), source=list(mapping.keys())
    )
