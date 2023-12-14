"""Different cloud masking strategies for an OpenEO datacubes."""

import openeo

SCL_HARMONIZED_NAME: str = "S2-SCL"

def mask_scl_dilation(cube: openeo.DataCube, **params: dict) -> openeo.DataCube:
    """Creates a mask from the SCL, dilates it and applies the mask to the optical
    bands of the datacube. The other bands such as DEM, SAR and METEO will not
    be affected by the mask.
    """
    # Asserts if the SCL layer exists
    assert SCL_HARMONIZED_NAME in cube.metadata.band_names, (
        f"The SCL band ({SCL_HARMONIZED_NAME}) is not present in the datacube."
    )

    kernel1_size = params.get("kernel1_size", 17)
    kernel2_size = params.get("kernel2_size", 3)
    erosion_kernel_size = params.get("erosion_kernel_size", 3)

    # TODO adapt the dilation size given the mask size in meters
    # TODO check how to get the spatial resolution from the cube metadata

    # Only applies the filtering to the optical part of the cube
    optical_cube = cube.filter_bands(
        bands=list(
            filter(lambda band: band.startswith("S2"), cube.metadata.band_names)
        )
    )

    nonoptical_cube = cube.filter_bands(
        bands=list(
            filter(lambda band: not band.startswith("S2"), cube.metadata.band_names)
        )
    )

    optical_cube = optical_cube.process(
        "mask_scl_dilation",
        data=optical_cube,
        scl_band_name=SCL_HARMONIZED_NAME,
        kernel1_size=kernel1_size,
        kernel2_size=kernel2_size,
        mask1_values=[2, 4, 5, 6, 7],
        mask2_values=[3, 8, 9, 10, 11],
        erosion_kernel_size=erosion_kernel_size
    )

    if len(nonoptical_cube.metadata.band_names) == 0:
        return optical_cube

    return optical_cube.merge_cubes(nonoptical_cube)
