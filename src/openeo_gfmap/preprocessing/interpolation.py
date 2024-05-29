"""Utilities to perform interpolation on missing values using the temporal
dimension.
"""

import openeo


def linear_interpolation(
    cube: openeo.DataCube,
) -> openeo.DataCube:
    """Perform linear interpolation on the given datacube."""
    return cube.apply_dimension(dimension="t", process="array_interpolate_linear")
