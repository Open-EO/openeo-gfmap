"""Routines to pre-process sar signals."""

import openeo
from openeo.processes import array_create, power


def compress_backscatter_uint16(cube: openeo.DataCube) -> openeo.DataCube:
    """
    Scaling the bands from float32 power values to uint16 for memory optimization. The scaling
    casts the values from power to decibels and applies a linear scaling from 0 to 65534.

    The resulting datacube has a uint16 memory representation which makes an optimization
    before passing through any UDFs.

    Parameters
    ----------
    cube : openeo.DataCube
        The datacube to compress the backscatter values.
    Returns
    -------
    openeo.DataCube
        The datacube with the backscatter values compressed to uint16.
    """

    # Apply rescaling of power values in a logarithmic way
    cube = cube.apply_dimension(
        dimension="bands",
        process=lambda x: array_create(
            [
                power(base=10, p=(10.0 * x[0].log(base=10) + 83.0) / 20.0),
                power(base=10, p=(10.0 * x[1].log(base=10) + 83.0) / 20.0),
            ]
        ),
    )

    # Change the data type to uint16 for optimization purposes
    return cube.linear_scale_range(1, 65534, 1, 65534)
