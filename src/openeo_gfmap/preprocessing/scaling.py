"""Scaling and compressing methods for datacubes."""

import openeo


def _compress(
    cube: openeo.DataCube,
    min_val: int,
    max_val: int,
    alpha: float,
    beta: float,
):
    if (
        alpha != 1.0 or beta != 0.0
    ):  # Avoid adding a node in the computing graph if scaling is not necessary
        cube = (cube * alpha) + beta

    return cube.linear_scale_range(min_val, max_val, min_val, max_val)


def compress_uint16(
    cube: openeo.DataCube, alpha: float = 1.0, beta: float = 0.0
) -> openeo.DataCube:
    """Scales the data linearly using the formula `output = (input * a) + b` and compresses values
    from float32 to uint16 for memory optimization.

    Parameters
    ----------
    cube : openeo.DataCube
        The input datacube to compress, only meteo data should be present.
    alpha : float, optional (default=1.0)
        The scaling factor. Values in the input datacube are multiplied by this coefficient.
    beta : float, optional (default=0.0)
        The offset. Values in the input datacube are added by this value.

    Returns
    -------
    cube : openeo.DataCube
        The datacube with the data linearly scaled and compressed to uint16 and rescaled frome.
    """
    return _compress(cube, 0, 65534, alpha, beta)


def compress_uint8(
    cube: openeo.DataCube, alpha: float = 1.0, beta: float = 0.0
) -> openeo.DataCube:
    """
    Scales the data linearly using the formula `output = (input * a) + b` and compresses values
    from float32 to uint8 for memory optimization.

    Parameters
    ----------
    cube : openeo.DataCube
        The input datacube to compress, only meteo data should be present.
    alpha : float, optional (default=1.0)
        The scaling factor. Values in the input datacube are multiplied by this coefficient.
    beta : float, optional (default=0.0)
        The offset. Values in the input datacube are added by this value.

    Returns
    -------
    cube : openeo.DataCube
        The datacube with the data linearly scaled and compressed to uint8 and rescaled frome.
    """
    return _compress(cube, 0, 253, alpha, beta)
