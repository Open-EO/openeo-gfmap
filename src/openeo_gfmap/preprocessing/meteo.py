import openeo


def compress_meteo_uint16(cube: openeo.DataCube) -> openeo.DataCube:
    """Compresses the meteo data in float32 value to uint16 for memory optimization.
    The data is multiplied by 100 for a better representation in uint16.

    Parameters
    ----------
    cube : openeo.DataCube
        The input datacube to compress, only meteo data should be present.

    Returns
    -------
    cube : openeo.DataCube
        The datacube with the meteo data compressed to uint16 and rescaled by a factor 100.
    """
    cube = cube * 100

    return cube.linear_scale_range(0, 65534, 0, 65534)
