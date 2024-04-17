import numpy as np
import xarray as xr
from openeo.udf import XarrayDataCube


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """
    Computes a cloud mask covering a full observation or nothing depending on the percentage of
    high probability cloud pixels. If the amount of before mentioned pixels is higher than 95%,
    then returns a mask covering the whole observation, otherwise returns an empty mask.
    """
    array = cube.get_array().transpose("t", "bands", "y", "x")

    output_array = np.zeros(
        shape=(array.shape[0], 1, array.shape[2], array.shape[3]), dtype=np.uint8
    )

    for i in range(array.shape[0]):
        high_proba_count = ((array[i] == 9) * 1).sum()
        high_proba_percentage = high_proba_count / (array.shape[2] * array.shape[3])

        if high_proba_percentage > 0.95:
            output_array[i] = 1

    output_array = xr.DataArray(
        output_array,
        dims=["t", "bands", "y", "x"],
        coords={
            "t": array.t,
            "bands": ["mask"],
            "y": array.y,
            "x": array.x,
        },
    )

    return XarrayDataCube(output_array)
