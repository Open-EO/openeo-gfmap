import numpy as np
import xarray as xr
from openeo.udf import XarrayDataCube


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """For a cube having the BAP score, and a given period of list of intervals,
    create a binary mask that for each pixel, is True if the BAP score is the
    best within the given interval. The output has the same dimensions as the
    input, but only has binary values.

    This UDF do not support yet the implementation of string periods such as
    "month", "dekad", etc...
    """
    # First check if the period is defined in the context
    intervals = context.get("intervals", None)
    array = cube.get_array().transpose("t", "bands", "y", "x")

    bap_score = array.sel(bands=["S2-L2A-BAPSCORE"])

    def select_maximum(score: xr.DataArray):
        max_score = score.max(dim="t")
        return score == max_score

    if isinstance(intervals, str):
        raise NotImplementedError(
            "Period as string is not implemented yet, please provide a list of interval tuples."
        )
    elif isinstance(intervals, list):
        # Convert YYYY-mm-dd to datetime64 objects
        time_bins = [np.datetime64(interval[0]) for interval in intervals]

        rank_mask = bap_score.groupby_bins("t", bins=time_bins).map(select_maximum)
    else:
        raise ValueError("Period is not defined in the UDF. Cannot run it.")

    return XarrayDataCube(rank_mask)
