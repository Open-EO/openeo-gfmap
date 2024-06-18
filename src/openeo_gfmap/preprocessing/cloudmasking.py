"""Different cloud masking strategies for an OpenEO datacubes."""

from pathlib import Path
from typing import Union

import openeo
from openeo.processes import if_, is_nan

SCL_HARMONIZED_NAME: str = "S2-L2A-SCL"
BAPSCORE_HARMONIZED_NAME: str = "S2-L2A-BAPSCORE"


def mask_scl_dilation(cube: openeo.DataCube, **params: dict) -> openeo.DataCube:
    """Creates a mask from the SCL, dilates it and applies the mask to the optical
    bands of the datacube. The other bands such as DEM, SAR and METEO will not
    be affected by the mask.
    """
    # Asserts if the SCL layer exists
    assert (
        SCL_HARMONIZED_NAME in cube.metadata.band_names
    ), f"The SCL band ({SCL_HARMONIZED_NAME}) is not present in the datacube."

    kernel1_size = params.get("kernel1_size", 17)
    kernel2_size = params.get("kernel2_size", 3)
    erosion_kernel_size = params.get("erosion_kernel_size", 3)

    # TODO adapt the dilation size given the mask size in meters
    # TODO check how to get the spatial resolution from the cube metadata

    # Only applies the filtering to the optical part of the cube
    optical_cube = cube.filter_bands(
        bands=list(filter(lambda band: band.startswith("S2"), cube.metadata.band_names))
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
        erosion_kernel_size=erosion_kernel_size,
    )

    if len(nonoptical_cube.metadata.band_names) == 0:
        return optical_cube

    return optical_cube.merge_cubes(nonoptical_cube)


def get_bap_score(cube: openeo.DataCube, **params: dict) -> openeo.DataCube:
    """Calculates the Best Available Pixel (BAP) score for the given datacube,
    computed from the SCL layer.

    The BAP score is calculated via using a UDF, which gives a lot of
    flexibility on the calculation methodology. The BAP score is a weighted
    average of three scores:
    * Distance-to-Cloud Score: Pixels that are clouds are given score 0.
      Pixels that are moren than 50 pixels - calculated with the Manhattan
      distance measure - away from a cloud pixel are given score 1. The pixels
      in between are given a score versus distance-to-cloud that follows a
      Gaussian shape.
    * Coverage Score: Per date, the percentage of all pixels that are classified
      as a cloud over the entire spatial extent is calculated. The Coverage
      Score is then equal to 1 - the cloud percentage.
    * Date Score: In order to favor pixels that are observed in the middle of a
      month, a date score is calculated, which follows a Gaussian shape. I.e.
      the largest scores are given for days in the middle of the month, the
      lowest scores are given for days at the beginning and end of the month.

    The final BAP score is a weighted average of the three aforementioned
    scores. The weights are 1, 0.5 and 0.8 for the Distance-to-Cloud, Coverage
    and Date Score respectively.

    Parameters
    ----------
    cube : openeo.DataCube
        The datacube to compute the BAP score from, only the SCL band is used.
    params : dict
        Addtional parameters to add to this routine.
        * `apply_scl_dilation`: Whether to apply dilation to the SCL mask before computing the BAP.
        * `kernel1_size`: The size of the first kernel used for the dilation of the SCL mask.
        * `kernel2_size`: The size of the second kernel used for the dilation of the SCL mask.
        * `erosion_kernel_size`: The size of the kernel used for the erosion of the SCL mask.

    Returns
    -------
    openeo.DataCube
        A 4D datacube containing the BAP score as name 'S2-L2A-BAPSCORE'.
    """
    udf_path = Path(__file__).parent / "udf_score.py"

    # Select the SCL band
    scl_cube = cube.filter_bands([SCL_HARMONIZED_NAME])

    if params.get("apply_scl_dilation", False):
        kernel1_size = params.get("kernel1_size", 17)
        kernel2_size = params.get("kernel2_size", 3)
        erosion_kernel_size = params.get("erosion_kernel_size", 3)

        scl_cube = scl_cube.process(
            "to_scl_dilation_mask",
            data=scl_cube,
            scl_band_name=SCL_HARMONIZED_NAME,
            kernel1_size=kernel1_size,
            kernel2_size=kernel2_size,
            mask1_values=[2, 4, 5, 6, 7],
            mask2_values=[3, 8, 9, 10, 11],
            erosion_kernel_size=erosion_kernel_size,
        )

    # Replace NaN to 0 to avoid issues in the UDF
    scl_cube = scl_cube.apply(lambda x: if_(is_nan(x), 0, x))

    score = scl_cube.apply_neighborhood(
        process=openeo.UDF.from_file(str(udf_path)),
        size=[
            {"dimension": "x", "unit": "px", "value": 256},
            {"dimension": "y", "unit": "px", "value": 256},
        ],
        overlap=[
            {"dimension": "x", "unit": "px", "value": 16},
            {"dimension": "y", "unit": "px", "value": 16},
        ],
    )

    score = score.rename_labels("bands", [BAPSCORE_HARMONIZED_NAME])

    # Merge the score to the scl cube
    return score


def get_bap_mask(cube: openeo.DataCube, period: Union[str, list], **params: dict):
    """Computes the bap score and masks the optical bands of the datacube using
    the best scores for each pixel on a given time period. This method both
    performs cloud masking but also a type of compositing.

    The BAP mask is computed using the method `get_bap_score`, from which the
    maximum argument is taken on every pixel on the given period. This will
    therefore return an array that for each optical observation, will return
    if the pixel must be loaded or not, allowing for high cost optimization.

    Parameters
    ----------
    cube : openeo.DataCube
        The datacube to be processed.
    period : Union[str, list]
        A string or a list of dates (in YYYY-mm-dd format) to be used as the
        temporal period to compute the BAP score.
    params : dict
        Additionals parameters, not used yet.
    Returns
    -------
    openeo.DataCube
        The datacube with the BAP mask applied.
    """
    # Checks if the S2-L2A-SCL band is present in the datacube
    assert (
        SCL_HARMONIZED_NAME in cube.metadata.band_names
    ), f"The {SCL_HARMONIZED_NAME} band is not present in the datacube."

    bap_score = get_bap_score(cube, **params)

    if isinstance(period, str):

        def max_score_selection(score):
            max_score = score.max()
            return score.array_apply(lambda x: x != max_score)

        rank_mask = bap_score.apply_neighborhood(
            max_score_selection,
            size=[
                {"dimension": "x", "unit": "px", "value": 1},
                {"dimension": "y", "unit": "px", "value": 1},
                {"dimension": "t", "value": period},
            ],
            overlap=[],
        )
    elif isinstance(period, list):
        udf_path = Path(__file__).parent / "udf_rank.py"
        rank_mask = bap_score.apply_neighborhood(
            process=openeo.UDF.from_file(str(udf_path), context={"intervals": period}),
            size=[
                {"dimension": "x", "unit": "px", "value": 256},
                {"dimension": "y", "unit": "px", "value": 256},
            ],
            overlap=[],
        )
    else:
        raise ValueError(
            f"'period' must be a string or a list of dates (in YYYY-mm-dd format), got {period}."
        )

    return rank_mask.rename_labels("bands", ["S2-L2A-BAPMASK"])


def bap_masking(cube: openeo.DataCube, period: Union[str, list], **params: dict):
    """Computes the bap mask as described in `get_bap_mask` and applies it to
    the optical part of the cube.

    Parameters
    ----------
    cube : openeo.DataCube
        The datacube to be processed.
    period : Union[str, list]
        A string or a list of dates (in YYYY-mm-dd format) to be used as the
        temporal period to compute the BAP score.
    params : dict
        Additionals parameters, not used yet.
    Returns
    -------
    openeo.DataCube
        The datacube with the BAP mask applied.
    """
    optical_cube = cube.filter_bands(
        bands=list(filter(lambda band: band.startswith("S2"), cube.metadata.band_names))
    )

    nonoptical_cube = cube.filter_bands(
        bands=list(
            filter(lambda band: not band.startswith("S2"), cube.metadata.band_names)
        )
    )

    rank_mask = get_bap_mask(optical_cube, period, **params)

    optical_cube = optical_cube.mask(rank_mask.resample_cube_spatial(cube))

    # Do not merge if bands are empty!
    if len(nonoptical_cube.metadata.band_names) == 0:
        return optical_cube

    return optical_cube.merge_cubes(nonoptical_cube)


def cloudmask_percentage(
    cube: openeo.DataCube, percentage: float = 0.95
) -> openeo.DataCube:
    """Compute a cloud mask array, that either fully covers an observation or is empty.
    It computes the percentage of HIGH_CLOUD_PROBABILITY pixels in the SCL mask. If the percentage
    is higher than the given threshold, the mask will be covering the observation, otherwise False.
    """
    non_scl_cube = cube.filter_bands(
        bands=list(filter(lambda band: "SCL" not in band, cube.metadata.band_names))
    )

    scl_cube = cube.filter_bands(["SCL"])

    cloud_mask = scl_cube.apply_neighborhood(
        process=openeo.UDF.from_file("udf_mask.py", context={}),
        size=[
            {"dimension": "x", "unit": "px", "value": 1024},
            {"dimension": "y", "unit": "px", "value": 1024},
            {"dimension": "t", "value": 1},
        ],
        overlap=[],
    )

    non_scl_cube = non_scl_cube.mask(cloud_mask.resample_cube_spatial(cube))

    return non_scl_cube.merge_cubes(scl_cube)
