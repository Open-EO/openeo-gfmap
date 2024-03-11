"""Utilities to build a `pandas.DataFrame` from the output of a VectorCube
based job. Usefull to collect the output of point based extraction.
"""

from pathlib import Path

import pandas as pd

VECTORCUBE_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S%z"
TIMESTAMP_FORMAT = "%Y-%m-%d"


def load_json(input_file: Path, bands: list) -> pd.DataFrame:
    """Reads a json file and outputs it as a proper pandas dataframe.

    Parameters
    ----------
    input_file: PathLike
        The path of the JSON file to read.
    bands: list
        The name of the bands that will be used in the columns names. The band
        names must be the same as the vector cube that resulted into the parsed
        JSON file.
    Returns
    -------
    df: pd.DataFrame
        A `pandas.DataFrame` containing a combination of the band names and the
        timestamps as column names.
        For example, the Sentinel-2 green band on the 1st October 2020 is will
        have the column name `S2-L2A-B02:2020-10-01`
    """

    df = pd.read_json(input_file)

    target_timestamps = list(
        map(lambda date: date.strftime(TIMESTAMP_FORMAT), df.columns.to_pydatetime())
    )

    df = df.rename(dict(zip(df.columns, target_timestamps)), axis=1)

    expanded_df = pd.DataFrame()
    for col in df.columns:
        expanded_col = pd.DataFrame(
            df[col].to_list(), columns=[f"{feature}:{col}" for feature in bands]
        )
        expanded_df = pd.concat([expanded_df, expanded_col], axis=1)

    return expanded_df
