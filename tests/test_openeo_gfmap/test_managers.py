"""Test the job splitters and managers of OpenEO GFMAP."""

from pathlib import Path

import geopandas as gpd

from openeo_gfmap.manager.job_splitters import split_job_hex


# TODO can we instead assert on exact numbers ?
# would remove the print statement
def test_split_jobs():
    dataset_path = Path(__file__).parent / "resources/wc_extraction_dataset.gpkg"

    # Load the dataset
    dataset = gpd.read_file(dataset_path)

    # Split the dataset
    split_dataset = split_job_hex(dataset, max_points=500)

    # Check the number of splits
    assert len(split_dataset) > 1

    for ds in split_dataset:
        print(len(ds))
        assert len(ds) <= 500
