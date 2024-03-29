import json
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from pathlib import Path
from typing import Callable, Optional, Union

import pandas as pd
import pystac
from openeo.extra.job_management import MultiBackendJobManager
from openeo.rest.job import BatchJob
from pystac import CatalogType

from openeo_gfmap.manager import _log
from openeo_gfmap.stac import constants


class PostJobStatus(Enum):
    """Indicates the workers if the job finished as sucessful or with an error."""

    FINISHED = "finished"
    ERROR = "error"


class GFMAPJobManager(MultiBackendJobManager):
    """A job manager for the GFMAP backend."""

    def __init__(
        self,
        output_dir: Path,
        output_path_generator: Callable,
        collection_id: str,
        collection_description: str = "",
        post_job_action: Optional[Callable] = None,
        poll_sleep: int = 5,
        n_threads: int = 1,
        post_job_params: dict = {},
    ):
        self._output_dir = output_dir

        # Setup the threads to work on the on_job_done and on_job_error methods
        self._n_threads = n_threads
        self._executor = None  # Will be set in run_jobs, is a threadpool executor
        self._futures = []

        self._output_path_gen = output_path_generator
        self._post_job_action = post_job_action
        self._post_job_params = post_job_params

        # Monkey patching the _normalize_df method to ensure we have no modification on the
        # geometry column
        MultiBackendJobManager._normalize_df = self._normalize_df
        super().__init__(poll_sleep)

        # Generate the root STAC collection
        self._root_collection = pystac.Collection(
            id=collection_id,
            description=collection_description,
            extent=None,
        )

    def _update_statuses(self, df: pd.DataFrame):
        """Updates the statues of the jobs in the dataframe from the backend. If a job is finished
        or failed, it will be queued to the `on_job_done` or `on_job_error` methods.

        The method is executed every `poll_sleep` seconds.
        """
        active = df[df.status.isin(["created", "queued", "running"])]
        for idx, row in active.iterrows():
            # Parses the backend from the csv
            connection = self._get_connection(row.backend_name)
            job = connection.job(row.id)
            job_metadata = job.describe_job()
            job_status = job_metadata["status"]
            _log.debug(
                msg=f"Status of job {job.job_id} is {job_status} (on backend {row.backend_name}).",
            )

            # Update the status if the job finished since last check
            # Case is which it finished sucessfully
            if (df.loc[idx, "status"] in ["created", "queued", "running"]) and (
                job_metadata["status"] == "finished"
            ):
                _log.info(f"Job {job.job_id} finished successfully, queueing on_job_done...")
                self._futures.append(self._executor.submit(self.on_job_done, job, row))
                df.loc[idx, "costs"] = job_metadata["costs"]

            # Case in which it failed
            if (df.loc[idx, "status"] != "error") and (job_metadata["status"] == "error"):
                _log.info(f"Job {job.job_id} finished with error, queueing on_job_error...")
                self._futures.append(self._executor.submit(self.on_job_error, job, row))
                df.loc[idx, "costs"] = job_metadata["costs"]

            df.loc[idx, "status"] = job_status

        futures_to_clear = []
        for future in self._futures:
            if future.done():
                exception = future.exception(timeout=1.0)
                if exception:
                    raise exception
                futures_to_clear.append(future)
        for future in futures_to_clear:
            self._futures.remove(future)

    def on_job_error(self, job: BatchJob, row: pd.Series):
        """Method called when a job finishes with an error.

        Parameters
        ----------
        job: BatchJob
            The job that finished with an error.
        row: pd.Series
            The row in the dataframe that contains the job relative information.
        """
        logs = job.logs()
        error_logs = [log for log in logs if log.level.lower() == "error"]

        job_metadata = job.describe_job()
        title = job_metadata["title"]
        job_id = job_metadata["id"]

        output_log_path = Path(self._output_dir) / "failed_jobs" / f"{title}_{job_id}.log"
        output_log_path.parent.mkdir(parents=True, exist_ok=True)

        if len(error_logs) > 0:
            output_log_path.write_text(json.dumps(error_logs, indent=2))
        else:
            output_log_path.write_text(
                f"Couldn't find any error logs. Please check the error manually on job ID: {job.job_id}."
            )

    def on_job_done(self, job: BatchJob, row: pd.Series):
        """Method called when a job finishes successfully. It will first download the results of
        the job and then call the `post_job_action` method.
        """
        job_products = {}
        for idx, asset in enumerate(job.get_results().get_assets()):
            try:
                _log.debug(
                    f"Generating output path for asset {asset.name} from job {job.job_id}..."
                )
                output_path = self._output_path_gen(self._output_dir, idx, row)
                # Make the output path
                output_path.parent.mkdir(parents=True, exist_ok=True)
                asset.download(output_path)
                # Add to the list of downloaded products
                job_products[f"{job.job_id}_{asset.name}"] = [output_path]
                _log.debug(f"Downloaded {asset.name} from job {job.job_id} -> {output_path}")
            except Exception as e:
                _log.exception(f"Error downloading asset {asset.name} from job {job.job_id}", e)
                raise e

        # First update the STAC collection with the assets directly resulting from the OpenEO batch job
        job_metadata = pystac.Collection.from_dict(job.get_results().get_metadata())
        job_items = []

        for item_metadata in job_metadata.get_all_items():
            try:
                item = pystac.read_file(item_metadata.get_self_href())
                asset_path = job_products[f"{job.job_id}_{item.id}"][0]

                assert len(item.assets.values()) == 1, "Each item should only contain one asset"
                for asset in item.assets.values():
                    asset.href = str(
                        asset_path
                    )  # Update the asset href to the output location set by the output_path_generator
                item.id = f"{job.job_id}_{item.id}"
                # Add the item to the the current job items.
                job_items.append(item)
                _log.info(f"Parsed item {item.id} from job {job.job_id}")
            except Exception as e:
                _log.exception(
                    f"Error failed to add item {item.id} from job {job.job_id} to STAC collection",
                    e,
                )
                raise e

        # _post_job_action returns an updated list of stac items. Post job action can therefore
        # update the stac items and access their products through the HREF. It is also the
        # reponsible of adding the appropriate metadata/assets to the items.
        if self._post_job_action is not None:
            _log.debug(f"Calling post job action for job {job.job_id}...")
            job_items = self._post_job_action(job_items, row, self._post_job_params)

        _log.info(f"Adding {len(job_items)} items to the STAC collection...")
        self._root_collection.add_items(job_items)
        _log.info(f"Added {len(job_items)} items to the STAC collection.")

        _log.info(f"Job {job.job_id} and post job action finished successfully.")

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure we have the required columns and the expected type for the geometry column.

        :param df: The dataframe to normalize.
        :return: a new dataframe that is normalized.
        """

        # check for some required columns.
        required_with_default = [
            ("status", "not_started"),
            ("id", None),
            ("start_time", None),
            ("cpu", None),
            ("memory", None),
            ("duration", None),
            ("backend_name", None),
            ("description", None),
            ("costs", None),
        ]
        new_columns = {col: val for (col, val) in required_with_default if col not in df.columns}
        df = df.assign(**new_columns)

        _log.debug(f"Normalizing dataframe. Columns: {df.columns}")

        return df

    def run_jobs(self, df: pd.DataFrame, start_job: Callable, output_file: Union[str, Path]):
        """Starts the jobs defined in the dataframe and runs the `start_job` function on each job.

        Parameters
        ----------
        df: pd.DataFrame
            The dataframe containing the jobs to be started. The dataframe expects the following columns:

            * `backend_name`: Name of the backend to use.
            * Additional fields that will be used in your custom job creation function `start_job`
            as well as in post-job actions and path generator.

            The following column names are RESERVED for the managed of the jobs, please do not
            provide them in the input df:

            * `status`: Current status of the job.
            * `id`: Job ID, used to access job information from the backend.
            * `start_time`: The time at which the job was started.
            * `cpu`: The amount of CPU used by the job.
            * `memory`: The amount of memory used by the job.
            * `duration`: The duration of the job.

        start_job: Callable
            Callable function that will take in argument the rows of each job and that will
            create a datacube.
        output_file: Union[str, Path]
            The file to track the results of the jobs.
        """
        # Starts the thread pool to work on the on_job_done and on_job_error methods
        _log.info(f"Starting ThreadPoolExecutor with {self._n_threads} workers.")
        with ThreadPoolExecutor(max_workers=self._n_threads) as executor:
            _log.info("Creating and running jobs.")
            self._executor = executor
            super().run_jobs(df, start_job, output_file)
            self._executor = None

    def create_stac(
        self, output_path: Optional[Union[str, Path]] = None, asset_definitions: dict = None
    ):
        """Method to be called after run_jobs to create a STAC catalog
        and write it to self._output_dir
        """
        if output_path is None:
            output_path = self._output_dir / "stac"

        item_assets = constants.ITEM_ASSETS
        if asset_definitions:
            item_assets = {**constants.ITEM_ASSETS, **asset_definitions}

        self._root_collection.license = constants.LICENSE
        self._root_collection.add_link(constants.LICENSE_LINK)
        self._root_collection.stac_extensions = constants.STAC_EXTENSIONS

        datacube_extension = pystac.extensions.datacube.DatacubeExtension.ext(
            self._root_collection, add_if_missing=True
        )
        datacube_extension.apply(constants.CUBE_DIMENSIONS)

        item_asset_extension = pystac.extensions.item_assets.ItemAssetsExtension.ext(
            self._root_collection, add_if_missing=True
        )
        item_asset_extension.item_assets = item_assets

        self._root_collection.update_extent_from_items()
        self._root_collection.normalize_hrefs(str(output_path))
        self._root_collection.save(catalog_type=CatalogType.SELF_CONTAINED)
