import json
import threading
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from functools import partial
from pathlib import Path
from typing import Callable, Optional, Union

import pandas as pd
import pystac
from openeo.extra.job_management import MultiBackendJobManager
from openeo.rest.job import BatchJob
from pystac import CatalogType

from openeo_gfmap.manager import _log
from openeo_gfmap.stac import constants

# Lock to use when writing to the STAC collection
_stac_lock = threading.Lock()


def done_callback(future, df, idx):
    """Sets the status of the job to the given status when the future is done."""
    current_status = df.loc[idx, "status"]
    if not future.exception():
        if current_status == "postprocessing":
            df.loc[idx, "status"] = "finished"
        elif current_status == "postprocessing-error":
            df.loc[idx, "status"] = "error"
        else:
            raise ValueError(
                f"Invalid status {current_status} for job {df.loc[idx, 'id']} for done_callback!"
            )


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
        collection_id: Optional[str] = None,
        collection_description: Optional[str] = "",
        stac: Optional[Union[str, Path]] = None,
        post_job_action: Optional[Callable] = None,
        poll_sleep: int = 5,
        n_threads: int = 1,
        post_job_params: dict = {},
        resume_postproc: bool = True,  # If we need to check for post-job actions that crashed
        restart_failed: bool = False,  # If we need to restart failed jobs
    ):
        self._output_dir = output_dir

        self.stac = stac
        self.collection_id = collection_id
        self.collection_description = collection_description

        # Setup the threads to work on the on_job_done and on_job_error methods
        self._n_threads = n_threads
        self._executor = None  # Will be set in run_jobs, is a threadpool executor
        self._futures = []
        self._to_resume_postjob = (
            resume_postproc  # If we need to check for post-job actions that crashed
        )
        self._to_restart_failed = restart_failed  # If we need to restart failed jobs

        self._output_path_gen = output_path_generator
        self._post_job_action = post_job_action
        self._post_job_params = post_job_params

        # Monkey patching the _normalize_df method to ensure we have no modification on the
        # geometry column
        MultiBackendJobManager._normalize_df = self._normalize_df
        super().__init__(poll_sleep)

        self._root_collection = self._normalize_stac()

    def _normalize_stac(self):
        default_collection_path = self._output_dir / "stac/collection.json"
        if self.stac is not None:
            _log.info(
                f"Reloading the STAC collection from the provided path: {self.stac}."
            )
            root_collection = pystac.read_file(str(self.stac))
        elif default_collection_path.exists():
            _log.info(
                f"Reload the STAC collection from the default path: {default_collection_path}."
            )
            self.stac = default_collection_path
            root_collection = pystac.read_file(str(self.stac))
        else:
            _log.info("Starting a fresh STAC collection.")
            assert (
                self.collection_id is not None
            ), "A collection ID is required to generate a STAC collection."
            root_collection = pystac.Collection(
                id=self.collection_id,
                description=self.collection_description,
                extent=None,
            )
            root_collection.license = constants.LICENSE
            root_collection.add_link(constants.LICENSE_LINK)
            root_collection.stac_extensions = constants.STAC_EXTENSIONS

        return root_collection

    def _clear_queued_actions(self):
        """Checks if the post-job actions are finished and clears them from the list of futures.
        If an exception occured, it is raised to the GFMAPJobManage main thread.
        """
        # Checking if any post-job action has finished or not
        futures_to_clear = []
        for future in self._futures:
            if future.done():
                exception = future.exception(timeout=1.0)
                if exception:
                    raise exception
                futures_to_clear.append(future)
        for future in futures_to_clear:
            self._futures.remove(future)

    def _wait_queued_actions(self):
        """Waits for all the queued actions to finish."""
        for future in self._futures:
            # Wait for the future to finish and get the potential exception
            exception = future.exception(timeout=None)
            if exception:
                raise exception

    def _resume_postjob_actions(self, df: pd.DataFrame):
        """Resumes the jobs that were in the `postprocessing` or `postprocessing-error` state, as
        they most likely crashed before finishing their post-job action.

        df: pd.DataFrame
            The job-tracking dataframe initialized or loaded by the multibackend job manager.
        """
        postprocessing_tasks = df[
            df.status.isin(["postprocessing", "postprocessing-error"])
        ]
        for idx, row in postprocessing_tasks.iterrows():
            connection = self._get_connection(row.backend_name)
            job = connection.job(row.id)
            if row.status == "postprocessing":
                _log.info(
                    f"Resuming postprocessing of job {row.id}, queueing on_job_finished..."
                )
                future = self._executor.submit(self.on_job_done, job, row)
                future.add_done_callback(partial(done_callback, df=df, idx=idx))
            else:
                _log.info(
                    f"Resuming postprocessing of job {row.id}, queueing on_job_error..."
                )
                future = self._executor.submit(self.on_job_error, job, row)
                future.add_done_callback(partial(done_callback, df=df, idx=idx))
            self._futures.append(future)

    def _restart_failed_jobs(self, df: pd.DataFrame):
        """Sets-up failed jobs as "not_started" as they will be restarted by the manager."""
        failed_tasks = df[df.status == "error"]
        not_started_tasks = df[df.status == "not_started"]
        _log.info(
            f"Resetting {len(failed_tasks)} failed jobs to 'not_started'. {len(not_started_tasks)} jobs are already 'not_started'."
        )
        for idx, _ in failed_tasks.iterrows():
            df.loc[idx, "status"] = "not_started"

    def _update_statuses(self, df: pd.DataFrame):
        """Updates the statues of the jobs in the dataframe from the backend. If a job is finished
        or failed, it will be queued to the `on_job_done` or `on_job_error` methods.

        The method is executed every `poll_sleep` seconds.
        """
        if self._to_restart_failed:  # Make sure it runs only the first time
            self._restart_failed_jobs(df)
            self._to_restart_failed = False

        if self._to_resume_postjob:  # Make sure it runs only the first time
            self._resume_postjob_actions(df)
            self._to_resume_postjob = False

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
                _log.info(
                    f"Job {job.job_id} finished successfully, queueing on_job_done..."
                )
                job_status = "postprocessing"
                future = self._executor.submit(self.on_job_done, job, row)
                # Future will setup the status to finished when the job is done
                future.add_done_callback(partial(done_callback, df=df, idx=idx))
                self._futures.append(future)
                df.loc[idx, "costs"] = job_metadata["costs"]

            # Case in which it failed
            if (df.loc[idx, "status"] != "error") and (
                job_metadata["status"] == "error"
            ):
                _log.info(
                    f"Job {job.job_id} finished with error, queueing on_job_error..."
                )
                job_status = "postprocessing-error"
                future = self._executor.submit(self.on_job_error, job, row)
                # Future will setup the status to error when the job is done
                future.add_done_callback(partial(done_callback, df=df, idx=idx))
                self._futures.append(future)
                df.loc[idx, "costs"] = job_metadata["costs"]

            df.loc[idx, "status"] = job_status

        # Clear the futures that are done and raise their potential exceptions if they occurred.
        self._clear_queued_actions()

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

        output_log_path = (
            Path(self._output_dir) / "failed_jobs" / f"{title}_{job_id}.log"
        )
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
                _log.debug(
                    f"Downloaded {asset.name} from job {job.job_id} -> {output_path}"
                )
            except Exception as e:
                _log.exception(
                    f"Error downloading asset {asset.name} from job {job.job_id}", e
                )
                raise e

        # First update the STAC collection with the assets directly resulting from the OpenEO batch job
        job_metadata = pystac.Collection.from_dict(job.get_results().get_metadata())
        job_items = []

        for item_metadata in job_metadata.get_all_items():
            try:
                item = pystac.read_file(item_metadata.get_self_href())
                asset_name = list(item.assets.values())[0].title
                asset_path = job_products[f"{job.job_id}_{asset_name}"][0]

                assert (
                    len(item.assets.values()) == 1
                ), "Each item should only contain one asset"
                for asset in item.assets.values():
                    asset.href = str(
                        asset_path
                    )  # Update the asset href to the output location set by the output_path_generator
                # item.id = f"{job.job_id}_{item.id}"
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

        with _stac_lock:  # Take the STAC lock to avoid concurrence issues
            # Filters the job items to only keep the ones that are not already in the collection
            existing_ids = [item.id for item in self._root_collection.get_all_items()]
            job_items = [item for item in job_items if item.id not in existing_ids]

            self._root_collection.add_items(job_items)
            _log.info(f"Added {len(job_items)} items to the STAC collection.")

            _log.info(f"Writing STAC collection for {job.job_id} to file...")
            try:
                self._write_stac()
            except Exception as e:
                _log.exception(
                    f"Error writing STAC collection for job {job.job_id} to file.", e
                )
                raise e
            _log.info(f"Wrote STAC collection for {job.job_id} to file.")

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
        new_columns = {
            col: val for (col, val) in required_with_default if col not in df.columns
        }
        df = df.assign(**new_columns)

        _log.debug(f"Normalizing dataframe. Columns: {df.columns}")

        return df

    def run_jobs(
        self, df: pd.DataFrame, start_job: Callable, output_file: Union[str, Path]
    ):
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
            _log.info(
                "Quitting job tracking & waiting for last post-job actions to finish."
            )
            self._wait_queued_actions()
            _log.info("Exiting ThreadPoolExecutor.")
            self._executor = None

    def _write_stac(self):
        """Writes the STAC collection to the output directory."""
        if not self._root_collection.get_self_href():
            self._root_collection.set_self_href(str(self._output_dir / "stac"))

        self._root_collection.update_extent_from_items()

        # Setups the root path for the normalization
        root_path = Path(self._root_collection.self_href)
        if root_path.is_file():
            root_path = root_path.parent

        self._root_collection.normalize_hrefs(str(root_path))
        self._root_collection.save(catalog_type=CatalogType.SELF_CONTAINED)

    def setup_stac(
        self,
        constellation: Optional[str] = None,
        output_path: Optional[Union[str, Path]] = None,
        item_assets: Optional[dict] = None,
    ):
        """Method to be called after run_jobs to setup details of the STAC collection
        such as the constellation, root directory and item assets extensions.

        Parameters
        ----------
        constellation: Optional[str]
            The constellation for which to create the STAC metadata, if None no STAC metadata will be added
            The following constellations are supported:

            * 'sentinel1'
            * 'sentinel2'

        output_path: Optional[Union[str, Path]]
            The path to write the STAC collection to. If None, the STAC collection will be written to self.output_dir / 'stac'
        item_assets: Optional[dict]
            A dictionary containing pystac.extensions.item_assets.AssetDefinition objects to be added to the STAC collection
            https://github.com/stac-extensions/item-assets
        """
        if output_path:
            self._root_collection.set_self_href(str(output_path))

        if constellation and "summaries" not in self._root_collection.extra_fields:
            self._root_collection.extra_fields["summaries"] = constants.SUMMARIES.get(
                constellation, pystac.summaries.Summaries({})
            ).to_dict()

        if item_assets and "item_assets" not in self._root_collection.extra_fields:
            item_asset_extension = (
                pystac.extensions.item_assets.ItemAssetsExtension.ext(
                    self._root_collection, add_if_missing=True
                )
            )
            item_asset_extension.item_assets = item_assets
