import json
import logging
import queue
import threading
from enum import Enum
from pathlib import Path
from typing import Callable, Union

import pandas as pd
from openeo.extra.job_management import MultiBackendJobManager
from openeo.rest.job import BatchJob

from openeo_gfmap.manager import _log


class PostJobStatus(Enum):
    """Indicates the workers if the job finished as sucessful or with an error."""

    FINISHED = "finished"
    ERROR = "error"


class GFMAPJobManager(MultiBackendJobManager):
    """A job manager for the GFMAP backend."""

    def __init__(
        self,
        output_dir: Path,
        post_job_action: Callable,
        poll_sleep: int = 5,
        n_threads: int = 1,
        post_job_params: dict = {},
    ):
        self._output_dir = output_dir

        # Setup the threads to work on the on_job_done and on_job_error methods
        self._finished_job_queue = queue.Queue()
        self._n_threads = n_threads

        self._threads = []

        self._post_job_action = post_job_action
        self._post_job_params = post_job_params
        super().__init__(poll_sleep)

    def _post_job_worker(self):
        """Checks which jobs are finished or failed and calls the `on_job_done` or `on_job_error`
        methods."""
        while True:
            status, job, row = self._finished_job_queue.get()
            if status == PostJobStatus.ERROR:
                self.on_job_error(job, row)
            elif status == PostJobStatus.FINISHED:
                self.on_job_done(job, row)
            else:
                raise ValueError(f"Unknown status: {status}")
            self.on_job_done(job, row)
            self.job_done_queue.task_done()

    def _update_statuses(self, df: pd.DataFrame):
        """Updates the statues of the jobs in the dataframe from the backend. If a job is finished
        or failed, it will be queued to the `on_job_done` or `on_job_error` methods.

        The method is executed every `poll_sleep` seconds.
        """
        active = df[df.status.isin(["created", "queued", "running"])]
        _log.info(f"Updating status. {len(active)} on {len(df)} active jobs...")
        for idx, row in active.iterrows():
            # Parses the backend from the csv
            connection = self._get_connection(row.backend_name)
            job = connection.job(row.job_id)
            job_metadata = job.describe_job()
            _log.log(
                level=logging.DEBUG,
                msg=f"Status of job {job.job_id} is {job_metadata} (on backend {row.backend_name}).",
            )

            # Update the status if the job finished since last check
            # Case is which it finished sucessfully
            if (df.loc[idx, "status"] in ["created", "queued", "running"]) and (
                job_metadata["status"] == "finished"
            ):
                _log.info(
                    f"Job {job.job_id} finished successfully, queueing on_job_done..."
                )
                self._finished_job_queue.put((PostJobStatus.FINISHED, job, row))
                df.loc[idx, "description"] = job_metadata["description"]

            # Case in which it failed
            if (df.loc[idx, "status"] != "error") and (
                job_metadata["status"] == "error"
            ):
                _log.info(
                    f"Job {job.job_id} finished with error, queueing on_job_error..."
                )
                self._finished_job_queue.put((PostJobStatus.ERROR, job, row))
                df.loc[idx, "description"] = job_metadata["description"]

            df.loc[idx, "status"] = job_metadata["status"]

            # Additional parameters
            self._update_status(job, row)

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

        # TODO figure out how to handle output file structures.
        # job_metadata = job.describe_job()
        # title = job_metadata["title"]
        output_file = "/temp/error.log"
        if len(error_logs > 0):
            Path(output_file).write_text(json.dumps(error_logs, indent=2))
        else:
            Path(output_file).write_text(
                f"Couldn't find any error logs. Please check the error manually on job ID: {job.job_id}."
            )

    def on_job_done(self, job: BatchJob, row: pd.Series):
        """Method called when a job finishes successfully. It will first download the results of
        the job and then call the `post_job_action` method.
        """
        output_folder = self._output_dir / row.job_id
        output_folder.mkdir(parents=True, exist_ok=True)

        for idx, asset in enumerate(job.results().get_assets()):
            # file_name = f"{row.output_prefix}_{idx}.{asset.file_extension}"
            asset.download(output_folder / asset.filename)

        # TODO trigger post-job action and write STAC metadata

    def run_jobs(
        self, df: pd.DataFrame, start_job: Callable, output_file: Union[str, Path]
    ):
        """Starts the jobs defined in the dataframe and runs the `start_job` function on each job.

        Parameters
        ----------
        df: pd.DataFrame
            The dataframe containing the jobs to be started. The dataframe expects the following columns:

            * `output_folder`: Folder in which the results of the job will be stored.
            * `output_prefix`: Prefix to be used in the output files.
            * `file_extension`: Extension of the output files.
            * `backend_name`: Name of the backend to use.
            * `task_id`: ID of the task to be executed, will be used to retrieve geometry from
              another dataset.

        start_job: Callable
            Callable function that will take in argument the rows of each job and that will
            create a datacube.
        output_file: Union[str, Path]
            The file to track the results of the jobs.
        """
        super(MultiBackendJobManager).run_jobs(df, start_job, output_file)

        # Starts the thread pool to work on the on_job_done and on_job_error methods
        for _ in range(self._n_threads):
            thread = threading.Thread(target=self._post_job_worker)
            thread.start()
            self._threads.append(thread)
