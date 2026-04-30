# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""This module contains the Amazon SageMaker Unified Studio Notebook Run hook."""

from __future__ import annotations

import json
import logging
import math
import time
import uuid
from typing import Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

TWELVE_HOURS_IN_MINUTES = 12 * 60


class SageMakerUnifiedStudioNotebookHook(AwsBaseHook):
    """
    Interact with Sagemaker Unified Studio Workflows for asynchronous notebook execution.

    This hook provides a wrapper around the DataZone StartNotebookRun / GetNotebookRun APIs.

    Examples:
     .. code-block:: python

        from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio_notebook import (
            SageMakerUnifiedStudioNotebookHook,
        )

        hook = SageMakerUnifiedStudioNotebookHook(aws_conn_id="my_aws_conn")

    Additional arguments (such as ``aws_conn_id`` or ``region_name``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args: Any, **kwargs: Any):
        kwargs.setdefault("client_type", "datazone")
        super().__init__(*args, **kwargs)

    def _validate_api_availability(self) -> None:
        """
        Verify that the NotebookRun APIs are available in the installed boto3/botocore version.

        :raises RuntimeError: If the required APIs are not available.
        """
        required_methods = ("start_notebook_run", "get_notebook_run")
        for method_name in required_methods:
            if not hasattr(self.conn, method_name):
                raise RuntimeError(
                    f"The '{method_name}' API is not available in the installed boto3/botocore version. "
                    "Please upgrade boto3/botocore to a version that supports the DataZone "
                    "NotebookRun APIs."
                )

    def start_notebook_run(
        self,
        notebook_identifier: str,
        domain_identifier: str,
        owning_project_identifier: str,
        client_token: str | None = None,
        notebook_parameters: dict | None = None,
        compute_configuration: dict | None = None,
        timeout_configuration: dict | None = None,
        workflow_name: str | None = None,
    ) -> dict:
        """
        Start an asynchronous notebook run via the DataZone StartNotebookRun API.

        :param notebook_identifier: The ID of the notebook to execute.
        :param domain_identifier: The ID of the DataZone domain containing the notebook.
        :param owning_project_identifier: The ID of the DataZone project containing the notebook.
        :param client_token: Idempotency token. Auto-generated if not provided.
        :param notebook_parameters: Parameters to pass to the notebook.
        :param compute_configuration: Compute config (e.g. instance_type).
        :param timeout_configuration: Timeout settings (run_timeout_in_minutes).
        :param workflow_name: Name of the workflow (DAG) that triggered this run.
        :return: The StartNotebookRun API response dict.
        """
        self._validate_api_availability()

        params: dict = {
            "domain_identifier": domain_identifier,
            "owning_project_identifier": owning_project_identifier,
            "notebook_identifier": notebook_identifier,
            "client_token": client_token or str(uuid.uuid4()),
        }

        if notebook_parameters:
            params["parameters"] = {"notebook_parameters": notebook_parameters}
        if compute_configuration:
            params["compute_configuration"] = compute_configuration
        if timeout_configuration:
            params["timeout_configuration"] = timeout_configuration
        if workflow_name:
            params["trigger_source"] = {"type": "workflow", "name": workflow_name}

        self.log.info(
            "Starting notebook run for notebook %s in domain %s", notebook_identifier, domain_identifier
        )
        return self.conn.start_notebook_run(**params)

    def get_notebook_run(self, notebook_run_id: str, domain_identifier: str) -> dict:
        """
        Get the status of a notebook run via the DataZone GetNotebookRun API.

        :param notebook_run_id: The ID of the notebook run.
        :param domain_identifier: The ID of the DataZone domain.
        :return: The GetNotebookRun API response dict.
        """
        self._validate_api_availability()
        return self.conn.get_notebook_run(
            domain_identifier=domain_identifier,
            identifier=notebook_run_id,
        )

    def wait_for_notebook_run(
        self,
        notebook_run_id: str,
        domain_identifier: str,
        waiter_delay: int = 10,
        timeout_configuration: dict | None = None,
    ) -> dict:
        """
        Poll GetNotebookRun until the run reaches a terminal state.

        :param notebook_run_id: The ID of the notebook run to monitor.
        :param domain_identifier: The ID of the DataZone domain.
        :param waiter_delay: Interval in seconds to poll the notebook run status.
        :param timeout_configuration: Timeout settings for the notebook execution.
            When provided, the maximum number of poll attempts is derived from
            ``run_timeout_in_minutes * 60 / waiter_delay``. Defaults to 12 hours.
        :return: A dict with Status and NotebookRunId on success.
        :raises RuntimeError: If the run fails or times out.
        """
        if waiter_delay <= 0:
            raise ValueError("waiter_delay must be a positive integer")
        run_timeout = (timeout_configuration or {}).get("run_timeout_in_minutes", TWELVE_HOURS_IN_MINUTES)
        waiter_max_attempts = max(1, math.ceil(run_timeout * 60 / waiter_delay))

        for _attempt in range(waiter_max_attempts):
            response = self.get_notebook_run(notebook_run_id, domain_identifier=domain_identifier)
            status = response.get("status", "")
            error_message = response.get("errorMessage", "")

            ret = self._handle_status(notebook_run_id, status, error_message, waiter_delay)
            if ret:
                return ret
            time.sleep(waiter_delay)

        error_message = "Execution timed out"
        self.log.error("Notebook run %s failed with error: %s", notebook_run_id, error_message)
        raise RuntimeError(error_message)

    def _handle_status(
        self, notebook_run_id: str, status: str, error_message: str, waiter_delay: int = 10
    ) -> dict | None:
        """
        Evaluate the current notebook run status and return or raise accordingly.

        :param notebook_run_id: The ID of the notebook run.
        :param status: The current status string.
        :param error_message: Error message from the API response, if any.
        :param waiter_delay: Interval in seconds between polls (for logging).
        :return: A dict with Status and NotebookRunId on success, None if still in progress.
        :raises RuntimeError: If the run has failed.
        """
        in_progress_statuses = {"QUEUED", "STARTING", "RUNNING", "STOPPING"}
        finished_statuses = {"SUCCEEDED"}
        failure_statuses = {"FAILED", "STOPPED"}

        if status in in_progress_statuses:
            self.log.info(
                "Notebook run %s is still in progress with status: %s, "
                "will check for a terminal status again in %ss",
                notebook_run_id,
                status,
                waiter_delay,
            )
            return None

        execution_message = f"Exiting notebook run {notebook_run_id}. Status: {status}"

        if status in finished_statuses:
            self.log.info(execution_message)
            return {"Status": status, "NotebookRunId": notebook_run_id}

        if status in failure_statuses:
            self.log.error("Notebook run %s failed with error: %s", notebook_run_id, error_message)
        else:
            self.log.error("Notebook run %s reached unexpected status: %s", notebook_run_id, status)

        if error_message == "":
            error_message = execution_message
        raise RuntimeError(error_message)

    def get_project_s3_path(self, project_id: str) -> str:
        """
        Construct the S3 path for a SageMaker Unified Studio project bucket.

        :param project_id: The ID of the DataZone project.
        :return: The S3 bucket name for the project.
        """
        account_id = self.account_id
        region = self.conn_region_name
        return f"amazon-sagemaker-{account_id}-{region}-{project_id}"

    def get_notebook_outputs(
        self,
        notebook_identifier: str,
        notebook_run_id: str,
        owning_project_identifier: str,
    ) -> dict[str, Any]:
        """
        Read notebook output artifacts from the S3 project bucket.

        After a notebook run completes, the SDK writes output variables as a JSON
        file to a well-known S3 location within the project bucket. This method
        reads that file and returns the parsed key-value pairs.

        :param notebook_identifier: The ID of the notebook that was executed.
        :param notebook_run_id: The ID of the completed notebook run.
        :param owning_project_identifier: The ID of the DataZone project.
        :return: A dict of notebook output key-value pairs. Returns an empty dict
            if no outputs were written or the file cannot be parsed.
        """
        bucket = self.get_project_s3_path(owning_project_identifier)
        key = f"sys/notebooks/{notebook_identifier}/runs/{notebook_run_id}/notebook_outputs.json"

        log = logging.getLogger(__name__)
        log.info("Reading notebook outputs from s3://%s/%s", bucket, key)

        try:
            s3_client = self.get_session().client("s3", region_name=self.conn_region_name)
            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            outputs = json.loads(content)
            if not isinstance(outputs, dict):
                log.warning(
                    "Notebook outputs at s3://%s/%s is not a JSON object, ignoring.",
                    bucket,
                    key,
                )
                return {}
            log.info("Successfully read %d notebook output(s).", len(outputs))
            return outputs
        except s3_client.exceptions.NoSuchKey:
            log.info("No notebook outputs found at s3://%s/%s.", bucket, key)
            return {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            log.warning(
                "Failed to parse notebook outputs at s3://%s/%s as JSON, ignoring.",
                bucket,
                key,
            )
            return {}
        except Exception:
            log.warning(
                "Unexpected error reading notebook outputs from s3://%s/%s, ignoring.",
                bucket,
                key,
                exc_info=True,
            )
            return {}
