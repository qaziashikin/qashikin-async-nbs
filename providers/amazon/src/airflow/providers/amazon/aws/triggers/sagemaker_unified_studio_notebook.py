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

"""Trigger for monitoring SageMaker Unified Studio Notebook runs asynchronously."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from functools import partial
from typing import Any

import boto3

from airflow.triggers.base import BaseTrigger, TriggerEvent

IN_PROGRESS_STATES = {"QUEUED", "STARTING", "RUNNING", "STOPPING"}
FINISHED_STATES = {"SUCCEEDED", "STOPPED"}
FAILURE_STATES = {"FAILED"}

TWELVE_HOURS_IN_SECONDS = 12 * 60 * 60


class SageMakerUnifiedStudioNotebookTrigger(BaseTrigger):
    """
    Watches an asynchronous notebook run, triggering when it reaches a terminal state.

    :param notebook_run_id: The ID of the notebook run to monitor.
    :param domain_id: The ID of the DataZone domain.
    :param project_id: The ID of the DataZone project.
    :param waiter_delay: Interval in seconds between polls (default: 10).
    :param timeout_configuration: Optional timeout settings. When provided, the maximum
        number of poll attempts is derived from ``run_timeout_in_minutes * 60 / waiter_delay``.
        Defaults to a 12-hour timeout when omitted.
        Example: {"run_timeout_in_minutes": 720}
    """

    def __init__(
        self,
        notebook_run_id: str,
        domain_id: str,
        project_id: str,
        waiter_delay: int = 10,
        timeout_configuration: dict | None = None,
    ):
        super().__init__()
        self.notebook_run_id = notebook_run_id
        self.domain_id = domain_id
        self.project_id = project_id
        self.waiter_delay = waiter_delay
        self.timeout_configuration = timeout_configuration
        run_timeout = (timeout_configuration or {}).get("run_timeout_in_minutes")
        if run_timeout:
            self.waiter_max_attempts = int(run_timeout * 60 / self.waiter_delay)
        else:
            self.waiter_max_attempts = int(TWELVE_HOURS_IN_SECONDS / self.waiter_delay)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "notebook_run_id": self.notebook_run_id,
                "domain_id": self.domain_id,
                "project_id": self.project_id,
                "waiter_delay": self.waiter_delay,
                "timeout_configuration": self.timeout_configuration,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        client = boto3.client("datazone")
        if not hasattr(client, "get_notebook_run"):
            yield TriggerEvent(
                {
                    "status": "error",
                    "notebook_run_id": self.notebook_run_id,
                    "message": "The 'get_notebook_run' API is not available in the installed "
                    "boto3/botocore version. Please upgrade boto3/botocore to a version "
                    "that supports the DataZone NotebookRun APIs.",
                }
            )
            return
        try:
            for _ in range(self.waiter_max_attempts):
                loop = asyncio.get_running_loop()
                response = await loop.run_in_executor(
                    None,
                    partial(
                        client.get_notebook_run,
                        domain_id=self.domain_id,
                        notebook_run_id=self.notebook_run_id,
                    ),
                )
                status = response.get("status", "")
                error_message = response.get("errorMessage", "")

                if status in FINISHED_STATES:
                    yield TriggerEvent(
                        {"status": "success", "notebook_run_id": self.notebook_run_id, "state": status}
                    )
                    return

                if status in FAILURE_STATES:
                    yield TriggerEvent(
                        {
                            "status": "failed",
                            "notebook_run_id": self.notebook_run_id,
                            "message": error_message or f"Notebook run {self.notebook_run_id} failed",
                        }
                    )
                    return

                if status not in IN_PROGRESS_STATES:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "notebook_run_id": self.notebook_run_id,
                            "message": f"Notebook run {self.notebook_run_id} reached unexpected state: {status}",
                        }
                    )
                    return

                self.log.info(
                    "Notebook run %s is %s, checking again in %ss",
                    self.notebook_run_id,
                    status,
                    self.waiter_delay,
                )
                await asyncio.sleep(self.waiter_delay)

            yield TriggerEvent(
                {
                    "status": "error",
                    "notebook_run_id": self.notebook_run_id,
                    "message": f"Notebook run {self.notebook_run_id} timed out after {self.waiter_max_attempts} attempts",
                }
            )
        finally:
            client.close()
