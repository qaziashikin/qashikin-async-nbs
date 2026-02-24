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
from __future__ import annotations

from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.providers.amazon.aws.triggers.sagemaker_unified_studio_notebook import (
    TWELVE_HOURS_IN_MINUTES,
    SageMakerUnifiedStudioNotebookTrigger,
)
from airflow.triggers.base import TriggerEvent

DOMAIN_ID = "dzd_example"
PROJECT_ID = "proj_example"
NOTEBOOK_RUN_ID = "run_456"
MODULE_PATH = "airflow.providers.amazon.aws.triggers.sagemaker_unified_studio_notebook"


class TestSageMakerUnifiedStudioNotebookTrigger:
    def _create_trigger(self, **kwargs):
        defaults = {
            "notebook_run_id": NOTEBOOK_RUN_ID,
            "domain_id": DOMAIN_ID,
            "project_id": PROJECT_ID,
            "waiter_delay": 1,
        }
        defaults.update(kwargs)
        return SageMakerUnifiedStudioNotebookTrigger(**defaults)

    # --- serialization ---

    def test_serialize(self):
        trigger = self._create_trigger(waiter_delay=10, timeout_configuration={"run_timeout_in_minutes": 60})
        classpath, kwargs = trigger.serialize()
        assert classpath == f"{MODULE_PATH}.SageMakerUnifiedStudioNotebookTrigger"
        assert kwargs == {
            "notebook_run_id": NOTEBOOK_RUN_ID,
            "domain_id": DOMAIN_ID,
            "project_id": PROJECT_ID,
            "waiter_delay": 10,
            "timeout_configuration": {"run_timeout_in_minutes": 60},
        }

    # --- timeout calculation ---

    def test_default_timeout(self):
        trigger = self._create_trigger(waiter_delay=10)
        assert trigger.waiter_max_attempts == int(TWELVE_HOURS_IN_MINUTES * 60 / 10)

    def test_custom_timeout_configuration(self):
        trigger = self._create_trigger(waiter_delay=10, timeout_configuration={"run_timeout_in_minutes": 60})
        assert trigger.waiter_max_attempts == int(60 * 60 / 10)

    def test_empty_timeout_configuration_falls_back_to_default(self):
        trigger = self._create_trigger(waiter_delay=10, timeout_configuration={})
        assert trigger.waiter_max_attempts == int(TWELVE_HOURS_IN_MINUTES * 60 / 10)

    # --- run: success ---

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE_PATH}.boto3.client")
    @mock.patch(f"{MODULE_PATH}.asyncio.sleep", return_value=None)
    async def test_run_succeeds_immediately(self, mock_sleep, mock_boto):
        mock_client = MagicMock()
        mock_client.get_notebook_run.return_value = {"status": "SUCCEEDED"}
        mock_boto.return_value = mock_client

        trigger = self._create_trigger()
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0] == TriggerEvent({"status": "SUCCEEDED", "notebook_run_id": NOTEBOOK_RUN_ID})

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE_PATH}.boto3.client")
    @mock.patch(f"{MODULE_PATH}.asyncio.sleep", return_value=None)
    async def test_run_stopped_is_stopped(self, mock_sleep, mock_boto):
        mock_client = MagicMock()
        mock_client.get_notebook_run.return_value = {"status": "STOPPED"}
        mock_boto.return_value = mock_client

        trigger = self._create_trigger()
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "STOPPED"
        assert NOTEBOOK_RUN_ID in events[0].payload["message"]

    # --- run: polls then succeeds ---

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE_PATH}.boto3.client")
    @mock.patch(f"{MODULE_PATH}.asyncio.sleep", return_value=None)
    async def test_run_polls_then_succeeds(self, mock_sleep, mock_boto):
        mock_client = MagicMock()
        mock_client.get_notebook_run.side_effect = [
            {"status": "QUEUED"},
            {"status": "STARTING"},
            {"status": "RUNNING"},
            {"status": "SUCCEEDED"},
        ]
        mock_boto.return_value = mock_client

        trigger = self._create_trigger()
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "SUCCEEDED"

    # --- run: failure ---

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE_PATH}.boto3.client")
    @mock.patch(f"{MODULE_PATH}.asyncio.sleep", return_value=None)
    async def test_run_fails_with_error_message(self, mock_sleep, mock_boto):
        mock_client = MagicMock()
        mock_client.get_notebook_run.return_value = {
            "status": "FAILED",
            "errorMessage": "Notebook crashed",
        }
        mock_boto.return_value = mock_client

        trigger = self._create_trigger()
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "FAILED"
        assert events[0].payload["message"] == "Notebook crashed"

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE_PATH}.boto3.client")
    @mock.patch(f"{MODULE_PATH}.asyncio.sleep", return_value=None)
    async def test_run_fails_without_error_message(self, mock_sleep, mock_boto):
        mock_client = MagicMock()
        mock_client.get_notebook_run.return_value = {"status": "FAILED"}
        mock_boto.return_value = mock_client

        trigger = self._create_trigger()
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "FAILED"
        assert NOTEBOOK_RUN_ID in events[0].payload["message"]

    # --- run: unexpected state ---

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE_PATH}.boto3.client")
    @mock.patch(f"{MODULE_PATH}.asyncio.sleep", return_value=None)
    async def test_run_unexpected_state(self, mock_sleep, mock_boto):
        mock_client = MagicMock()
        mock_client.get_notebook_run.return_value = {"status": "UNKNOWN_STATE"}
        mock_boto.return_value = mock_client

        trigger = self._create_trigger()
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "ERROR"
        assert "unexpected state" in events[0].payload["message"]

    # --- run: timeout ---

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE_PATH}.boto3.client")
    @mock.patch(f"{MODULE_PATH}.asyncio.sleep", return_value=None)
    async def test_run_times_out(self, mock_sleep, mock_boto):
        mock_client = MagicMock()
        mock_client.get_notebook_run.return_value = {"status": "RUNNING"}
        mock_boto.return_value = mock_client

        trigger = self._create_trigger(waiter_delay=1, timeout_configuration={"run_timeout_in_minutes": 1})
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "ERROR"
        assert "timed out" in events[0].payload["message"]

    # --- run: API not available ---

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE_PATH}.boto3.client")
    async def test_run_api_not_available(self, mock_boto):
        mock_client = MagicMock(spec=[])  # no attributes
        mock_boto.return_value = mock_client

        trigger = self._create_trigger()
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "ERROR"
        assert "not available" in events[0].payload["message"]

    # --- run: client is closed in finally ---

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE_PATH}.boto3.client")
    @mock.patch(f"{MODULE_PATH}.asyncio.sleep", return_value=None)
    async def test_run_closes_client(self, mock_sleep, mock_boto):
        mock_client = MagicMock()
        mock_client.get_notebook_run.return_value = {"status": "SUCCEEDED"}
        mock_boto.return_value = mock_client

        trigger = self._create_trigger()
        _ = [event async for event in trigger.run()]

        mock_client.close.assert_called_once()
