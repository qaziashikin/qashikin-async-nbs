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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.amazon.aws.operators.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookOperator,
)
from airflow.providers.amazon.aws.triggers.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookTrigger,
)
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred

TASK_ID = "test_notebook_run"
NOTEBOOK_ID = "nb-1234567890"
DOMAIN_ID = "dzd_example"
PROJECT_ID = "proj_example"
DAG_ID = "test_dag"
NOTEBOOK_RUN_ID = "run_abc123"

HOOK_PATH = (
    "airflow.providers.amazon.aws.operators.sagemaker_unified_studio_notebook"
    ".SageMakerUnifiedStudioNotebookHook"
)


def _make_context(dag_id=DAG_ID):
    """Build a minimal mock context with a dag that has a dag_id."""
    dag = MagicMock()
    dag.dag_id = dag_id
    return {"dag": dag}


class TestSageMakerUnifiedStudioNotebookOperator:
    def test_init_defaults(self):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
        )
        assert op.notebook_id == NOTEBOOK_ID
        assert op.domain_id == DOMAIN_ID
        assert op.project_id == PROJECT_ID
        assert op.client_token is None
        assert op.notebook_parameters is None
        assert op.compute_configuration is None
        assert op.timeout_configuration is None
        assert op.wait_for_completion is True
        assert op.waiter_delay == 10
        assert op.deferrable is False

    def test_init_all_params(self):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            client_token="tok-123",
            notebook_parameters={"key": "val"},
            compute_configuration={"instance_type": "ml.m5.large"},
            timeout_configuration={"run_timeout_in_minutes": 60},
            wait_for_completion=False,
            waiter_delay=30,
            deferrable=True,
        )
        assert op.client_token == "tok-123"
        assert op.notebook_parameters == {"key": "val"}
        assert op.compute_configuration == {"instance_type": "ml.m5.large"}
        assert op.timeout_configuration == {"run_timeout_in_minutes": 60}
        assert op.wait_for_completion is False
        assert op.waiter_delay == 30
        assert op.deferrable is True

    # --- hook property ---

    @patch(HOOK_PATH)
    def test_hook_property(self, mock_hook_cls):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            waiter_delay=15,
            timeout_configuration={"run_timeout_in_minutes": 120},
        )
        hook = op.hook
        mock_hook_cls.assert_called_once_with(
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            waiter_delay=15,
            timeout_configuration={"run_timeout_in_minutes": 120},
        )
        assert hook is mock_hook_cls.return_value

    # --- execute success ---

    @patch(HOOK_PATH)
    def test_execute_success(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.start_notebook_run.return_value = {"notebook_run_id": NOTEBOOK_RUN_ID}
        mock_hook.wait_for_notebook_run.return_value = {
            "Status": "COMPLETED",
            "NotebookRunId": NOTEBOOK_RUN_ID,
        }

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            client_token="my-token",
            notebook_parameters={"p1": "v1"},
            compute_configuration={"instance_type": "ml.m5.large"},
            timeout_configuration={"run_timeout_in_minutes": 60},
        )

        result = op.execute(_make_context())

        assert result == NOTEBOOK_RUN_ID
        mock_hook.start_notebook_run.assert_called_once_with(
            notebook_id=NOTEBOOK_ID,
            client_token="my-token",
            notebook_parameters={"p1": "v1"},
            compute_configuration={"instance_type": "ml.m5.large"},
            timeout_configuration={"run_timeout_in_minutes": 60},
            workflow_name=DAG_ID,
        )
        mock_hook.wait_for_notebook_run.assert_called_once_with(NOTEBOOK_RUN_ID)

    @patch(HOOK_PATH)
    def test_execute_passes_dag_id_as_workflow_name(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.start_notebook_run.return_value = {"notebook_run_id": NOTEBOOK_RUN_ID}
        mock_hook.wait_for_notebook_run.return_value = {}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
        )
        op.execute(_make_context(dag_id="my_custom_dag"))

        call_kwargs = mock_hook.start_notebook_run.call_args[1]
        assert call_kwargs["workflow_name"] == "my_custom_dag"

    # --- execute validation errors ---

    @patch(HOOK_PATH)
    def test_execute_raises_when_notebook_id_empty(self, mock_hook_cls):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id="",
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
        )
        with pytest.raises(AirflowException, match="notebook_id is required"):
            op.execute(_make_context())
        mock_hook_cls.return_value.start_notebook_run.assert_not_called()

    @patch(HOOK_PATH)
    def test_execute_raises_when_domain_id_empty(self, mock_hook_cls):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id="",
            project_id=PROJECT_ID,
        )
        with pytest.raises(AirflowException, match="domain_id is required"):
            op.execute(_make_context())
        mock_hook_cls.return_value.start_notebook_run.assert_not_called()

    @patch(HOOK_PATH)
    def test_execute_raises_when_project_id_empty(self, mock_hook_cls):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id="",
        )
        with pytest.raises(AirflowException, match="project_id is required"):
            op.execute(_make_context())
        mock_hook_cls.return_value.start_notebook_run.assert_not_called()

    # --- execute propagates hook failures ---

    @patch(HOOK_PATH)
    def test_execute_propagates_start_failure(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.start_notebook_run.side_effect = AirflowException("API unavailable")

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
        )
        with pytest.raises(AirflowException, match="API unavailable"):
            op.execute(_make_context())

    @patch(HOOK_PATH)
    def test_execute_propagates_wait_failure(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.start_notebook_run.return_value = {"notebook_run_id": NOTEBOOK_RUN_ID}
        mock_hook.wait_for_notebook_run.side_effect = AirflowException("Notebook crashed")

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
        )
        with pytest.raises(AirflowException, match="Notebook crashed"):
            op.execute(_make_context())

    # --- execute with minimal params (no optionals) ---

    @patch(HOOK_PATH)
    def test_execute_minimal_params(self, mock_hook_cls):
        """Execute with only required params passes None for all optionals."""
        mock_hook = mock_hook_cls.return_value
        mock_hook.start_notebook_run.return_value = {"notebook_run_id": NOTEBOOK_RUN_ID}
        mock_hook.wait_for_notebook_run.return_value = {}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
        )
        result = op.execute(_make_context())

        assert result == NOTEBOOK_RUN_ID
        call_kwargs = mock_hook.start_notebook_run.call_args[1]
        assert call_kwargs["client_token"] is None
        assert call_kwargs["notebook_parameters"] is None
        assert call_kwargs["compute_configuration"] is None
        assert call_kwargs["timeout_configuration"] is None

    # --- wait_for_completion=False ---

    @patch(HOOK_PATH)
    def test_execute_no_wait(self, mock_hook_cls):
        """When wait_for_completion=False, execute returns immediately without polling."""
        mock_hook = mock_hook_cls.return_value
        mock_hook.start_notebook_run.return_value = {"notebook_run_id": NOTEBOOK_RUN_ID}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            wait_for_completion=False,
        )
        result = op.execute(_make_context())

        assert result == NOTEBOOK_RUN_ID
        mock_hook.start_notebook_run.assert_called_once()
        mock_hook.wait_for_notebook_run.assert_not_called()

    # --- deferrable mode ---

    @patch(HOOK_PATH)
    def test_execute_deferrable(self, mock_hook_cls):
        """When deferrable=True, execute defers to the trigger instead of polling."""
        mock_hook = mock_hook_cls.return_value
        mock_hook.start_notebook_run.return_value = {"notebook_run_id": NOTEBOOK_RUN_ID}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            deferrable=True,
            waiter_delay=20,
            timeout_configuration={"run_timeout_in_minutes": 120},
        )

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(_make_context())

        trigger = exc_info.value.trigger
        assert isinstance(trigger, SageMakerUnifiedStudioNotebookTrigger)
        assert trigger.notebook_run_id == NOTEBOOK_RUN_ID
        assert trigger.domain_id == DOMAIN_ID
        assert trigger.project_id == PROJECT_ID
        assert trigger.waiter_delay == 20
        assert trigger.timeout_configuration == {"run_timeout_in_minutes": 120}
        assert exc_info.value.method_name == "execute_complete"
        mock_hook.wait_for_notebook_run.assert_not_called()

    @patch(HOOK_PATH)
    def test_execute_deferrable_overrides_wait_for_completion(self, mock_hook_cls):
        """Deferrable takes precedence over wait_for_completion=False."""
        mock_hook = mock_hook_cls.return_value
        mock_hook.start_notebook_run.return_value = {"notebook_run_id": NOTEBOOK_RUN_ID}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            deferrable=True,
            wait_for_completion=False,
        )

        with pytest.raises(TaskDeferred):
            op.execute(_make_context())

        mock_hook.wait_for_notebook_run.assert_not_called()

    # --- execute_complete ---

    def test_execute_complete_success(self):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
        )
        event = {"status": "success", "notebook_run_id": NOTEBOOK_RUN_ID}
        result = op.execute_complete(context=_make_context(), event=event)
        assert result == NOTEBOOK_RUN_ID

    def test_execute_complete_failure(self):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
        )
        event = {"status": "failed", "notebook_run_id": NOTEBOOK_RUN_ID, "message": "OOM"}
        with pytest.raises(AirflowException, match="Notebook run failed"):
            op.execute_complete(context=_make_context(), event=event)

    def test_execute_complete_none_event(self):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
        )
        with pytest.raises(AirflowException, match="event is None"):
            op.execute_complete(context=_make_context(), event=None)
