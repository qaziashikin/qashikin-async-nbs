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

from datetime import datetime

from airflow.providers.amazon.aws.operators.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookOperator,
)
from airflow.providers.amazon.aws.sensors.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookSensor,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain
else:
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

"""
Prerequisites: The account which runs this test must have the following:
1. A SageMaker Unified Studio Domain (with default VPC and roles)
2. A project within the SageMaker Unified Studio Domain
3. A notebook asset registered in the project with a known notebook_id

This test calls the DataZone StartNotebookRun / GetNotebookRun APIs directly
via boto3 using standard IAM credentials. No MWAA environment emulation is performed.
"""

DAG_ID = "example_sagemaker_unified_studio_notebook"

# Externally fetched variables:
DOMAIN_ID_KEY = "DOMAIN_ID"
PROJECT_ID_KEY = "PROJECT_ID"
NOTEBOOK_ID_KEY = "NOTEBOOK_ID"

sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(DOMAIN_ID_KEY)
    .add_variable(PROJECT_ID_KEY)
    .add_variable(NOTEBOOK_ID_KEY)
    .build()
)

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    test_env_id = test_context[ENV_ID_KEY]
    domain_id = test_context[DOMAIN_ID_KEY]
    project_id = test_context[PROJECT_ID_KEY]
    notebook_id = test_context[NOTEBOOK_ID_KEY]

    # [START howto_operator_sagemaker_unified_studio_notebook]
    run_notebook = SageMakerUnifiedStudioNotebookOperator(
        task_id="run_notebook",
        notebook_id=notebook_id,
        domain_id=domain_id,
        project_id=project_id,
        wait_for_completion=True,
        waiter_delay=10,
        deferrable=False,
    )
    # [END howto_operator_sagemaker_unified_studio_notebook]

    # [START howto_operator_sagemaker_unified_studio_notebook_deferrable]
    run_notebook_deferrable = SageMakerUnifiedStudioNotebookOperator(
        task_id="run_notebook_deferrable",
        notebook_id=notebook_id,
        domain_id=domain_id,
        project_id=project_id,
        deferrable=True,
        waiter_delay=10,
    )
    # [END howto_operator_sagemaker_unified_studio_notebook_deferrable]

    # [START howto_sensor_sagemaker_unified_studio_notebook]
    run_sensor = SageMakerUnifiedStudioNotebookSensor(
        task_id="run_sensor",
        domain_id=domain_id,
        project_id=project_id,
        notebook_run_id=run_notebook.output,
        poke_interval=10,
    )
    # [END howto_sensor_sagemaker_unified_studio_notebook]

    chain(
        test_context,
        run_notebook,
        run_notebook_deferrable,
        run_sensor,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
