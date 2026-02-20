 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

===============================
Amazon SageMaker Unified Studio
===============================

`Amazon SageMaker Unified Studio <https://aws.amazon.com/sagemaker/unified-studio/>`__ is a unified development experience that
brings together AWS data, analytics, artificial intelligence (AI), and machine learning (ML) services.
It provides a place to build, deploy, execute, and monitor end-to-end workflows from a single interface.
This helps drive collaboration across teams and facilitate agile development.

Airflow provides different operators for running notebooks in SageMaker Unified Studio. They target
different APIs and suit different use cases. Be sure to read the descriptions below to understand their use cases.

Prerequisite Tasks
------------------

To use these operators, you must do a few things:

  * Create a SageMaker Unified Studio domain and project, following the instruction in `AWS documentation <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/getting-started.html>`__.
  * Within your project:
    * Navigate to the "Compute > Workflow environments" tab, and click "Create" to create a new MWAA environment.
    * Create a Notebook, Querybook, or Visual ETL job and save it to your project.

Operators
---------

.. _howto/operator:SageMakerNotebookOperator:

Run Notebooks via the SageMaker Studio SDK
==========================================

Use :class:`~airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookOperator`
to execute notebooks, SQL notebooks, and Visual ETL jobs through the SageMaker Studio SDK execution
client. This operator relies on the ``sagemaker_studio`` Python library, which converts the notebook
to a ``.ipynb`` file on S3, creates a SageMaker Training Job to run it, and captures outputs from S3.

The notebook is identified by its relative file path within the project (e.g. ``test_notebook.ipynb``).

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_unified_studio.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_unified_studio_notebook]
    :end-before: [END howto_operator_sagemaker_unified_studio_notebook]

.. _howto/operator:SageMakerUnifiedStudioNotebookOperator:

Run Notebooks via the DataZone NotebookRun API
===============================================

Use :class:`~airflow.providers.amazon.aws.operators.sagemaker_unified_studio_notebook.SageMakerUnifiedStudioNotebookOperator`
to execute notebooks through the DataZone ``StartNotebookRun`` API.
Instead of creating a training job, each run provisions its own dedicated compute instance, starts the
notebook kernel server, and executes cells sequentially. The DataZone ``GetNotebookRun`` API is used to 
retrieve notebook run information.

Importantly, this operator allows for creating and managing schedules for these notebook runs, with the ability to specify 
frequencies, compute environment settings, and notebook parameters. API-based execution or event-triggered capabilities outside 
of SageMaker Unified Studio are also possible with the documented DataZone APIs.

The notebook is identified by its notebook asset ID (e.g. ``nb-1234567890``), along with the
domain ID and project ID.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_unified_studio_notebook.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_unified_studio_notebook]
    :end-before: [END howto_operator_sagemaker_unified_studio_notebook]


Reference
---------

* `What is Amazon SageMaker Unified Studio <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/what-is-sagemaker-unified-studio.html>`__
