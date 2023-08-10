from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.apache.beam.operators.beam import DataflowConfiguration
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


GCS_PYTHON = 'gs://myproject-387020-mobile-services/dataflow-pipelines/gsheets_to_bigquery_pipeline.py'
GCS_TMP = 'gs://myproject-387020-mobile-services/temp'
GCS_STAGING = 'gs://myproject-387020-mobile-services/staging'
GCP_PROJECT_ID = 'myproject-387020'
TEMPLATE_SEARCHPATH = "/home/airflow/gcs/data"

with DAG(
    'mobile-services-dag',
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        
    },
    description= 'DAG for mobile services',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 8, 8),
    template_searchpath=['/home/airflow/gcs/plugins']
) as dag:
    t1 = BeamRunPythonPipelineOperator(
        task_id="dataflow_runner_pipeline",
        runner="DataflowRunner",
        py_file=GCS_PYTHON,
        pipeline_options={
            "tempLocation": GCS_TMP,
            "stagingLocation": GCS_STAGING,
            "prueba": "Esto es una prueba"
        },
        py_options=[],
        py_requirements=["apache-beam","apache-beam[gcp]", "google-api-python-client", "google-auth"],
        py_system_site_packages=False,
        dataflow_config=DataflowConfiguration(
            job_name="{{task.task_id}}", project_id=GCP_PROJECT_ID, location="us-east1"
        ),
    )

    t2 = BigQueryInsertJobOperator(
        task_id="execute_bigquey",
        job_id="mobile-services",
        project_id='myproject-387020',
        configuration={
            "query": {
                "query": "{% include 'data_cleaning.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    t1 >> t2
