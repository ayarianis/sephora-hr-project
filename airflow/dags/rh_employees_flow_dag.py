import json
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

CONFIG_PATH = Path(__file__).resolve().parent / "hr_flow_config.json"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CFG = json.load(f)

EMP = CFG["employees"]


def choose_mode(**context):
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    mode = conf.get("mode", "full")
    return "wait_for_employees_full" if mode == "full" else "wait_for_employees_delta"


with DAG(
    dag_id="rh_employees_flow",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["employees"],
) as dag:

    branch_mode = BranchPythonOperator(
        task_id="branch_mode",
        python_callable=choose_mode
    )

    wait_for_employees_full = GCSObjectExistenceSensor(
        task_id="wait_for_employees_full",
        bucket=CFG["bucket"],
        object=EMP["full_object"]
    )

    wait_for_employees_delta = GCSObjectExistenceSensor(
        task_id="wait_for_employees_delta",
        bucket=CFG["bucket"],
        object=EMP["delta_object"]
    )

    run_employees_full = DataflowStartFlexTemplateOperator(
        task_id="run_employees_full",
        project_id=CFG["project_id"],
        location=CFG["region"],
        body={
            "launchParameter": {
                "jobName": "employees-full-{{ ts_nodash }}",
                "containerSpecGcsPath": EMP["flex_template_path"],
                "parameters": {
                    "mode": "full",
                    "inputPath": EMP["full_input"],
                    "outputTable": EMP["full_output_table"],
                    "backupPath": EMP["backup_full"],
                    "writeDispositionValue": "WRITE_TRUNCATE",
                    "project": CFG["project_id"],
                    "region": CFG["region"],
                    "gcpTempLocation": CFG["temp_location"],
                    "stagingLocation": CFG["staging_location"]
                }
            }
        }
    )

    run_employees_delta = DataflowStartFlexTemplateOperator(
        task_id="run_employees_delta",
        project_id=CFG["project_id"],
        location=CFG["region"],
        body={
            "launchParameter": {
                "jobName": "employees-delta-{{ ts_nodash }}",
                "containerSpecGcsPath": EMP["flex_template_path"],
                "parameters": {
                    "mode": "delta",
                    "inputPath": EMP["delta_input"],
                    "outputTable": EMP["delta_output_table"],
                    "backupPath": EMP["backup_delta"],
                    "writeDispositionValue": "WRITE_APPEND",
                    "project": CFG["project_id"],
                    "region": CFG["region"],
                    "gcpTempLocation": CFG["temp_location"],
                    "stagingLocation": CFG["staging_location"]
                }
            }
        }
    )

    merge_employees_delta = BigQueryInsertJobOperator(
        task_id="merge_employees_delta",
        location=CFG["bq_location"],
        configuration={
            "query": {
                "query": EMP["merge_sql"],
                "useLegacySql": False
            }
        }
    )

    create_dataform_compilation = DataformCreateCompilationResultOperator(
        task_id="create_dataform_compilation",
        project_id=CFG["project_id"],
        region=CFG["region"],
        repository_id=CFG["dataform_repository_id"],
        compilation_result={"git_commitish": CFG["dataform_git_ref"]}
    )

    run_dataform_workflow = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform_workflow",
        project_id=CFG["project_id"],
        region=CFG["region"],
        repository_id=CFG["dataform_repository_id"],
        workflow_invocation={
            "compilation_result": "{{ ti.xcom_pull(task_ids='create_dataform_compilation')['name'] }}"
        }
    )

    branch_mode >> wait_for_employees_full >> run_employees_full >> create_dataform_compilation
    branch_mode >> wait_for_employees_delta >> run_employees_delta >> merge_employees_delta >> create_dataform_compilation
    create_dataform_compilation >> run_dataform_workflow