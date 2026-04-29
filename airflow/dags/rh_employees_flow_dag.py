import json
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration

CONFIG_PATH = Path(__file__).resolve().parent / "config" / "rh_flow_config.json"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CFG = json.load(f)


def choose_mode(**context):
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    mode = conf.get("mode", "full")

    if mode == "delta":
        return "wait_for_employees_delta"
    return "wait_for_employees_full"


with DAG(
    dag_id="rh_employees_flow",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["rh", "employees"],
) as dag:

    branch_mode = BranchPythonOperator(
        task_id="branch_mode",
        python_callable=choose_mode
    )

    wait_for_employees_full = GCSObjectExistenceSensor(
        task_id="wait_for_employees_full",
        bucket=CFG["bucket"],
        object="full/employees_full.csv"
    )

    wait_for_employees_delta = GCSObjectExistenceSensor(
        task_id="wait_for_employees_delta",
        bucket=CFG["bucket"],
        object="delta/employees_delta.json"
    )

    run_employees_full_dataflow = BeamRunJavaPipelineOperator(
        task_id="run_employees_full_dataflow",
        jar=CFG["jar_gcs_path"],
        job_class=CFG["employee_job_class"],
        runner="DataflowRunner",
        pipeline_options={
            "project": CFG["project_id"],
            "region": CFG["region"],
            "gcpTempLocation": CFG["temp_location"],
            "stagingLocation": CFG["staging_location"],
            "mode": "full",
            "inputPath": CFG["employees_full_input"],
            "outputTable": CFG["raw_employees_table"],
            "backupPath": CFG["backup_full"],
            "writeDispositionValue": "WRITE_TRUNCATE"
        },
        dataflow_config=DataflowConfiguration(
            job_name="rh-employees-full",
            location=CFG["region"],
            wait_until_finished=True
        )
    )

    run_employees_delta_dataflow = BeamRunJavaPipelineOperator(
        task_id="run_employees_delta_dataflow",
        jar=CFG["jar_gcs_path"],
        job_class=CFG["employee_job_class"],
        runner="DataflowRunner",
        pipeline_options={
            "project": CFG["project_id"],
            "region": CFG["region"],
            "gcpTempLocation": CFG["temp_location"],
            "stagingLocation": CFG["staging_location"],
            "mode": "delta",
            "inputPath": CFG["employees_delta_input"],
            "outputTable": CFG["raw_employees_delta_staging_table"],
            "backupPath": CFG["backup_delta"],
            "writeDispositionValue": "WRITE_APPEND"
        },
        dataflow_config=DataflowConfiguration(
            job_name="rh-employees-delta",
            location=CFG["region"],
            wait_until_finished=True
        )
    )

    merge_employees_delta = BigQueryInsertJobOperator(
        task_id="merge_employees_delta",
        location=CFG["bq_location"],
        configuration={
            "query": {
                "query": f"""
MERGE `{CFG["project_id"]}.sephora_raw.raw_employees` T
USING (
  SELECT *
  FROM `{CFG["project_id"]}.sephora_raw.raw_employees_delta_staging`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id
    ORDER BY event_timestamp DESC, ingestion_ts DESC
  ) = 1
) S
ON T.id = S.id

WHEN MATCHED AND S.delta_action = 'DELETE' THEN
  UPDATE SET
    load_mode = S.load_mode,
    delta_action = S.delta_action,
    event_timestamp = S.event_timestamp,
    is_deleted = TRUE,
    ingestion_ts = S.ingestion_ts

WHEN MATCHED AND S.delta_action = 'UPDATE' THEN
  UPDATE SET
    nom = COALESCE(S.nom, T.nom),
    prenom = COALESCE(S.prenom, T.prenom),
    store_id = COALESCE(S.store_id, T.store_id),
    poste = COALESCE(S.poste, T.poste),
    departement = COALESCE(S.departement, T.departement),
    date_embauche = COALESCE(S.date_embauche, T.date_embauche),
    type_contrat = COALESCE(S.type_contrat, T.type_contrat),
    salaire_brut = COALESCE(S.salaire_brut, T.salaire_brut),
    load_mode = S.load_mode,
    delta_action = S.delta_action,
    event_timestamp = S.event_timestamp,
    is_deleted = FALSE,
    ingestion_ts = S.ingestion_ts

WHEN NOT MATCHED AND S.delta_action = 'INSERT' THEN
  INSERT (
    id, nom, prenom, store_id, poste, departement, date_embauche,
    type_contrat, salaire_brut, load_mode, delta_action,
    event_timestamp, is_deleted, ingestion_ts
  )
  VALUES (
    S.id, S.nom, S.prenom, S.store_id, S.poste, S.departement, S.date_embauche,
    S.type_contrat, S.salaire_brut, S.load_mode, S.delta_action,
    S.event_timestamp, FALSE, S.ingestion_ts
  )
                """,
                "useLegacySql": False
            }
        }
    )

    create_dataform_compilation = DataformCreateCompilationResultOperator(
        task_id="create_dataform_compilation",
        project_id=CFG["project_id"],
        region=CFG["region"],
        repository_id=CFG["dataform_repository_id"],
        compilation_result={
            "git_commitish": CFG["dataform_git_ref"]
        }
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

    branch_mode >> wait_for_employees_full >> run_employees_full_dataflow >> create_dataform_compilation
    branch_mode >> wait_for_employees_delta >> run_employees_delta_dataflow >> merge_employees_delta >> create_dataform_compilation
    create_dataform_compilation >> run_dataform_workflow