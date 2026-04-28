import json
import logging
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataflow import DataflowCreateJavaJobOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

LOG = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parent / "config" / "hr_flow_config.json"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CFG = json.load(f)


def choose_mode(**context):
    mode = context["dag_run"].conf.get("mode", "full")
    if mode == "delta":
        return "wait_for_employees_delta"
    return "wait_for_employees_full"


def notify_success():
    LOG.info("Le DAG hr_employees_flow s'est terminé avec succès.")


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1
}


with DAG(
    dag_id="hr_employees_flow",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["hr", "employees", "beam", "dataform", "bigquery"],
) as dag:

    start = EmptyOperator(task_id="start")

    branch_mode = BranchPythonOperator(
        task_id="branch_mode",
        python_callable=choose_mode
    )

    wait_for_employees_full = GCSObjectExistenceSensor(
        task_id="wait_for_employees_full",
        bucket=CFG["bucket"],
        object="full/employees_full.csv",
        gcp_conn_id=CFG["gcp_conn_id"]
    )

    wait_for_employees_delta = GCSObjectExistenceSensor(
        task_id="wait_for_employees_delta",
        bucket=CFG["bucket"],
        object="delta/employees_delta.json",
        gcp_conn_id=CFG["gcp_conn_id"]
    )

    run_employees_full_dataflow = DataflowCreateJavaJobOperator(
        task_id="run_employees_full_dataflow",
        jar=CFG["jar_gcs_path"],
        job_name="hr-employees-full-{{ ts_nodash }}",
        job_class=CFG["employee_job_class"],
        location=CFG["region"],
        gcp_conn_id=CFG["gcp_conn_id"],
        options={
            "runner": "DataflowRunner",
            "project": CFG["project_id"],
            "region": CFG["region"],
            "tempLocation": CFG["temp_location"],
            "stagingLocation": CFG["staging_location"],
            "mode": "full",
            "inputPath": CFG["employees_full_input"],
            "outputTable": CFG["raw_employees_table"],
            "backupPath": CFG["backup_full"],
            "writeDispositionValue": "WRITE_TRUNCATE"
        }
    )

    run_employees_delta_dataflow = DataflowCreateJavaJobOperator(
        task_id="run_employees_delta_dataflow",
        jar=CFG["jar_gcs_path"],
        job_name="hr-employees-delta-{{ ts_nodash }}",
        job_class=CFG["employee_job_class"],
        location=CFG["region"],
        gcp_conn_id=CFG["gcp_conn_id"],
        options={
            "runner": "DataflowRunner",
            "project": CFG["project_id"],
            "region": CFG["region"],
            "tempLocation": CFG["temp_location"],
            "stagingLocation": CFG["staging_location"],
            "mode": "delta",
            "inputPath": CFG["employees_delta_input"],
            "outputTable": CFG["raw_employees_delta_staging_table"],
            "backupPath": CFG["backup_delta"],
            "writeDispositionValue": "WRITE_APPEND"
        }
    )

    merge_employees_delta = BigQueryInsertJobOperator(
        task_id="merge_employees_delta",
        gcp_conn_id=CFG["gcp_conn_id"],
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

    after_ingestion = EmptyOperator(
        task_id="after_ingestion",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    check_raw_stores_available = BigQueryCheckOperator(
        task_id="check_raw_stores_available",
        gcp_conn_id=CFG["gcp_conn_id"],
        use_legacy_sql=False,
        location=CFG["bq_location"],
        sql=f"""
SELECT COUNT(*) >= {CFG["quality_min_raw_stores_count"]}
FROM `{CFG["project_id"]}.sephora_raw.raw_stores`
        """
    )

    create_dataform_compilation = DataformCreateCompilationResultOperator(
        task_id="create_dataform_compilation",
        gcp_conn_id=CFG["gcp_conn_id"],
        project_id=CFG["project_id"],
        region=CFG["region"],
        repository_id=CFG["dataform_repository_id"],
        compilation_result={
            "git_commitish": CFG["dataform_git_ref"]
        }
    )

    run_dataform_workflow = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform_workflow",
        gcp_conn_id=CFG["gcp_conn_id"],
        project_id=CFG["project_id"],
        region=CFG["region"],
        repository_id=CFG["dataform_repository_id"],
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_dataform_compilation')['name'] }}"
        }
    )

    check_dwh_employee_not_null = BigQueryCheckOperator(
        task_id="check_dwh_employee_not_null",
        gcp_conn_id=CFG["gcp_conn_id"],
        use_legacy_sql=False,
        location=CFG["bq_location"],
        sql=f"""
SELECT COUNT(*) = 0
FROM `{CFG["project_id"]}.sephora_dwh.dwh_employee_enriched`
WHERE id IS NULL
        """
    )

    check_dwh_store_join_coverage = BigQueryCheckOperator(
        task_id="check_dwh_store_join_coverage",
        gcp_conn_id=CFG["gcp_conn_id"],
        use_legacy_sql=False,
        location=CFG["bq_location"],
        sql=f"""
SELECT COUNT(*) = 0
FROM `{CFG["project_id"]}.sephora_dwh.dwh_employee_enriched`
WHERE store_id IS NOT NULL
  AND region IS NULL
        """
    )

    notify_success_task = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success
    )

    end = EmptyOperator(task_id="end")

    start >> branch_mode

    branch_mode >> wait_for_employees_full >> run_employees_full_dataflow >> after_ingestion
    branch_mode >> wait_for_employees_delta >> run_employees_delta_dataflow >> merge_employees_delta >> after_ingestion

    after_ingestion >> check_raw_stores_available
    check_raw_stores_available >> create_dataform_compilation >> run_dataform_workflow
    run_dataform_workflow >> check_dwh_employee_not_null >> check_dwh_store_join_coverage
    check_dwh_store_join_coverage >> notify_success_task >> end