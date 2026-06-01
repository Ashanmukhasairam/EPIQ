from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import timedelta
from datetime import datetime
import boto3, json, re, time, uuid
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.log.logging_mixin import LoggingMixin
import pendulum

local_tz = pendulum.timezone("America/New_York")
log = LoggingMixin().log

PIPELINE_TABLES = ["contract_header", "contract_items"]

# ---------------- CONFIG LOAD ---------------- #
def load_config(**context):
    s3_path = "s3://epiq-edp-dl-qa-configs/dags/configs/edp_sap_contractdetails_config_qa.json"
    match = re.match(r"s3://([^/]+)/(.+)", s3_path)
    s3 = boto3.client("s3")

    config = json.loads(
        s3.get_object(
            Bucket=match.group(1),
            Key=match.group(2)
        )["Body"].read()
    )
    context["ti"].xcom_push(key="config", value=config)
    return config

# ---------------- RUN ID ---------------- #
def generate_run_id(**context):
    run_id = str(uuid.uuid4())
    context["ti"].xcom_push(key="etl_run_id", value=run_id)
    return run_id

# ---------------- CONFIG LOAD ---------------- #
# Add DynamoDB resource at the top alongside boto3 imports
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
audit_table = dynamodb.Table("pipeline_execution_summary")

# ---------------- INSERT AUDIT ---------------- #
def insert_audit(layer, tables, job_name, **context):
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="load_config", key="config")
    etl_run_id = ti.xcom_pull(task_ids="generate_run_id", key="etl_run_id")

    run_id = context["run_id"]
    layer_cfg = config["LAYERS"][layer]
    pipeline_name = layer_cfg["PIPELINE_NAME"]

    for table in tables:
        layer_source_table = f"{layer}#{config['SOURCE_SYSTEM']}#{table}"  # e.g. SILVER#sap#contract_header

        # Only insert if not already exists (idempotency)
        existing = audit_table.get_item(
            Key={
                "etl_run_id": etl_run_id,
                "layer":      layer_source_table,
            }
        ).get("Item")

        if existing:
            log.info(f"Audit record already exists for {layer_source_table} — skipping insert.")
            continue

        audit_table.put_item(
            Item={
                "etl_run_id":          etl_run_id,
                "layer":               layer_source_table,   # ← composite sort key
                "run_id":              run_id,
                "pipeline_name":       pipeline_name,
                "source_system":       config["SOURCE_SYSTEM"],
                "table_name":          table,
                "environment":         config["ENVIRONMENT"],
                "load_type":           layer_cfg["LOAD_TYPE"],
                "glue_job_name":       job_name,
                "glue_job_run_id":     "NA",
                "status":              "RUNNING",
                "pipeline_start_time": datetime.utcnow().isoformat(),
                "pipeline_end_time":   None,
                "records_read":        0,
                "records_written":     0,
                "records_rejected":    0,
                "records_deleted":     0,
                "records_inserted":    0,
                "records_updated":     0,
                "error_message":       None,
                "airflow_run_id":      run_id,
                "created_at":          datetime.utcnow().isoformat(),
                "updated_at":          datetime.utcnow().isoformat(),
            }
        )
        log.info(f"Audit record inserted for {layer_source_table}")


# ---------------- UPDATE STATUS ---------------- #
def update_status(layer, status, glue_run_id, tables, **context):
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="load_config", key="config")
    etl_run_id = ti.xcom_pull(task_ids="generate_run_id", key="etl_run_id")

    for table in tables:
        layer_source_table = f"{layer}#{config['SOURCE_SYSTEM']}#{table}"  # e.g. SILVER#sap#contract_header

        audit_table.update_item(
            Key={
                "etl_run_id": etl_run_id,
                "layer":      layer_source_table,
            },
            UpdateExpression="""
                SET
                    #st               = :st,
                    glue_job_run_id   = :gid,
                    pipeline_end_time = :pet,
                    updated_at        = :uat
            """,
            ExpressionAttributeNames={
                "#st": "status",
            },
            ExpressionAttributeValues={
                ":st":  status,
                ":gid": str(glue_run_id),
                ":pet": datetime.utcnow().isoformat(),
                ":uat": datetime.utcnow().isoformat(),
            },
        )
        log.info(f"Audit status updated to {status} for {layer_source_table}")
# ---------------- GLUE WAIT ---------------- #
def wait_for_glue(glue, job, run_id):
    start = time.time()
    timeout = 60 * 60
    while True:
        state = glue.get_job_run(JobName=job, RunId=run_id)["JobRun"]["JobRunState"]
        if state == "SUCCEEDED":
            return "SUCCESS"
        if state in ["FAILED", "STOPPED", "TIMEOUT"]:
            return "FAILED"
        if time.time() - start > timeout:
            raise AirflowException("Glue job timeout")
        time.sleep(5)

# ---------------- SILVER ---------------- #
def run_silver(**context):
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="load_config", key="config")
    etl_run_id = ti.xcom_pull(task_ids="generate_run_id", key="etl_run_id")

    glue = boto3.client("glue")
    silver_cfg = config["LAYERS"]["SILVER"]
    job_name = "edp-qa-sap-silver-contractdetails"
    tables = PIPELINE_TABLES
    glue_run_id = "NA"

    try:
        insert_audit("SILVER", tables, job_name, **context)
        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                "--ACCOUNT_ID": silver_cfg["ACCOUNT_ID"],
                "--AIRFLOW_RUN_ID": context["run_id"],
                "--EXECUTION_ID": etl_run_id,
                "--SILVER_DATABASE": silver_cfg["SILVER_DATABASE"],
                "--SILVER_TABLE": ",".join(tables),
                "--TABLE_BUCKET_NAME": silver_cfg["TABLE_BUCKET_NAME"],
                "--BRONZE_BUCKET_NAME": silver_cfg["BRONZE_BUCKET_NAME"],
                "--CONFIG_BUCKET": silver_cfg["CONFIG_BUCKET"],
                "--ETL_RUN_DATE": context["dag_run"].conf.get("etl_run_date", context["ds"]),
                "--SOURCE": silver_cfg["SOURCE"],
                "--AWS_REGION": silver_cfg["AWS_REGION"],
            },
        )
        glue_run_id = response["JobRunId"]
        status = wait_for_glue(glue, job_name, glue_run_id)
    except Exception as e:
        ti.xcom_push(key="silver_error", value=str(e))
        update_status("SILVER", "FAILED", glue_run_id, tables, **context)
        raise

    update_status("SILVER", status, glue_run_id, tables, **context)
    if status != "SUCCESS":
        raise AirflowException("Silver failed")

# ---------------- GOLD ---------------- #
def run_gold(**context):
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="load_config", key="config")
    etl_run_id = ti.xcom_pull(task_ids="generate_run_id", key="etl_run_id")

    glue = boto3.client("glue")
    gold_cfg = config["LAYERS"]["GOLD"]
    job_name = "edp-qa-sap-gold-contractdetails"
    tables = PIPELINE_TABLES
    glue_run_id = "NA"

    try:
        insert_audit("GOLD", tables, job_name, **context)
        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                "--ACCOUNT_ID": gold_cfg["ACCOUNT_ID"],
                "--AIRFLOW_RUN_ID": context["run_id"],
                "--EXECUTION_ID": etl_run_id,
                "--CONFIG_BUCKET": gold_cfg["CONFIG_BUCKET"],
                "--SOURCE": gold_cfg["SOURCE"],
                "--SILVER_BUCKET": gold_cfg["SILVER_BUCKET"],
                "--GOLD_BUCKET": gold_cfg["GOLD_BUCKET"],
                "--SILVER_DATABASE": gold_cfg["SILVER_DATABASE"],
                "--GOLD_DATABASE": gold_cfg["GOLD_DATABASE"],
                "--SILVER_TABLE": ",".join(tables),
                "--GOLD_TABLE": ",".join(tables),
                "--AWS_REGION": gold_cfg["AWS_REGION"],
            },
        )
        glue_run_id = response["JobRunId"]
        status = wait_for_glue(glue, job_name, glue_run_id)
    except Exception as e:
        ti.xcom_push(key="gold_error", value=str(e))
        update_status("GOLD", "FAILED", glue_run_id, tables, **context)
        raise

    update_status("GOLD", status, glue_run_id, tables, **context)
    if status != "SUCCESS":
        raise AirflowException("Gold failed")

# ---------------- DAG ---------------- #
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag-sap-contractdetails-silver-gold-pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz=local_tz),
    schedule="0 11 * * *",
    catchup=False,
    default_args=default_args,
) as dag:

    load_config_task = PythonOperator(task_id="load_config", python_callable=load_config)
    generate_run_id_task = PythonOperator(task_id="generate_run_id", python_callable=generate_run_id)
    run_silver_task = PythonOperator(task_id="run_silver", python_callable=run_silver)
    run_gold_task = PythonOperator(task_id="run_gold", python_callable=run_gold, trigger_rule=TriggerRule.ALL_SUCCESS)

    notify_silver_success = SnsPublishOperator(
        task_id="notify_silver_success",
        target_arn="{{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['SILVER']['SNS_TOPIC_ARN'] }}",
        subject="SILVER SUCCESS",
        message="Pipeline: {{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['SILVER']['PIPELINE_NAME'] }}",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    notify_silver_failure = SnsPublishOperator(
        task_id="notify_silver_failure",
        target_arn="{{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['SILVER']['SNS_TOPIC_ARN_FAILURE'] }}",
        subject="SILVER FAILED",
        message="Pipeline: {{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['SILVER']['PIPELINE_NAME'] }}",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    notify_gold_success = SnsPublishOperator(
        task_id="notify_gold_success",
        target_arn="{{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['GOLD']['SNS_TOPIC_ARN'] }}",
        subject="GOLD SUCCESS",
        message="Pipeline: {{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['GOLD']['PIPELINE_NAME'] }}",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    notify_gold_failure = SnsPublishOperator(
        task_id="notify_gold_failure",
        target_arn="{{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['GOLD']['SNS_TOPIC_ARN_FAILURE'] }}",
        subject="GOLD FAILED",
        message="Pipeline: {{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['GOLD']['PIPELINE_NAME'] }}",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    load_config_task >> generate_run_id_task >> run_silver_task
    run_silver_task >> [notify_silver_success, notify_silver_failure]
    run_silver_task >> run_gold_task
    run_gold_task >> [notify_gold_success, notify_gold_failure]