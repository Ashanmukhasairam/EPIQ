from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import boto3, json, re, time, uuid
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.log.logging_mixin import LoggingMixin
import pendulum

local_tz = pendulum.timezone("America/New_York")
log = LoggingMixin().log


# ---------------- CONFIG LOAD ---------------- #
def load_config(**context):
    s3_path = "s3://epiq-edp-dl-qa-configs/dags/configs/edp_sap_contractlist_config_qa.json"
    match = re.match(r"s3://([^/]+)/(.+)", s3_path)
    s3 = boto3.client("s3")
    config = json.loads(
        s3.get_object(Bucket=match.group(1), Key=match.group(2))["Body"].read()
    )
    context["ti"].xcom_push(key="config", value=config)
    return config


# ---------------- RUN ID ---------------- #
def generate_run_id(**context):
    run_id = str(uuid.uuid4())
    context["ti"].xcom_push(key="etl_run_id", value=run_id)
    return run_id


# ---------------- ATHENA HELPER ---------------- #
def run_athena_query(hook, query, database, output_location):
    """Run an Athena query and block until it completes."""
    query_execution_id = hook.run_query(
        query=query,
        query_context={"Database": database},
        result_configuration={"OutputLocation": output_location},
    )
    hook.poll_query_status(query_execution_id)


# ---------------- AUDIT INSERT ---------------- #
def insert_audit(**context):
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="load_config", key="config")
    etl_run_id = ti.xcom_pull(task_ids="generate_run_id", key="etl_run_id")

    hook = AthenaHook(aws_conn_id=config["AWS_CONN_ID"])
    run_id = context["run_id"]

    silver_cfg = config["LAYERS"]["SILVER"]
    gold_cfg = config["LAYERS"]["GOLD"]

    table = config["TABLE_NAME"]

    job_name_silver = silver_cfg["GLUE_JOBS"][0]["JOB_NAME"]
    job_name_gold = gold_cfg["GLUE_JOBS"][0]["JOB_NAME"]

    # ── CHANGE: check if etl_run_id already exists before inserting ──
    check_query = f"""
        SELECT COUNT(*) as cnt
        FROM {config['DATABASE_NAME']}.{config['SUMMARY_TABLE']}
        WHERE etl_run_id = '{etl_run_id}'
    """

    check_execution_id = hook.run_query(
        query=check_query,
        query_context={"Database": config["DATABASE_NAME"]},
        result_configuration={"OutputLocation": config["ATHENA_OUTPUT"]},
    )
    hook.poll_query_status(check_execution_id)

    athena_client = boto3.client("athena")
    results = athena_client.get_query_results(QueryExecutionId=check_execution_id)
    count = int(results["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"])

    if count > 0:
        log.info(f"etl_run_id '{etl_run_id}' already exists in audit table — skipping insert.")
        return
    # ── END CHANGE ────────────────────────────────────────────────────

    queries = [
        # SILVER
        f"""
        INSERT INTO {config['DATABASE_NAME']}.{config['SUMMARY_TABLE']}
        VALUES (
        '{run_id}',
        '{silver_cfg['PIPELINE_NAME']}',
        '{config['SOURCE_SYSTEM']}',
        '{table}',
        'SILVER',
        '{config['ENVIRONMENT']}',
        '{silver_cfg['LOAD_TYPE']}',
        'NA','NA',
        NULL,
        current_timestamp,
        NULL,
        NULL,
        0,0,0,0,0,0,
        current_timestamp,
        NULL,
        NULL,
        0,
        'RUNNING',
        NULL,
        current_timestamp,
        '{etl_run_id}',
        current_date,
        '{job_name_silver}',
        NULL
        )
        """,
        # GOLD
        f"""
        INSERT INTO {config['DATABASE_NAME']}.{config['SUMMARY_TABLE']}
        VALUES (
        '{run_id}',
        '{gold_cfg['PIPELINE_NAME']}',
        '{config['SOURCE_SYSTEM']}',
        '{table}',
        'GOLD',
        '{config['ENVIRONMENT']}',
        '{gold_cfg['LOAD_TYPE']}',
        'NA','NA',
        NULL,
        current_timestamp,
        NULL,
        NULL,
        0,0,0,0,0,0,
        current_timestamp,
        NULL,
        NULL,
        0,
        'RUNNING',
        NULL,
        current_timestamp,
        '{etl_run_id}',
        current_date,
        '{job_name_gold}',
        NULL
        )
        """,
    ]

    for query in queries:
        run_athena_query(hook, query, config["DATABASE_NAME"], config["ATHENA_OUTPUT"])


# ---------------- STATUS UPDATE ---------------- #
def update_status(layer, status, **context):
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="load_config", key="config")
    etl_run_id = ti.xcom_pull(task_ids="generate_run_id", key="etl_run_id")

    hook = AthenaHook(aws_conn_id=config["AWS_CONN_ID"])
    table = config["TABLE_NAME"]

    query = f"""
    UPDATE {config['DATABASE_NAME']}.{config['SUMMARY_TABLE']}
    SET status='{status}',
        pipeline_end_time=current_timestamp
    WHERE etl_run_id='{etl_run_id}'
    AND table_name='{table}'
    AND layer='{layer}'
    """

    run_athena_query(hook, query, config["DATABASE_NAME"], config["ATHENA_OUTPUT"])


# ---------------- GLUE WAIT ---------------- #
def wait_for_glue(glue, job, run_id):
    start_time = time.time()
    timeout = 60 * 60  # 1 hour

    while True:
        status = glue.get_job_run(JobName=job, RunId=run_id)["JobRun"]["JobRunState"]

        if status == "SUCCEEDED":
            return "SUCCESS"

        if status in ["FAILED", "STOPPED", "TIMEOUT"]:
            return "FAILED"

        if time.time() - start_time > timeout:
            raise AirflowException("Glue job timeout")

        time.sleep(5)


# ---------------- SILVER ---------------- #
def run_silver(**context):
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="load_config", key="config")
    etl_run_id = ti.xcom_pull(task_ids="generate_run_id", key="etl_run_id")

    glue = boto3.client("glue")
    silver_cfg = config["LAYERS"]["SILVER"]
    job_name = silver_cfg["GLUE_JOBS"][0]["JOB_NAME"]

    TABLE_NAME = config["TABLE_NAME"]
    try:
        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                "--ACCOUNT_ID":        silver_cfg["ACCOUNT_ID"],
                "--AIRFLOW_RUN_ID":    context["run_id"],
                "--EXECUTION_ID":      etl_run_id,
                "--ETL_RUN_DATE":      context["dag_run"].conf.get("etl_run_date", context["ds"]),
                "--SILVER_DATABASE":   silver_cfg["SILVER_DATABASE"],
                "--SILVER_TABLE":      silver_cfg["SILVER_TABLE"],
                "--TABLE_BUCKET_NAME": silver_cfg["TABLE_BUCKET_NAME"],
                "--BRONZE_BUCKET_NAME":silver_cfg["BRONZE_BUCKET_NAME"],
                "--SOURCE":            silver_cfg["SOURCE"],
                "--AWS_REGION":        silver_cfg["AWS_REGION"],
            },
        )

        glue_run_id = response["JobRunId"]
        status = wait_for_glue(glue, job_name, glue_run_id)

    except Exception as e:
        ti.xcom_push(key="silver_error", value=f"{type(e).__name__}: {str(e)}")
        update_status("SILVER", "FAILED", **context)
        raise

    update_status("SILVER", status, **context)

    if status != "SUCCESS":
        ti.xcom_push(key="silver_error", value="Glue job returned FAILED")
        raise AirflowException(f"Silver failed for {TABLE_NAME}")

    log.info("Waiting 5 seconds after silver...")
    time.sleep(5)


# ---------------- GOLD ---------------- #
def run_gold(**context):
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="load_config", key="config")
    etl_run_id = ti.xcom_pull(task_ids="generate_run_id", key="etl_run_id")

    glue = boto3.client("glue")
    gold_cfg = config["LAYERS"]["GOLD"]
    job_name = gold_cfg["GLUE_JOBS"][0]["JOB_NAME"]

    TABLE_NAME = config["TABLE_NAME"]
    try:
        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                "--ACCOUNT_ID":      gold_cfg["ACCOUNT_ID"],
                "--GOLD_BUCKET":     gold_cfg["GOLD_BUCKET"],
                "--GOLD_DATABASE":   gold_cfg["GOLD_DATABASE"],
                "--SILVER_BUCKET":   gold_cfg["SILVER_BUCKET"],
                "--SILVER_DATABASE": gold_cfg["SILVER_DATABASE"],
                "--TABLE_NAME":      gold_cfg["GOLD_TABLE"],
                "--EXECUTION_ID":    etl_run_id,             
                "--AWS_REGION":      gold_cfg["AWS_REGION"],   
            },
        )

        glue_run_id = response["JobRunId"]
        status = wait_for_glue(glue, job_name, glue_run_id)

    except Exception as e:
        ti.xcom_push(key="gold_error", value=f"{type(e).__name__}: {str(e)}")
        update_status("GOLD", "FAILED", **context)
        raise

    update_status("GOLD", status, **context)

    if status != "SUCCESS":
        ti.xcom_push(key="gold_error", value="Glue job returned FAILED")
        raise AirflowException(f"Gold failed for {TABLE_NAME}")


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------- DAG ---------------- #
with DAG(
    dag_id="dag-sap-contractlist-silver-gold-pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz=local_tz),
    schedule="0 10 * * *",   # 10 AM US time
    catchup=False,
    default_args=default_args,
) as dag:

    load_config_task = PythonOperator(
        task_id="load_config",
        python_callable=load_config,
    )

    generate_run_id_task = PythonOperator(
        task_id="generate_run_id",
        python_callable=generate_run_id,
    )

    audit_task = PythonOperator(
        task_id="insert_audit",
        python_callable=insert_audit,
    )

    run_silver_task = PythonOperator(
        task_id="run_silver",
        python_callable=run_silver,
        execution_timeout=timedelta(hours=2),
    )

    gold_task = PythonOperator(
        task_id="run_gold",
        python_callable=run_gold,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        execution_timeout=timedelta(hours=2),
    )

    # ---------------- SILVER SUCCESS ---------------- #
    notify_silver_success = SnsPublishOperator(
        task_id="notify_silver_success",
        target_arn="{{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['SILVER']['SNS_TOPIC_ARN'] }}",
        subject="SILVER SUCCESS",
        message="""
        Pipeline: {{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['SILVER']['PIPELINE_NAME'] }}
        Table: {{ ti.xcom_pull(task_ids='load_config', key='config')['TABLE_NAME'] }}
        Status: SUCCESS
        Run ID: {{ run_id }}
        Execution Date: {{ ds }}
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        aws_conn_id="aws_default",
    )

    notify_silver_failure = SnsPublishOperator(
        task_id="notify_silver_failure",
        target_arn="{{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['SILVER']['SNS_TOPIC_ARN_FAILURE'] }}",
        subject="SILVER FAILED",
        message="""
        {% set cfg = ti.xcom_pull(task_ids='load_config', key='config') %}
        Pipeline: {{ cfg['LAYERS']['SILVER']['PIPELINE_NAME'] }}
        Table: {{ cfg['TABLE_NAME'] }}
        Status: FAILED
        Run ID: {{ run_id }}
        Execution Date: {{ ds }}
        Error: {{ ti.xcom_pull(task_ids='run_silver', key='silver_error') or 'No error captured' }}
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
        aws_conn_id="aws_default",
    )

    # ---------------- GOLD SUCCESS ---------------- #
    notify_gold_success = SnsPublishOperator(
        task_id="notify_gold_success",
        target_arn="{{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['GOLD']['SNS_TOPIC_ARN'] }}",
        subject="GOLD SUCCESS",
        message="""
        Pipeline: {{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['GOLD']['PIPELINE_NAME'] }}
        Table: {{ ti.xcom_pull(task_ids='load_config', key='config')['TABLE_NAME'] }}
        Status: SUCCESS
        Run ID: {{ run_id }}
        Execution Date: {{ ds }}
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        aws_conn_id="aws_default",
    )

    notify_gold_failure = SnsPublishOperator(
        task_id="notify_gold_failure",
        target_arn="{{ ti.xcom_pull(task_ids='load_config', key='config')['LAYERS']['GOLD']['SNS_TOPIC_ARN_FAILURE'] }}",
        subject="GOLD FAILED",
        message="""
        {% set cfg = ti.xcom_pull(task_ids='load_config', key='config') %}
        Pipeline: {{ cfg['LAYERS']['GOLD']['PIPELINE_NAME'] }}
        Table: {{ cfg['TABLE_NAME'] }}
        Status: FAILED
        Run ID: {{ run_id }}
        Execution Date: {{ ds }}
        Error: {{ ti.xcom_pull(task_ids='run_gold', key='gold_error') or 'No error captured' }}
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
        aws_conn_id="aws_default",
    )

    # Main flow
    load_config_task >> generate_run_id_task >> audit_task >> run_silver_task

    # SILVER notifications
    run_silver_task >> [notify_silver_success, notify_silver_failure]

    run_silver_task >> gold_task

    # GOLD notifications
    gold_task >> [notify_gold_success, notify_gold_failure]