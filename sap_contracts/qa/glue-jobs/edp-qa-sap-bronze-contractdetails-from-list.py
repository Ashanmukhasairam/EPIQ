'''
aws glue start-job-run \
--job-name edp-qa-sap-bronze-contractdetails-from-list \
--arguments '{
  "--JOB_NAME":"edp-qa-sap-bronze-contractdetails-from-list",
  "--ETL_RUN_DATE":"2026-04-26",
  "--EXECUTION_ID":"bronze_2026_04_26",
  "--AIRFLOW_RUN_ID":"manual_run",
  "--SECRET_NAME":"epiq-edp-sap-qa-secret",
  "--BUCKET_NAME":"epiq-edp-dl-qa-bronze",
  "--SOURCE_SYSTEM":"sap",
  "--TABLE_NAME":"contractdetails",
  "--MAX_WORKERS":"10"
}'
'''

# ============================================================
# SAP BRONZE GLUE JOB — contractdetails (from contractlists)
#
# Flow:
#   1. Read contract numbers from contractlists bronze for ETL_RUN_DATE
#   2. Call SAP ContractHeader API (with _ContractItems expand) per contract
#   3. Write good records  → s3://{BUCKET}/{source}/contracts/{TABLE}/{Y}/{M}/{D}/{EXEC_ID}/
#      Write corrupt records → .../corrupt/
#   4. Update pipeline audit table
# ============================================================

import sys
import json
import logging
import time
import boto3
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from requests.auth import HTTPBasicAuth

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ============================================================
# Logging
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("===== JOB STARTED =====")

# ============================================================
# Parameters
# ============================================================
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ETL_RUN_DATE",
        "EXECUTION_ID",
        "AIRFLOW_RUN_ID",
        "SECRET_NAME",
        "BUCKET_NAME",
        "SOURCE_SYSTEM",
        "TABLE_NAME",
    ],
)

JOB_NAME      = args["JOB_NAME"]
ETL_RUN_DATE  = args["ETL_RUN_DATE"]
EXECUTION_ID  = args["EXECUTION_ID"]
AIRFLOW_RUN_ID = args["AIRFLOW_RUN_ID"]
SECRET_NAME   = args["SECRET_NAME"]
BUCKET_NAME   = args["BUCKET_NAME"]
SOURCE_SYSTEM = args["SOURCE_SYSTEM"].lower()
TABLE_NAME    = args["TABLE_NAME"]          # e.g. "contractdetails"

# MAX_WORKERS is optional — default 10 parallel API threads
MAX_WORKERS = int(args.get("MAX_WORKERS", "10")) if "MAX_WORKERS" in args else 10

etl_dt = datetime.strptime(ETL_RUN_DATE, "%Y-%m-%d")
YEAR   = etl_dt.strftime("%Y")
MONTH  = etl_dt.strftime("%m")
DAY    = etl_dt.strftime("%d")

LOAD_TIME = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H/%M")

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder.appName(JOB_NAME)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BUCKET_NAME}/")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(JOB_NAME, args)

glue_client = boto3.client("glue")
GLUE_JOB_RUN_ID = glue_client.get_job_runs(JobName=JOB_NAME, MaxResults=1)["JobRuns"][0]["Id"]
logger.info(f"Glue Job Run ID: {GLUE_JOB_RUN_ID}")

# ============================================================
# Audit
# ============================================================
def update_audit(status, records_read=0, records_written=0, error_message=None):
    table_ref  = "glue_catalog.edp_configs_dev.pipeline_execution_summary"
    safe_error = error_message.replace("'", " ") if error_message else None

    if status == "FAILED":
        sql = f"""
        UPDATE {table_ref}
        SET status = 'FAILED',
            error_message = {f"'{safe_error}'" if safe_error else "NULL"},
            pipeline_end_time = current_timestamp(),
            duration_seconds = unix_timestamp(current_timestamp()) - unix_timestamp(pipeline_start_time)
        WHERE etl_run_id = '{EXECUTION_ID}'
          AND run_id = '{AIRFLOW_RUN_ID}'
          AND table_name = '{TABLE_NAME}'
        """
    else:
        sql = f"""
        UPDATE {table_ref}
        SET status = 'SUCCESS',
            records_read    = {records_read},
            records_written = {records_written},
            glue_job_run_id = '{GLUE_JOB_RUN_ID}',
            pipeline_end_time = current_timestamp(),
            duration_seconds = unix_timestamp(current_timestamp()) - unix_timestamp(pipeline_start_time)
        WHERE etl_run_id = '{EXECUTION_ID}'
          AND run_id = '{AIRFLOW_RUN_ID}'
          AND table_name = '{TABLE_NAME}'
        """

    logger.info(f"AUDIT UPDATE ({status}):\n{sql}")
    spark.sql(sql)

# ============================================================
# Secrets
# ============================================================
def get_credentials(secret_name):
    client = boto3.client("secretsmanager")
    secret = client.get_secret_value(SecretId=secret_name)["SecretString"]
    creds  = json.loads(secret)
    return creds["username"], creds["password"], creds["sap_base_url"]

username, password, sap_base_url = get_credentials(SECRET_NAME)

# ============================================================
# Read contract numbers from contractlists bronze
# ============================================================
def read_contract_numbers():
    """
    Reads the contractlists bronze written for ETL_RUN_DATE.
    Uses recursiveFileLookup to cover all HH/MM sub-partitions and
    any EXECUTION_ID sub-folders written on that date.
    """
    path = f"s3://{BUCKET_NAME}/{SOURCE_SYSTEM}/contracts/contractlists/{YEAR}/{MONTH}/{DAY}/"
    logger.info(f"Reading contractlists bronze from: {path}")

    try:
        df = (
            spark.read
            .option("recursiveFileLookup", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(path)
        )
    except Exception as e:
        raise Exception(f"Cannot read contractlists bronze at {path}: {e}")

    # Drop unparseable rows
    if "_corrupt_record" in df.columns:
        bad = df.filter(col("_corrupt_record").isNotNull()).count()
        if bad:
            logger.warning(f"Dropping {bad} corrupt rows from contractlists bronze")
        df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

    if "ContractNumber" not in df.columns:
        raise Exception(
            f"ContractNumber column not found in contractlists bronze. "
            f"Available columns: {df.columns}"
        )

    contracts = (
        df.select(col("ContractNumber").cast("string"))
        .filter(col("ContractNumber").isNotNull() & (col("ContractNumber") != ""))
        .distinct()
        .rdd.flatMap(lambda r: r)
        .collect()
    )

    logger.info(f"Unique contract numbers loaded: {len(contracts)}")
    return contracts

# ============================================================
# SAP API — single contract with retry + backoff
# ============================================================
def call_api(contract_number, max_retries=3, backoff_base=2):
    url = (
        f"{sap_base_url}/sap/opu/odata4/sap/zsb_contracts_data_api/"
        "srvd_a2x/sap/zsd_contracts_data_api/0001/"
        f"ContractHeader('{contract_number}')"
        "?$expand=_ContractItems&$format=json"
    )

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(
                url,
                auth=HTTPBasicAuth(username, password),
                timeout=60,
            )

            if resp.status_code == 200:
                try:
                    return json.loads(resp.text)
                except json.JSONDecodeError as exc:
                    # Non-retryable parse error
                    return {
                        "_corrupt_record": f"JSON parse error: {exc}",
                        "_contract_number": contract_number,
                    }

            if resp.status_code == 404:
                # Contract does not exist — not retryable
                logger.warning(f"[{contract_number}] 404 Not Found — skipping")
                return {
                    "_corrupt_record": "404 Not Found",
                    "_contract_number": contract_number,
                }

            # 5xx or unexpected status — retryable
            logger.warning(
                f"[{contract_number}] HTTP {resp.status_code} (attempt {attempt}/{max_retries})"
            )

        except requests.exceptions.Timeout:
            logger.warning(f"[{contract_number}] Timeout (attempt {attempt}/{max_retries})")

        except requests.exceptions.ConnectionError as exc:
            logger.warning(f"[{contract_number}] Connection error: {exc} (attempt {attempt}/{max_retries})")

        except Exception as exc:
            logger.warning(f"[{contract_number}] Unexpected error: {exc} (attempt {attempt}/{max_retries})")

        if attempt < max_retries:
            sleep_secs = backoff_base ** attempt
            logger.info(f"[{contract_number}] Retrying in {sleep_secs}s")
            time.sleep(sleep_secs)

    return {
        "_corrupt_record": f"Failed after {max_retries} retries",
        "_contract_number": contract_number,
    }

# ============================================================
# Parallel fetch
# ============================================================
def fetch_all_details(contract_numbers):
    good    = []
    corrupt = []
    total   = len(contract_numbers)

    logger.info(f"Starting parallel fetch: {total} contracts, {MAX_WORKERS} workers")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(call_api, cn): cn for cn in contract_numbers}

        for idx, future in enumerate(as_completed(futures), 1):
            contract = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "_corrupt_record": f"Future exception: {exc}",
                    "_contract_number": contract,
                }

            if "_corrupt_record" in result:
                corrupt.append(result)
            else:
                good.append(result)

            if idx % 100 == 0 or idx == total:
                logger.info(
                    f"Progress: {idx}/{total} | Good: {len(good)} | Corrupt: {len(corrupt)}"
                )

    logger.info(f"Fetch complete — Good: {len(good)} | Corrupt: {len(corrupt)}")
    return good, corrupt

# ============================================================
# Write records to S3
# ============================================================
def write_records(records, s3_path, label):
    if not records:
        logger.info(f"No {label} records to write — skipping")
        return 0

    json_strings = [json.dumps(r) for r in records]
    rdd = spark.sparkContext.parallelize(json_strings)
    df  = spark.read.json(rdd)

    df.coalesce(1).write.mode("append").json(s3_path)
    count = df.count()
    logger.info(f"{label} records written: {count} → {s3_path}")
    return count

# ============================================================
# Write empty S3 marker when there is genuinely no input data
# ============================================================
def write_empty_marker(base_s3_key):
    s3 = boto3.client("s3")
    key = f"{base_s3_key}/empty.json"
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=b"")
    logger.info(f"Empty marker written: s3://{BUCKET_NAME}/{key}")

# ============================================================
# Main
# ============================================================
try:
    logger.info("===== PIPELINE START =====")

    contract_numbers = read_contract_numbers()

    base_s3_prefix = (
        f"{SOURCE_SYSTEM}/contracts/{TABLE_NAME}/{YEAR}/{MONTH}/{DAY}/{EXECUTION_ID}"
    )
    base_s3_path = f"s3://{BUCKET_NAME}/{base_s3_prefix}"

    if not contract_numbers:
        logger.warning("Contractlists bronze has no contract numbers for this date — writing empty marker")
        write_empty_marker(base_s3_prefix)
        update_audit("SUCCESS", records_read=0, records_written=0)
        job.commit()
        logger.info("===== JOB COMPLETED (NO INPUT DATA) =====")
        sys.exit(0)

    good_records, corrupt_records = fetch_all_details(contract_numbers)

    good_written    = write_records(good_records,    f"{base_s3_path}/",        "good")
    corrupt_written = write_records(corrupt_records, f"{base_s3_path}/corrupt/", "corrupt")

    if good_written == 0 and corrupt_written == 0:
        # All contracts came back empty responses — still write marker so silver doesn't error
        write_empty_marker(base_s3_prefix)

    total_read = len(contract_numbers)
    logger.info(
        f"Records processed: {total_read} | Written: {good_written} | Corrupt: {corrupt_written}"
    )

    update_audit("SUCCESS", records_read=total_read, records_written=good_written)

    job.commit()
    logger.info("===== JOB COMPLETED SUCCESSFULLY =====")

except Exception as e:
    logger.error(f"===== JOB FAILED: {e} =====")
    try:
        update_audit("FAILED", error_message=str(e))
    except Exception as audit_err:
        logger.error(f"Audit update also failed: {audit_err}")
    raise
