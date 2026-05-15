import sys
import json
import logging
import boto3
import requests
from datetime import datetime, timezone
from requests.auth import HTTPBasicAuth
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("===== JOB STARTED =====")

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "TABLE_NAME",
        "PREVIOUS_DATE",
        "NEXT_DATE",
        "EXECUTION_ID",
        "AIRFLOW_RUN_ID",
        "ETL_RUN_DATE",
        "SECRET_NAME",
        "BUCKET_NAME",
        "SOURCE_SYSTEM",
    ],
)

JOB_NAME = args["JOB_NAME"]
TABLE_NAME = args["TABLE_NAME"]
PREVIOUS_DATE = args["PREVIOUS_DATE"]
NEXT_DATE = args["NEXT_DATE"]
EXECUTION_ID = args["EXECUTION_ID"]
SECRET_NAME = args["SECRET_NAME"]
BUCKET_NAME = args["BUCKET_NAME"]
SOURCE_SYSTEM = args["SOURCE_SYSTEM"].lower()

LOAD_TIME = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H/%M")

spark = (
    SparkSession.builder.appName(JOB_NAME)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BUCKET_NAME}/")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)

job = Job(glueContext)
job.init(JOB_NAME, args)

logger.info("Spark initialized")

# --------------------------------------------------
# Glue Job Run ID
# --------------------------------------------------
glue_client = boto3.client("glue")

GLUE_JOB_RUN_ID = glue_client.get_job_runs(JobName=JOB_NAME, MaxResults=1)["JobRuns"][0]["Id"]

# --------------------------------------------------
# Update Audit Table
# --------------------------------------------------
def update_audit(status, records_read=0, records_written=0, error_message=None):

    table_ref = "glue_catalog.edp_configs_dev.pipeline_execution_summary"

    safe_error = error_message.replace("'", " ") if error_message else None

    if status == "FAILED":

        sql = f"""
        UPDATE {table_ref}
        SET status='FAILED',
        error_message={f"'{safe_error}'" if safe_error else "NULL"}
        WHERE etl_run_id='{EXECUTION_ID}'
        and table_name='contractdetails'
        """

        spark.sql(sql)
        return

    sql = f"""
    UPDATE {table_ref}
    SET status='SUCCESS',
    records_read={records_read},
    records_written={records_written},
    glue_job_run_id='{GLUE_JOB_RUN_ID}',
    pipeline_end_time=current_timestamp()
    WHERE etl_run_id='{EXECUTION_ID}'
    and table_name='contractdetails'
    """

    spark.sql(sql)


# --------------------------------------------------
# Get Secrets
# --------------------------------------------------
def get_credentials(secret_name):

    client = boto3.client("secretsmanager")

    secret = client.get_secret_value(SecretId=secret_name)["SecretString"]

    creds = json.loads(secret)

    return (
        creds["username"],
        creds["password"],
        creds["sap_base_url"],
        creds["sap_path"],
    )


username, password, sap_base_url, sap_path = get_credentials(SECRET_NAME)

# --------------------------------------------------
# API Wrapper
# --------------------------------------------------
def call_api(url):

    response = requests.get(url, auth=HTTPBasicAuth(username, password), timeout=60)

    if response.status_code != 200:
        raise Exception(f"API failed {response.status_code} : {response.text}")

    return response.json()


# --------------------------------------------------
# Fetch Contract Numbers
# --------------------------------------------------
def fetch_contract_numbers(action):

    logger.info(f"Fetching {action} contracts")

    url = (
        f"{sap_base_url}{sap_path}ZSD_API_CONTRACT_LIST"
        f"?$filter=ContractDate ge {PREVIOUS_DATE}"
        f" and ContractDate lt {NEXT_DATE}"
        f" and attribute eq '{action}'"
        "&$select=ContractNumber"
        "&$top=5000"
        "&$format=json"
    )

    contracts = set()

    while url:

        data = call_api(url)

        batch = data.get("value", [])

        for rec in batch:
            contracts.add(rec["ContractNumber"])

        url = data.get("@odata.nextLink")

    logger.info(f"{action} contracts fetched: {len(contracts)}")

    return list(contracts)


# --------------------------------------------------
# Fetch Contract Details
# --------------------------------------------------
def fetch_contract_details(contract_numbers):

    records = []

    for contract in contract_numbers:

        logger.info(f"Processing contract {contract}")

        url = (
            f"{sap_base_url}/sap/opu/odata4/sap/zsb_contracts_data_api/"
            "srvd_a2x/sap/zsd_contracts_data_api/0001/"
            f"ContractHeader('{contract}')"
            "?$expand=_ContractItems"
            "&$format=json"
        )

        data = call_api(url)

        records.append(data)

    return records


# --------------------------------------------------
# Write JSON
# --------------------------------------------------
def write_json(all_records):

    prefix = (
        f"{SOURCE_SYSTEM}/contracts/{TABLE_NAME}/"
        f"{LOAD_TIME}/{EXECUTION_ID}"
    )

    s3 = boto3.client("s3")

    logger.info(f"Writing output to s3://{BUCKET_NAME}/{prefix}")

    # No data → create empty JSON file
    if not all_records:

        logger.info("No data returned. Creating empty JSON file.")

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{prefix}empty.json",
            Body=""
        )

        return 0

    # Data exists
    path = f"s3://{BUCKET_NAME}/{prefix}"

    df = spark.read.json(spark.sparkContext.parallelize(all_records))

    df.coalesce(1).write.mode("overwrite").json(path)

    return df.count()


# --------------------------------------------------
# Main Pipeline
# --------------------------------------------------
try:

    logger.info("===== PIPELINE START =====")

    total_read = 0
    all_records = []

    # CREATE
    create_contracts = fetch_contract_numbers("CREATE")
    total_read += len(create_contracts)

    create_details = fetch_contract_details(create_contracts)
    all_records.extend(create_details)

    # CHANGE
    change_contracts = fetch_contract_numbers("CHANGE")
    total_read += len(change_contracts)

    change_details = fetch_contract_details(change_contracts)
    all_records.extend(change_details)

    total_written = write_json(all_records)

    update_audit("SUCCESS", total_read, total_written)

    job.commit()

    logger.info("===== JOB COMPLETED SUCCESSFULLY =====")

except Exception as e:

    logger.error("===== JOB FAILED =====")

    update_audit("FAILED", error_message=str(e))

    raise