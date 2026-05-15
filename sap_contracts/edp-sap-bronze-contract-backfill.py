import sys
import json
import logging
import boto3
import requests
from requests.auth import HTTPBasicAuth

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# Job Params
# --------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ACCOUNT_ID",
        "SECRET_NAME",
        "POC_BUCKET",
        "SILVER_BUCKET",
        "TARGET_BUCKET"
    ],
)

JOB_NAME = args["JOB_NAME"]
ACCOUNT_ID = args["ACCOUNT_ID"]
SECRET_NAME = args["SECRET_NAME"]
POC_BUCKET = args["POC_BUCKET"]
SILVER_BUCKET = args["SILVER_BUCKET"]
TARGET_BUCKET = args["TARGET_BUCKET"]

BACKFILL_DATE = "2026-04-04"

# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = (
    SparkSession.builder.appName("contract-backfill")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

    .config("spark.sql.catalog.stage", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.stage.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.stage.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{POC_BUCKET}")
    .config("spark.sql.catalog.stage.warehouse", f"s3://{POC_BUCKET}/bucket/{POC_BUCKET}")

    .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.silver.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.silver.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{SILVER_BUCKET}")
    .config("spark.sql.catalog.silver.warehouse", f"s3://{SILVER_BUCKET}/bucket/{SILVER_BUCKET}")

    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(JOB_NAME, args)

# --------------------------------------------------
# Get API Credentials
# --------------------------------------------------
def get_credentials(secret_name):
    client = boto3.client("secretsmanager")
    secret = client.get_secret_value(SecretId=secret_name)["SecretString"]
    creds = json.loads(secret)

    return creds["username"], creds["password"], creds["sap_base_url"]

username, password, sap_base_url = get_credentials(SECRET_NAME)

# --------------------------------------------------
# API CALL FUNCTION
# --------------------------------------------------
def call_api(contract_number):

    url = (
        f"{sap_base_url}/sap/opu/odata4/sap/zsb_contracts_data_api/"
        "srvd_a2x/sap/zsd_contracts_data_api/0001/"
        f"ContractHeader('{contract_number}')"
        "?$expand=_ContractItems&$format=json"
    )

    try:
        response = requests.get(url, auth=HTTPBasicAuth(username, password), timeout=60)

        if response.status_code != 200:
            return {"_corrupt_record": response.text}

        raw_text = response.text

        try:
            return json.loads(raw_text)
        except:
            return {"_corrupt_record": raw_text}

    except Exception as e:
        return {"_corrupt_record": str(e)}

# --------------------------------------------------
# READ TABLES
# --------------------------------------------------
contract_list_df = spark.read.table(
    "stage.stage_db.contract_list"
).select("contract_number").distinct()

contract_header_df = spark.read.table(
    "silver.silver_sap_db.contract_header"
).select("contract_number").distinct()

# --------------------------------------------------
# FIND MISSING CONTRACTS
# --------------------------------------------------
missing_contracts_df = contract_list_df.join(
    contract_header_df,
    on="contract_number",
    how="left_anti"
)

logger.info(f"Missing contracts: {missing_contracts_df.count()}")

# --------------------------------------------------
# CALL API
# --------------------------------------------------
all_records = []

for row in missing_contracts_df.toLocalIterator():
    contract = row["contract_number"]
    logger.info(f"Processing {contract}")

    data = call_api(contract)
    all_records.append(data)

# --------------------------------------------------
# CREATE DF
# json.dumps() converts Python dicts to valid JSON strings.
# Without this, Spark uses repr() → single quotes, True/False/None → corrupt data.
# --------------------------------------------------
json_strings = [json.dumps(r) for r in all_records]
df = spark.read.json(spark.sparkContext.parallelize(json_strings))

# --------------------------------------------------
# SPLIT GOOD vs CORRUPT
# --------------------------------------------------
year, month, day = BACKFILL_DATE.split("-")
base_path = f"s3://{TARGET_BUCKET}/sap/contracts/contractdetails/{year}/{month}/{day}"

if "_corrupt_record" in df.columns:
    good_df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
    corrupt_df = df.filter(col("_corrupt_record").isNotNull())
else:
    good_df = df
    corrupt_df = None

# --------------------------------------------------
# WRITE GOOD RECORDS
# --------------------------------------------------
good_path = f"{base_path}/backfill/"
good_count = good_df.count()

if good_count > 0:
    good_df.coalesce(1).write.mode("append").json(good_path)
    logger.info(f"Good records written: {good_count} → {good_path}")
else:
    logger.info("No good records to write")

# --------------------------------------------------
# WRITE CORRUPT RECORDS (separate folder)
# --------------------------------------------------
if corrupt_df is not None:
    corrupt_count = corrupt_df.count()
    if corrupt_count > 0:
        corrupt_path = f"{base_path}/corrupt/"
        corrupt_df.coalesce(1).write.mode("append").json(corrupt_path)
        logger.info(f"Corrupt records written: {corrupt_count} → {corrupt_path}")
    else:
        corrupt_count = 0
        logger.info("No corrupt records")
else:
    corrupt_count = 0
    logger.info("No corrupt records")

logger.info(f"Total: {good_count + corrupt_count} | Good: {good_count} | Corrupt: {corrupt_count}")

# --------------------------------------------------
# END JOB
# --------------------------------------------------
job.commit()
