'''
aws glue start-job-run \
--job-name edp-qa-sap-silver-contract_header_items \
--arguments '{
  "--ACCOUNT_ID":"730878889077",
  "--AIRFLOW_RUN_ID":"manual_run",
  "--BRONZE_BUCKET_NAME":"epiq-edp-dl-qa-bronze",
  "--CONFIG_BUCKET":"epiq-edp-dl-qa-configs",
  "--ETL_RUN_DATE":"2026-04-26",
  "--EXECUTION_ID":"silver_2026_04_26",
  "--SILVER_DATABASE":"silver_sap_db",
  "--SOURCE":"sap",
  "--TABLE_BUCKET_NAME":"epiq-edp-dl-qa-silver",
  "--SILVER_TABLE":"contract_header,contract_items",
  "--BRONZE_FOLDER":"contractdetails"
}'
'''

# ============================================================
# SAP SILVER GLUE JOB — contract_header + contract_items
#
# Reads contractdetails JSON from bronze, applies mapping CSV,
# deduplicates, and upserts into Iceberg S3 Tables.
#
# SILVER_TABLE accepts a comma-separated list, e.g.:
#   "contract_header,contract_items"
#
# BRONZE_FOLDER is the bronze subfolder name (e.g. "contractdetails").
# The -qa suffix is NOT appended automatically — pass exactly what
# was written by the bronze job.
# ============================================================

import sys
import re
import logging
from datetime import datetime, date

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date,
    explode, row_number, md5, concat_ws, when,
)
from pyspark.sql.types import DecimalType, StringType, IntegerType, DoubleType

# ============================================================
# Logging
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# Arguments
# ============================================================
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ACCOUNT_ID",
        "AIRFLOW_RUN_ID",
        "BRONZE_BUCKET_NAME",
        "CONFIG_BUCKET",
        "ETL_RUN_DATE",
        "EXECUTION_ID",
        "SILVER_DATABASE",
        "SOURCE",
        "TABLE_BUCKET_NAME",
        "SILVER_TABLE",
        "BRONZE_FOLDER",
    ],
)

JOB_NAME          = args["JOB_NAME"]
ACCOUNT_ID        = args["ACCOUNT_ID"]
AIRFLOW_RUN_ID    = args["AIRFLOW_RUN_ID"]
BRONZE_BUCKET     = args["BRONZE_BUCKET_NAME"]
CONFIG_BUCKET     = args["CONFIG_BUCKET"]
ETL_RUN_DATE      = args["ETL_RUN_DATE"]
EXECUTION_ID      = args["EXECUTION_ID"]
DB                = args["SILVER_DATABASE"]
SOURCE            = args["SOURCE"]
TABLE_BUCKET_NAME = args["TABLE_BUCKET_NAME"]
SILVER_TABLE      = args["SILVER_TABLE"]
BRONZE_FOLDER     = args["BRONZE_FOLDER"]          # e.g. "contractdetails"

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder.appName(JOB_NAME)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.defaultCatalog", "s3tables")
    .config("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.s3tables.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config(
        "spark.sql.catalog.s3tables.glue.id",
        f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}",
    )
    .config("spark.sql.catalog.s3tables.warehouse", TABLE_BUCKET_NAME)
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config("spark.sql.catalog.glue_catalog.glue.region", "us-east-1")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(JOB_NAME, args)

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tables.{DB}")

# ============================================================
# Read mapping CSV
# ============================================================
def read_mapping(base_object):
    mapping_path = f"s3://{CONFIG_BUCKET}/{SOURCE}/mappings/contracts_mapping.csv"

    df = (
        spark.read.option("header", "true")
        .csv(mapping_path)
        .filter(
            (col("source_system") == SOURCE)
            & (col("source_object") == base_object)
            & (col("is_active") == "TRUE")
            & (col("is_silver") == "TRUE")
        )
    )

    rows = df.collect()
    if not rows:
        raise Exception(
            f"No active silver mapping rows found for source_object='{base_object}'. "
            f"Check {mapping_path}."
        )

    pk_columns = (
        df.filter(col("is_primary_key") == "TRUE")
        .select("target_field")
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    if not pk_columns:
        raise Exception(f"No primary key defined for '{base_object}' in mapping")

    logger.info(f"[{base_object}] Mapping rows: {len(rows)} | PKs: {pk_columns}")
    return rows, pk_columns

# ============================================================
# Create Iceberg table from mapping if not exists
# ============================================================
def create_ddl(base_object, mapping_rows):
    ddl_columns = []

    for row in mapping_rows:
        target = row["target_field"].strip()
        dtype  = row["target_datatype"].strip().lower().replace(" ", "")

        if dtype.startswith("decimal"):
            nums = re.findall(r"\d+", dtype)
            ddl_columns.append(f"{target} decimal({nums[0]},{nums[1]})")
        elif dtype == "date":
            ddl_columns.append(f"{target} date")
        elif dtype == "int":
            ddl_columns.append(f"{target} int")
        elif dtype == "double":
            ddl_columns.append(f"{target} double")
        else:
            ddl_columns.append(f"{target} string")

    ddl_columns += [
        "execution_id string",
        "airflow_run_id string",
        "etl_run_date string",
        "source_system string",
        "ingestion_ts timestamp",
    ]

    ddl = f"""
        CREATE TABLE IF NOT EXISTS s3tables.{DB}.{base_object}
        ({', '.join(ddl_columns)})
        USING iceberg
        PARTITIONED BY (etl_run_date)
    """
    logger.info(f"[{base_object}] Creating table if not exists")
    spark.sql(ddl)

# ============================================================
# Read bronze contractdetails
# ============================================================
def read_bronze(etl_run_date_str):
    etl_dt = datetime.strptime(etl_run_date_str, "%Y-%m-%d")
    year   = etl_dt.strftime("%Y")
    month  = etl_dt.strftime("%m")
    day    = etl_dt.strftime("%d")
    path   = f"s3://{BRONZE_BUCKET}/{SOURCE}/contracts/{BRONZE_FOLDER}/{year}/{month}/{day}/"

    logger.info(f"Reading bronze from: {path}")

    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(path)
    )

    # Separate and log corrupt JSON rows
    if "_corrupt_record" in df.columns:
        corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
        if corrupt_count > 0:
            logger.warning(f"Bronze contains {corrupt_count} unparseable record(s) — dropping")
        df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

    # Drop rows that are completely null (empty marker files produce these)
    df = df.dropna(how="all")

    rows_read = df.count()
    logger.info(f"Bronze rows after cleaning: {rows_read}")
    return df, rows_read

# ============================================================
# Transform bronze → silver
# ============================================================
def transform_data(bronze_df, mapping_rows, base_object, pk_cols):
    """
    For contract_items: explode _ContractItems nested array.
    Applies mapping casts, filters null PKs, adds audit columns,
    deduplicates deterministically.
    """

    if base_object == "contract_items":
        items_col = None
        for candidate in ("_ContractItems", "ContractItems", "_contractitems", "contractitems"):
            if candidate in bronze_df.columns:
                items_col = candidate
                break

        if items_col is None:
            logger.warning(
                f"[{base_object}] No ContractItems column found in bronze. "
                f"Available columns: {bronze_df.columns}. Returning empty DataFrame."
            )
            # Build an empty DF with expected target columns
            empty_cols = (
                [row["target_field"].strip() for row in mapping_rows]
                + ["execution_id", "airflow_run_id", "etl_run_date", "source_system", "ingestion_ts"]
            )
            return spark.createDataFrame([], _build_schema_from_mapping(mapping_rows)), \
                   spark.createDataFrame([], _build_schema_from_mapping(mapping_rows))

        bronze_df = (
            bronze_df
            .select(col("ContractNumber"), explode(col(items_col)).alias("item"))
            .select(col("ContractNumber"), col("item.*"))
        )

    # Build select expressions from mapping
    select_expr = []
    missing_source_cols = []

    for row in mapping_rows:
        source = row["source_field"].strip()
        target = row["target_field"].strip()
        dtype  = row["target_datatype"].strip().lower().replace(" ", "")

        if source not in bronze_df.columns:
            missing_source_cols.append(source)
            select_expr.append(lit(None).cast(StringType()).alias(target))
            continue

        if dtype.startswith("decimal"):
            nums = re.findall(r"\d+", dtype)
            select_expr.append(
                col(source).cast(DecimalType(int(nums[0]), int(nums[1]))).alias(target)
            )
        elif dtype == "date":
            select_expr.append(to_date(col(source)).alias(target))
        elif dtype == "int":
            select_expr.append(col(source).cast(IntegerType()).alias(target))
        elif dtype == "double":
            select_expr.append(col(source).cast(DoubleType()).alias(target))
        else:
            select_expr.append(col(source).cast(StringType()).alias(target))

    if missing_source_cols:
        logger.warning(
            f"[{base_object}] Source fields missing in bronze, will be NULL: {missing_source_cols}"
        )

    df = bronze_df.select(*select_expr)

    # Filter rows where any PK is null
    for pk in pk_cols:
        if pk in df.columns:
            null_count = df.filter(col(pk).isNull()).count()
            if null_count:
                logger.warning(f"[{base_object}] Dropping {null_count} rows with null PK '{pk}'")
            df = df.filter(col(pk).isNotNull())

    df = (
        df.withColumn("execution_id",   lit(EXECUTION_ID))
          .withColumn("airflow_run_id", lit(AIRFLOW_RUN_ID))
          .withColumn("etl_run_date",   lit(ETL_RUN_DATE))
          .withColumn("source_system",  lit(SOURCE))
          .withColumn("ingestion_ts",   current_timestamp())
    )

    # Deterministic deduplication: latest ingestion_ts wins; hash breaks ties
    business_cols = [row["target_field"].strip() for row in mapping_rows]
    df = df.withColumn(
        "_row_hash",
        md5(concat_ws("||", *[col(c).cast("string") for c in business_cols])),
    )

    window = Window.partitionBy(*pk_cols).orderBy(
        col("ingestion_ts").desc(),
        col("_row_hash").asc(),
    )
    df_ranked = df.withColumn("rn", row_number().over(window))

    total_rows  = df_ranked.count()
    df_clean    = df_ranked.filter(col("rn") == 1).drop("rn", "_row_hash")
    df_dups     = df_ranked.filter(col("rn") > 1).drop("rn", "_row_hash")
    dup_count   = df_dups.count()

    logger.info(
        f"[{base_object}] Total: {total_rows} | Clean: {total_rows - dup_count} | Duplicates: {dup_count}"
    )
    return df_clean, df_dups

def _build_schema_from_mapping(mapping_rows):
    from pyspark.sql.types import StructType, StructField, StringType as ST
    fields = [StructField(row["target_field"].strip(), ST(), True) for row in mapping_rows]
    fields += [
        StructField("execution_id",   ST(), True),
        StructField("airflow_run_id", ST(), True),
        StructField("etl_run_date",   ST(), True),
        StructField("source_system",  ST(), True),
        StructField("ingestion_ts",   ST(), True),
    ]
    return StructType(fields)

# ============================================================
# Process one silver object end-to-end
# ============================================================
def process_object(base_object, bronze_df, rows_read):
    table_fqn = f"s3tables.{DB}.{base_object}"
    logger.info(f"Processing: {table_fqn}")

    mapping_rows, pk_columns = read_mapping(base_object)

    if not spark.catalog.tableExists(table_fqn):
        create_ddl(base_object, mapping_rows)

    df_clean, df_dups = transform_data(bronze_df, mapping_rows, base_object, pk_columns)

    rows_written      = df_clean.count()
    records_rejected  = df_dups.count()

    if rows_written > 0:
        df_clean.writeTo(table_fqn).overwritePartitions()
        logger.info(f"[{base_object}] Wrote {rows_written} rows to {table_fqn}")
    else:
        logger.warning(f"[{base_object}] No clean rows to write — silver table not updated")

    if records_rejected > 0:
        today    = str(date.today())
        log_path = f"s3://{CONFIG_BUCKET}/logs/{SOURCE}/{base_object}/log_date={today}/"
        df_dups.write.mode("overwrite").parquet(log_path)
        logger.info(f"[{base_object}] Wrote {records_rejected} duplicate(s) to {log_path}")
        _delete_folder_markers(CONFIG_BUCKET, f"logs/{SOURCE}/{base_object}/")
    else:
        logger.info(f"[{base_object}] No duplicates")

    return base_object, rows_read, rows_written, records_rejected

# ============================================================
# Audit update
# ============================================================
def update_audit(table_name, rows_read=None, rows_written=None,
                 records_rejected=None, status=None, error_message=None):
    table_ref  = "glue_catalog.edp_configs_dev.pipeline_execution_summary"
    safe_error = error_message.replace("'", " ") if error_message else None

    if status == "FAILED":
        query = f"""
        UPDATE {table_ref}
        SET status = 'FAILED',
            error_message = '{safe_error}',
            pipeline_end_time = current_timestamp(),
            duration_seconds  =
                unix_timestamp(current_timestamp()) - unix_timestamp(pipeline_start_time)
        WHERE etl_run_id = '{EXECUTION_ID}'
          AND run_id     = '{AIRFLOW_RUN_ID}'
          AND layer      = 'SILVER'
          AND table_name = '{table_name}'
        """
    else:
        query = f"""
        UPDATE {table_ref}
        SET records_read      = {rows_read},
            records_written   = {rows_written},
            records_rejected  = {records_rejected},
            status            = 'SUCCESS',
            error_message     = NULL,
            pipeline_end_time = current_timestamp(),
            duration_seconds  =
                unix_timestamp(current_timestamp()) - unix_timestamp(pipeline_start_time)
        WHERE etl_run_id = '{EXECUTION_ID}'
          AND run_id     = '{AIRFLOW_RUN_ID}'
          AND layer      = 'SILVER'
          AND table_name = '{table_name}'
        """

    logger.info(f"AUDIT UPDATE ({status}) for {table_name}")
    spark.sql(query)

# ============================================================
# Clean up Hadoop _$folder$ marker files on S3
# ============================================================
def _delete_folder_markers(bucket, prefix):
    s3        = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if "$folder$" in obj["Key"]:
                to_delete.append({"Key": obj["Key"]})

    if to_delete:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
        logger.info(f"Deleted {len(to_delete)} _$folder$ marker(s) under s3://{bucket}/{prefix}")

# ============================================================
# Main
# ============================================================
tables = [t.strip().lower() for t in SILVER_TABLE.split(",")]

# Bronze is shared between contract_header and contract_items — read once
bronze_df    = None
rows_read    = 0
bronze_error = None

try:
    bronze_df, rows_read = read_bronze(ETL_RUN_DATE)
except Exception as e:
    bronze_error = str(e)
    logger.error(f"Bronze read failed: {bronze_error}")

overall_success = True

for table in tables:
    logger.info(f"===== Processing silver table: {table} =====")

    if bronze_error:
        update_audit(
            table_name=table,
            status="FAILED",
            error_message=f"Bronze read failed: {bronze_error}",
        )
        overall_success = False
        continue

    if bronze_df is None or rows_read == 0:
        logger.warning(f"[{table}] Bronze is empty for {ETL_RUN_DATE} — writing 0 rows to silver")
        update_audit(
            table_name=table,
            rows_read=0,
            rows_written=0,
            records_rejected=0,
            status="SUCCESS",
        )
        continue

    if table not in ("contract_header", "contract_items"):
        msg = f"Unknown SILVER_TABLE value: '{table}'. Expected 'contract_header' or 'contract_items'."
        logger.error(msg)
        update_audit(table_name=table, status="FAILED", error_message=msg)
        overall_success = False
        continue

    try:
        tbl, r_read, r_written, r_rejected = process_object(table, bronze_df, rows_read)
        update_audit(
            table_name=tbl,
            rows_read=r_read,
            rows_written=r_written,
            records_rejected=r_rejected,
            status="SUCCESS",
        )
        logger.info(
            f"[{table}] DONE — Read: {r_read} | Written: {r_written} | Rejected: {r_rejected}"
        )

    except Exception as e:
        logger.error(f"[{table}] Processing failed: {e}")
        update_audit(table_name=table, status="FAILED", error_message=str(e))
        overall_success = False

if not overall_success:
    job.commit()
    raise RuntimeError(
        "One or more silver tables failed — check logs above and audit table for details"
    )

job.commit()
logger.info("===== SILVER JOB COMPLETED SUCCESSFULLY =====")
