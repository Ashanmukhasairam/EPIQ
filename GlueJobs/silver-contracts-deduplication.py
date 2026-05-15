# ============================================================
# SAP SILVER GLUE JOB contract_list,contract_header,contract_items
# ============================================================
import sys
import re
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import col, lit, current_timestamp, to_date, explode
from pyspark.sql.types import DecimalType, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F


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
        "SILVER_TABLE"
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

# S3 path where duplicate records are logged as Parquet
LOGS_BASE_PATH    = f"s3://epiq-edp-dl-dev-configs/logs/sap"

# ============================================================
# Table name normalizer
# Handles both "contractitems" and "contract_items" style inputs
# ============================================================

TABLE_NAME_MAP = {
    "contractlist"    : "contract_list",
    "contractheader"  : "contract_header",
    "contractitems"   : "contract_items",
    "contract_list"   : "contract_list",
    "contract_header" : "contract_header",
    "contract_items"  : "contract_items",
}

# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder.appName(JOB_NAME)
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog", "s3tables")
    .config("spark.sql.catalog.s3tables",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.s3tables.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.s3tables.glue.id",
            f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}")
    .config("spark.sql.catalog.s3tables.warehouse", TABLE_BUCKET_NAME)
    .config("spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.glue.region",
            "us-east-1")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(JOB_NAME, args)

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tables.{DB}")

# ============================================================
# READ MAPPING
# ============================================================

def read_mapping(base_object):

    mapping_path = f"s3://{CONFIG_BUCKET}/{SOURCE}/mappings/contracts_mapping.csv"

    df = (
        spark.read.option("header", "true")
        .csv(mapping_path)
        .filter(
            (col("source_system") == SOURCE) &
            (col("source_object") == base_object) &
            (col("is_active") == "TRUE") &
            (col("is_silver") == "TRUE")
        )
    )

    rows = df.collect()

    if not rows:
        raise Exception(f"Mapping not found for {base_object}")

    return rows

# ============================================================
# CREATE TABLE
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

    ddl_columns.extend([
        "execution_id string",
        "airflow_run_id string",
        "etl_run_date string",
        "source_system string",
        "ingestion_ts timestamp",
    ])

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS s3tables.{DB}.{base_object}
        ({', '.join(ddl_columns)})
        USING iceberg
        PARTITIONED BY (etl_run_date)
    """)

# ============================================================
# READ BRONZE
# ============================================================

def read_bronze(folder):

    path = f"s3://{BRONZE_BUCKET}/{SOURCE}/contracts/{folder}/{EXECUTION_ID}/"
    df = spark.read.option("recursiveFileLookup", "true").json(path)
    rows_read = df.count()
    return df, rows_read

# ============================================================
# TRANSFORM
# Returns: (df_clean, df_duplicates)
#   df_clean      — one row per PK (latest ingestion_ts wins)
#   df_duplicates — all older/duplicate rows that were deduplicated out
# ============================================================

def transform_data(bronze_df, mapping_rows, base_object):

    if base_object == "contract_list":
        bronze_df = bronze_df.withColumn("ContractDate", col("CreatedDate"))

    if base_object == "contract_items":
        bronze_df = bronze_df.select(
            col("ContractNumber"),
            explode(col("_ContractItems")).alias("item")
        ).select(col("ContractNumber"), col("item.*"))

    select_expr = []

    for row in mapping_rows:

        source = row["source_field"].strip()
        target = row["target_field"].strip()
        dtype  = row["target_datatype"].strip().lower().replace(" ", "")

        if dtype.startswith("decimal"):
            nums = re.findall(r"\d+", dtype)
            select_expr.append(
                col(source).cast(
                    DecimalType(int(nums[0]), int(nums[1]))
                ).alias(target)
            )

        elif dtype == "date":
            select_expr.append(to_date(col(source)).alias(target))

        elif dtype == "int":
            select_expr.append(col(source).cast(IntegerType()).alias(target))

        elif dtype == "double":
            select_expr.append(col(source).cast(DoubleType()).alias(target))

        else:
            select_expr.append(col(source).cast(StringType()).alias(target))

    df = bronze_df.select(*select_expr)

    if base_object in ["contract_header", "contract_list"]:
        pk_cols = ["contract_number"]
    else:
        pk_cols = ["contract_number", "item_id"]

    for pk in pk_cols:
        df = df.filter(col(pk).isNotNull())

    df = (
        df.withColumn("execution_id",   lit(EXECUTION_ID))
          .withColumn("airflow_run_id", lit(AIRFLOW_RUN_ID))
          .withColumn("etl_run_date",   lit(ETL_RUN_DATE))
          .withColumn("source_system",  lit(SOURCE))
          .withColumn("ingestion_ts",   current_timestamp())
    )

    # ── Deduplication via window function ───────────────────────────────
    window = Window.partitionBy(*pk_cols).orderBy(col("ingestion_ts").desc())

    df_ranked = df.withColumn("rn", row_number().over(window))

    # rn == 1  →  latest / winner record (clean)
    df_clean      = df_ranked.filter(col("rn") == 1).drop("rn")

    # rn  > 1  →  older / duplicate records
    df_duplicates = df_ranked.filter(col("rn") > 1).drop("rn")

    return df_clean, df_duplicates

# ============================================================
# AUDIT UPDATE FUNCTION
# ============================================================

def update_audit(table_name, rows_read=None, rows_written=None,
                 records_rejected=None, status=None, error_message=None):

    table_ref  = "glue_catalog.edp_configs_dev.pipeline_execution_summary"
    safe_error = error_message.replace("'", " ") if error_message else None

    if status == "FAILED":

        spark.sql(f"""
            UPDATE {table_ref}
            SET
                status = 'FAILED',
                error_message = '{safe_error}',
                pipeline_end_time = current_timestamp(),
                duration_seconds =
                    unix_timestamp(current_timestamp()) -
                    unix_timestamp(pipeline_start_time)
            WHERE etl_run_id = '{EXECUTION_ID}'
              AND run_id = '{AIRFLOW_RUN_ID}'
              AND layer = 'SILVER'
              AND environment='DEV'
              AND table_name = '{table_name}'
        """)

    elif status == "SUCCESS":

        spark.sql(f"""
            UPDATE {table_ref}
            SET
                records_read     = {rows_read},
                records_written  = {rows_written},
                records_rejected = {records_rejected},
                status = 'SUCCESS',
                pipeline_end_time = current_timestamp(),
                duration_seconds =
                    unix_timestamp(current_timestamp()) -
                    unix_timestamp(pipeline_start_time)
            WHERE etl_run_id = '{EXECUTION_ID}'
              AND run_id = '{AIRFLOW_RUN_ID}'
              AND layer = 'SILVER'
              AND environment='DEV'
              AND table_name = '{table_name}'
        """)

# ============================================================
# MAIN
# ============================================================

try:

    tables_raw = [t.strip().lower() for t in SILVER_TABLE.split(",")]

    # ── Normalize table names ────────────────────────────────────────────
    tables = []
    for raw in tables_raw:
        normalized = TABLE_NAME_MAP.get(raw)
        if not normalized:
            raise ValueError(f"Unknown table name received: '{raw}'")
        tables.append(normalized)

    # ── Step 1: Process all tables, collect DataFrames ───────────────────
    processed_tables = []

    for table in tables:

        logger.info(f"Processing table: {table}")

        if table == "contract_list":
            bronze_df, rows_read = read_bronze("contractlists")

        elif table in ("contract_header", "contract_items"):
            bronze_df, rows_read = read_bronze("contractdetails")

        else:
            raise ValueError(f"Invalid table: {table}")

        mapping_rows = read_mapping(table)

        if not spark.catalog.tableExists(f"s3tables.{DB}.{table}"):
            create_ddl(table, mapping_rows)

        df_clean, df_duplicates = transform_data(bronze_df, mapping_rows, table)

        rows_written     = df_clean.count()
        duplicates_count = df_duplicates.count()

        logger.info(f"[{table}] records read     : {rows_read}")
        logger.info(f"[{table}] records written  : {rows_written}")
        logger.info(f"[{table}] records rejected : {duplicates_count}")

        # ── Step 2: Write duplicates to S3 log as Parquet ────────────────
        if duplicates_count > 0:
            log_path = f"{LOGS_BASE_PATH}/{table}/"
            logger.info(f"Writing {duplicates_count} duplicate(s) to {log_path}")
            (
                df_duplicates
                .write
                .mode("append")
                .partitionBy("etl_run_date")
                .parquet(log_path)
            )
            logger.info(f"Duplicates successfully written to {log_path}")
        else:
            logger.info(f"No duplicates found for {table} — skipping log write")

        # ── Store for Silver write ────────────────────────────────────────
        processed_tables.append((
            table,
            df_clean,
            df_duplicates,
            rows_read,
            rows_written,
            duplicates_count
        ))

    # ── Step 3: Write ALL tables to Silver Iceberg ───────────────────────
    for table, df_clean, df_duplicates, rows_read, rows_written, duplicates_count in processed_tables:

        table_name = f"s3tables.{DB}.{table}"
        logger.info(f"Writing clean data to Silver table: {table_name}")

        # Write clean records
        df_clean.writeTo(table_name).overwritePartitions()
        logger.info(f"Clean records written to: {table_name}")

        # Append duplicates into same Silver table
        if duplicates_count > 0:
            df_duplicates.writeTo(table_name).append()
            logger.info(f"Duplicate records appended to: {table_name}")

        # ── Step 4: Update audit with records_rejected ───────────────────
        update_audit(
            table_name=table,
            rows_read=rows_read,
            rows_written=rows_written,
            records_rejected=duplicates_count,
            status="SUCCESS"
        )
        logger.info(
            f"Audit updated → table: {table} | "
            f"read: {rows_read} | "
            f"written: {rows_written} | "
            f"rejected: {duplicates_count}"
        )

    job.commit()
    logger.info("Silver job completed successfully")

except Exception as e:

    logger.error(f"Job failed: {str(e)}")

    update_audit(
        table_name=table,
        status="FAILED",
        error_message=str(e)
    )

    raise






# # ============================================================
# # SAP SILVER GLUE JOB contract_list,contract_header,contract_items
# # ============================================================

# import sys
# import re
# import logging
# from awsglue.utils import getResolvedOptions
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql import SparkSession
# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number
# from pyspark.sql.functions import col, lit, current_timestamp, to_date, explode
# from pyspark.sql.types import DecimalType, StringType, IntegerType, DoubleType
# from pyspark.sql import functions as F

# # ============================================================
# # Logging
# # ============================================================

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # ============================================================
# # Arguments
# # ============================================================

# args = getResolvedOptions(
#     sys.argv,
#     [
#         "JOB_NAME",
#         "ACCOUNT_ID",
#         "AIRFLOW_RUN_ID",
#         "BRONZE_BUCKET_NAME",
#         "CONFIG_BUCKET",
#         "ETL_RUN_DATE",
#         "EXECUTION_ID",
#         "SILVER_DATABASE",
#         "SOURCE",
#         "TABLE_BUCKET_NAME",
#         "SILVER_TABLE"
#     ],
# )

# JOB_NAME        = args["JOB_NAME"]
# ACCOUNT_ID      = args["ACCOUNT_ID"]
# AIRFLOW_RUN_ID  = args["AIRFLOW_RUN_ID"]
# BRONZE_BUCKET   = args["BRONZE_BUCKET_NAME"]
# CONFIG_BUCKET   = args["CONFIG_BUCKET"]
# ETL_RUN_DATE    = args["ETL_RUN_DATE"]
# EXECUTION_ID    = args["EXECUTION_ID"]
# DB              = args["SILVER_DATABASE"]
# SOURCE          = args["SOURCE"]
# TABLE_BUCKET_NAME = args["TABLE_BUCKET_NAME"]
# SILVER_TABLE    = args["SILVER_TABLE"]

# # S3 path where duplicate records are logged as Parquet
# LOGS_BASE_PATH  = f"s3://epiq-edp-dl-dev-configs/logs/sap"

# # ============================================================
# # Spark Session
# # ============================================================

# spark = (
#     SparkSession.builder.appName(JOB_NAME)
#     .config("spark.sql.extensions",
#             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
#     .config("spark.sql.defaultCatalog", "s3tables")
#     .config("spark.sql.catalog.s3tables",
#             "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.s3tables.catalog-impl",
#             "org.apache.iceberg.aws.glue.GlueCatalog")
#     .config("spark.sql.catalog.s3tables.glue.id",
#             f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}")
#     .config("spark.sql.catalog.s3tables.warehouse", TABLE_BUCKET_NAME)
#     .config("spark.sql.catalog.glue_catalog",
#             "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.glue_catalog.catalog-impl",
#             "org.apache.iceberg.aws.glue.GlueCatalog")
#     .config("spark.sql.catalog.glue_catalog.glue.region",
#             "us-east-1")
#     .config("spark.sql.session.timeZone", "UTC")
#     .getOrCreate()
# )

# glueContext = GlueContext(spark.sparkContext)
# job = Job(glueContext)
# job.init(JOB_NAME, args)

# spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tables.{DB}")

# # ============================================================
# # READ MAPPING
# # ============================================================

# def read_mapping(base_object):

#     mapping_path = f"s3://{CONFIG_BUCKET}/{SOURCE}/mappings/contracts_mapping.csv"

#     df = (
#         spark.read.option("header", "true")
#         .csv(mapping_path)
#         .filter(
#             (col("source_system") == SOURCE) &
#             (col("source_object") == base_object) &
#             (col("is_active") == "TRUE") &
#             (col("is_silver") == "TRUE")
#         )
#     )

#     rows = df.collect()

#     if not rows:
#         raise Exception(f"Mapping not found for {base_object}")

#     return rows

# # ============================================================
# # CREATE TABLE
# # ============================================================

# def create_ddl(base_object, mapping_rows):

#     ddl_columns = []

#     for row in mapping_rows:

#         target = row["target_field"].strip()
#         dtype  = row["target_datatype"].strip().lower().replace(" ", "")

#         if dtype.startswith("decimal"):
#             nums = re.findall(r"\d+", dtype)
#             ddl_columns.append(f"{target} decimal({nums[0]},{nums[1]})")

#         elif dtype == "date":
#             ddl_columns.append(f"{target} date")

#         elif dtype == "int":
#             ddl_columns.append(f"{target} int")

#         elif dtype == "double":
#             ddl_columns.append(f"{target} double")

#         else:
#             ddl_columns.append(f"{target} string")

#     ddl_columns.extend([
#         "execution_id string",
#         "airflow_run_id string",
#         "etl_run_date string",
#         "source_system string",
#         "ingestion_ts timestamp",
#     ])

#     spark.sql(f"""
#         CREATE TABLE IF NOT EXISTS s3tables.{DB}.{base_object}
#         ({', '.join(ddl_columns)})
#         USING iceberg
#         PARTITIONED BY (etl_run_date)
#     """)

# # ============================================================
# # READ BRONZE
# # ============================================================

# def read_bronze(folder):

#     path = f"s3://{BRONZE_BUCKET}/{SOURCE}/contracts/{folder}/{EXECUTION_ID}/"
#     df = spark.read.option("recursiveFileLookup", "true").json(path)
#     rows_read = df.count()
#     return df, rows_read

# # ============================================================
# # TRANSFORM
# # Returns: (df_clean, df_duplicates)
# #   df_clean      — one row per PK (latest ingestion_ts wins)
# #   df_duplicates — all older/duplicate rows that were deduplicated out
# # ============================================================

# def transform_data(bronze_df, mapping_rows, base_object):

#     if base_object == "contract_list":
#         bronze_df = bronze_df.withColumn("ContractDate", col("CreatedDate"))

#     if base_object == "contract_items":
#         bronze_df = bronze_df.select(
#             col("ContractNumber"),
#             explode(col("_ContractItems")).alias("item")
#         ).select(col("ContractNumber"), col("item.*"))

#     select_expr = []

#     for row in mapping_rows:

#         source = row["source_field"].strip()
#         target = row["target_field"].strip()
#         dtype  = row["target_datatype"].strip().lower().replace(" ", "")

#         if dtype.startswith("decimal"):
#             nums = re.findall(r"\d+", dtype)
#             select_expr.append(
#                 col(source).cast(
#                     DecimalType(int(nums[0]), int(nums[1]))
#                 ).alias(target)
#             )

#         elif dtype == "date":
#             select_expr.append(to_date(col(source)).alias(target))

#         elif dtype == "int":
#             select_expr.append(col(source).cast(IntegerType()).alias(target))

#         elif dtype == "double":
#             select_expr.append(col(source).cast(DoubleType()).alias(target))

#         else:
#             select_expr.append(col(source).cast(StringType()).alias(target))

#     df = bronze_df.select(*select_expr)

#     if base_object in ["contract_header", "contract_list"]:
#         pk_cols = ["contract_number"]
#     else:
#         pk_cols = ["contract_number", "item_id"]

#     for pk in pk_cols:
#         df = df.filter(col(pk).isNotNull())

#     df = (
#         df.withColumn("execution_id",   lit(EXECUTION_ID))
#           .withColumn("airflow_run_id", lit(AIRFLOW_RUN_ID))
#           .withColumn("etl_run_date",   lit(ETL_RUN_DATE))
#           .withColumn("source_system",  lit(SOURCE))
#           .withColumn("ingestion_ts",   current_timestamp())
#     )

#     # ── Deduplication via window function ───────────────────────────────
#     window = Window.partitionBy(*pk_cols).orderBy(col("ingestion_ts").desc())

#     df_ranked = df.withColumn("rn", row_number().over(window))

#     # rn == 1  →  latest / winner record (clean)
#     df_clean      = df_ranked.filter(col("rn") == 1).drop("rn")

#     # rn  > 1  →  older / duplicate records
#     df_duplicates = df_ranked.filter(col("rn") > 1).drop("rn")

#     return df_clean, df_duplicates

# # ============================================================
# # PROCESS TABLE
# # ============================================================

# def process_object(base_object, bronze_df, rows_read):

#     table_name = f"s3tables.{DB}.{base_object}"
#     logger.info(f"Target table: {table_name}")

#     mapping_rows = read_mapping(base_object)
#     logger.info(f"Mapping rows fetched: {len(mapping_rows)}")

#     if not spark.catalog.tableExists(f"s3tables.{DB}.{base_object}"):
#         create_ddl(base_object, mapping_rows)

#     df_clean, df_duplicates = transform_data(bronze_df, mapping_rows, base_object)

#     df_clean.printSchema()

#     rows_written      = df_clean.count()
#     duplicates_count  = df_duplicates.count()

#     logger.info(f"[{base_object}] clean rows    : {rows_written}")
#     logger.info(f"[{base_object}] duplicate rows: {duplicates_count}")

#     # ── 1. Write duplicate records to S3 logs as Parquet ────────────────
#     if duplicates_count > 0:
#         log_path = f"{LOGS_BASE_PATH}/{base_object}/"
#         logger.info(f"Writing {duplicates_count} duplicate(s) to {log_path}")
#         (
#             df_duplicates
#             .write
#             .mode("append")
#             .partitionBy("etl_run_date")
#             .parquet(log_path)
#         )
#         logger.info(f"Duplicate records successfully written to {log_path}")
#     else:
#         logger.info(f"No duplicates found for {base_object} — skipping log write")

#     # ── 2. Write clean (deduplicated) records to Iceberg table ──────────
#     df_clean.writeTo(table_name).overwritePartitions()
#     logger.info(f"Clean records written to Iceberg table: {table_name}")

#     # ── 3. Append duplicate records into the same Iceberg table ─────────
#     if duplicates_count > 0:
#         df_duplicates.writeTo(table_name).append()
#         logger.info(f"Duplicate records appended to Iceberg table: {table_name}")

#     return (base_object, rows_read, rows_written)

# # ============================================================
# # AUDIT UPDATE FUNCTION
# # ============================================================

# def update_audit(table_name, rows_read=None, rows_written=None,
#                  status=None, error_message=None):

#     table_ref  = "glue_catalog.edp_configs_dev.pipeline_execution_summary"
#     safe_error = error_message.replace("'", " ") if error_message else None

#     if status == "FAILED":

#         spark.sql(f"""
#             UPDATE {table_ref}
#             SET
#                 status = 'FAILED',
#                 error_message = '{safe_error}',
#                 pipeline_end_time = current_timestamp(),
#                 duration_seconds =
#                     unix_timestamp(current_timestamp()) -
#                     unix_timestamp(pipeline_start_time)
#             WHERE etl_run_id = '{EXECUTION_ID}'
#               AND run_id = '{AIRFLOW_RUN_ID}'
#               AND layer = 'SILVER'
#               AND environment='DEV'
#               AND table_name = '{table_name}'
#         """)

#     elif status == "SUCCESS":

#         spark.sql(f"""
#             UPDATE {table_ref}
#             SET
#                 records_read = {rows_read},
#                 records_written = {rows_written},
#                 status = 'SUCCESS',
#                 pipeline_end_time = current_timestamp(),
#                 duration_seconds =
#                     unix_timestamp(current_timestamp()) -
#                     unix_timestamp(pipeline_start_time)
#             WHERE etl_run_id = '{EXECUTION_ID}'
#               AND run_id = '{AIRFLOW_RUN_ID}'
#               AND layer = 'SILVER'
#               AND environment='DEV'
#               AND table_name = '{table_name}'
#         """)

# # ============================================================
# # MAIN
# # ============================================================

# try:

#     tables = [t.strip().lower() for t in SILVER_TABLE.split(",")]

#     for table in tables:

#         logger.info(f"Processing table: {table}")

#         if table == "contract_list":
#             bronze_df, rows_read = read_bronze("contractlists")

#         elif table in ("contract_header", "contract_items"):
#             bronze_df, rows_read = read_bronze("contractdetails")

#         else:
#             raise ValueError(f"Invalid table: {table}")

#         table_name, rows_read, rows_written = process_object(table, bronze_df, rows_read)

#         logger.info(f"Audit update → table: {table_name} | read: {rows_read} | written: {rows_written}")
#         update_audit(
#             table_name=table_name,
#             rows_read=rows_read,
#             rows_written=rows_written,
#             status="SUCCESS"
#         )

#     job.commit()
#     logger.info("Silver job completed successfully")

# except Exception as e:

#     logger.error(f"Job failed: {str(e)}")

#     update_audit(
#         table_name=table,
#         status="FAILED",
#         error_message=str(e)
#     )

#     raise