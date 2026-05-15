import sys
import re
import logging
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date, explode, input_file_name, regexp_extract, row_number
from pyspark.sql.types import DecimalType, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =====================================================
# RESOLVE JOB ARGUMENTS
# =====================================================
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
        "SOURCE",
        "TABLE_BUCKET_NAME",
    ],
)

JOB_NAME          = args["JOB_NAME"]
ACCOUNT_ID        = args["ACCOUNT_ID"]
AIRFLOW_RUN_ID    = args["AIRFLOW_RUN_ID"]
BRONZE_BUCKET     = args["BRONZE_BUCKET_NAME"]
CONFIG_BUCKET     = args["CONFIG_BUCKET"]
ETL_RUN_DATE      = args["ETL_RUN_DATE"]
EXECUTION_ID      = args["EXECUTION_ID"]
SOURCE            = args["SOURCE"]
TABLE_BUCKET_NAME = args["TABLE_BUCKET_NAME"]

# ── Target database ───────────────────────────────────────────────────────────
DB = "stage_db"

# =====================================================
# SPARK SESSION
# =====================================================
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
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.iceberg.write.parquet.compression-codec", "snappy")
    .config("spark.sql.catalog.s3tables.write.target-file-size-bytes", "134217728")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.broadcastTimeout", "600")
    .config("spark.network.timeout", "800s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.executor.memoryOverhead", "1024")
    .config("spark.driver.memoryOverhead", "1024")
    .config("spark.sql.iceberg.write.distribution-mode", "hash")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(JOB_NAME, args)

# ── Ensure namespace exists ───────────────────────────────────────────────────
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tables.{DB}")
logger.info(f"Namespace s3tables.{DB} is ready")

# =====================================================
# DROP ALL 3 TABLES WITH PURGE
# Removes old schema tables.
# Will be recreated fresh with correct snake_case schema.
# Remove this block after first successful run.
# =====================================================
for table in ["contractlist", "contractheader", "contractitems"]:
    spark.sql(f"DROP TABLE IF EXISTS s3tables.{DB}.{table} PURGE")
    logger.info(f"Dropped s3tables.{DB}.{table} (PURGE)")


# =====================================================
# HELPER: READ MAPPING CSV
# =====================================================
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
            f"No active silver mapping found for source_system={SOURCE}, "
            f"source_object={base_object}. Check contracts_mapping.csv."
        )

    logger.info(f"Loaded {len(rows)} mapping rows for {base_object}")
    return rows


# =====================================================
# HELPER: CREATE ICEBERG TABLE (IF NOT EXISTS)
# =====================================================
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

    # Audit columns
    ddl_columns.extend(
        [
            "execution_id   string",
            "airflow_run_id string",
            "etl_run_date   string",
            "source_system  string",
            "ingestion_ts   timestamp",
        ]
    )

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS s3tables.{DB}.{base_object}
        ({', '.join(ddl_columns)})
        USING iceberg
        PARTITIONED BY (etl_run_date)
    """
    logger.info(f"DDL for {base_object}:\n{create_sql}")
    spark.sql(create_sql)


# =====================================================
# HELPER: READ BRONZE — contractlist
# Path: s3://{BRONZE_BUCKET}/{SOURCE}/contracts_list/
# =====================================================
def read_bronze_contractlist():


    logger.info(f"Reading contractlist path: {bronze_path}")

    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .json(bronze_path)
        .withColumn("_source_file", input_file_name())
    )

    if df.rdd.isEmpty():
        raise Exception(f"No data found at {bronze_path}")

    logger.info(f"Bronze record count for contractlist: {df.count()}")
    return df


# =====================================================
# HELPER: READ BRONZE — contractheader + contractitems
# Path: s3://{BRONZE_BUCKET}/{SOURCE}/contractitems/
# =====================================================
def read_bronze_contractdetails():
    bronze_path = f"s3://{BRONZE_BUCKET}/{SOURCE}/ContractItems/"
    logger.info(f"Reading contractheader/contractitems path: {bronze_path}")

    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .json(bronze_path)
        .withColumn("_source_file", input_file_name())
    )

    if df.rdd.isEmpty():
        raise Exception(f"No data found at {bronze_path}")

    logger.info(f"Bronze record count for contractheader/contractitems: {df.count()}")
    return df


# =====================================================
# HELPER: TRANSFORM BRONZE → SILVER
# =====================================================
def transform_data(bronze_df, mapping_rows, base_object):

    # ── contractitems: explode nested _ContractItems array ───────────────────
    if base_object == "contractitems":
        bronze_df = (
            bronze_df
            .select(
                col("ContractNumber"),
                col("_source_file"),
                explode(col("_ContractItems")).alias("item"),
            )
            .select(
                col("ContractNumber"),
                col("_source_file"),
                col("item.*"),
            )
        )

    # ── Extract execution_id from S3 file path ───────────────────────────────
    execution_id_expr = regexp_extract(
        col("_source_file"),
        r"/([^/]+)/[^/]+$",
        1,
    )

    # ── Build SELECT expression from mapping ─────────────────────────────────
    select_expr = []

    for row in mapping_rows:
        source = row["source_field"].strip()
        target = row["target_field"].strip()
        dtype  = row["target_datatype"].strip().lower().replace(" ", "")

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

    select_expr.append(col("_source_file"))

    df = bronze_df.select(*select_expr)

    # ── Attach audit columns ──────────────────────────────────────────────────
    df = (
        df
        .withColumn("execution_id",   execution_id_expr)
        .withColumn("airflow_run_id", lit(AIRFLOW_RUN_ID))
        .withColumn("etl_run_date",   lit(ETL_RUN_DATE))
        .withColumn("source_system",  lit(SOURCE))
        .withColumn("ingestion_ts",   current_timestamp())
    )

    if "_source_file" in df.columns:
        df = df.drop("_source_file")

    logger.info(f"Transform complete for {base_object} — {df.count()} rows before dedup")
    return df


# =====================================================
# HELPER: DEDUPLICATE
#
# contractlist   → dedupe on contract_number
#                  keep latest by changed_date
#
# contractheader → dedupe on contract_number
#                  keep latest by ingestion_ts
#
# contractitems  → dedupe on contract_number + item_id
#                  keep latest by ingestion_ts
#                  This preserves ALL unique contract+item
#                  combinations while removing exact duplicates
#                  caused by multiple execution runs in S3
# =====================================================
def deduplicate(df, base_object, mapping_rows):

    if base_object == "contractlist":
        window = Window.partitionBy("contract_number").orderBy(
            col("changed_date").desc()
        )
        df = (
            df.withColumn("_rn", row_number().over(window))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
        logger.info(f"contractlist dedup key: contract_number | order: changed_date DESC")

    elif base_object == "contractheader":
        window = Window.partitionBy("contract_number").orderBy(
            col("ingestion_ts").desc()
        )
        df = (
            df.withColumn("_rn", row_number().over(window))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
        logger.info(f"contractheader dedup key: contract_number | order: ingestion_ts DESC")

    elif base_object == "contractitems":
        # ── Dedupe on contract_number + item_id ──────────────────────────────
        # Each contract has multiple items (e.g. 45 items per contract).
        # The same contract+item combination appears across multiple
        # execution_id runs in S3 — those are the true duplicates.
        # Deduping on contract_number + item_id keeps all unique items
        # per contract while removing cross-run duplicates.
        # Expected result: ~843,877 / number_of_execution_runs
        window = Window.partitionBy("contract_number", "item_id").orderBy(
            col("ingestion_ts").desc()
        )
        df = (
            df.withColumn("_rn", row_number().over(window))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
        logger.info(f"contractitems dedup key: contract_number + item_id | order: ingestion_ts DESC")

    logger.info(f"After dedup for {base_object} — {df.count()} rows")
    return df


# =====================================================
# MAIN PROCESSOR
# =====================================================
def process_object(base_object, bronze_df):
    logger.info(f"── Processing {base_object} ──────────────────────────────")

    mapping_rows = read_mapping(base_object)
    create_ddl(base_object, mapping_rows)

    final_df = transform_data(bronze_df, mapping_rows, base_object)
    final_df = deduplicate(final_df, base_object, mapping_rows)

    target_table = f"s3tables.{DB}.{base_object}"

    # ── Deduplicate DESCRIBE TABLE columns ────────────────────────────────────
    # etl_run_date appears twice (schema column + partition info)
    # seen set keeps only first occurrence
    seen = set()
    iceberg_columns = []
    for row in spark.sql(f"DESCRIBE TABLE {target_table}").collect():
        col_name = row["col_name"].strip()
        if (
            col_name
            and not col_name.startswith("#")
            and col_name not in seen
        ):
            seen.add(col_name)
            iceberg_columns.append(col_name)

    logger.info(
        f"Iceberg table columns for {base_object} ({len(iceberg_columns)}): {iceberg_columns}"
    )

    # ── Align DataFrame columns to Iceberg table ──────────────────────────────
    final_df = final_df.select([col(c) for c in iceberg_columns])

    logger.info(f"Writing {final_df.count()} rows to {target_table} ...")
    final_df.writeTo(target_table).overwritePartitions()

    written_count = spark.table(target_table).count()
    logger.info(f"✓ {base_object} — {written_count} total rows in Iceberg table")


# =====================================================
# JOB EXECUTION
# =====================================================
try:
    # ------------------------------------------------------------------
    # TABLE 1 : contractlist
    # Source  : s3://{BRONZE_BUCKET}/{SOURCE}/contracts_list/
    # Expected: ~19,918 rows after dedup
    # ------------------------------------------------------------------
    contractlist_df = read_bronze_contractlist()
    process_object("contractlist", contractlist_df)

    # ------------------------------------------------------------------
    # TABLE 2 : contractheader   (~18,642 rows after dedup)
    # TABLE 3 : contractitems    (~843,877 / runs after dedup)
    # Source  : s3://{BRONZE_BUCKET}/{SOURCE}/contractitems/
    # ------------------------------------------------------------------
    contractdetails_df = read_bronze_contractdetails()

    # ── Debug: row counts before processing ──────────────────────────
    raw_count = contractdetails_df.count()
    logger.info(f"[DEBUG] Raw records in S3 bronze (contractitems path): {raw_count}")
    logger.info(f"[DEBUG] contractheader will load (before dedup): {raw_count} rows")

    items_df = contractdetails_df.select(
        col("ContractNumber"),
        explode(col("_ContractItems")).alias("item")
    )
    items_count = items_df.count()
    logger.info(f"[DEBUG] contractitems will load after explode (before dedup): {items_count} rows")

    contractdetails_df.cache()
    logger.info("contractdetails DataFrame cached for reuse across contractheader and contractitems")

    process_object("contractheader", contractdetails_df)
    process_object("contractitems",  contractdetails_df)

    contractdetails_df.unpersist()

    # =====================================================
    # GET MAX changed_date FROM contractlist
    # =====================================================
    max_query = f"""
        SELECT MAX(changed_date) AS max_changed_date
        FROM s3tables.{DB}.contractlist
    """
    MAX_CHANGED_DATE = spark.sql(max_query).first()[0]
    logger.info(f"Max changed_date from contractlist: {MAX_CHANGED_DATE}")

    # =====================================================
    # TRIGGER NEXT GLUE JOB
    # Commented out — no downstream job configured yet.
    # To enable: replace NEXT_GLUE_JOB_NAME with actual
    # job name from Glue console and uncomment this block.
    # =====================================================
    # NEXT_GLUE_JOB_NAME = "your-actual-next-glue-job-name"
    # glue_client = boto3.client("glue")
    # response = glue_client.start_job_run(
    #     JobName=NEXT_GLUE_JOB_NAME,
    #     Arguments={
    #         "--EXECUTION_ID":     EXECUTION_ID,
    #         "--MAX_CHANGED_DATE": str(MAX_CHANGED_DATE),
    #     },
    # )
    # logger.info(
    #     f"Triggered next Glue job '{NEXT_GLUE_JOB_NAME}' "
    #     f"with JobRunId: {response['JobRunId']}"
    # )
    logger.info("Next Glue job trigger skipped — no downstream job configured yet")

    # ── Commit ────────────────────────────────────────────────────────
    job.commit()
    logger.info("Glue job completed successfully ✓")

except Exception as e:
    logger.error(f"Job failed: {str(e)}", exc_info=True)
    raise e