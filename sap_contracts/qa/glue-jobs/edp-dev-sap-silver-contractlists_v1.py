# ============================================================
# SAP SILVER GLUE JOB contract_list — SCD TYPE 2
# ============================================================

import sys
import re
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from datetime import datetime, date
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date,
    row_number, md5, concat_ws, from_json
)
from pyspark.sql.types import DecimalType, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F
import boto3
from pyspark.sql.utils import AnalysisException

# ============================================================
# Logging
# ============================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def log_section(title: str):
    bar = "=" * 65
    logger.info(bar)
    logger.info(f"   {title}")
    logger.info(bar)


def log_counts(label: str, df: DataFrame):
    logger.info(f"  COUNT [{label}] → {df.count():,} rows")


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

SCD2_OPEN_END = "9999-12-31"
ACTIVE        = "Y"
INACTIVE      = "N"

log_section("JOB PARAMETERS")
logger.info(f"  JOB_NAME        : {JOB_NAME}")
logger.info(f"  SOURCE          : {SOURCE}")
logger.info(f"  SILVER_TABLE    : {SILVER_TABLE}")
logger.info(f"  ETL_RUN_DATE    : {ETL_RUN_DATE}")
logger.info(f"  EXECUTION_ID    : {EXECUTION_ID}")
logger.info(f"  AIRFLOW_RUN_ID  : {AIRFLOW_RUN_ID}")
logger.info(f"  SILVER_DATABASE : {DB}")

# ============================================================
# Spark Session
# ============================================================

log_section("INITIALISING SPARK SESSION")

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
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(JOB_NAME, args)

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tables.{DB}")
logger.info(f"  Namespace ready : s3tables.{DB}")

# ============================================================
# READ MAPPING
# ============================================================


def read_mapping(base_object: str):
    log_section(f"READ MAPPING — {base_object}")

    path = f"s3://{CONFIG_BUCKET}/{SOURCE}/mappings/contracts_mapping.csv"
    logger.info(f"  Mapping CSV : {path}")

    df = (
        spark.read.option("header", "true")
        .csv(path)
        .filter(
            (col("source_system")  == SOURCE)
            & (col("source_object") == "contract_list")
            & (col("is_active")     == "TRUE")
            & (col("is_silver")     == "TRUE")
        )
    )

    rows = df.collect()
    if not rows:
        raise RuntimeError(
            f"[MAPPING] No active silver rows found for "
            f"source_system='{SOURCE}', source_object='contract_list'."
        )

    pk_columns = (
        df.filter(col("is_primary_key") == "TRUE")
        .select("target_field")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    if not pk_columns:
        raise RuntimeError(
            f"[MAPPING] No primary key defined for '{base_object}'."
        )

    logger.info(f"  Mapped columns : {len(rows)}")
    logger.info(f"  PK columns     : {pk_columns}")

    return rows, pk_columns


# ============================================================
# CREATE TABLE — SCD2 columns + partitioned by is_active
# ============================================================


def create_ddl(base_object: str, mapping_rows):
    log_section(f"CREATE TABLE — {base_object}")

    cols = []

    for row in mapping_rows:
        target = row["target_field"].strip()
        dtype  = row["target_datatype"].strip().lower().replace(" ", "")

        if dtype.startswith("decimal"):
            nums = re.findall(r"\d+", dtype)
            cols.append(f"{target} DECIMAL({nums[0]},{nums[1]})")
        elif dtype == "date":
            cols.append(f"{target} DATE")
        elif dtype == "int":
            cols.append(f"{target} INT")
        elif dtype == "double":
            cols.append(f"{target} DOUBLE")
        else:
            cols.append(f"{target} STRING")

    cols += [
        "edp_surrogate_key  STRING",
        "edp_record_hash    STRING",
        "edp_start_date     DATE",
        "edp_end_date       DATE",
        "is_active          STRING",
        "edp_modified_date  TIMESTAMP",
        "execution_id       STRING",
        "airflow_run_id     STRING",
        "etl_run_date       STRING",
        "source_system      STRING",
        "ingestion_ts       TIMESTAMP",
    ]

    ddl = f"""
        CREATE TABLE IF NOT EXISTS s3tables.{DB}.{base_object}
        ({", ".join(cols)})
        USING iceberg
        PARTITIONED BY (is_active)
    """

    logger.info(f"  DDL:\n{ddl}")
    spark.sql(ddl)
    logger.info(f"  Table ready : s3tables.{DB}.{base_object}")


# ============================================================
# READ BRONZE
# ============================================================


# def read_bronze(folder: str, etl_run_date):
#     log_section(f"READ BRONZE — folder={folder}")

#     if etl_run_date is None:
#         etl_run_date = datetime.today()
#     if isinstance(etl_run_date, str):
#         etl_run_date = datetime.strptime(etl_run_date, "%Y-%m-%d")

#     year  = etl_run_date.strftime("%Y")
#     month = etl_run_date.strftime("%m")
#     day   = etl_run_date.strftime("%d")

#     path = (
#         f"s3://{BRONZE_BUCKET}/{SOURCE}/contracts/"
#         f"{folder}-dev/{year}/{month}/{day}/"
#     )
#     logger.info(f"  Bronze path : {path}")

#     try:
#         df        = spark.read.option("recursiveFileLookup", "true").json(path)
#         row_count = df.count()
#         logger.info(f"  Bronze rows read : {row_count:,}")
#         return df, row_count

#     except AnalysisException as exc:
#         if "Path does not exist" in str(exc):
#             logger.warning(
#                 f"  Bronze path does not exist — "
#                 f"no data for ETL_RUN_DATE={ETL_RUN_DATE}. Skipping."
#             )
#             return None, 0
#         raise

def read_bronze(folder: str, etl_run_date):
    log_section(f"READ BRONZE — folder={folder}")

    if etl_run_date is None:
        etl_run_date = datetime.today()
    if isinstance(etl_run_date, str):
        etl_run_date = datetime.strptime(etl_run_date, "%Y-%m-%d")

    year  = etl_run_date.strftime("%Y")
    month = etl_run_date.strftime("%m")
    day   = etl_run_date.strftime("%d")

    path = (
        f"s3://{BRONZE_BUCKET}/{SOURCE}/contracts/"
        f"{folder}-dev/{year}/{month}/{day}/"
    )
    logger.info(f"  Bronze path : {path}")

    try:
        df = spark.read.option("recursiveFileLookup", "true").json(path)

        # ============================================================
        # NEW: HANDLE STRINGIFIED JSON ("raw_json")
        # ============================================================
        if "raw_json" in df.columns:

            logger.info("  Detected stringified JSON column: raw_json")

            # Infer schema dynamically from raw_json
            json_rdd = df.select("raw_json").rdd.map(lambda r: r[0])
            json_schema = spark.read.json(json_rdd).schema

            # Convert string to structured JSON
            df = df.withColumn(
                "parsed",
                from_json(col("raw_json"), json_schema)
            ).select("parsed.*")

            logger.info("  Successfully parsed raw_json into structured columns")
        # ============================================================

        row_count = df.count()
        logger.info(f"  Bronze rows read : {row_count:,}")

        return df, row_count

    except AnalysisException as exc:
        if "Path does not exist" in str(exc):
            logger.warning(
                f"  Bronze path does not exist — "
                f"no data for ETL_RUN_DATE={ETL_RUN_DATE}. Skipping."
            )
            return None, 0
        raise


# ============================================================
# IDEMPOTENCY CHECK
# ============================================================


def already_processed(base_object: str) -> bool:
    """
    Returns True if this etl_run_date was already successfully
    loaded into the silver table.
    Prevents double processing on reruns of the same date.
    """
    table_ref = f"s3tables.{DB}.{base_object}"

    # Table doesn't exist yet — first ever run
    if not spark.catalog.tableExists(table_ref):
        logger.info(
            "  Idempotency check — table does not exist yet. Proceeding."
        )
        return False

    count = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM   {table_ref}
        WHERE  etl_run_date = '{ETL_RUN_DATE}'
        AND    is_active    = '{ACTIVE}'
    """).collect()[0]["n"]

    if count > 0:
        logger.info(
            f"  Idempotency check — {count:,} active rows already exist "
            f"for etl_run_date={ETL_RUN_DATE}. Skipping merge."
        )
        return True

    logger.info(
        f"  Idempotency check — 0 rows found for "
        f"etl_run_date={ETL_RUN_DATE}. Proceeding."
    )
    return False


# ============================================================
# TRANSFORM
# ============================================================


def transform_data(
    bronze_df: DataFrame,
    mapping_rows,
    base_object: str,
    pk_cols: list,
):
    log_section(f"TRANSFORM — {base_object}")

    # Apply column mapping
    select_expr = []
    for row in mapping_rows:
        source = row["source_field"].strip()
        target = row["target_field"].strip()
        dtype  = row["target_datatype"].strip().lower().replace(" ", "")

        if dtype.startswith("decimal"):
            nums = re.findall(r"\d+", dtype)
            select_expr.append(
                col(source)
                .cast(DecimalType(int(nums[0]), int(nums[1])))
                .alias(target)
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

    # Drop null PKs
    for pk in pk_cols:
        if pk in df.columns:
            df = df.filter(col(pk).isNotNull())

    # ── Dedup within batch ────────────────────────────────────
    # contract_list has changed_date — use it for meaningful order
    if "changed_date" in df.columns:
        order_col = col("changed_date").desc()
        logger.info("  Dedup order : changed_date DESC")
    else:
        order_col = lit(1)
        logger.info("  Dedup order : no order column (changed_date missing)")

    w_dedup      = Window.partitionBy(*pk_cols).orderBy(order_col)
    df_ranked    = df.withColumn("_rn", row_number().over(w_dedup))
    df_deduped   = df_ranked.filter(col("_rn") == 1).drop("_rn")
    df_intra_dup = df_ranked.filter(col("_rn") >  1).drop("_rn")

    log_counts("after dedup", df_deduped)
    log_counts("intra-batch duplicates", df_intra_dup)

    # ── Record hash on business columns (non-PK) ─────────────

    all_mapped_cols = [r["target_field"].strip() for r in mapping_rows]
    
    df_deduped = df_deduped.withColumn(
        "edp_record_hash",
        md5(concat_ws("||", *[col(c).cast("string") for c in all_mapped_cols])),
    )

    # ── SCD2 metadata ─────────────────────────────────────────
    df_enriched = (
        df_deduped
        .withColumn("edp_start_date",    to_date(lit(ETL_RUN_DATE)))
        .withColumn("edp_end_date",      to_date(lit(SCD2_OPEN_END)))
        .withColumn("is_active",         lit(ACTIVE))
        .withColumn("edp_modified_date", current_timestamp())
    )

    surrogate_parts = (
        [col(pk).cast("string") for pk in pk_cols]
        + [col("edp_start_date").cast("string")]
    )
    df_enriched = df_enriched.withColumn(
        "edp_surrogate_key",
        md5(concat_ws("||", *surrogate_parts)),
    )

    # ── Audit columns ─────────────────────────────────────────
    df_final = (
        df_enriched
        .withColumn("execution_id",   lit(EXECUTION_ID))
        .withColumn("airflow_run_id", lit(AIRFLOW_RUN_ID))
        .withColumn("etl_run_date",   lit(ETL_RUN_DATE))
        .withColumn("source_system",  lit(SOURCE))
        .withColumn("ingestion_ts",   current_timestamp())
    )

    log_counts("final transformed", df_final)

    return df_final, df_intra_dup


# ============================================================
# SCD TYPE 2 MERGE
#
# Full load  → table empty → Step 1 no matches → Step 2 inserts all
# Incremental:
#   hash changed → expire old row → insert new version
#   hash same    → do nothing (no churn)
#   new record   → insert directly
# ============================================================


def apply_scd2_merge(
    base_object: str,
    df_incoming: DataFrame,
    pk_cols: list,
):
    log_section(f"SCD2 MERGE — {base_object}")

    table_ref = f"s3tables.{DB}.{base_object}"
    view_name = f"incoming_{base_object}_{EXECUTION_ID.replace('-', '_')}"
    df_incoming.createOrReplaceTempView(view_name)

    pk_join = " AND ".join(
        [f"silver.{pk} = src.{pk}" for pk in pk_cols]
    )

    # ── STEP 1: Expire changed active rows ────────────────────
    
    logger.info("  Step 1 — Expiring changed active rows...")

    expire_sql = f"""
        MERGE INTO {table_ref} AS silver
        USING {view_name} AS src
        ON ({pk_join})
        WHEN MATCHED
            AND silver.is_active        = '{ACTIVE}'
            AND silver.edp_record_hash != src.edp_record_hash
        THEN UPDATE SET
            silver.is_active         = '{INACTIVE}',
            silver.edp_end_date      = date_sub(src.edp_start_date, 1),
            silver.edp_modified_date = current_timestamp()
    """

    spark.sql(expire_sql)
    logger.info("  Step 1 complete")

    # ── STEP 2: Insert new and changed rows ───────────────────

    logger.info("  Step 2 — Inserting new and changed rows...")

    all_cols       = df_incoming.columns
    insert_cols    = ", ".join(all_cols)
    insert_vals    = ", ".join([f"src.{c}" for c in all_cols])
    no_active_join = " AND ".join(
        [f"s2.{pk} = src.{pk}" for pk in pk_cols]
    )

    insert_sql = f"""
        MERGE INTO {table_ref} AS silver
        USING (
            SELECT src.*
            FROM {view_name} AS src
            WHERE NOT EXISTS (
                SELECT 1
                FROM   {table_ref} AS s2
                WHERE  {no_active_join}
                AND    s2.is_active = '{ACTIVE}'
            )
        ) AS src
        ON ({pk_join} AND silver.is_active = '{ACTIVE}')
        WHEN NOT MATCHED
        THEN INSERT ({insert_cols})
             VALUES ({insert_vals})
    """

    spark.sql(insert_sql)
    logger.info("  Step 2 complete")

    # ── Post-merge counts ─────────────────────────────────────
    active_count = spark.sql(
        f"SELECT COUNT(*) AS n FROM {table_ref} WHERE is_active = '{ACTIVE}'"
    ).collect()[0]["n"]

    inactive_count = spark.sql(
        f"SELECT COUNT(*) AS n FROM {table_ref} WHERE is_active = '{INACTIVE}'"
    ).collect()[0]["n"]

    logger.info(f"  Active rows   : {active_count:,}")
    logger.info(f"  Inactive rows : {inactive_count:,}")

    return active_count, inactive_count


# ============================================================
# DUPLICATE LOG
# ============================================================


def write_duplicate_log(
    base_object: str,
    df_duplicates: DataFrame,
    count: int,
):
    if count == 0:
        logger.info("  No intra-batch duplicates to log.")
        return

    log_path = (
        f"s3://{CONFIG_BUCKET}/logs/{SOURCE}/{base_object}/"
        f"log_date={date.today()}/"
    )
    logger.info(f"  Writing {count:,} duplicates to: {log_path}")
    df_duplicates.write.mode("overwrite").parquet(log_path)
    delete_folder_markers(CONFIG_BUCKET, f"logs/{SOURCE}/{base_object}/")


# ============================================================
# PROCESS OBJECT
# ============================================================


def process_object(
    base_object: str,
    bronze_df: DataFrame,
    rows_read: int,
):
    log_section(f"PROCESS OBJECT — {base_object}")

    table_ref             = f"s3tables.{DB}.{base_object}"
    mapping_rows, pk_cols = read_mapping(base_object)

    # Create on first ever run (full load)
    if not spark.catalog.tableExists(table_ref):
        logger.info("  Table does not exist — creating for full load...")
        create_ddl(base_object, mapping_rows)

    # ── Idempotency check ─────────────────────────────────────
    was_already_processed = already_processed(base_object)
    # ─────────────────────────────────────────────────────────

    df_incoming, df_intra_dup = transform_data(
        bronze_df, mapping_rows, base_object, pk_cols
    )

    records_rejected = df_intra_dup.count()

    active_count, inactive_count = apply_scd2_merge(
        base_object, df_incoming, pk_cols
    )

    write_duplicate_log(base_object, df_intra_dup, records_rejected)

    logger.info(f"  rows_read        : {rows_read:,}")
    logger.info(f"  active_written   : {active_count:,}")
    logger.info(f"  inactive_total   : {inactive_count:,}")
    logger.info(f"  records_rejected : {records_rejected:,}")
    
    return base_object, rows_read, active_count, records_rejected, was_already_processed


# ============================================================
# AUDIT
# ============================================================


def update_audit(
    table_name: str,
    rows_read=None,
    rows_written=None,
    records_rejected=None,
    status=None,
    error_message=None,
):
    table_ref  = "glue_catalog.edp_configs_dev.pipeline_execution_summary"
    safe_error = error_message.replace("'", " ") if error_message else None

    if status == "FAILED":
        sql = f"""
            UPDATE {table_ref}
            SET
                status            = 'FAILED',
                error_message     = '{safe_error}',
                pipeline_end_time = current_timestamp(),
                duration_seconds  =
                    unix_timestamp(current_timestamp()) -
                    unix_timestamp(pipeline_start_time)
            WHERE etl_run_id  = '{EXECUTION_ID}'
              AND run_id       = '{AIRFLOW_RUN_ID}'
              AND layer        = 'SILVER'
              AND environment  = 'DEV'
              AND table_name   = '{table_name}'
        """
    else:
        sql = f"""
            UPDATE {table_ref}
            SET
                records_read      = {rows_read},
                records_written   = {rows_written},
                records_rejected  = {records_rejected},
                status            = 'SUCCESS',
                error_message     = NULL,
                pipeline_end_time = current_timestamp(),
                duration_seconds  =
                    unix_timestamp(current_timestamp()) -
                    unix_timestamp(pipeline_start_time)
            WHERE etl_run_id  = '{EXECUTION_ID}'
              AND run_id       = '{AIRFLOW_RUN_ID}'
              AND layer        = 'SILVER'
              AND environment  = 'DEV'
              AND table_name   = '{table_name}'
        """

    logger.info(f"  Audit SQL:\n{sql}")
    spark.sql(sql)


# ============================================================
# S3 FOLDER MARKER CLEANUP
# ============================================================


def delete_folder_markers(bucket: str, prefix: str):
    s3        = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    markers = [
        {"Key": obj["Key"]}
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
        if "$folder$" in obj["Key"]
    ]

    if markers:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": markers})
        logger.info(f"  Deleted {len(markers)} folder markers")


# ============================================================
# MAIN
# ============================================================

log_section("MAIN — JOB START")

log_section("MAIN — JOB START")

try:

    if SILVER_TABLE == "contract_list":

        bronze_df, rows_read = read_bronze("contractlist", ETL_RUN_DATE)

        if bronze_df is None or rows_read == 0:
            logger.warning("  No bronze data found. Skipping processing.")
            update_audit(
                table_name=SILVER_TABLE,
                rows_read=0,
                rows_written=0,
                records_rejected=0,
                status="SUCCESS",
            )
            job.commit()

        else:
            tbl, rows_read, rows_written, records_rejected, was_already_processed = process_object(
                SILVER_TABLE, bronze_df, rows_read
            )

            # ── Only update audit on a real run ──────────────
            if was_already_processed:
                logger.info(
                    "  Rerun detected — audit table not updated. "
                    "Original run metrics preserved."
                )
            else:
                update_audit(
                    table_name=tbl,
                    rows_read=rows_read,
                    rows_written=rows_written,
                    records_rejected=records_rejected,
                    status="SUCCESS",
                )
            # ─────────────────────────────────────────────────

            job.commit()
            log_section("JOB COMPLETED SUCCESSFULLY")

    else:
        raise ValueError(f"Invalid SILVER_TABLE value: '{SILVER_TABLE}'")


except Exception as exc:
    failed_table = SILVER_TABLE if "SILVER_TABLE" in locals() else "UNKNOWN"
    logger.error(f"  Job failed on table : {failed_table}")
    logger.error(f"  Error               : {str(exc)}")

    update_audit(
        table_name=failed_table,
        status="FAILED",
        error_message=str(exc),
    )
    raise