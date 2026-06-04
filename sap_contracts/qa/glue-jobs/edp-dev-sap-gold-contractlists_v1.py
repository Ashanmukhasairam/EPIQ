# ============================================================
# SAP GOLD GLUE JOB
# ============================================================

import sys
import re
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, max as spark_max
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# ============================================================
# Logging
# ============================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("========== STARTING GOLD JOB ==========")

# ============================================================
# Job Parameters
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ACCOUNT_ID",
        "EXECUTION_ID",
        "AIRFLOW_RUN_ID",
        "CONFIG_BUCKET",
        "SOURCE",
        "GOLD_BUCKET",
        "SILVER_BUCKET",
        "SILVER_DATABASE",
        "GOLD_DATABASE",
        "SILVER_TABLE",
        "GOLD_TABLE",
    ],
)

JOB_NAME       = args["JOB_NAME"]
ACCOUNT_ID     = args["ACCOUNT_ID"]
EXECUTION_ID   = args["EXECUTION_ID"]
AIRFLOW_RUN_ID = args["AIRFLOW_RUN_ID"]
CONFIG_BUCKET  = args["CONFIG_BUCKET"]
SOURCE         = args["SOURCE"]
GOLD_BUCKET    = args["GOLD_BUCKET"]
SILVER_BUCKET  = args["SILVER_BUCKET"]

SILVER_NAMESPACE = args["SILVER_DATABASE"]
GOLD_NAMESPACE   = args["GOLD_DATABASE"]

silver_tables = args["SILVER_TABLE"].split(",")
gold_tables   = args["GOLD_TABLE"].split(",")

if len(silver_tables) != len(gold_tables):
    raise Exception("silver_table and gold_table count mismatch")

SILVER_WAREHOUSE = f"s3://{SILVER_BUCKET}/bucket/{SILVER_BUCKET}"
GOLD_WAREHOUSE   = f"s3://{GOLD_BUCKET}/bucket/{GOLD_BUCKET}"

# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder.appName(JOB_NAME)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.silver.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config(
        "spark.sql.catalog.silver.glue.id",
        f"{ACCOUNT_ID}:s3tablescatalog/{SILVER_BUCKET}",
    )
    .config("spark.sql.catalog.silver.warehouse", SILVER_WAREHOUSE)
    .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.gold.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config(
        "spark.sql.catalog.gold.glue.id",
        f"{ACCOUNT_ID}:s3tablescatalog/{GOLD_BUCKET}",
    )
    .config("spark.sql.catalog.gold.warehouse", GOLD_WAREHOUSE)
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog",
    )
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

# ============================================================
# READ MAPPING
# ============================================================


def read_mapping_and_keys(base_object):

    mapping_path = f"s3://{CONFIG_BUCKET}/{SOURCE}/mappings/contracts_mapping.csv"

    df = (
        spark.read.option("header", "true")
        .csv(mapping_path)
        .filter(
            (col("source_system") == SOURCE)
            & (col("source_object") == base_object)
            & (col("is_active")     == "TRUE")
            & (col("is_gold")       == "TRUE")
        )
    )

    rows = df.collect()

    if not rows:
        raise Exception(f"Mapping not found for {base_object}")

    business_cols = [r["target_field"] for r in rows]

    pk_cols = [
        r["target_field"]
        for r in rows
        if r["is_primary_key"] == "TRUE"
    ]

    if not pk_cols:
        raise Exception(f"No primary key defined for {base_object}")

    return rows, pk_cols, business_cols


# ============================================================
# GENERATE DDL
# ============================================================


def generate_ddl(base_object, mapping_rows):

    ddl_columns = []

    for row in mapping_rows:

        target = row["target_field"].strip()
        dtype  = row["target_datatype"].strip().lower().replace(" ", "")

        if dtype.startswith("decimal"):
            nums = re.findall(r"\d+", dtype)
            ddl_columns.append(f"{target} decimal({nums[0]},{nums[1]})")

        elif dtype in ["int", "integer"]:
            ddl_columns.append(f"{target} int")

        elif dtype == "double":
            ddl_columns.append(f"{target} double")

        elif dtype == "date":
            ddl_columns.append(f"{target} date")

        elif dtype == "timestamp":
            ddl_columns.append(f"{target} timestamp")

        else:
            ddl_columns.append(f"{target} string")

    # Audit columns — no ETL pipeline metadata
    ddl_columns.extend([
        "source_system    string",
        "ingestion_ts     timestamp",
        "created_date_ts  timestamp",
        "modified_date_ts timestamp",
    ])

    return ", ".join(ddl_columns)


# ============================================================
# CREATE GOLD TABLE
# ============================================================


def create_gold_table(table_fqn, ddl_string):

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS gold.{GOLD_NAMESPACE}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_fqn} (
            {ddl_string}
        )
        USING iceberg
        PARTITIONED BY (source_system)
    """)


# ============================================================
# DEDUPLICATION
# ============================================================


def deduplicate_records(df, pk_cols):

    window = Window.partitionBy(*pk_cols).orderBy(
        col("ingestion_ts").desc()
    )

    df = (
        df.withColumn("rn", row_number().over(window))
          .filter(col("rn") == 1)
          .drop("rn")
    )

    return df


# ============================================================
# SCD TYPE 1 MERGE
# ============================================================


def process_scd1(dedup_df, gold_df, gold_fqn, pk_cols, business_cols):

    view_name = f"merge_source_{gold_fqn.replace('.', '_')}"
    dedup_df.createOrReplaceTempView(view_name)

    rows_inserted = (
        dedup_df.join(gold_df.select(*pk_cols), pk_cols, "left_anti").count()
    )
    rows_updated = (
        dedup_df.join(gold_df.select(*pk_cols), pk_cols, "inner").count()
    )

    join_condition = " AND ".join(
        [f"target.{c} = source.{c}" for c in pk_cols]
    )

    # Update business columns + audit columns
    update_cols = []
    for c in business_cols:
        if c not in pk_cols:
            update_cols.append(f"target.{c} = source.{c}")
    update_cols.append("target.source_system    = source.source_system")
    update_cols.append("target.ingestion_ts     = source.ingestion_ts")
    update_cols.append("target.modified_date_ts = current_timestamp()")

    # Insert business columns + audit columns
    insert_cols = business_cols + [
        "source_system",
        "ingestion_ts",
        "created_date_ts",
        "modified_date_ts",
    ]

    insert_vals = []
    for c in insert_cols:
        if c == "created_date_ts":
            insert_vals.append("current_timestamp()")
        elif c == "modified_date_ts":
            insert_vals.append("current_timestamp()")
        else:
            insert_vals.append(f"source.{c}")

    spark.sql(f"""
        MERGE INTO {gold_fqn} target
        USING {view_name} source
        ON {join_condition}

        WHEN MATCHED THEN
            UPDATE SET {", ".join(update_cols)}

        WHEN NOT MATCHED THEN
            INSERT ({", ".join(insert_cols)})
            VALUES ({", ".join(insert_vals)})
    """)

    spark.catalog.dropTempView(view_name)

    return rows_inserted, rows_updated


# ============================================================
# AUDIT
# ============================================================


def update_audit(
    table_name,
    rows_read=None,
    rows_written=None,
    rows_inserted=None,
    rows_updated=None,
    status=None,
    error_message=None,
):
    table_ref  = "glue_catalog.edp_configs_dev.pipeline_execution_summary"
    safe_error = error_message.replace("'", " ") if error_message else None

    if status == "FAILED":
        query = f"""
            UPDATE {table_ref}
            SET
                status            = 'FAILED',
                error_message     = '{safe_error}',
                pipeline_end_time = current_timestamp(),
                duration_seconds  =
                    unix_timestamp(current_timestamp()) -
                    unix_timestamp(pipeline_start_time)
            WHERE etl_run_id  = '{EXECUTION_ID}'
              AND layer        = 'GOLD'
              AND environment  = 'DEV'
              AND table_name   = '{table_name}'
        """
    else:
        query = f"""
            UPDATE {table_ref}
            SET
                records_read     = {rows_read},
                records_written  = {rows_written},
                records_inserted = {rows_inserted},
                records_updated  = {rows_updated},
                status           = 'SUCCESS',
                error_message    = NULL,
                pipeline_end_time = current_timestamp(),
                duration_seconds  =
                    unix_timestamp(current_timestamp()) -
                    unix_timestamp(pipeline_start_time)
            WHERE etl_run_id  = '{EXECUTION_ID}'
              AND layer        = 'GOLD'
              AND environment  = 'DEV'
              AND table_name   = '{table_name}'
        """

    logger.info(f"AUDIT UPDATE ({status}):\n{query}")
    spark.sql(query)


# ============================================================
# INCREMENTAL AUDIT UPDATE
# ============================================================


def update_audit_incremental():

    try:
        control_table = "glue_catalog.edp_configs_dev.edp_incremental_control"
        table_name    = "contracts"
        source_system = "sap"

        watermark_records = []

        for gold_tbl in gold_tables:

            gold_fqn = f"gold.{GOLD_NAMESPACE}.{gold_tbl}"

            try:
                row = spark.sql(f"""
                    SELECT
                        MAX(created_date) AS max_created,
                        MAX(changed_date) AS max_changed
                    FROM {gold_fqn}
                """).collect()[0]

                max_created = row["max_created"]
                max_changed = row["max_changed"]

                if max_created or max_changed:
                    if max_created and (
                        not max_changed or max_created >= max_changed
                    ):
                        watermark_records.append((max_created, "created_date"))
                    else:
                        watermark_records.append((max_changed, "changed_date"))

            except Exception as e:
                print(f"Error reading {gold_fqn}: {e}")

        if not watermark_records:
            print("No watermark found. Skipping update.")
            return

        latest_watermark, watermark_column = max(
            watermark_records, key=lambda x: x[0]
        )

        try:
            spark.sql(f"""
                UPDATE {control_table}
                SET
                    source_system             = '{source_system}',
                    watermark_column          = '{watermark_column}',
                    last_successful_watermark = TIMESTAMP '{latest_watermark}',
                    updated_at                = current_timestamp(),
                    etl_run_id                = '{EXECUTION_ID}'
                WHERE table_name = '{table_name}'
            """)
            print("Incremental control update successful")

        except Exception as e:
            print(f"Incremental control update failed: {e}")

    except Exception as e:
        print(f"update_audit_incremental failed: {e}")


# ============================================================
# MAIN
# ============================================================

current_table = None

try:

    for silver_tbl, gold_tbl in zip(silver_tables, gold_tables):

        current_table = gold_tbl

        # ── Read only active (latest) records from silver ─────
        # is_active = 'Y' means current version in SCD2
        # No watermark needed — active filter always gives latest
        silver_df = (
            spark.read
            .table(f"silver.{SILVER_NAMESPACE}.{silver_tbl}")
            .filter(col("is_active") == "Y")
        )

        mapping_rows, pk_cols, business_cols = read_mapping_and_keys(silver_tbl)

        gold_fqn   = f"gold.{GOLD_NAMESPACE}.{gold_tbl}"
        ddl_string = generate_ddl(silver_tbl, mapping_rows)

        create_gold_table(gold_fqn, ddl_string)

        gold_df = spark.read.table(gold_fqn)

        silver_df = silver_df.cache()
        rows_read = silver_df.count()

        dedup_df = deduplicate_records(silver_df, pk_cols).cache()

        rows_inserted, rows_updated = process_scd1(
            dedup_df, gold_df, gold_fqn, pk_cols, business_cols
        )

        rows_written = rows_inserted + rows_updated

        update_audit(
            table_name=gold_tbl,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            status="SUCCESS",
        )

        if current_table == "contract_list":
            update_audit_incremental()

    job.commit()
    print("========== GOLD JOB COMPLETED SUCCESSFULLY ==========")

except Exception as e:
    logger.error(f"GOLD Job Failed: {str(e)}", exc_info=True)
    update_audit(
        table_name=current_table,
        status="FAILED",
        error_message=str(e),
    )
    raise

finally:
    spark.stop()