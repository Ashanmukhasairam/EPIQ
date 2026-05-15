#Senior Code__________________________________________________________________________________

import sys
import re
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,current_timestamp,lit,max as spark_max)
from pyspark.sql.utils import AnalysisException
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# Job Parameters
# --------------------------------------------------
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
        "GOLD_TABLE"
    ]
)

JOB_NAME = args["JOB_NAME"]
ACCOUNT_ID = args["ACCOUNT_ID"]
EXECUTION_ID = args["EXECUTION_ID"]
AIRFLOW_RUN_ID = args["AIRFLOW_RUN_ID"]
CONFIG_BUCKET = args["CONFIG_BUCKET"]
SOURCE = args["SOURCE"]
GOLD_BUCKET = args["GOLD_BUCKET"]
SILVER_BUCKET = args["SILVER_BUCKET"]
SILVER_NAMESPACE = args["SILVER_DATABASE"]
GOLD_NAMESPACE = args["GOLD_DATABASE"]

silver_tables = args["SILVER_TABLE"].split(",")
gold_tables = args["GOLD_TABLE"].split(",")

if len(silver_tables) != len(gold_tables):
    raise Exception("silver_table and gold_table count mismatch")

SILVER_WAREHOUSE = f"s3://{SILVER_BUCKET}/bucket/{SILVER_BUCKET}"
GOLD_WAREHOUSE = f"s3://{GOLD_BUCKET}/bucket/{GOLD_BUCKET}"

# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder.appName(JOB_NAME)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.silver.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.silver.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{SILVER_BUCKET}")
    .config("spark.sql.catalog.silver.warehouse", SILVER_WAREHOUSE)
    .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.gold.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.gold.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{GOLD_BUCKET}")
    .config("spark.sql.catalog.gold.warehouse", GOLD_WAREHOUSE)
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.glue.region", "us-east-1")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(JOB_NAME, args)


# --------------------------------------------------
# Mapping
# --------------------------------------------------
def read_mapping(base_object):
    mapping_path = f"s3://{CONFIG_BUCKET}/{SOURCE}/mappings/contracts_mapping.csv"

    df = (
        spark.read.option("header", "true")
        .csv(mapping_path)
        .filter(
            (col("source_system") == SOURCE) &
            (col("source_object") == base_object) &
            (col("is_active") == "TRUE") &
            (col("is_gold") == "TRUE")
        )
    )

    rows = df.collect()
    if not rows:
        raise Exception(f"Mapping not found for {base_object}")

    return rows


def generate_ddl(base_object, mapping_rows):
    ddl_columns = []

    for row in mapping_rows:
        target = row["target_field"].strip()
        dtype = row["target_datatype"].strip().lower().replace(" ", "")

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

    ddl_columns.extend([
        "execution_id string",
        "airflow_run_id string",
        "source_system string",
        "etl_run_date date",
        "ingestion_ts timestamp"
    ])

    if base_object in ["contractheader", "contractitems"]:
        ddl_columns.extend([
            "create_date timestamp",
            "modified_date timestamp"
        ])

    return ", ".join(ddl_columns)


def create_gold_table(table_fqn, ddl_string):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS gold.{GOLD_NAMESPACE}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_fqn} (
            {ddl_string}
        )
        USING iceberg
        PARTITIONED BY (etl_run_date)
    """)


# --------------------------------------------------
# Watermark
# --------------------------------------------------
def get_last_watermark(gold_df):
    if gold_df.limit(1).count() == 0:
        return None
    return gold_df.select(spark_max("etl_run_date")).collect()[0][0]


# --------------------------------------------------
# Deduplication
# --------------------------------------------------
def deduplicate_records(df, table_name):
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    keys = ["contract_number", "item_id"] if table_name == "contractitems" else ["contract_number"]

    window = Window.partitionBy(*keys).orderBy(col("etl_run_date").desc())

    return (
        df.withColumn("rn", row_number().over(window))
          .filter(col("rn") == 1)
          .drop("rn")
    )


# --------------------------------------------------
# Contract List SCD1
# --------------------------------------------------
def process_contract_list(dedup_df, gold_df, gold_fqn, last_watermark):

    base_df = (
        dedup_df
        .withColumn("execution_id", lit(EXECUTION_ID))
        .withColumn("airflow_run_id", lit(AIRFLOW_RUN_ID))
        .withColumn("source_system", lit(SOURCE))
        .withColumn("etl_run_date", col("etl_run_date").cast("date"))
        .withColumn("ingestion_ts", current_timestamp())
    )

    rows_inserted = 0
    rows_updated = 0

    if last_watermark is None:
        base_df.writeTo(gold_fqn).append()
        rows_inserted = base_df.count()
        return rows_inserted, rows_updated

    insert_df = (
        base_df.alias("s")
        .join(gold_df.alias("g"), "contract_number", "left_anti")
        .filter(
            (col("created_date") >= last_watermark) &
            (col("changed_date").isNull() | (col("changed_date") == ""))
        )
    )

    if insert_df.limit(1).count() > 0:
        insert_df.writeTo(gold_fqn).append()
        rows_inserted = insert_df.count()

    update_df = (
        base_df.alias("s")
        .join(gold_df.alias("g"), "contract_number", "inner")
        .filter(col("changed_date") >= last_watermark)
    )

    if update_df.limit(1).count() > 0:
        update_df.createOrReplaceTempView("update_view")

        update_cols = [c for c in gold_df.columns if c != "contract_number"]
        set_clause = ", ".join([f"target.{c} = source.{c}" for c in update_cols])

        spark.sql(f"""
            MERGE INTO {gold_fqn} target
            USING update_view source
            ON target.contract_number = source.contract_number
            WHEN MATCHED THEN UPDATE SET {set_clause}
        """)

        rows_updated = update_df.count()

    return rows_inserted, rows_updated


# --------------------------------------------------
# Standard SCD1
# --------------------------------------------------
def process_standard_scd1(dedup_df, gold_df, gold_fqn, merge_keys):

    base_df = (
        dedup_df
        .withColumn("execution_id", lit(EXECUTION_ID))
        .withColumn("airflow_run_id", lit(AIRFLOW_RUN_ID))
        .withColumn("source_system", lit(SOURCE))
        .withColumn("etl_run_date", col("etl_run_date").cast("date"))
        .withColumn("ingestion_ts", current_timestamp())
    )

    base_df.createOrReplaceTempView("source_view")

    on_clause = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    update_cols = []
    for c in gold_df.columns:
        if c in merge_keys:
            continue
        elif c == "create_date":
            continue
        elif c == "modified_date":
            update_cols.append("target.modified_date = current_timestamp()")
        else:
            update_cols.append(f"target.{c} = source.{c}")

    insert_cols = ", ".join(gold_df.columns)
    insert_vals = ", ".join(
        ["current_timestamp()" if c in ["create_date", "modified_date"]
         else f"source.{c}" for c in gold_df.columns]
    )

    spark.sql(f"""
        MERGE INTO {gold_fqn} target
        USING source_view source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {", ".join(update_cols)}
        WHEN NOT MATCHED THEN INSERT ({insert_cols})
        VALUES ({insert_vals})
    """)

    return base_df.count()


# --------------------------------------------------
# Audit
# --------------------------------------------------
def update_audit_table(metrics):
    for m in metrics:
        table_name, rows_read, rows_written, rows_inserted, rows_updated, rows_deleted, rows_rejected = m

        exists = spark.sql(f"""
            SELECT COUNT(1) cnt
            FROM glue_catalog.edp_configs_dev.pipeline_execution_summary
            WHERE etl_run_id = '{EXECUTION_ID}'
              AND run_id = '{AIRFLOW_RUN_ID}'
              AND layer = 'GOLD'
              AND table_name = '{table_name}'
        """).collect()[0]["cnt"]

        if exists > 0:
            spark.sql(f"""
                UPDATE glue_catalog.edp_configs_dev.pipeline_execution_summary
                SET records_read={rows_read},
                    records_written={rows_written},
                    records_inserted={rows_inserted},
                    records_updated={rows_updated},
                    records_deleted={rows_deleted},
                    records_rejected={rows_rejected}
                WHERE etl_run_id='{EXECUTION_ID}'
                  AND run_id='{AIRFLOW_RUN_ID}'
                  AND layer='GOLD'
                  AND table_name='{table_name}'
            """)


# --------------------------------------------------
# MAIN
# --------------------------------------------------
try:

    metrics = []

    for silver_tbl, gold_tbl in zip(silver_tables, gold_tables):

        silver_df = spark.read.table(f"silver.{SILVER_NAMESPACE}.{silver_tbl}")
        gold_fqn = f"gold.{GOLD_NAMESPACE}.{gold_tbl}"

        mapping_rows = read_mapping(silver_tbl)
        ddl_string = generate_ddl(silver_tbl, mapping_rows)
        create_gold_table(gold_fqn, ddl_string)

        gold_df = spark.read.table(gold_fqn)
        last_watermark = get_last_watermark(gold_df)

        if last_watermark:
            silver_df = silver_df.filter(col("etl_run_date") >= last_watermark)

        dedup_df = deduplicate_records(silver_df, silver_tbl).cache()

        rows_read = dedup_df.count()
        rows_inserted = 0
        rows_updated = 0

        if silver_tbl == "contractlist":
            rows_inserted, rows_updated = process_contract_list(
                dedup_df, gold_df, gold_fqn, last_watermark
            )
        elif silver_tbl == "contractheader":
            rows_inserted = process_standard_scd1(
                dedup_df, gold_df, gold_fqn, ["contract_number"]
            )
        elif silver_tbl == "contractitems":
            rows_inserted = process_standard_scd1(
                dedup_df, gold_df, gold_fqn, ["contract_number", "item_id"]
            )

        rows_written = rows_inserted + rows_updated

        metrics.append((
            gold_tbl,
            rows_read,
            rows_written,
            rows_inserted,
            rows_updated,
            0,
            0
        ))

    update_audit_table(metrics)

    job.commit()

except Exception as e:
    logger.error(f"GOLD Job Failed: {str(e)}", exc_info=True)
    raise

finally:
    spark.stop()
_________________________________________________________________________________________





















# import sys
# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import current_date, col
# from awsglue.utils import getResolvedOptions
# from awsglue.context import GlueContext
# from awsglue.job import Job

# print("\n========== SILVER ➜ GOLD (HEADER + ITEMS) ==========")
# print("Start Time:", datetime.utcnow())

# # ======================================================
# # 1. JOB PARAMETERS (FROM GLUE)
# # ======================================================
# args = getResolvedOptions(
#     sys.argv,
#     [
#         "JOB_NAME",
#         "ACCOUNT_ID",
#         "SILVER_BUCKET",
#         "GOLD_BUCKET",
#         "TARGET_HEADER",
#         "TARGET_ITEM",
#         "SILVER_DATABASE",
#         "GOLD_NAMESPACE"
#     ]
# )

# JOB_NAME        = args["JOB_NAME"]
# ACCOUNT_ID      = args["ACCOUNT_ID"]
# SILVER_BUCKET   = args["SILVER_BUCKET"]
# GOLD_BUCKET     = args["GOLD_BUCKET"]
# TARGET_HEADER   = args["TARGET_HEADER"]
# TARGET_ITEM     = args["TARGET_ITEM"]
# GOLD_NAMESPACE  = args["GOLD_NAMESPACE"]
# SILVER_DATABASE = args["SILVER_DATABASE"]

# SILVER_WAREHOUSE = f"s3://{SILVER_BUCKET}/bucket/{SILVER_BUCKET}"
# GOLD_WAREHOUSE   = f"s3://{GOLD_BUCKET}/bucket/{GOLD_BUCKET}"

# # ======================================================
# # 2. SPARK SESSION (DUAL ICEBERG CATALOG)
# # ======================================================
# spark = (
#     SparkSession.builder.appName(JOB_NAME)
#     .config("spark.sql.extensions",
#             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

#     # SILVER CATALOG
#     .config("spark.sql.catalog.silver",
#             "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.silver.catalog-impl",
#             "org.apache.iceberg.aws.glue.GlueCatalog")
#     .config("spark.sql.catalog.silver.glue.id",
#             f"{ACCOUNT_ID}:s3tablescatalog/{SILVER_BUCKET}")
#     .config("spark.sql.catalog.silver.warehouse", SILVER_WAREHOUSE)

#     # GOLD CATALOG
#     .config("spark.sql.catalog.gold",
#             "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.gold.catalog-impl",
#             "org.apache.iceberg.aws.glue.GlueCatalog")
#     .config("spark.sql.catalog.gold.glue.id",
#             f"{ACCOUNT_ID}:s3tablescatalog/{GOLD_BUCKET}")
#     .config("spark.sql.catalog.gold.warehouse", GOLD_WAREHOUSE)

#     .config("spark.sql.adaptive.enabled", "true")
#     .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
#     .config("spark.sql.shuffle.partitions", "200")
#     .config("spark.sql.session.timeZone", "UTC")
#     .getOrCreate()
# )

# glueContext = GlueContext(spark.sparkContext)
# job = Job(glueContext)
# job.init(JOB_NAME, args)

# print("\n[STEP 1] Spark Session Created")

# # ======================================================
# # 3. FIND SILVER TABLES DYNAMICALLY
# # ======================================================
# namespaces = spark.sql("SHOW NAMESPACES IN silver").collect()

# HEADER_FQN = None
# ITEM_FQN   = None

# for row in namespaces:
#     ns = row[0]
#     tables = spark.sql(f"SHOW TABLES IN silver.{ns}").collect()
#     for t in tables:
#         if t.tableName.lower() == TARGET_HEADER.lower():
#             HEADER_FQN = f"silver.{ns}.{t.tableName}"
#         if t.tableName.lower() == TARGET_ITEM.lower():
#             ITEM_FQN = f"silver.{ns}.{t.tableName}"

# if not HEADER_FQN or not ITEM_FQN:
#     raise Exception(
#         f"Silver tables not found. Looking for '{TARGET_HEADER}' and '{TARGET_ITEM}'. "
#         f"Check TARGET_HEADER and TARGET_ITEM arguments."
#     )

# print("Header Table Found:", HEADER_FQN)
# print("Item Table Found  :", ITEM_FQN)

# # ======================================================
# # 4. READ SILVER TABLES
# # ======================================================
# header_df = spark.read.table(HEADER_FQN)
# item_df   = spark.read.table(ITEM_FQN)

# if header_df.rdd.isEmpty() and item_df.rdd.isEmpty():
#     print("No data found in Silver tables. Exiting job.")
#     job.commit()
#     spark.stop()
#     sys.exit(0)

# # Drop corrupt records from header if present
# if "_corrupt_record" in header_df.columns:
#     header_df = header_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
#     print("Corrupt records removed from header.")

# # Drop OData metadata columns from header if present
# odata_cols = ["odata_context", "odata_metadataetag"]
# for c in odata_cols:
#     if c in header_df.columns:
#         header_df = header_df.drop(c)

# print("Header Silver Count:", header_df.count())
# print("Item Silver Count  :", item_df.count())
# print(item_df.printSchema())
# # ======================================================
# # 5. DEDUPLICATE SILVER DATA
# # Header key : contractnumber
# # Items key  : itemid only (contractnumber not in items table)
# # ======================================================
# header_dedup_df = header_df.dropDuplicates(["contractnumber"])


# print("Header Deduplicated Count:", header_dedup_df.count())


# # ======================================================
# # 6. ADD LOAD DATE
# # ======================================================
# header_gold_df = header_dedup_df.withColumn("load_date", current_date())
# item_gold_df   = item_df.withColumn("load_date", current_date())

# # ======================================================
# # 7. GOLD TABLE FULLY QUALIFIED NAMES
# # ======================================================
# GOLD_HEADER_FQN = f"gold.{GOLD_NAMESPACE}.{TARGET_HEADER}"
# GOLD_ITEM_FQN   = f"gold.{GOLD_NAMESPACE}.{TARGET_ITEM}"

# # ======================================================
# # 8. CREATE GOLD TABLES (DYNAMIC SCHEMA FROM DF)
# # ======================================================
# def generate_schema_ddl(df):
#     type_mapping = {
#         "string":    "STRING",
#         "integer":   "INT",
#         "long":      "BIGINT",
#         "double":    "DOUBLE",
#         "float":     "FLOAT",
#         "boolean":   "BOOLEAN",
#         "date":      "DATE",
#         "timestamp": "TIMESTAMP",
#     }
#     columns = []
#     for field in df.schema:
#         col_name   = field.name.lower()
#         spark_type = field.dataType.simpleString().lower()
#         sql_type   = spark_type.upper() if spark_type.startswith("decimal") else type_mapping.get(spark_type, "STRING")
#         columns.append(f"{col_name} {sql_type}")
#     return ",\n  ".join(columns)

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {GOLD_HEADER_FQN} (
#       {generate_schema_ddl(header_gold_df)}
#     )
#     USING iceberg
#     PARTITIONED BY (load_date)
# """)

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {GOLD_ITEM_FQN} (
#       {generate_schema_ddl(item_gold_df)}
#     )
#     USING iceberg
#     PARTITIONED BY (load_date)
# """)

# print("Gold Tables Verified/Created")

# # ======================================================
# # 9. TRUNCATE GOLD TABLES — FULL OVERWRITE
# # ======================================================
# print("\n[STEP 2] Truncating Gold tables...")
# spark.sql(f"TRUNCATE TABLE {GOLD_HEADER_FQN}")
# spark.sql(f"TRUNCATE TABLE {GOLD_ITEM_FQN}")
# print("Gold tables truncated.")

# # ======================================================
# # 10. WRITE DEDUPLICATED DATA TO GOLD
# # ======================================================
# print("\n[STEP 3] Writing Header to Gold...")
# header_gold_df.writeTo(GOLD_HEADER_FQN).append()
# print("Header written successfully.")

# print("\n[STEP 4] Writing Items to Gold...")
# item_gold_df.writeTo(GOLD_ITEM_FQN).append()
# print("Items written successfully.")

# # ======================================================
# # 11. FINAL COUNT VERIFICATION
# # ======================================================
# final_header_count = spark.read.table(GOLD_HEADER_FQN).count()
# final_item_count   = spark.read.table(GOLD_ITEM_FQN).count()

# print(f"\nGold Header Final Count : {final_header_count}")
# print(f"Gold Item Final Count   : {final_item_count}")

# job.commit()
# print("\n========== GOLD LOAD COMPLETED SUCCESSFULLY ==========")
# print("End Time:", datetime.utcnow())

# spark.stop()





# import sys
# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import current_date
# from awsglue.utils import getResolvedOptions

# print("\n========== SILVER ➜ GOLD (HEADER + ITEMS) ==========")
# print("Start Time:", datetime.utcnow())

# # ======================================================
# # 1. JOB PARAMETERS (FROM GLUE)
# # ======================================================
# args = getResolvedOptions(
#     sys.argv,
#     [
#         "JOB_NAME",
#         "ACCOUNT_ID",
#         "SILVER_BUCKET",
#         "GOLD_BUCKET",
#         "TARGET_HEADER",
#         "TARGET_ITEM",
#         "GOLD_NAMESPACE"
#     ]
# )

# JOB_NAME       = args["JOB_NAME"]
# ACCOUNT_ID     = args["ACCOUNT_ID"]
# SILVER_BUCKET  = args["SILVER_BUCKET"]
# GOLD_BUCKET    = args["GOLD_BUCKET"]
# TARGET_HEADER  = args["TARGET_HEADER"]
# TARGET_ITEM    = args["TARGET_ITEM"]
# GOLD_NAMESPACE = args["GOLD_NAMESPACE"]

# SILVER_WAREHOUSE = f"s3://{SILVER_BUCKET}/bucket/{SILVER_BUCKET}"
# GOLD_WAREHOUSE   = f"s3://{GOLD_BUCKET}/bucket/{GOLD_BUCKET}"

# # ======================================================
# # 2. SPARK SESSION (DUAL ICEBERG CATALOG)
# # ======================================================
# spark = (
#     SparkSession.builder.appName(JOB_NAME)
#     .config("spark.sql.extensions",
#             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

#     # SILVER CATALOG
#     .config("spark.sql.catalog.silver",
#             "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.silver.catalog-impl",
#             "org.apache.iceberg.aws.glue.GlueCatalog")
#     .config("spark.sql.catalog.silver.glue.id",
#             f"{ACCOUNT_ID}:s3tablescatalog/{SILVER_BUCKET}")
#     .config("spark.sql.catalog.silver.warehouse",
#             SILVER_WAREHOUSE)

#     # GOLD CATALOG
#     .config("spark.sql.catalog.gold",
#             "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.gold.catalog-impl",
#             "org.apache.iceberg.aws.glue.GlueCatalog")
#     .config("spark.sql.catalog.gold.glue.id",
#             f"{ACCOUNT_ID}:s3tablescatalog/{GOLD_BUCKET}")
#     .config("spark.sql.catalog.gold.warehouse",
#             GOLD_WAREHOUSE)

#     .getOrCreate()
# )

# print("\n[STEP 1] Spark Session Created")

# # ======================================================
# # 3. FIND SILVER TABLES DYNAMICALLY
# # ======================================================
# namespaces = spark.sql("SHOW NAMESPACES IN silver").collect()

# HEADER_FQN = None
# ITEM_FQN   = None

# for row in namespaces:
#     ns = row[0]
#     tables = spark.sql(f"SHOW TABLES IN silver.{ns}").collect()
#     for t in tables:
#         if t.tableName.lower() == TARGET_HEADER.lower():
#             HEADER_FQN = f"silver.{ns}.{TARGET_HEADER}"
#         if t.tableName.lower() == TARGET_ITEM.lower():
#             ITEM_FQN = f"silver.{ns}.{TARGET_ITEM}"

# if not HEADER_FQN or not ITEM_FQN:
#     raise Exception("Silver tables not found. Check namespace configuration.")

# print("Header Table Found:", HEADER_FQN)
# print("Item Table Found:", ITEM_FQN)

# # ======================================================
# # 4. READ SILVER TABLES
# # ======================================================
# header_df = spark.read.table(HEADER_FQN)
# item_df   = spark.read.table(ITEM_FQN)

# if header_df.rdd.isEmpty() and item_df.rdd.isEmpty():
#     print("No data found in Silver tables. Exiting job.")
#     spark.stop()
#     sys.exit(0)

# print("Header Record Count:", header_df.count())
# print("Item Record Count:", item_df.count())

# # ======================================================
# # 5. ADD LOAD DATE
# # ======================================================
# header_gold_df = header_df.withColumn("load_date", current_date())
# item_gold_df   = item_df.withColumn("load_date", current_date())

# # ======================================================
# # 6. GOLD TABLE FULLY QUALIFIED NAMES
# # ======================================================
# GOLD_HEADER_FQN = f"gold.{GOLD_NAMESPACE}.{TARGET_HEADER}"
# GOLD_ITEM_FQN   = f"gold.{GOLD_NAMESPACE}.{TARGET_ITEM}"

# # ======================================================
# # 7. CREATE GOLD TABLES (DYNAMIC SCHEMA FROM DF)
# # ======================================================
# def generate_schema_ddl(df):
#     return ",\n  ".join(
#         [f"{field.name.lower()} {field.dataType.simpleString().upper()}"
#          for field in df.schema]
#     )

# header_schema_ddl = generate_schema_ddl(header_gold_df)
# item_schema_ddl   = generate_schema_ddl(item_gold_df)

# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {GOLD_HEADER_FQN} (
#   {header_schema_ddl}
# )
# USING iceberg
# PARTITIONED BY (load_date)
# """)

# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {GOLD_ITEM_FQN} (
#   {item_schema_ddl}
# )
# USING iceberg
# PARTITIONED BY (load_date)
# """)

# print("Gold Tables Verified/Created")

# # ======================================================
# # 8. WRITE TO GOLD (PARTITION OVERWRITE)
# # ======================================================
# print("\n[STEP 2] Writing Header to Gold...")
# header_gold_df.writeTo(GOLD_HEADER_FQN).overwritePartitions()

# print("[STEP 3] Writing Items to Gold...")
# item_gold_df.writeTo(GOLD_ITEM_FQN).overwritePartitions()

# print("\n========== GOLD LOAD COMPLETED SUCCESSFULLY ==========")
# print("End Time:", datetime.utcnow())

# spark.stop()