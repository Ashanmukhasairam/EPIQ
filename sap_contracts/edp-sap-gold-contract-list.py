import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, col
from pyspark.sql.utils import AnalysisException

# ======================================================
# INITIALIZE
# ======================================================

print("\n========== SILVER ➜ GOLD JOB START ==========")
print("Start Time:", datetime.utcnow())

# ======================================================
# PARAMETERS
# ======================================================
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ACCOUNT_ID",
        "SILVER_BUCKET",
        "GOLD_BUCKET",
        "SILVER_DB",
        "SILVER_TABLE",
        "GOLD_DB",
        "GOLD_TABLE"
    ]
)

JOB_NAME      = args["JOB_NAME"]
ACCOUNT_ID    = args["ACCOUNT_ID"]
SILVER_BUCKET = args["SILVER_BUCKET"]
GOLD_BUCKET   = args["GOLD_BUCKET"]
SILVER_DB     = args["SILVER_DB"]
SILVER_TABLE  = args["SILVER_TABLE"]
GOLD_DB       = args["GOLD_DB"]
GOLD_TABLE    = args["GOLD_TABLE"]

SILVER_FQN = f"silver.{SILVER_DB}.{SILVER_TABLE}"
GOLD_FQN   = f"gold.{GOLD_DB}.{GOLD_TABLE}"

# print(f"\nReading from: {SILVER_FQN}")
# print(f"Writing to  : {GOLD_FQN}")

# ======================================================
# SPARK SESSION (DUAL CATALOG)
# ======================================================

spark = (
    SparkSession.builder.appName(JOB_NAME)
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

    # Silver
    .config("spark.sql.catalog.silver",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.silver.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.silver.glue.id",
            f"{ACCOUNT_ID}:s3tablescatalog/{SILVER_BUCKET}")
    .config("spark.sql.catalog.silver.warehouse",
            f"s3://{SILVER_BUCKET}/bucket/{SILVER_BUCKET}")

    # Gold
    .config("spark.sql.catalog.gold",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.gold.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.gold.glue.id",
            f"{ACCOUNT_ID}:s3tablescatalog/{GOLD_BUCKET}")
    .config("spark.sql.catalog.gold.warehouse",
            f"s3://{GOLD_BUCKET}/bucket/{GOLD_BUCKET}")
    .getOrCreate()
)

# ======================================================
# READ SILVER
# ======================================================
print("\n[STEP 1] Loading Silver Data")

silver_df = spark.read.table(SILVER_FQN)

if silver_df.rdd.isEmpty():
    print("No data found in Silver. Exiting.")
    spark.stop()
    sys.exit(0)

print("Silver Record Count:", silver_df.count())

# ======================================================
# CHECK GOLD TABLE EXISTENCE
# ======================================================
print("\n[STEP 2] Checking Gold Table")

table_exists = True
try:
    spark.read.table(GOLD_FQN)
except AnalysisException:
    table_exists = False

# ======================================================
# INCREMENTAL FILTER
# ======================================================
max_load_date = None

if table_exists:
    max_row = spark.sql(
        f"SELECT MAX(load_date) AS max_ld FROM {GOLD_FQN}"
    ).collect()[0]

    max_load_date = max_row["max_ld"]
    print("Max load_date in Gold:", max_load_date)

    if max_load_date:
        silver_df = silver_df.filter(
            col("contract_date") > max_load_date
        )

# Exit if nothing new
if silver_df.rdd.isEmpty():
    print("No new incremental data found. Exiting.")
    spark.stop()
    sys.exit(0)

# ======================================================
# PREPARE GOLD DATASET
# ======================================================
print("\n[STEP 3] Preparing Gold Dataset")

exclude_cols = ["year", "month", "day"]

gold_df = (
    silver_df
    .drop(*exclude_cols)
    .withColumn("ingestion_date", current_timestamp())
    .withColumn("load_date", current_date())
)

print("Gold Incremental Count:", gold_df.count())

# ======================================================
# CREATE TABLE IF NOT EXISTS
# ======================================================
if not table_exists:
    print("\n[STEP 4] Creating Gold Table")

    schema_ddl = ",\n  ".join(
        [f"{field.name} {field.dataType.simpleString().upper()}"
         for field in gold_df.schema]
    )
    spark.sql(f"""
    CREATE TABLE {GOLD_FQN} (
      {schema_ddl}
    )
    USING iceberg
    PARTITIONED BY (load_date)
    """)

    gold_df.writeTo(GOLD_FQN).overwritePartitions()

else:
    # ======================================================
    # UPSERT (MERGE)
    # ======================================================
    print("\n[STEP 4] Performing MERGE (UPSERT)")

    gold_df.createOrReplaceTempView("source_data")

    spark.sql(f"""
        MERGE INTO {GOLD_FQN} AS target
        USING source_data AS source
        ON target.contract_number = source.contract_number
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print("\n========== JOB COMPLETED SUCCESSFULLY ==========")
print("End Time:", datetime.utcnow())

spark.stop()




# import sys
# from datetime import datetime
# from awsglue.utils import getResolvedOptions
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import current_date
# from pyspark.sql.functions import current_date, current_timestamp


# # ======================================================
# # EXTRACT JOB PARAMETERS (FROM GLUE)
# # ======================================================
# args = getResolvedOptions(
#     sys.argv,
#     [
#         "JOB_NAME",
#         "ACCOUNT_ID",
#         "SILVER_BUCKET",
#         "GOLD_BUCKET",
#         "SILVER_DB",
#         "SILVER_TABLE",
#         "GOLD_DB",
#         "GOLD_TABLE"
#     ]
# )


# JOB_NAME      = args["JOB_NAME"]
# ACCOUNT_ID    = args["ACCOUNT_ID"]
# SILVER_BUCKET = args["SILVER_BUCKET"]
# GOLD_BUCKET   = args["GOLD_BUCKET"]
# SILVER_DB     = args["SILVER_DB"]
# SILVER_TABLE  = args["SILVER_TABLE"]
# GOLD_DB       = args["GOLD_DB"]
# GOLD_TABLE    = args["GOLD_TABLE"]

# SILVER_WAREHOUSE = f"s3://{SILVER_BUCKET}/bucket/{SILVER_BUCKET}"
# GOLD_WAREHOUSE   = f"s3://{GOLD_BUCKET}/bucket/{GOLD_BUCKET}"

# SILVER_FQN = f"silver.{SILVER_DB}.{SILVER_TABLE}"
# GOLD_FQN   = f"gold.{GOLD_DB}.{GOLD_TABLE}"

# # ======================================================
# # SPARK SESSION (DUAL CATALOG)
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

# # ======================================================
# # READ SILVER DIRECTLY (NO SEARCH NEEDED)
# # ======================================================
# print("\n[STEP 2] Reading Silver Table...")

# silver_df = spark.read.table(SILVER_FQN)

# if silver_df.rdd.isEmpty():
#     print("No data in Silver table.")
#     spark.stop()
#     sys.exit(0)

# print("Silver Row Count:", silver_df.count())


# # ======================================================
# # EXCLUDE PARTITION COLUMNS
# # ======================================================
# exclude_cols = ["year", "month", "day"]

# # ======================================================
# # EXTRACT SCHEMA FROM SILVER (EXCLUDING PARTITIONS)
# # ======================================================
# print("\n[STEP 3] Extracting Schema From Silver...")

# gold_columns = []

# for field in silver_df.schema:
#     if field.name.lower() not in exclude_cols:
#         gold_columns.append(
#             f"{field.name} {field.dataType.simpleString().upper()}"
#         )

# # Add ingestion_date and load_date to schema
# gold_columns.append("ingestion_date TIMESTAMP")
# gold_columns.append("load_date DATE")

# schema_ddl = ",\n  ".join(gold_columns)

# print("Generated Gold Schema:\n", schema_ddl)

# # ======================================================
# # TRANSFORM DATA (DROP PARTITION COLS + ADD NEW COLS)
# # ======================================================
# print("\n[STEP 4] Transforming Data...")

# gold_df = (
#     silver_df
#     .drop(*exclude_cols)
#     .withColumn("ingestion_date", current_timestamp())
#     .withColumn("load_date", current_date())
# )


# # ======================================================
# # CREATE GOLD TABLE
# # ======================================================

# print("\n[STEP 5] Creating Gold Table if not exists...")

# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {GOLD_FQN} (
#   {schema_ddl}
# )
# USING iceberg
# PARTITIONED BY (load_date)
# """)

# # ======================================================
# # WRITE TO GOLD
# # ======================================================
# print("\n[STEP 6] Writing to Gold...")

# gold_df.writeTo(GOLD_FQN).overwritePartitions()

# print("Gold Row Count:", gold_df.count())

# print("\n========== JOB COMPLETED SUCCESSFULLY ==========")
# print("End Time:", datetime.utcnow())

# spark.stop()




# # import sys
# # from datetime import datetime
# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import col, current_date

# # print("\n========== SILVER ➜ GOLD JOB START ==========")
# # print("Start Time:", datetime.utcnow())

# # # ======================================================
# # # PARAMETERS
# # # ======================================================

# # JOB_NAME = "sap-contracts-gold"
# # ACCOUNT_ID = "730878889077"

# # SILVER_BUCKET = "epiq-edp-dl-dev-silver"
# # GOLD_BUCKET   = "epiq-edp-dl-dev-gold"

# # SILVER_WAREHOUSE = f"s3://{SILVER_BUCKET}/bucket/{SILVER_BUCKET}"
# # GOLD_WAREHOUSE   = f"s3://{GOLD_BUCKET}/bucket/{GOLD_BUCKET}"

# # TARGET_TABLES = ["contractlineitem", "contractlist"]
# # GOLD_FQN      = "gold.gold_sap_db.contractlist"

# # # ======================================================
# # # SPARK SESSION (DUAL CATALOG)
# # # ======================================================
# # spark = (
# #     SparkSession.builder.appName(JOB_NAME)
# #     .config("spark.sql.extensions",
# #             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

# #     # SILVER
# #     .config("spark.sql.catalog.silver",
# #             "org.apache.iceberg.spark.SparkCatalog")
# #     .config("spark.sql.catalog.silver.catalog-impl",
# #             "org.apache.iceberg.aws.glue.GlueCatalog")
# #     .config("spark.sql.catalog.silver.glue.id",
# #             f"{ACCOUNT_ID}:s3tablescatalog/{SILVER_BUCKET}")
# #     .config("spark.sql.catalog.silver.warehouse",
# #             SILVER_WAREHOUSE)

# #     # GOLD
# #     .config("spark.sql.catalog.gold",
# #             "org.apache.iceberg.spark.SparkCatalog")
# #     .config("spark.sql.catalog.gold.catalog-impl",
# #             "org.apache.iceberg.aws.glue.GlueCatalog")
# #     .config("spark.sql.catalog.gold.glue.id",
# #             f"{ACCOUNT_ID}:s3tablescatalog/{GOLD_BUCKET}")
# #     .config("spark.sql.catalog.gold.warehouse",
# #             GOLD_WAREHOUSE)

# #     .getOrCreate()
# # )

# # print("\n[STEP 1] Spark Ready")

# # # ======================================================
# # # FIND SILVER TABLE
# # # ======================================================
# # print("\n[STEP 2] Searching Silver Bucket...")

# # spark.sql("SHOW NAMESPACES IN silver").show(truncate=False)

# # namespaces = spark.sql("SHOW NAMESPACES IN silver").collect()

# # SILVER_FQN = None

# # for row in namespaces:
# #     ns = row[0]
# #     tables = spark.sql(f"SHOW TABLES IN silver.{ns}").collect()
# #     for t in tables:
# #         if t.tableName in TARGET_TABLES:
# #             SILVER_FQN = f"silver.{ns}.{t.tableName}"
# #             break
# #     if SILVER_FQN:
# #         break

# # if not SILVER_FQN:
# #     print("❌ No matching Silver table found.")
# #     print("Please verify table name in Silver bucket.")
# #     spark.stop()
# #     sys.exit(0)

# # print("✅ Found Silver Table:", SILVER_FQN)

# # # ======================================================
# # # READ SILVER
# # # ======================================================
# # silver_df = spark.read.table(SILVER_FQN)

# # if silver_df.rdd.isEmpty():
# #     print("No data in Silver table.")
# #     spark.stop()
# #     sys.exit(0)

# # print("Silver Row Count:", silver_df.count())

# # # ======================================================
# # # TRANSFORM
# # # ======================================================
# # print("\n[STEP 3] Transforming Data...")

# # gold_df = (
# #     silver_df
# #     .select(
# #         col("contract_number"),
# #         col("sales_organization"),
# #         col("created_date"),
# #         col("changed_date"),
# #         col("contract_date")
# #     )
# #     .withColumn("load_date", current_date())
# # )

# # # ======================================================
# # # CREATE GOLD TABLE
# # # ======================================================
# # print("\n[STEP 4] Creating Gold Table if not exists...")

# # spark.sql(f"""
# # CREATE TABLE IF NOT EXISTS {GOLD_FQN} (
# #   contract_number STRING,
# #   sales_organization STRING,
# #   created_date DATE,
# #   changed_date DATE,
# #   contract_date DATE,
# #   load_date DATE
# # )
# # USING iceberg
# # PARTITIONED BY (load_date)
# # """)

# # # ======================================================
# # # WRITE TO GOLD
# # # ======================================================
# # print("\n[STEP 5] Writing to Gold...")

# # gold_df.writeTo(GOLD_FQN).overwritePartitions()

# # print("Gold Row Count:", gold_df.count())

# # print("\n========== JOB COMPLETED SUCCESSFULLY ==========")
# # print("End Time:", datetime.utcnow())

# # spark.stop()