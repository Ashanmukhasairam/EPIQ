import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, when,
    current_timestamp, lit,
    input_file_name, regexp_extract
)

# ======================================================
# 1. JOB PARAMETERS
# ======================================================
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ACCOUNT_ID",
        "TABLE_BUCKET_NAME",
        "SILVER_DB",
        "SILVER_TABLE",
        "SOURCE_SYSTEM",
        "SOURCE",
        "ENTITY",
        "BRONZE_BUCKET"
    ]
)

JOB_NAME = args["JOB_NAME"]
ACCOUNT_ID = args["ACCOUNT_ID"]
TABLE_BUCKET_NAME = args["TABLE_BUCKET_NAME"]
SILVER_DB = args["SILVER_DB"]
SILVER_TABLE = args["SILVER_TABLE"]
SOURCE_SYSTEM = args["SOURCE_SYSTEM"]
SOURCE = args["SOURCE"]
ENTITY = args["ENTITY"]
BRONZE_BUCKET = args["BRONZE_BUCKET"]

ICEBERG_WAREHOUSE = f"s3://{TABLE_BUCKET_NAME}/bucket/{TABLE_BUCKET_NAME}"
SILVER_FQN = f"s3tables.{SILVER_DB}.{SILVER_TABLE}"
BRONZE_PATH = f"s3://{BRONZE_BUCKET}/{SOURCE.lower()}/{ENTITY}/"

print("\n========== SILVER JOB START ==========")
print("Start Time:", datetime.utcnow())

# ======================================================
# 2. SPARK SESSION (ICEBERG + GLUE CATALOG)
# ======================================================

spark = (
    SparkSession.builder.appName(JOB_NAME)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog", "s3tables")
    .config("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.s3tables.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.s3tables.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}")
    .config("spark.sql.catalog.s3tables.warehouse", ICEBERG_WAREHOUSE)
    .getOrCreate()
)

print("[STEP 1] Spark Session Ready")

# ======================================================
# 3. READ BRONZE DATA
# ======================================================
print("[STEP 2] Reading Bronze JSON...")

bronze_df = (
    spark.read
    .format("json")
    .option("recursiveFileLookup", "true")
    .load(BRONZE_PATH)
)

if bronze_df.rdd.isEmpty():
    print("No Bronze data found. Exiting job.")
    sys.exit(0)

bronze_df = bronze_df.cache()
print("Bronze Count:", bronze_df.count())

# ======================================================
# 4. EXTRACT PARTITION COLUMNS
# ======================================================
bronze_df = (
    bronze_df
    .withColumn("file_path", input_file_name())
    .withColumn("year",  regexp_extract(col("file_path"), r'/(\d{4})/', 1).cast("int"))
    .withColumn("month", regexp_extract(col("file_path"), r'/\d{4}/(\d{2})/', 1).cast("int"))
    .withColumn("day",   regexp_extract(col("file_path"), r'/\d{4}/\d{2}/(\d{2})/', 1).cast("int"))
)

# ======================================================
# 5. TRANSFORMATION (BRONZE → SILVER)
# ======================================================
print("[STEP 3] Transforming Bronze → Silver...")

silver_df = (
    bronze_df
    .withColumn("contract_number", col("ContractNumber").cast("string"))
    .withColumn("sales_organization", col("SalesOrganization").cast("string"))
    .withColumn("created_date", to_date(col("CreatedDate"), "yyyy-MM-dd"))
    .withColumn(
        "changed_date",
        when(col("ChangedDate") == "", None)
        .otherwise(to_date(col("ChangedDate"), "yyyy-MM-dd"))
    )
    .withColumn("contract_date", to_date(col("ContractDate"), "yyyy-MM-dd"))
    .withColumn("attribute", col("attribute").cast("string"))
    .withColumn("source_system", lit(SOURCE_SYSTEM))
    .withColumn("effective_start_ts", current_timestamp())
    .withColumn("effective_end_ts", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
    .drop("file_path")
)

silver_df = silver_df.dropDuplicates()

print("Silver Count:", silver_df.count())

# ======================================================
# 6. DYNAMIC SCHEMA EXTRACTION
# ======================================================
print("[STEP 4] Generating Dynamic Schema...")

# Convert Spark schema to Iceberg DDL format
schema_ddl = ",\n  ".join(
    [f"{field.name} {field.dataType.simpleString().upper()}"
     for field in silver_df.schema.fields]
)

print("Generated Schema DDL:")
print(schema_ddl)

# ======================================================
# 7. CREATE TABLE (DYNAMIC)
# ======================================================
print("[STEP 5] Creating Silver Table (if not exists)...")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_FQN} (
  {schema_ddl}
)
USING iceberg
PARTITIONED BY (year, month, day)
""")

print("[STEP 5 COMPLETE] Table Ready")

# ======================================================
# 8. WRITE TO ICEBERG
# ======================================================
print("[STEP 6] Writing to Silver Iceberg...")

(
    silver_df.writeTo(SILVER_FQN)
    .overwritePartitions()
)

print("[STEP 6 COMPLETE] Data Written Successfully")

print("\n========== SILVER JOB END ==========")
print("End Time:", datetime.utcnow())

spark.stop()





# import sys
# from datetime import datetime

# from awsglue.utils import getResolvedOptions
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, to_date, when,
#     current_timestamp, lit,
#     input_file_name, regexp_extract
# )

# # ======================================================
# # 1. JOB PARAMETERS
# # ======================================================
# args = getResolvedOptions(
#     sys.argv,
#     [
#         "JOB_NAME",
#         "ACCOUNT_ID",
#         "TABLE_BUCKET_NAME",
#         "SILVER_DB",
#         "SILVER_TABLE",
#         "SOURCE_SYSTEM",
#         "SOURCE",
#         "ENTITY",
#         "BRONZE_BUCKET"
#     ]
# )

# JOB_NAME = args["JOB_NAME"]
# ACCOUNT_ID = args["ACCOUNT_ID"]
# TABLE_BUCKET_NAME = args["TABLE_BUCKET_NAME"]
# SILVER_DB = args["SILVER_DB"]
# SILVER_TABLE = args["SILVER_TABLE"]
# SOURCE_SYSTEM = args["SOURCE_SYSTEM"]
# SOURCE = args["SOURCE"]
# ENTITY = args["ENTITY"]
# BRONZE_BUCKET = args["BRONZE_BUCKET"]

# ICEBERG_WAREHOUSE = f"s3://{TABLE_BUCKET_NAME}/bucket/{TABLE_BUCKET_NAME}"
# SILVER_FQN = f"s3tables.{SILVER_DB}.{SILVER_TABLE}"
# BRONZE_PATH = f"s3://{BRONZE_BUCKET}/{SOURCE.lower()}/{ENTITY}/"

# print("\n========== SILVER JOB START ==========")
# print("Start Time:", datetime.utcnow())
# print("\n[JOB PARAMETERS]")
# for k, v in args.items():
#     print(f"{k}: {v}")

# # ======================================================
# # 2. SPARK SESSION (ICEBERG + GLUE CATALOG)
# # ======================================================
# spark = (
#     SparkSession.builder.appName(JOB_NAME)
#     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
#     .config("spark.sql.defaultCatalog", "s3tables")
#     .config("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.s3tables.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
#     .config("spark.sql.catalog.s3tables.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}")
#     .config("spark.sql.catalog.s3tables.warehouse", ICEBERG_WAREHOUSE)
#     .getOrCreate()
# )

# print("\n[STEP 1] Spark Session Ready")

# # ======================================================
# # 3. READ BRONZE DATA
# # ======================================================
# print("\n[STEP 2] Reading Bronze JSON...")

# bronze_df = (
#     spark.read
#     .format("json")
#     .option("recursiveFileLookup", "true")
#     .load(BRONZE_PATH)
# )

# if bronze_df.rdd.isEmpty():
#     print("No Bronze data found. Exiting job.")
#     sys.exit(0)

# bronze_df = bronze_df.cache()

# print("Bronze Count:", bronze_df.count())
# bronze_df.printSchema()

# # ======================================================
# # 4. EXTRACT PARTITION COLUMNS FROM FILE PATH
# # ======================================================
# print("\n[STEP 3] Extracting Partition Columns...")

# bronze_df = (
#     bronze_df
#     .withColumn("file_path", input_file_name())
#     .withColumn("year",  regexp_extract(col("file_path"), r'/(\d{4})/', 1).cast("int"))
#     .withColumn("month", regexp_extract(col("file_path"), r'/\d{4}/(\d{2})/', 1).cast("int"))
#     .withColumn("day",   regexp_extract(col("file_path"), r'/\d{4}/\d{2}/(\d{2})/', 1).cast("int"))
# )

# # ======================================================
# # 5. TRANSFORMATION (BRONZE → SILVER)
# # ======================================================
# print("\n[STEP 4] Transforming Bronze → Silver...")

# silver_df = (
#     bronze_df
#     .withColumn("contract_number", col("ContractNumber").cast("string"))
#     .withColumn("sales_organization", col("SalesOrganization").cast("string"))
#     .withColumn("created_date", to_date(col("CreatedDate"), "yyyy-MM-dd"))
#     .withColumn(
#         "changed_date",
#         when(col("ChangedDate") == "", None)
#         .otherwise(to_date(col("ChangedDate"), "yyyy-MM-dd"))
#     )
#     .withColumn("contract_date", to_date(col("ContractDate"), "yyyy-MM-dd"))
#     .withColumn("attribute", col("attribute").cast("string"))
#     .withColumn("source_system", lit(SOURCE_SYSTEM))
#     .withColumn("effective_start_ts", current_timestamp())
#     .withColumn("effective_end_ts", lit(None).cast("timestamp"))
#     .withColumn("is_current", lit(True))
# )

# silver_df = silver_df.select(
#     "contract_number",
#     "sales_organization",
#     "created_date",
#     "changed_date",
#     "contract_date",
#     "attribute",
#     "source_system",
#     "effective_start_ts",
#     "effective_end_ts",
#     "is_current",
#     "year",
#     "month",
#     "day"
# ).dropDuplicates()

# print("Silver Count:", silver_df.count())

# # ======================================================
# # 6. CREATE SILVER TABLE IF NOT EXISTS
# # ======================================================
# print("\n[STEP 5] Creating Silver Table (if not exists)...")

# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {SILVER_FQN} (
#   contract_number STRING,
#   sales_organization STRING,
#   created_date DATE,
#   changed_date DATE,
#   contract_date DATE,
#   attribute STRING,
#   source_system STRING,
#   effective_start_ts TIMESTAMP,
#   effective_end_ts TIMESTAMP,
#   is_current BOOLEAN,
#   year INT,
#   month INT,
#   day INT
# )
# USING iceberg
# PARTITIONED BY (year, month, day)
# """)

# print("[STEP 5 COMPLETE] Table Ready")

# # ======================================================
# # 7. WRITE TO ICEBERG
# # ======================================================
# print("\n[STEP 6] Writing to Silver Iceberg...")

# (
#     silver_df.writeTo(SILVER_FQN)
#     .overwritePartitions()
# )

# print("[STEP 6 COMPLETE] Data Written Successfully")

# # ======================================================
# # 8. JOB END
# # ======================================================
# print("\n========== SILVER JOB END ==========")
# print("End Time:", datetime.utcnow())

# spark.stop()



# # import sys
# # from datetime import datetime

# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import (
# #     col, to_date, when,
# #     current_timestamp, lit,
# #     input_file_name, regexp_extract
# # )

# # # ======================================================
# # # PARAMETERS
# # # ======================================================
# # JOB_NAME = "sap-contractslist-silver"
# # ACCOUNT_ID = "730878889077"
# # TABLE_BUCKET_NAME = "epiq-edp-dl-dev-silver"
# # ICEBERG_WAREHOUSE = f"s3://{TABLE_BUCKET_NAME}/bucket/{TABLE_BUCKET_NAME}"

# # SILVER_DB = "silver_sap_db"
# # SILVER_TABLE = "contractlist"
# # SOURCE_SYSTEM = "sap"

# # # BRONZE_PATH = "s3://epiq-edp-dl-dev-bronze/sap/"


# # SILVER_FQN = f"s3tables.{SILVER_DB}.{SILVER_TABLE}"

# # ENTITY="contracts_list"
# # SOURCE="sap"
# # BRONZE_BUCKET="epiq-edp-dl-dev-bronze"
# # bronze_path = f"s3://{BRONZE_BUCKET}/{SOURCE.lower()}/{ENTITY}/"
# # print("\n[PARAMETERS]")
# # print("Bronze Path:", bronze_path)
# # print("Silver Table:", SILVER_FQN)

# # # ======================================================
# # # SPARK SESSION
# # # ======================================================
# # spark = (
# #     SparkSession.builder.appName(JOB_NAME)
# #     .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
# #     .config("spark.sql.defaultCatalog","s3tables")
# #     .config("spark.sql.catalog.s3tables","org.apache.iceberg.spark.SparkCatalog")
# #     .config("spark.sql.catalog.s3tables.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
# #     .config("spark.sql.catalog.s3tables.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}")
# #     .config("spark.sql.catalog.s3tables.warehouse", ICEBERG_WAREHOUSE)
# #     .getOrCreate()
# # )

# # print("\n[STEP 1] Spark Ready")

# # # ======================================================
# # # READ BRONZE JSON
# # # ======================================================
# # print("\n[STEP 2] Reading Bronze JSON...")


# # bronze_df = spark.read \
# #     .format("json") \
# #     .option("recursiveFileLookup", "true") \
# #     .load(bronze_path)

# # bronze_df = bronze_df.cache()

# # if bronze_df.rdd.isEmpty():
# #     print("No Bronze data found. Exiting...")
# #     sys.exit(0)

# # print("Bronze Schema:")
# # bronze_df.printSchema()

# # # ======================================================
# # # EXTRACT YEAR / MONTH / DAY FROM FILE PATH
# # # ======================================================
# # print("\n[STEP 3] Extracting Partition Columns...")

# # bronze_df = bronze_df.withColumn("file_path", input_file_name()) \
# #     .withColumn("year",  regexp_extract(col("file_path"), r'/(\d{4})/', 1).cast("int")) \
# #     .withColumn("month", regexp_extract(col("file_path"), r'/\d{4}/(\d{2})/', 1).cast("int")) \
# #     .withColumn("day",   regexp_extract(col("file_path"), r'/\d{4}/\d{2}/(\d{2})/', 1).cast("int"))

# # bronze_df.select("file_path","year","month","day").show(5, False)

# # # ======================================================
# # # TRANSFORMATION
# # # ======================================================
# # print("\n[STEP 4] Transforming Bronze → Silver...")

# # silver_df = (
# #     bronze_df
# #     .withColumn("contract_number", col("ContractNumber").cast("string"))
# #     .withColumn("sales_organization", col("SalesOrganization").cast("string"))
# #     .withColumn("created_date", to_date(col("CreatedDate"), "yyyy-MM-dd"))
# #     .withColumn(
# #         "changed_date",
# #         when(col("ChangedDate") == "", None)
# #         .otherwise(to_date(col("ChangedDate"), "yyyy-MM-dd"))
# #     )
# #     .withColumn("contract_date", to_date(col("ContractDate"), "yyyy-MM-dd"))
# #     .withColumn("attribute", col("attribute").cast("string"))
# #     .withColumn("source_system", lit(SOURCE_SYSTEM))
# #     .withColumn("effective_start_ts", current_timestamp())
# #     .withColumn("effective_end_ts", lit(None).cast("timestamp"))
# #     .withColumn("is_current", lit(True))
# # )

# # silver_df = silver_df.select(
# #     "contract_number",
# #     "sales_organization",
# #     "created_date",
# #     "changed_date",
# #     "contract_date",
# #     "attribute",
# #     "source_system",
# #     "effective_start_ts",
# #     "effective_end_ts",
# #     "is_current",
# #     "year",
# #     "month",
# #     "day"
# # )

# # print("Silver Preview:")
# # silver_df.show(5, False)

# # # ======================================================
# # # CREATE SILVER TABLE
# # # ======================================================
# # print("\n[STEP 5] Creating Silver Table if not exists...")

# # spark.sql(f"""
# # CREATE TABLE IF NOT EXISTS {SILVER_FQN} (
# #   contract_number STRING,
# #   sales_organization STRING,
# #   created_date DATE,
# #   changed_date DATE,
# #   contract_date DATE,
# #   attribute STRING,
# #   source_system STRING,
# #   effective_start_ts TIMESTAMP,
# #   effective_end_ts TIMESTAMP,
# #   is_current BOOLEAN,
# #   year INT,
# #   month INT,
# #   day INT
# # )
# # USING iceberg
# # PARTITIONED BY (year, month, day)
# # """)

# # print("[STEP 5 COMPLETE] Table Ready")

# # # ======================================================
# # # WRITE SILVER
# # # ======================================================
# # print("\n[STEP 6] Writing to Silver Iceberg...")

# # silver_df = silver_df.dropDuplicates()
# # silver_df.writeTo(SILVER_FQN).overwritePartitions()

# # print("[STEP 6 COMPLETE] Data Written")

# # # ======================================================
# # # FINAL METRICS
# # # ======================================================
# # print("\nBronze Count:", bronze_df.count())
# # print("Silver Count:", silver_df.count())

# # print("\n========== SILVER JOB END ==========")
# # print("End Time:", datetime.utcnow())

# # spark.stop()




# # # import sys
# # # from datetime import datetime

# # # from pyspark.sql import SparkSession
# # # from pyspark.sql.functions import (
# # #     col, to_date, when,
# # #     current_timestamp, lit
# # # )

# # # # ======================================================
# # # # PARAMETERS – CHANGE THESE
# # # # ======================================================
# # # JOB_NAME = "sap-contractslist-silver"
# # # ACCOUNT_ID = "730878889077"
# # # TABLE_BUCKET_NAME = "epiq-edp-dl-dev-silver"
# # # ICEBERG_WAREHOUSE = f"s3://{TABLE_BUCKET_NAME}/bucket/{TABLE_BUCKET_NAME}"
# # # SILVER_DB = "silver_sap_db"
# # # SILVER_TABLE = "contractlineitem"
# # # SOURCE_SYSTEM = "sap"

# # # BRONZE_PATH = "s3://epiq-edp-dl-dev-bronze/sap/contractlists/"

# # # SILVER_FQN = f"s3tables.{SILVER_DB}.{SILVER_TABLE}"

# # # # ======================================================
# # # # SPARK SESSION – S3 TABLE BUCKET CONFIG
# # # # ======================================================
# # # spark = (
# # #     SparkSession.builder.appName(JOB_NAME)
# # #     .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
# # #     .config("spark.sql.defaultCatalog","s3tables")

# # #     .config("spark.sql.catalog.s3tables","org.apache.iceberg.spark.SparkCatalog")
# # #     .config("spark.sql.catalog.s3tables.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
# # #     .config("spark.sql.catalog.s3tables.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}")
# # #     .config("spark.sql.catalog.s3tables.warehouse", ICEBERG_WAREHOUSE)
# # #     .getOrCreate()
# # # )

# # # # ======================================================
# # # # READ BRONZE JSON (ALL YEARS)
# # # # ======================================================
# # # bronze_df = (
# # #     spark.read
# # #     .option("basePath", BRONZE_PATH)   # IMPORTANT
# # #     .json(BRONZE_PATH)
# # # )

# # # print("Bronze Schema:")
# # # bronze_df.printSchema()

# # # if bronze_df.rdd.isEmpty():
# # #     print("No Bronze data found")
# # #     sys.exit(0)

# # # # ======================================================
# # # # MANUAL TRANSFORMATION
# # # # ======================================================
# # # silver_df = (
# # #     bronze_df
# # #     .withColumn("contract_number", col("ContractNumber").cast("string"))
# # #     .withColumn("sales_organization", col("SalesOrganization").cast("string"))
# # #     .withColumn("created_date", to_date(col("CreatedDate"), "yyyy-MM-dd"))
# # #     .withColumn(
# # #         "changed_date",
# # #         when(col("ChangedDate") == "", None)
# # #         .otherwise(to_date(col("ChangedDate"), "yyyy-MM-dd"))
# # #     )
# # #     .withColumn("contract_date", to_date(col("ContractDate"), "yyyy-MM-dd"))
# # #     .withColumn("attribute", col("attribute").cast("string"))

# # #     # SCD / AUDIT COLUMNS
# # #     .withColumn("source_system", lit(SOURCE_SYSTEM))
# # #     .withColumn("effective_start_ts", current_timestamp())
# # #     .withColumn("effective_end_ts", lit(None).cast("timestamp"))
# # #     .withColumn("is_current", lit(True))
# # # )

# # # # KEEP year/month/day FROM BRONZE FOR PARTITIONS
# # # silver_df = silver_df.select(
# # #     "contract_number",
# # #     "sales_organization",
# # #     "created_date",
# # #     "changed_date",
# # #     "contract_date",
# # #     "attribute",
# # #     "source_system",
# # #     "effective_start_ts",
# # #     "effective_end_ts",
# # #     "is_current",
# # #     "year",
# # #     "month",
# # #     "day"
# # # )

# # # print("Silver Preview:")
# # # silver_df.show(5)

# # # # ======================================================
# # # # CREATE SILVER ICEBERG TABLE
# # # # ======================================================
# # # def create_silver_table():
# # #     spark.sql(f"""
# # #     CREATE TABLE IF NOT EXISTS {SILVER_FQN} (
# # #       contract_number STRING,
# # #       sales_organization STRING,
# # #       created_date DATE,
# # #       changed_date DATE,
# # #       contract_date DATE,
# # #       attribute STRING,
# # #       source_system STRING,
# # #       effective_start_ts TIMESTAMP,
# # #       effective_end_ts TIMESTAMP,
# # #       is_current BOOLEAN,
# # #       year INT,
# # #       month INT,
# # #       day INT
# # #     )
# # #     USING iceberg
# # #     PARTITIONED BY (year, month, day)
# # #     """)
# # # create_silver_table()

# # # # ======================================================
# # # # WRITE TO ICEBERG TABLE BUCKET
# # # # ======================================================
# # # silver_df.writeTo(SILVER_FQN).append()

# # # print("Data successfully written to Silver Iceberg table")

# # # # ======================================================
# # # # OPTIONAL – COUNTS
# # # # ======================================================
# # # print("Bronze Count:", bronze_df.count())
# # # print("Silver Count:", silver_df.count())

# # # spark.stop()
