# ===================== IMPORTS =====================
import sys
import re
import logging
import boto3
from botocore.exceptions import ClientError

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, lit, current_timestamp, to_date, explode, lower
from pyspark.sql.types import DecimalType, StringType, IntegerType, DoubleType

# ===================== LOGGING =====================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===================== AWS CLIENT =====================
lf_client = boto3.client("lakeformation")

# ===================== ARGUMENTS =====================
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME","ACCOUNT_ID","AIRFLOW_RUN_ID",
        "BRONZE_BUCKET_NAME","CONFIG_BUCKET","ETL_RUN_DATE",
        "EXECUTION_ID","SILVER_DATABASE","SOURCE",
        "TABLE_BUCKET_NAME","SILVER_TABLE"
    ],
)

JOB_NAME = args["JOB_NAME"]
ACCOUNT_ID = args["ACCOUNT_ID"]
AIRFLOW_RUN_ID = args["AIRFLOW_RUN_ID"]
BRONZE_BUCKET = args["BRONZE_BUCKET_NAME"]
CONFIG_BUCKET = args["CONFIG_BUCKET"]
ETL_RUN_DATE = args["ETL_RUN_DATE"]
EXECUTION_ID = args["EXECUTION_ID"]
DB = args["SILVER_DATABASE"]
SOURCE = args["SOURCE"]
TABLE_BUCKET_NAME = args["TABLE_BUCKET_NAME"]
SILVER_TABLE = args["SILVER_TABLE"]

S3_TABLES_CATALOG_ID = f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME.split('/')[-1]}"

# ===================== SPARK =====================
spark = (
    SparkSession.builder.appName(JOB_NAME)
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog","s3tables")
    .config("spark.sql.catalog.s3tables","org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.s3tables.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.s3tables.glue.id",f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}")
    .config("spark.sql.catalog.s3tables.warehouse",TABLE_BUCKET_NAME)
    .config("spark.sql.session.timeZone","UTC")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(JOB_NAME, args)

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tables.{DB}")

# ===================== TABLE TAG MAP =====================
TABLE_TAG_MAP = {
    "contract_list": {
        "DataClassification": "internal",
        "data_owner": "finance_team",
        "domain": "finance",
        "data_layer": "silver",
        "access_level": "analyst_visible",
        "environment": "dev",
        "source_system": "sap",
        "data_quality": "validated",
        "PII": "false"
    },
    "contract_header": {
        "DataClassification": "internal",
        "data_owner": "finance_team",
        "domain": "finance",
        "data_layer": "silver",
        "access_level": "engineer_only",
        "environment": "dev",
        "source_system": "sap",
        "data_quality": "validated",
        "PII": "false"
    },
    "contract_items": {
        "DataClassification": "internal",
        "data_owner": "finance_team",
        "domain": "finance",
        "data_layer": "silver",
        "access_level": "engineer_only",
        "environment": "dev",
        "source_system": "sap",
        "data_quality": "validated",
        "PII": "false"
    }
}

# ===================== TAGGING =====================
def apply_table_tags(database, table):
    if table not in TABLE_TAG_MAP:
        return

    tags = [{"TagKey": k, "TagValues": [v]} for k,v in TABLE_TAG_MAP[table].items()]

    try:
        lf_client.add_lf_tags_to_resource(
            Resource={"Table":{
                "CatalogId": S3_TABLES_CATALOG_ID,
                "DatabaseName": database,
                "Name": table
            }},
            LFTags=tags
        )
        logger.info(f"Table tagged: {table}")
    except ClientError as e:
        logger.warning(f"Table tag failed: {e}")

def get_existing_column_tags(database, table, column):
    try:
        resp = lf_client.get_resource_lf_tags(
            Resource={"TableWithColumns":{
                "CatalogId":S3_TABLES_CATALOG_ID,
                "DatabaseName":database,
                "Name":table,
                "ColumnNames":[column]
            }}
        )
        existing={}
        for e in resp.get("LFTagOnColumns",[]):
            for t in e.get("LFTags",[]):
                existing[t["TagKey"]] = t["TagValues"]
        return existing
    except:
        return {}

def apply_column_tags_full(database, table, mapping_rows):

    for row in mapping_rows:

        column = row["target_field"]
        r = row.asDict()

        tags = {
            "PII": str(r.get("pii","false")),
            "access_level": str(r.get("access_level","analyst_visible")),
            "data_quality": str(r.get("data_quality","validated")),
            "source_system": SOURCE,
            "data_layer": "silver",
            "environment": "dev"
        }

        if r.get("data_classification"):
            tags["DataClassification"] = r["data_classification"]
        if r.get("data_owner"):
            tags["data_owner"] = r["data_owner"]
        if r.get("domain"):
            tags["domain"] = r["domain"]

        existing = get_existing_column_tags(database, table, column)

        missing = {
            k:v for k,v in tags.items()
            if k not in existing or v not in existing.get(k, [])
        }

        if not missing:
            continue

        lf_tags=[{"TagKey":k,"TagValues":[v]} for k,v in missing.items()]

        try:
            lf_client.add_lf_tags_to_resource(
                Resource={"TableWithColumns":{
                    "CatalogId":S3_TABLES_CATALOG_ID,
                    "DatabaseName":database,
                    "Name":table,
                    "ColumnNames":[column]
                }},
                LFTags=lf_tags
            )
            logger.info(f"Tagged column {column}: {missing}")

        except ClientError as e:
            logger.warning(f"Column tag failed: {e}")

def run_lf_tagging(database, table, mapping_rows):
    apply_table_tags(database, table)
    apply_column_tags_full(database, table, mapping_rows)

# ===================== READ MAPPING =====================
def read_mapping(base_object):

    path = f"s3://{CONFIG_BUCKET}/{SOURCE}/mappings/contracts_mapping_updated.csv"

    df = spark.read.option("header","true").csv(path)

    df = df.filter(
        (col("source_system")==SOURCE) &
        (col("source_object")==base_object) &
        (lower(col("is_active"))=="true") &
        (lower(col("is_silver"))=="true")
    )

    rows = df.collect()

    if not rows:
        raise Exception(f"Mapping not found for {base_object}")

    return rows

# ===================== CREATE TABLE =====================
def create_ddl(obj, mapping):

    cols=[]

    for r in mapping:
        t=r["target_field"]
        d=r["target_datatype"].lower()

        if d.startswith("decimal"):
            n=re.findall(r"\d+",d)
            cols.append(f"{t} decimal({n[0]},{n[1]})")
        elif d=="date":
            cols.append(f"{t} date")
        elif d=="int":
            cols.append(f"{t} int")
        elif d=="double":
            cols.append(f"{t} double")
        else:
            cols.append(f"{t} string")

    cols+=["execution_id string","airflow_run_id string","etl_run_date string","source_system string","ingestion_ts timestamp"]

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS s3tables.{DB}.{obj}
    ({','.join(cols)})
    USING iceberg
    PARTITIONED BY (etl_run_date)
    """)

# ===================== READ BRONZE =====================
def read_bronze(folder):
    path = f"s3://{BRONZE_BUCKET}/{SOURCE}/contracts/{folder}"
    df = spark.read.option("recursiveFileLookup","true").json(path)
    return df, df.count()

# ===================== TRANSFORM =====================
def transform_data(df, mapping, obj):

    if obj=="contract_items":
        df=df.select(col("ContractNumber"),explode(col("_ContractItems")).alias("item")).select(col("ContractNumber"),col("item.*"))

    expr=[]

    for r in mapping:

        source=r["source_field"]
        target=r["target_field"]
        dtype=r["target_datatype"].lower()

        if dtype.startswith("decimal"):
            n=re.findall(r"\d+",dtype)
            expr.append(col(source).cast(DecimalType(int(n[0]),int(n[1]))).alias(target))
        elif dtype=="date":
            expr.append(to_date(col(source)).alias(target))
        elif dtype=="int":
            expr.append(col(source).cast(IntegerType()).alias(target))
        elif dtype=="double":
            expr.append(col(source).cast(DoubleType()).alias(target))
        else:
            expr.append(col(source).cast(StringType()).alias(target))

    df=df.select(*expr)

    df=df.withColumn("execution_id",lit(EXECUTION_ID))\
         .withColumn("airflow_run_id",lit(AIRFLOW_RUN_ID))\
         .withColumn("etl_run_date",lit(ETL_RUN_DATE))\
         .withColumn("source_system",lit(SOURCE))\
         .withColumn("ingestion_ts",current_timestamp())

    return df

# ===================== PROCESS =====================
def process_object(obj, df):

    mapping=read_mapping(obj)
    table=f"s3tables.{DB}.{obj}"

    if not spark.catalog.tableExists(table):
        create_ddl(obj, mapping)

    final=transform_data(df,mapping,obj)
    final.writeTo(table).overwritePartitions()

    run_lf_tagging(DB,obj,mapping)

# ===================== MAIN =====================
try:

    tables=[t.strip().lower() for t in SILVER_TABLE.split(",")]

    for t in tables:

        logger.info(f"Processing {t}")

        if t=="contract_list":
            df,_=read_bronze("contractlists")
        else:
            df,_=read_bronze("contractdetails")

        process_object(t,df)

    job.commit()
    logger.info("SUCCESS")

except Exception as e:
    logger.error(str(e))
    raise


# import sys
# import re
# import logging
# import boto3
# from botocore.exceptions import ClientError

# from awsglue.utils import getResolvedOptions
# from awsglue.context import GlueContext
# from awsglue.job import Job

# from pyspark.sql import SparkSession
# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number
# from pyspark.sql.functions import col, lit, current_timestamp, to_date, explode
# from pyspark.sql.types import DecimalType, StringType, IntegerType, DoubleType

# # ============================================================
# # Logging
# # ============================================================

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # ============================================================
# # AWS Clients
# # ============================================================

# lf_client = boto3.client("lakeformation")

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

# JOB_NAME          = args["JOB_NAME"]
# ACCOUNT_ID        = args["ACCOUNT_ID"]
# AIRFLOW_RUN_ID    = args["AIRFLOW_RUN_ID"]
# BRONZE_BUCKET     = args["BRONZE_BUCKET_NAME"]
# CONFIG_BUCKET     = args["CONFIG_BUCKET"]
# ETL_RUN_DATE      = args["ETL_RUN_DATE"]
# EXECUTION_ID      = args["EXECUTION_ID"]
# DB                = args["SILVER_DATABASE"]
# SOURCE            = args["SOURCE"]
# TABLE_BUCKET_NAME = args["TABLE_BUCKET_NAME"]
# SILVER_TABLE      = args["SILVER_TABLE"]

# # ── Catalog ID for Lake Formation tagging ───────────────────
# # stage_db lives in the S3 Tables catalog confirmed via:
# #   aws glue get-database --name stage_db
# #     --catalog-id "730878889077:s3tablescatalog/test"
# #   → CatalogId = 730878889077:s3tablescatalog/test
# #   → FederatedDatabase → aws:s3tables
# # TABLE_BUCKET_NAME arg contains the full bucket name
# # Extract just the bucket short name for the catalog ID
# S3_TABLES_CATALOG_ID = f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME.split('/')[-1]}"

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
#     .config("spark.sql.session.timeZone", "UTC")
#     .getOrCreate()
# )

# glueContext = GlueContext(spark.sparkContext)
# job = Job(glueContext)
# job.init(JOB_NAME, args)

# spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tables.{DB}")


# # ============================================================
# # ── LF TAG DEFINITIONS ──────────────────────────────────────
# # Database → which team owns it
# # Used for database-level TeamOwner tag
# DATABASE_TEAM_MAP = {
#     "hr_db":          "hr",
#     "finance_db":     "finance",
#     "engineering_db": "engineering",
#     "common_db":      "shared",
# }

# # Table-level tags
# # These are applied to the WHOLE TABLE
# # Real example: fact_payroll table itself is tagged as restricted + high + hr
# TABLE_TAG_MAP = {
#     TABLE_TAG_MAP = {
#     "contract_list": {
#         "DataClassification": "confidential",
#         "data_owner": "finance_team",
#         "domain": "finance",
#         "data_layer": "silver",
#         "access_level": "analyst_visible",
#         "environment": "dev",
#         "source_system": "sap",
#         "data_quality": "validated",
#         "PII": "false"
#     },
#     "contract_header": {
#         "DataClassification": "confidential",
#         "data_owner": "finance_team",
#         "domain": "finance",
#         "data_layer": "silver",
#         "access_level": "engineer_only",
#         "environment": "dev",
#         "source_system": "sap",
#         "data_quality": "validated",
#         "PII": "false"
#     },
#     "contract_items": {
#         "DataClassification": "confidential",
#         "data_owner": "finance_team",
#         "domain": "finance",
#         "data_layer": "silver",
#         "access_level": "engineer_only",
#         "environment": "dev",
#         "source_system": "sap",
#         "data_quality": "validated",
#         "PII": "false"
#     }
# }
# }


# # ============================================================
# # ── STEP 1: APPLY DATABASE LEVEL TAGS ───────────────────────
# # Tags the whole database with TeamOwner and DataClassification
# # Every table in this database inherits these tags automatically
# #
# # Real World:
# #   Tagging the finance_db folder itself as "owned by finance team"
# #   So anyone looking at the folder knows it belongs to Finance.
# #   All tables inside it automatically inherit this ownership.
# # ============================================================

# def apply_database_tags(database):

#     logger.info(f"STEP 1 — Applying database-level tags to: {database}")

#     # Determine TeamOwner from the database name
#     team_owner = DATABASE_TEAM_MAP.get(database, "shared")

#     db_tags = [
#         {"TagKey": "TeamOwner",          "TagValues": [team_owner]},
#         {"TagKey": "DataClassification", "TagValues": ["internal"]},
#     ]

#     try:
#         lf_client.add_lf_tags_to_resource(
#             Resource={
#                 "Database": {
#                     "CatalogId": S3_TABLES_CATALOG_ID,
#                     "Name": database
#                 }
#             },
#             LFTags=db_tags
#         )
#         logger.info(f"  ✅ {database} → TeamOwner = {team_owner}")
#         logger.info(f"  ✅ {database} → DataClassification = internal")

#     except ClientError as e:
#         if "AccessDeniedException" in str(e):
#             logger.warning(f"  ⚠️  Skipping database tags for {database} — insufficient permissions")
#             logger.warning(f"  ℹ️  Apply database tags manually in Lake Formation console")
#         elif "EntityNotFoundException" in str(e):
#             logger.warning(f"  ⚠️  Database {database} not found in catalog — skipping DB tags")
#         else:
#             logger.warning(f"  ⚠️  Database tag failed for {database}: {e}")


# # ============================================================
# # ── STEP 3: APPLY TABLE LEVEL TAGS ──────────────────────────
# # Tags the whole table with DataClassification + Sensitivity + TeamOwner
# # Overrides the database default for this specific table
# #
# # Real World:
# #   The finance_db database default is "internal"
# #   But contract_header table overrides it to "confidential"
# #   Because contract data is more sensitive than generic finance data
# # ============================================================

# def apply_table_tags(database, table):

#     logger.info(f"STEP 3 — Applying table-level tags to: {database}.{table}")

#     if table not in TABLE_TAG_MAP:
#         logger.info(f"  ℹ️  No table-level tags defined for {table} — using database defaults")
#         return

#     tags_to_apply = TABLE_TAG_MAP[table]

#     lf_tags = [
#         {"TagKey": key, "TagValues": [value]}
#         for key, value in tags_to_apply.items()
#     ]

#     try:
#         lf_client.add_lf_tags_to_resource(
#             Resource={
#                 "Table": {
#                     "CatalogId":    S3_TABLES_CATALOG_ID,
#                     "DatabaseName": database,
#                     "Name":         table
#                 }
#             },
#             LFTags=lf_tags
#         )
#         for key, value in tags_to_apply.items():
#             logger.info(f"  ✅ {table} → {key} = {value}")

#     except ClientError as e:
#         if "EntityNotFoundException" in str(e):
#             logger.warning(f"Table {table} not found in catalog — skipping table tags")
#         else:
#             logger.warning(f"Table tag failed for {table}: {e}")


# # ============================================================
# # ── STEP 4 HELPER: GET EXISTING COLUMN TAGS ─────────────────
# # Checks what tags are already on a column
# # Used to avoid re-applying tags (idempotent check)
# # ============================================================

# def get_existing_column_tags(database, table, column):

#     try:
#         resp = lf_client.get_resource_lf_tags(
#             Resource={
#                 "TableWithColumns": {
#                     "CatalogId":    S3_TABLES_CATALOG_ID,
#                     "DatabaseName": database,
#                     "Name":         table,
#                     "ColumnNames":  [column]
#                 }
#             }
#         )
#         existing = {}
#         for tag_entry in resp.get("LFTagOnColumns", []):
#             for tag in tag_entry.get("LFTags", []):
#                 existing[tag["TagKey"]] = tag.get("TagValues", [])
#         return existing

#     except ClientError:
#         return {}


# # ============================================================
# # ── STEP 4: APPLY COLUMN LEVEL TAGS ─────────────────────────
# # Tags each individual column using values from mapping CSV
# # This is the most granular level — column by column
# #
# # Reads these columns from your mapping CSV:
# #   data_classification → DataClassification tag value
# #   sensitivity         → Sensitivity tag value
# #   pii                 → PII tag value
# #   team_owner          → TeamOwner tag value (optional, inherits from table if missing)
# #
# # Real World:
# #   contract_header.total_value  → DataClassification=restricted, Sensitivity=high, PII=false
# #   contract_header.contract_number → DataClassification=internal, Sensitivity=low, PII=false
# #   contract_header.vendor_name  → DataClassification=confidential, Sensitivity=medium, PII=false
# # ============================================================

# def apply_column_tags_full(database, table, mapping_rows):

#     logger.info(f"STEP 4 — Applying column-level tags to: {database}.{table}")

#     for row in mapping_rows:

#         column = row["target_field"].strip()
#         row_dict = row.asDict()

#         tags_to_apply = {}

#         # ✅ DataClassification
#         classification = row_dict.get("data_classification")
#         if classification:
#             tags_to_apply["DataClassification"] = str(classification).strip()

#         # ✅ PII
#         tags_to_apply["PII"] = str(row_dict.get("pii", "false"))

#         # ✅ access_level
#         tags_to_apply["access_level"] = str(row_dict.get("access_level", "analyst_visible"))

#         # ✅ data_owner
#         if row_dict.get("data_owner"):
#             tags_to_apply["data_owner"] = str(row_dict.get("data_owner"))

#         # ✅ domain
#         if row_dict.get("domain"):
#             tags_to_apply["domain"] = str(row_dict.get("domain"))

#         # ✅ data_quality
#         tags_to_apply["data_quality"] = str(row_dict.get("data_quality", "validated"))

#         # ✅ static tags
#         tags_to_apply["source_system"] = SOURCE
#         tags_to_apply["data_layer"] = "silver"
#         tags_to_apply["environment"] = "dev"

#         logger.info(f"Applying tags to {column}: {tags_to_apply}")

#         existing = get_existing_column_tags(database, table, column)

#         # ✅ FIXED LOGIC (IMPORTANT)
#         missing_tags = {
#             k: v for k, v in tags_to_apply.items()
#             if k not in existing or v not in existing.get(k, [])
#         }

#         if not missing_tags:
#             logger.info(f"Skipping {column} — already tagged")
#             continue

#         lf_tags = [
#             {"TagKey": k, "TagValues": [v]}
#             for k, v in missing_tags.items()
#         ]

#         try:
#             response = lf_client.add_lf_tags_to_resource(
#                 Resource={
#                     "TableWithColumns": {
#                         "CatalogId": S3_TABLES_CATALOG_ID,
#                         "DatabaseName": database,
#                         "Name": table,
#                         "ColumnNames": [column]
#                     }
#                 },
#                 LFTags=lf_tags
#             )

#             logger.info(f"SUCCESS tagging {column}: {missing_tags}")

#         except ClientError as e:
#             logger.warning(f"FAILED tagging {column}: {e}")


# # ============================================================
# # ── MASTER TAG FUNCTION ──────────────────────────────────────
# # Runs all 4 steps in order for a given database + table
# # Call this once per table from process_object()
# # ============================================================

# def run_lf_tagging(database, table, mapping_rows):

#     logger.info("")
#     logger.info("=" * 60)
#     logger.info(f"  LF TAGGING: {database}.{table}")
#     logger.info("=" * 60)

#     # Step 1 — Database level
#     apply_database_tags(database)

#     # Step 2 — Table level
#     apply_table_tags(database, table)

#     # Step 3 — Column level
#     apply_column_tags_full(database, table, mapping_rows)

#     logger.info(f"  ✅ Tagging complete: {database}.{table}")
#     logger.info("")


# # ============================================================
# # READ MAPPING
# # ============================================================

# def read_mapping(base_object):

#     mapping_path = f"s3://{CONFIG_BUCKET}/{SOURCE}/mappings/contracts_mapping_updated.csv"

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

#     path = f"s3://{BRONZE_BUCKET}/{SOURCE}/contracts/{folder}"
#     df   = spark.read.option("recursiveFileLookup", "true").json(path)
#     rows_read = df.count()

#     return df, rows_read


# # ============================================================
# # TRANSFORM
# # ============================================================

# def transform_data(bronze_df, mapping_rows, base_object):

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
#                 col(source).cast(DecimalType(int(nums[0]), int(nums[1]))).alias(target)
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

#     window = Window.partitionBy(*pk_cols).orderBy(col("ingestion_ts").desc())

#     df = (
#         df.withColumn("rn", row_number().over(window))
#           .filter(col("rn") == 1)
#           .drop("rn")
#     )

#     return df


# # ============================================================
# # PROCESS TABLE
# # ─────────────────────────────────────────────────────────────
# # WHAT CHANGED FROM ORIGINAL:
# #   OLD → apply_column_tags(DB, base_object, mapping_rows)
# #         Only DataClassification, only column level, only on new tables
# #
# #   NEW → run_lf_tagging(DB, base_object, mapping_rows)
# #         All 4 tags, all 3 levels, every run (idempotent)
# # ============================================================

# def process_object(base_object, bronze_df, rows_read):

#     table_name   = f"s3tables.{DB}.{base_object}"
#     mapping_rows = read_mapping(base_object)
#     table_exists = spark.catalog.tableExists(table_name)

#     if not table_exists:
#         logger.info(f"Creating table: {base_object}")
#         create_ddl(base_object, mapping_rows)

#     # ── Transform and write data first ───────────────────────
#     # Tagging runs AFTER writeTo so the table is fully registered
#     # in the Glue catalog before Lake Formation tries to tag it
#     final_df     = transform_data(bronze_df, mapping_rows, base_object)
#     rows_written = final_df.count()

#     final_df.writeTo(table_name).overwritePartitions()

#     # ── Run full LF tagging AFTER write: DB + Table + Column ─
#     # Table now exists in Glue catalog — tagging will succeed
#     # Idempotent — skips columns that are already tagged
#     run_lf_tagging(DB, base_object, mapping_rows)

#     return (base_object, rows_read, rows_written)


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

#         process_object(table, bronze_df, rows_read)

#     job.commit()
#     logger.info("Silver job completed successfully")

# except Exception as e:
#     logger.error(f"Job failed: {str(e)}")
#     raiseta