import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIG – EDIT HERE ONLY
# ============================================================

MIGRATION_TYPE = "New"  # "New" or "Incremental"

# ---------- SOURCE ----------
SRC_DB = "salesforce"
SRC_TBL = "contact"
INCREMENTAL_COLUMN = "_fivetran_synced"

# ---------- DESTINATION ----------
ACCOUNT_ID = "730878889077"
TABLE_BUCKET = "test"
DST_NAMESPACE = "edp_silver_db"
DST_TABLE = "contact"
DST_PARTITIONS = "months(_fivetran_synced)"

REGION = "us-east-1"   # <--- bucket region

# ============================================================
# SPARK CONFIG
# ============================================================
conf = (
    SparkConf()
    .set("spark.jars.packages",
         "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2")
    .set("spark.sql.extensions",
         "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

    # -------- S3 REGION FIX (IMPORTANT) --------
    .set("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
    .set("spark.hadoop.fs.s3.endpoint", f"s3.{REGION}.amazonaws.com")
    .set("spark.hadoop.fs.s3a.path.style.access", "true")

    # -------- DESTINATION → S3 TABLES --------
    .set("spark.sql.defaultCatalog", "s3tables")
    .set("spark.sql.catalog.s3tables",
         "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.s3tables.catalog-impl",
         "org.apache.iceberg.aws.glue.GlueCatalog")
    .set("spark.sql.catalog.s3tables.glue.id",
         f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET}")
    .set("spark.sql.catalog.s3tables.warehouse",
         f"s3://{TABLE_BUCKET}/warehouse/")

    # -------- SOURCE → GLUE --------
    .set("spark.sql.catalog.glue",
         "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.glue.catalog-impl",
         "org.apache.iceberg.aws.glue.GlueCatalog")
    .set("spark.sql.catalog.glue.io-impl",
         "org.apache.iceberg.aws.s3.S3FileIO")
)

spark = SparkSession.builder \
    .appName("IncrementalContactMigration") \
    .config(conf=conf) \
    .getOrCreate()

# ============================================================
# HELPERS
# ============================================================

def source_ref():
    return f"glue.`{SRC_DB}`.`{SRC_TBL}`"


def create_namespace():
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tables.`{DST_NAMESPACE}`")


def create_table_if_not_exists():
    partition_clause = ""
    if DST_PARTITIONS != "NotApplicable":
        partition_clause = f"PARTITIONED BY ({DST_PARTITIONS})"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS
        s3tables.`{DST_NAMESPACE}`.`{DST_TABLE}`
        USING iceberg
        {partition_clause}
        AS SELECT * FROM {source_ref()} LIMIT 0
    """)


def get_last_value():
    try:
        return spark.sql(f"""
            SELECT MAX({INCREMENTAL_COLUMN})
            FROM s3tables.`{DST_NAMESPACE}`.`{DST_TABLE}`
        """).collect()[0][0]
    except:
        return None


def incremental_insert():
    last_val = get_last_value()

    if last_val:
        where_clause = f"WHERE {INCREMENTAL_COLUMN} > '{last_val}'"
    else:
        where_clause = ""

    spark.sql(f"""
        INSERT INTO s3tables.`{DST_NAMESPACE}`.`{DST_TABLE}`
        SELECT *
        FROM {source_ref()}
        {where_clause}
    """)


def verify():
    spark.sql(f"""
        SELECT COUNT(*) 
        FROM s3tables.`{DST_NAMESPACE}`.`{DST_TABLE}`
    """).show()

# ============================================================
# MAIN
# ============================================================
try:
    logger.info("Starting Contact Migration → S3 Tables")

    create_namespace()

    if MIGRATION_TYPE == "New":
        create_table_if_not_exists()

    incremental_insert()
    verify()

    logger.info("Migration Successful")

except Exception as e:
    logger.exception("Job Failed")
    raise e
