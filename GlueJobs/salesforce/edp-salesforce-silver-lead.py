import sys
import uuid
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, row_number, lead, when, lit, coalesce, current_date, year, month, day
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# =========================================================
# 1. EXTRACT ALL PARAMETERS AT THE START
# =========================================================
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_DATABASE",
        "SILVER_TABLE",
        "BRONZE_DATABASE",
        "BRONZE_TABLE",
        "ACCOUNT_ID",
        "TABLE_BUCKET_NAME",
        "CONFIG_BUCKET",
        "SOURCE",
        "ENTITY",
    ],
)

JOB_NAME = args["JOB_NAME"]
SILVER_DB = args["SILVER_DATABASE"]
SILVER_TABLE = args["SILVER_TABLE"]
BRONZE_DB = args["BRONZE_DATABASE"]
BRONZE_TABLE = args["BRONZE_TABLE"]
ACCOUNT_ID = args["ACCOUNT_ID"]
TABLE_BUCKET_NAME = args["TABLE_BUCKET_NAME"]
CONFIG_BUCKET = args["CONFIG_BUCKET"]
SOURCE = args["SOURCE"]
ENTITY = args["ENTITY"]

# Derived FQNs and Warehouse paths
SILVER_FQN = f"s3tables.{SILVER_DB}.{SILVER_TABLE}"
BRONZE_FQN = f"glue_catalog.{BRONZE_DB}.{BRONZE_TABLE}"
ICEBERG_WAREHOUSE = f"s3://{TABLE_BUCKET_NAME}/bucket/{TABLE_BUCKET_NAME}"

# =========================================================
# 2. Spark Session
# =========================================================
spark = (
    SparkSession.builder.appName(JOB_NAME)
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog","s3tables")
    .config("spark.sql.catalog.s3tables","org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.s3tables.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.s3tables.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}")
    .config("spark.sql.catalog.s3tables.warehouse", ICEBERG_WAREHOUSE)
    .config("spark.sql.catalog.glue_catalog","org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.glue.region","us-east-1")
    .getOrCreate()
)

glue_context = GlueContext(spark.sparkContext)
job = Job(glue_context)
job.init(JOB_NAME, args)

# =========================================================
# 3. Helpers & Logging
# =========================================================
def derive_layer(job_name):
    return "silver" if "silver" in job_name.lower() else "bronze"

def get_environment(database):
    db = database.lower()
    if "dev" in db: return "DEV"
    if "qa" in db: return "QA"
    return "PROD"

def upsert_log(log_data):
    print(f"[LOG] {log_data['status']} execution_id={log_data['execution_id']}")
    
    error_message = "NULL"
    if log_data["error_message"]:
        error_message = f"'{log_data['error_message'].replace(chr(39), ' ')}'"

    data_end_time = f"TIMESTAMP '{log_data['data_end_time']}'" if log_data['data_end_time'] else "NULL"
    record_count = log_data["record_count"] if log_data["record_count"] else "NULL"

    spark.sql(f"""
    MERGE INTO glue_catalog.edp_configs_dev.pipeline_runs tgt
    USING (
        SELECT
            '{log_data["execution_id"]}' execution_id,
            '{SOURCE.upper()}' source_system,
            '{SILVER_TABLE.upper()}' table_name,
            '{log_data['layer'].upper()}' layer,
            '{log_data['environment'].upper()}' environment,
            TIMESTAMP '{log_data["data_start_time"]}' data_start_time,
            {data_end_time} data_end_time,
            {record_count} record_count,
            '{log_data['status'].upper()}' status,
            {error_message} error_message
    ) src
    ON tgt.execution_id = src.execution_id
    WHEN MATCHED THEN UPDATE SET
        tgt.data_end_time = src.data_end_time,
        tgt.record_count = src.record_count,
        tgt.status = src.status,
        tgt.error_message = src.error_message
    WHEN NOT MATCHED THEN INSERT *
    """)

# =========================================================
# 4. Table Logic
# =========================================================
def read_config():
    path = f"s3://{CONFIG_BUCKET}/{SOURCE.lower()}/mappings/{ENTITY.lower()}_mapping.csv"
    df = spark.read.option("header","true").csv(path)
    df = df.toDF(*[c.strip().lower() for c in df.columns])
    return df.filter(lower(trim(col("is_active")))=="true")

def extract_keys(config_df):
    primary_keys = config_df.filter(lower(col("is_primary_key"))=="yes").collect()
    pk_cols = [r["source_field"] for r in primary_keys]
    pk_target = primary_keys[0]["target_field"]
    return pk_cols, pk_target

def create_silver_table(config_df):
    schema_ddl = ",\n".join(f"{r['target_field']} {r['target_datatype']}"
                            for r in config_df.select("target_field","target_datatype").collect())
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_FQN} (
      {schema_ddl},
      source_system STRING,
      effective_start_ts TIMESTAMP,
      effective_end_ts TIMESTAMP,
      is_current BOOLEAN,
      year INT, month INT, day INT
    )
    USING iceberg
    PARTITIONED BY (year, month, day)
    """)

# =========================================================
# 5. MAIN EXECUTION
# =========================================================
execution_id = str(uuid.uuid4())
start_time_dt = datetime.utcnow()

log = {
    "execution_id": execution_id,
    "layer": derive_layer(JOB_NAME),
    "environment": get_environment(SILVER_DB),
    "data_start_time": start_time_dt.strftime("%Y-%m-%d %H:%M:%S"),
    "data_end_time": None,
    "record_count": None,
    "status": "RUNNING",
    "error_message": None
}
upsert_log(log)

try:
    config_df = read_config()
    pk_cols, pk_target = extract_keys(config_df)

    if not spark.catalog.tableExists(SILVER_FQN):
        print("Action: Creating Silver Table and Performing Initial Load")
        create_silver_table(config_df)
        bronze_df = spark.read.table(BRONZE_FQN)
    else:
        print("Action: Performing Incremental Snapshot Load")
        watermark_row = spark.sql(f"SELECT max(effective_start_ts) ts FROM {SILVER_FQN}").collect()[0]
        watermark = watermark_row["ts"]
        
        if watermark:
            ts_ms = int(watermark.timestamp() * 1000)
            # Capture all versions based on hourly snapshots in Bronze
            bronze_df = spark.read.format("iceberg") \
                .option("stream-from-timestamp", str(ts_ms)) \
                .load(BRONZE_FQN)
        else:
            bronze_df = spark.read.table(BRONZE_FQN)

    if bronze_df.limit(1).count() == 0:
        print("Status: No new data in Bronze. Job Complete.")
        log["status"] = "SUCCESS"
    else:
        log["record_count"] = bronze_df.count()

        # 1. Identify IDs with changes
        changed_ids_df = bronze_df.select(pk_cols).distinct()
        
        # 2. Pull history for those IDs from Silver
        silver_existing_df = spark.table(SILVER_FQN).join(changed_ids_df, pk_cols, "inner")

        # 3. Map Bronze to Silver Schema
        mapping = [col(r['source_field']).alias(r['target_field']) for r in config_df.collect()]
        bronze_transformed = bronze_df.select(
            *mapping,
            lit(SOURCE).alias("source_system"),
            col("last_modified_date").alias("effective_start_ts")
        )

        # 4. Union: Old History + New Hourly Versions
        combined_df = silver_existing_df.unionByName(bronze_transformed, allowMissingColumns=True)

        # 5. SCD Type 2 Logic (lead function to close previous versions)
        window_spec = Window.partitionBy(*pk_cols).orderBy("effective_start_ts")

        final_updates = combined_df.withColumn(
            "next_start", lead("effective_start_ts").over(window_spec)
        ).withColumn(
            "is_current", when(col("next_start").isNull(), True).otherwise(False)
        ).withColumn(
            "effective_end_ts", coalesce(col("next_start"), lit("9999-12-31 23:59:59").cast("timestamp"))
        ).withColumn(
            "year", year(col("effective_start_ts"))
        ).withColumn(
            "month", month(col("effective_start_ts"))
        ).withColumn(
            "day", day(col("effective_start_ts"))
        ).drop("next_start")

        # 6. Surgical Overwrite (Replace only affected IDs)
        id_list = [row[0] for row in changed_ids_df.collect()]
        replace_condition = f"{pk_target} IN ({','.join([f"'{i}'" for i in id_list])})"

        final_updates.writeTo(SILVER_FQN) \
            .option("replaceWhere", replace_condition) \
            .overwritePartitions()

        log["status"] = "SUCCESS"

except Exception as e:
    log["status"] = "FAILED"
    log["error_message"] = str(e)
    raise

finally:
    log["data_end_time"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    upsert_log(log)
    job.commit()

if __name__ == "__main__":
    main()