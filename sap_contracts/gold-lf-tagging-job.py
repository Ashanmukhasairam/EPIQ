import sys
import boto3
import logging
import pandas as pd

from io import StringIO
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext


# =========================================================
# LOGGING CONFIGURATION
# =========================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =========================================================
# AWS CLIENT INITIALIZATION
# =========================================================
lf_client = boto3.client("lakeformation")
s3_client = boto3.client("s3")


# =========================================================
# JOB ARGUMENT PARSING (GLUE PARAMETERS - GOLD LAYER)
# =========================================================
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ACCOUNT_ID",
        "AIRFLOW_RUN_ID",
        "CONFIG_BUCKET",
        "ENVIRONMENT",
        "ETL_RUN_DATE",
        "EXECUTION_ID",
        "LAYER",
        "GOLD_DATABASE",
        "GOLD_TABLE",
        "SOURCE",
        "TABLE_BUCKET_NAME",
    ],
)

JOB_NAME          = args["JOB_NAME"]
ACCOUNT_ID        = args["ACCOUNT_ID"]
CONFIG_BUCKET     = args["CONFIG_BUCKET"]
ENVIRONMENT       = args["ENVIRONMENT"].lower()
LAYER             = args["LAYER"].lower()
DATABASE          = args["GOLD_DATABASE"]
TABLES            = [t.strip().lower() for t in args["GOLD_TABLE"].split(",")]
SOURCE            = args["SOURCE"]
TABLE_BUCKET_NAME = args["TABLE_BUCKET_NAME"]

S3_TABLES_CATALOG_ID = f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME.split('/')[-1]}"
IS_LAYER_COL         = f"is_{LAYER}"


# =========================================================
# GLUE CONTEXT INITIALIZATION
# =========================================================
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(JOB_NAME, args)


# =========================================================
# LF-TAG KEY DEFINITIONS
# =========================================================
# Database-level tags: these are inherited by all tables in the database
DATABASE_TAG_KEYS = ["environment", "data_layer", "domain"]

# Table-level tags: only table-specific tags (NOT environment/data_layer/domain)
# environment, data_layer, domain are inherited from the database
TABLE_TAG_KEYS = ["source_system", "data_owner", "data_quality"]

COLUMN_TAG_KEYS = ["sensitivity", "dataclassificaton"]

ETL_METADATA_COLUMNS = [
    "execution_id",
    "airflow_run_id",
    "etl_run_date",
    "source_system",
    "ingestion_ts",
]


# =========================================================
# LF-TAG REGISTRY
# =========================================================
def load_lf_tag_registry():
    resp = lf_client.list_lf_tags()
    return {
        tag["TagKey"]: {
            v.lower().replace(" ", "_"): v
            for v in tag.get("TagValues", [])
        }
        for tag in resp.get("LFTags", [])
    }


LF_TAG_REGISTRY = load_lf_tag_registry()


# =========================================================
# RESOLVE TAG VALUE
# =========================================================
def resolve_tag_value(tag_key, raw_value):
    if not raw_value:
        return None
    return LF_TAG_REGISTRY.get(tag_key, {}).get(
        str(raw_value).lower().strip().replace(" ", "_")
    )


# =========================================================
# BUILD TAGS
# =========================================================
def build_tags(row_dict, tag_keys):
    tags = {}
    for key in tag_keys:

        if key == "environment":
            raw = ENVIRONMENT

        elif key == "data_layer":
            raw = LAYER

        else:
            raw = row_dict.get(key, "")

        val = resolve_tag_value(key, raw)
        if val:
            tags[key] = val
    return tags


# =========================================================
# APPLY TAGS
# =========================================================
def apply_tags_to_resource(resource, tags, label):
    if not tags:
        return
    try:
        lf_client.add_lf_tags_to_resource(
            Resource=resource,
            LFTags=[{"TagKey": k, "TagValues": [v]} for k, v in tags.items()],
        )
        logger.info(f"{label} -> {tags}")
    except ClientError as e:
        logger.error(e)


# =========================================================
# REMOVE DATABASE TAGS
# =========================================================
def remove_all_database_tags(database):
    logger.info(f"[DATABASE CLEANUP] Removing existing tags from database: {database}")
    try:
        resp = lf_client.get_resource_lf_tags(
            Resource={"Database": {"CatalogId": S3_TABLES_CATALOG_ID, "Name": database}}
        )
        existing = {t["TagKey"]: t.get("TagValues", []) for t in resp.get("LFTagsOnDatabase", [])}
        if existing:
            lf_client.remove_lf_tags_from_resource(
                Resource={"Database": {"CatalogId": S3_TABLES_CATALOG_ID, "Name": database}},
                LFTags=[{"TagKey": k, "TagValues": v} for k, v in existing.items()],
            )
            logger.info(f"[DATABASE CLEANUP] Removed {len(existing)} tags: {list(existing.keys())}")
        else:
            logger.info(f"[DATABASE CLEANUP] No existing tags found on database [{database}]")
    except Exception as e:
        logger.warning(f"[DATABASE CLEANUP] Could not remove tags from [{database}]: {e}")


# =========================================================
# REMOVE TABLE TAGS
# =========================================================
def remove_all_table_tags(database, table):
    logger.info(f"[TABLE CLEANUP] Removing existing tags from table: {database}.{table}")
    try:
        resp = lf_client.get_resource_lf_tags(
            Resource={"Table": {"CatalogId": S3_TABLES_CATALOG_ID, "DatabaseName": database, "Name": table}}
        )
        existing = {t["TagKey"]: t.get("TagValues", []) for t in resp.get("LFTagsOnTable", [])}
        if existing:
            lf_client.remove_lf_tags_from_resource(
                Resource={"Table": {"CatalogId": S3_TABLES_CATALOG_ID, "DatabaseName": database, "Name": table}},
                LFTags=[{"TagKey": k, "TagValues": v} for k, v in existing.items()],
            )
            logger.info(f"[TABLE CLEANUP] Removed {len(existing)} tags: {list(existing.keys())}")
        else:
            logger.info(f"[TABLE CLEANUP] No existing tags found on table [{database}.{table}]")
    except Exception as e:
        logger.warning(f"[TABLE CLEANUP] Could not remove tags from [{database}.{table}]: {e}")


# =========================================================
# COLUMN CLEANUP
# =========================================================
def cleanup_column_tags(database, table, mapping_rows):
    business_cols = [r.get("target_field") for r in mapping_rows if r.get("target_field")]
    all_cols = ETL_METADATA_COLUMNS + business_cols
    logger.info(f"[COLUMN CLEANUP] Removing existing tags from {len(all_cols)} columns in [{database}.{table}] ({len(ETL_METADATA_COLUMNS)} ETL + {len(business_cols)} business)")

    removed = 0
    skipped = 0

    for col in ETL_METADATA_COLUMNS:
        try:
            lf_client.remove_lf_tags_from_resource(
                Resource={"TableWithColumns": {"CatalogId": S3_TABLES_CATALOG_ID, "DatabaseName": database, "Name": table, "ColumnNames": [col]}}
            )
            removed += 1
        except:
            skipped += 1

    for row in mapping_rows:
        col = row.get("target_field")
        try:
            lf_client.remove_lf_tags_from_resource(
                Resource={"TableWithColumns": {"CatalogId": S3_TABLES_CATALOG_ID, "DatabaseName": database, "Name": table, "ColumnNames": [col]}}
            )
            removed += 1
        except:
            skipped += 1

    logger.info(f"[COLUMN CLEANUP] Done — removed: {removed}, skipped: {skipped}, total: {len(all_cols)}")


# =========================================================
# COLUMN TAGGING
# =========================================================
def apply_column_tags(database, table, mapping_rows, table_tags):
    logger.info(f"[COLUMN TAGGING] Starting column tagging for [{database}.{table}]")
    logger.info(f"[COLUMN TAGGING] ETL metadata columns: {len(ETL_METADATA_COLUMNS)} | Business columns: {len(mapping_rows)}")

    for col in ETL_METADATA_COLUMNS:
        apply_tags_to_resource(
            {"TableWithColumns": {"CatalogId": S3_TABLES_CATALOG_ID, "DatabaseName": database, "Name": table, "ColumnNames": [col]}},
            {
                "sensitivity": resolve_tag_value("sensitivity", "internal"),
                "dataclassification": resolve_tag_value("dataclassification", "business"),
            },
            f"ETL:{col}",
        )

    success_count = 0
    failed_count  = 0

    for row in mapping_rows:
        col = row.get("target_field")

        sens = resolve_tag_value("sensitivity", row.get("sensitivity", "internal"))
        cls  = resolve_tag_value("dataclassification", row.get("dataclassification", "business"))

        tags = {}
        if sens:
            tags["sensitivity"] = sens
        if cls:
            tags["dataclassification"] = cls

        try:
            apply_tags_to_resource(
                {"TableWithColumns": {"CatalogId": S3_TABLES_CATALOG_ID, "DatabaseName": database, "Name": table, "ColumnNames": [col]}},
                tags,
                f"COL:{col}",
            )
            success_count += 1
        except Exception as e:
            logger.error(f"[COLUMN TAGGING] Failed to tag column [{col}]: {e}")
            failed_count += 1

    logger.info(
        f"[COLUMN TAGGING] Summary for [{database}.{table}] — "
        f"success: {success_count}, failed: {failed_count}, "
        f"total business cols: {len(mapping_rows)}, ETL cols: {len(ETL_METADATA_COLUMNS)}"
    )


# =========================================================
# MAIN FLOW
# =========================================================
def run_lf_tagging(database, table, mapping_rows):
    logger.info(f"{'='*60}")
    logger.info(f"[START] LF Tagging — Database: {database} | Table: {table}")
    logger.info(f"[START] Catalog ID : {S3_TABLES_CATALOG_ID}")
    logger.info(f"[START] Environment: {ENVIRONMENT} | Layer: {LAYER} | Source: {SOURCE}")
    logger.info(f"[START] Mapping rows: {len(mapping_rows)}")
    logger.info(f"{'='*60}")

    # Phase 1: Cleanup existing tags
    logger.info(f"{'─'*60}")
    logger.info(f"[PHASE 1] CLEANUP — Removing existing tags")
    logger.info(f"{'─'*60}")
    remove_all_database_tags(database)
    remove_all_table_tags(database, table)
    cleanup_column_tags(database, table, mapping_rows)

    # Phase 2: Apply database-level tags (environment, data_layer, domain)
    # Tables will INHERIT these — shows "Inherited from: <database>"
    logger.info(f"{'─'*60}")
    logger.info(f"[PHASE 2] DATABASE TAGGING — Applying inherited tags to: {database}")
    logger.info(f"{'─'*60}")
    db_tags = build_tags(mapping_rows[0], DATABASE_TAG_KEYS)
    logger.info(f"[DATABASE TAGGING] Tags to apply: {db_tags}")
    apply_tags_to_resource(
        {"Database": {"CatalogId": S3_TABLES_CATALOG_ID, "Name": database}},
        db_tags,
        f"DATABASE:{database}",
    )
    logger.info(f"[DATABASE TAGGING] Completed for database: {database}")

    # Phase 3: Apply table-level tags (source_system, data_owner, data_quality only)
    # These are table-specific and NOT inherited from database
    logger.info(f"{'─'*60}")
    logger.info(f"[PHASE 3] TABLE TAGGING — Applying table-specific tags to: {database}.{table}")
    logger.info(f"{'─'*60}")
    table_tags = build_tags(mapping_rows[0], TABLE_TAG_KEYS)
    logger.info(f"[TABLE TAGGING] Tags to apply: {table_tags}")
    apply_tags_to_resource(
        {"Table": {"CatalogId": S3_TABLES_CATALOG_ID, "DatabaseName": database, "Name": table}},
        table_tags,
        f"TABLE:{table}",
    )
    logger.info(f"[TABLE TAGGING] Completed for table: {database}.{table}")

    # Phase 4: Apply column-level tags (sensitivity, dataclassification)
    logger.info(f"{'─'*60}")
    logger.info(f"[PHASE 4] COLUMN TAGGING — Applying column tags to: {database}.{table}")
    logger.info(f"{'─'*60}")
    apply_column_tags(database, table, mapping_rows, table_tags)

    logger.info(f"{'='*60}")
    logger.info(f"[COMPLETE] LF tagging complete for: {database}.{table}")
    logger.info(f"{'='*60}")


# =========================================================
# READ MAPPING
# =========================================================
def read_mapping(obj):
    key = f"{SOURCE}/mappings/account_mapping.csv"
    content = s3_client.get_object(Bucket=CONFIG_BUCKET, Key=key)["Body"].read().decode()
    df = pd.read_csv(StringIO(content), dtype=str)
    return df.to_dict("records")


# =========================================================
# MAIN
# =========================================================
def main():
    logger.info(f"{'='*60}")
    logger.info(f"[JOB START] {JOB_NAME}")
    logger.info(f"[JOB START] Tables to tag: {TABLES}")
    logger.info(f"[JOB START] Database: {DATABASE}")
    logger.info(f"[JOB START] Environment: {ENVIRONMENT} | Layer: {LAYER}")
    logger.info(f"{'='*60}")

    for table in TABLES:
        mapping = read_mapping(table)
        logger.info(f"[MAPPING] Loaded {len(mapping)} rows for table: {table}")
        run_lf_tagging(DATABASE, table, mapping)

    job.commit()
    logger.info(f"[JOB COMPLETE] All tables tagged successfully")


# =========================================================
# ENTRY
# =========================================================
if __name__ == "__main__":
    main()
