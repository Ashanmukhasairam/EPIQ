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

# ================= LOGGING =================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================= CLIENTS =================

lf_client = boto3.client("lakeformation")
s3_client = boto3.client("s3")

# ================= ARGUMENTS =================

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
AIRFLOW_RUN_ID    = args["AIRFLOW_RUN_ID"]
CONFIG_BUCKET     = args["CONFIG_BUCKET"]
ENVIRONMENT       = args["ENVIRONMENT"].lower()
ETL_RUN_DATE      = args["ETL_RUN_DATE"]
EXECUTION_ID      = args["EXECUTION_ID"]
LAYER             = args["LAYER"].lower()
DATABASE          = args["GOLD_DATABASE"]
TABLES            = [t.strip().lower() for t in args["GOLD_TABLE"].split(",")]
SOURCE            = args["SOURCE"]
TABLE_BUCKET_NAME = args["TABLE_BUCKET_NAME"]

S3_TABLES_CATALOG_ID = f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME.split('/')[-1]}"
IS_LAYER_COL         = "is_gold"

# ================= GLUE CONTEXT =================

sc          = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
job         = Job(glueContext)
job.init(JOB_NAME, args)

# ================= TAG LEVEL DEFINITIONS =================

DATABASE_TAG_KEYS = ["environment", "data_layer", "domain"]

# Fixed: added DataClassification and sensitivity back to TABLE_TAG_KEYS
TABLE_TAG_KEYS    = [
    "environment", "data_layer", "domain", "source_system",
    "data_owner", "data_quality"
]
COLUMN_TAG_KEYS     = ["sensitivity", "DataClassification"]
COLUMN_CLEANUP_KEYS = [k for k in TABLE_TAG_KEYS if k not in COLUMN_TAG_KEYS]

ETL_METADATA_COLUMNS = [
    "execution_id", "airflow_run_id",
    "etl_run_date", "source_system", "ingestion_ts",
]

# ================= TAG REGISTRY =================

def load_lf_tag_registry():
    try:
        resp     = lf_client.list_lf_tags()
        registry = {
            tag["TagKey"]: {
                v.lower().replace(" ", "_"): v
                for v in tag.get("TagValues", [])
            }
            for tag in resp.get("LFTags", [])
        }
        if not registry:
            raise Exception("LF-Tag registry is empty - check list_lf_tags permission")
        logger.info(f"LF-Tag registry loaded: {list(registry.keys())}")
        return registry
    except Exception as e:
        logger.error(f"Failed to load LF-Tag registry: {e}")
        raise

LF_TAG_REGISTRY = load_lf_tag_registry()

# ================= TAG RESOLVER =================

def resolve_tag_value(tag_key, raw_value):
    if not raw_value or not str(raw_value).strip():
        return None
    tag_map  = LF_TAG_REGISTRY.get(tag_key, {})
    resolved = tag_map.get(str(raw_value).lower().strip().replace(" ", "_"))
    if not resolved:
        logger.warning(f"Value '{raw_value}' not found for tag '{tag_key}'. Valid: {list(tag_map.values())}")
    return resolved

# ================= TAG BUILDER =================

def build_tags(row_dict, tag_keys):
    tags = {}
    for key in tag_keys:
        raw      = ENVIRONMENT if key == "environment" else row_dict.get(key, "")
        resolved = resolve_tag_value(key, raw)
        if resolved:
            tags[key] = resolved
    return tags

# ================= TAG APPLICATION =================

def apply_tags_to_resource(resource, tags, label):
    if not tags:
        return
    try:
        lf_client.add_lf_tags_to_resource(
            Resource=resource,
            LFTags=[{"TagKey": k, "TagValues": [v]} for k, v in tags.items()],
        )
        logger.info(f"Tags applied to {label}: {tags}")
    except ClientError as e:
        logger.error(f"Tag failed for {label}: {e}")

# ================= REMOVE ALL TAGS FROM RESOURCE =================

def remove_all_database_tags(database):
    """Remove ALL directly assigned tags from database before reapplying."""
    try:
        resp     = lf_client.get_resource_lf_tags(
            Resource={"Database": {"CatalogId": S3_TABLES_CATALOG_ID, "Name": database}}
        )
        existing = {t["TagKey"]: t.get("TagValues", []) for t in resp.get("LFTagsOnDatabase", [])}
        if not existing:
            return
        lf_client.remove_lf_tags_from_resource(
            Resource={"Database": {"CatalogId": S3_TABLES_CATALOG_ID, "Name": database}},
            LFTags=[{"TagKey": k, "TagValues": v} for k, v in existing.items()],
        )
        logger.info(f"Removed all tags from database {database}: {list(existing.keys())}")
    except ClientError as e:
        logger.warning(f"Failed to remove database tags: {e}")


def remove_all_table_tags(database, table):
    """Remove ALL directly assigned tags from table before reapplying."""
    try:
        resp     = lf_client.get_resource_lf_tags(
            Resource={"Table": {
                "CatalogId": S3_TABLES_CATALOG_ID,
                "DatabaseName": database,
                "Name": table
            }}
        )
        existing = {t["TagKey"]: t.get("TagValues", []) for t in resp.get("LFTagsOnTable", [])}
        if not existing:
            return
        lf_client.remove_lf_tags_from_resource(
            Resource={"Table": {
                "CatalogId": S3_TABLES_CATALOG_ID,
                "DatabaseName": database,
                "Name": table
            }},
            LFTags=[{"TagKey": k, "TagValues": v} for k, v in existing.items()],
        )
        logger.info(f"Removed all tags from table {table}: {list(existing.keys())}")
    except ClientError as e:
        logger.warning(f"Failed to remove table tags for {table}: {e}")

# ================= COLUMN TAG HELPERS =================

def get_existing_column_tags(database, table, column):
    try:
        resp     = lf_client.get_resource_lf_tags(
            Resource={
                "TableWithColumns": {
                    "CatalogId": S3_TABLES_CATALOG_ID,
                    "DatabaseName": database,
                    "Name": table,
                    "ColumnNames": [column],
                }
            }
        )
        existing = {}
        for t in resp.get("LFTagOnColumns", []):
            for x in t.get("LFTags", []):
                existing[x["TagKey"]] = x.get("TagValues", [])
        return existing
    except Exception as e:
        logger.warning(f"Could not fetch tags for {table}.{column}: {e}")
        return {}


def remove_column_tags(database, table, column, tags_to_remove):
    if not tags_to_remove:
        return
    try:
        lf_client.remove_lf_tags_from_resource(
            Resource={
                "TableWithColumns": {
                    "CatalogId": S3_TABLES_CATALOG_ID,
                    "DatabaseName": database,
                    "Name": table,
                    "ColumnNames": [column],
                }
            },
            LFTags=tags_to_remove,
        )
        logger.info(f"Removed tags from {column}: {[t['TagKey'] for t in tags_to_remove]}")
    except ClientError as e:
        logger.warning(f"Tag removal failed for {column}: {e}")

# ================= COLUMN TAG CLEANUP =================

def cleanup_column_tags(database, table, mapping_rows, table_tags):
    """Remove ALL directly assigned column-level tags before reapplying.
    Covers ETL metadata columns, table-level tags on columns,
    and stale sensitivity/DataClassification from previous runs.
    """
    # Step 1 - remove ALL tags from ETL metadata columns
    for column in ETL_METADATA_COLUMNS:
        existing  = get_existing_column_tags(database, table, column)
        to_remove = [{"TagKey": k, "TagValues": v} for k, v in existing.items()]
        if to_remove:
            logger.info(f"Cleaning ETL metadata column: {column}")
            remove_column_tags(database, table, column, to_remove)

    # Step 2 - for every mapped column remove ALL directly assigned tags
    # This wipes stale tags from previous runs before fresh ones are applied
    for row in mapping_rows:
        column = row.get("target_field", "").strip()
        if not row.get("source_field", "").strip():
            continue

        existing  = get_existing_column_tags(database, table, column)
        if not existing:
            continue

        # Remove everything that is directly assigned on this column
        to_remove = [{"TagKey": k, "TagValues": v} for k, v in existing.items()]
        logger.info(f"Wiping all existing column tags from {column}: {list(existing.keys())}")
        remove_column_tags(database, table, column, to_remove)

# ================= COLUMN TAG APPLICATION =================

def apply_column_tags(database, table, mapping_rows, table_tags):
    """Apply column-level tags only where value differs from table default."""
    for row in mapping_rows:
        column = row.get("target_field", "").strip()
        if not row.get("source_field", "").strip():
            continue

        col_tags      = build_tags(row, COLUMN_TAG_KEYS)
        tags_to_apply = {k: v for k, v in col_tags.items() if table_tags.get(k) != v}

        if not tags_to_apply:
            logger.info(f"No column tag override needed for: {column}")
            continue

        resource = {
            "TableWithColumns": {
                "CatalogId": S3_TABLES_CATALOG_ID,
                "DatabaseName": database,
                "Name": table,
                "ColumnNames": [column],
            }
        }
        apply_tags_to_resource(resource, tags_to_apply, f"column:{column}")

# ================= DATABASE TAG APPLICATION =================

def apply_database_tags(database, mapping_rows):
    """Remove all existing database tags then apply fresh ones."""
    remove_all_database_tags(database)
    tags     = build_tags(mapping_rows[0], DATABASE_TAG_KEYS)
    resource = {"Database": {"CatalogId": S3_TABLES_CATALOG_ID, "Name": database}}
    apply_tags_to_resource(resource, tags, f"database:{database}")

# ================= TABLE TAG APPLICATION =================

def apply_table_tags(database, table, mapping_rows):
    """Remove all existing table tags then apply fresh ones.
    Returns applied tags so column tagging can compare against them.
    """
    remove_all_table_tags(database, table)
    tags     = build_tags(mapping_rows[0], TABLE_TAG_KEYS)
    resource = {
        "Table": {
            "CatalogId": S3_TABLES_CATALOG_ID,
            "DatabaseName": database,
            "Name": table,
        }
    }
    apply_tags_to_resource(resource, tags, f"table:{table}")
    return tags

# ================= LF TAGGING ORCHESTRATOR =================

def run_lf_tagging(database, table, mapping_rows):
    """Full tagging cycle per table:
    1. Remove all database tags  -> apply fresh database tags
    2. Remove all table tags     -> apply fresh table tags
    3. Wipe all column tags      -> apply only PII overrides
    """
    logger.info(f"Starting LF tagging: {database}.{table}")
    apply_database_tags(database, mapping_rows)
    table_tags = apply_table_tags(database, table, mapping_rows)
    cleanup_column_tags(database, table, mapping_rows, table_tags)
    apply_column_tags(database, table, mapping_rows, table_tags)
    logger.info(f"Completed LF tagging: {database}.{table}")

# ================= MAPPING =================

def read_mapping(base_object):
    bucket  = CONFIG_BUCKET
    key     = f"{SOURCE}/mappings/contracts_mapping_updated.csv"

    resp    = s3_client.get_object(Bucket=bucket, Key=key)
    content = resp["Body"].read().decode("utf-8")

    df = pd.read_csv(
        StringIO(content),
        quotechar='"',
        skipinitialspace=True,
        dtype=str,
    )

    df.columns   = df.columns.str.strip()
    str_cols     = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())

    logger.info(f"CSV columns: {list(df.columns)}")
    logger.info(f"CSV shape: {df.shape}")

    required = ["source_system", "source_object", "is_active", IS_LAYER_COL, "data_layer"]
    missing  = [c for c in required if c not in df.columns]
    if missing:
        raise Exception(f"Missing columns in CSV: {missing}. Available: {list(df.columns)}")

    filtered = df[
        (df["source_system"]   == SOURCE)
        & (df["source_object"] == base_object)
        & (df["is_active"]     == "TRUE")
        & (df[IS_LAYER_COL]    == "TRUE")
        # & (df["data_layer"]    == "gold")
    ]

    if filtered.empty:
        logger.warning(f"No rows matched. data_layer values in CSV: {df['data_layer'].unique().tolist()}")
        logger.warning(f"{IS_LAYER_COL} values in CSV: {df[IS_LAYER_COL].unique().tolist()}")
        raise Exception(
            f"No mapping found for source={SOURCE}, object={base_object}, "
            f"layer={LAYER}, {IS_LAYER_COL}=TRUE"
        )

    rows = filtered.to_dict("records")
    logger.info(f"Loaded {len(rows)} mapping rows for {base_object} [{LAYER}]")
    return rows

# ================= MAIN =================

def main():
    logger.info(f"Governance job started")
    logger.info(f"LAYER:    {LAYER}")
    logger.info(f"Catalog:  {S3_TABLES_CATALOG_ID}")
    logger.info(f"Database: {DATABASE}")
    logger.info(f"Tables:   {TABLES}")
    logger.info(f"Filter:   {IS_LAYER_COL} = TRUE, data_layer = {LAYER}")

    for table in TABLES:
        try:
            mapping = read_mapping(table)
            run_lf_tagging(DATABASE, table, mapping)
        except Exception as e:
            logger.error(f"Governance failed for table '{table}': {e}")
            raise

    job.commit()
    logger.info("Governance job complete")

# ================= ENTRY =================

if __name__ == "__main__":
    main()










# import sys
# import boto3
# import logging
# import pandas as pd

# from io import StringIO
# from botocore.exceptions import ClientError
# from awsglue.utils import getResolvedOptions
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.context import SparkContext

# # ================= LOGGING =================

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # ================= CLIENTS =================

# lf_client = boto3.client("lakeformation")
# s3_client = boto3.client("s3")

# # ================= ARGUMENTS =================

# args = getResolvedOptions(
#     sys.argv,
#     [
#         "JOB_NAME",
#         "ACCOUNT_ID",
#         "AIRFLOW_RUN_ID",
#         "CONFIG_BUCKET",
#         "ENVIRONMENT",
#         "ETL_RUN_DATE",
#         "EXECUTION_ID",
#         "LAYER",
#         "SILVER_DATABASE",
#         "SILVER_TABLE",
#         "SOURCE",
#         "TABLE_BUCKET_NAME",
#     ],
# )

# JOB_NAME          = args["JOB_NAME"]
# ACCOUNT_ID        = args["ACCOUNT_ID"]
# AIRFLOW_RUN_ID    = args["AIRFLOW_RUN_ID"]
# CONFIG_BUCKET     = args["CONFIG_BUCKET"]
# ENVIRONMENT       = args["ENVIRONMENT"].lower()
# ETL_RUN_DATE      = args["ETL_RUN_DATE"]
# EXECUTION_ID      = args["EXECUTION_ID"]
# LAYER             = args["LAYER"].lower()
# DATABASE          = args["SILVER_DATABASE"]
# TABLES            = [t.strip().lower() for t in args["SILVER_TABLE"].split(",")]
# SOURCE            = args["SOURCE"]
# TABLE_BUCKET_NAME = args["TABLE_BUCKET_NAME"]

# S3_TABLES_CATALOG_ID = f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME.split('/')[-1]}"
# IS_LAYER_COL         = "is_gold"

# # ================= GLUE CONTEXT =================

# sc          = SparkContext.getOrCreate()
# glueContext = GlueContext(sc)
# job         = Job(glueContext)
# job.init(JOB_NAME, args)

# # ================= TAG LEVEL DEFINITIONS =================

# DATABASE_TAG_KEYS = ["environment", "data_layer", "domain"]

# # Fixed: added DataClassification and sensitivity back to TABLE_TAG_KEYS
# TABLE_TAG_KEYS    = [
#     "environment", "data_layer", "domain", "source_system",
#     "data_owner", "data_quality"
# ]
# COLUMN_TAG_KEYS     = ["sensitivity", "DataClassification"]
# COLUMN_CLEANUP_KEYS = [k for k in TABLE_TAG_KEYS if k not in COLUMN_TAG_KEYS]

# ETL_METADATA_COLUMNS = [
#     "execution_id", "airflow_run_id",
#     "etl_run_date", "source_system", "ingestion_ts",
# ]

# # ================= TAG REGISTRY =================

# def load_lf_tag_registry():
#     try:
#         resp     = lf_client.list_lf_tags()
#         registry = {
#             tag["TagKey"]: {
#                 v.lower().replace(" ", "_"): v
#                 for v in tag.get("TagValues", [])
#             }
#             for tag in resp.get("LFTags", [])
#         }
#         if not registry:
#             raise Exception("LF-Tag registry is empty - check list_lf_tags permission")
#         logger.info(f"LF-Tag registry loaded: {list(registry.keys())}")
#         return registry
#     except Exception as e:
#         logger.error(f"Failed to load LF-Tag registry: {e}")
#         raise

# LF_TAG_REGISTRY = load_lf_tag_registry()

# # ================= TAG RESOLVER =================

# def resolve_tag_value(tag_key, raw_value):
#     if not raw_value or not str(raw_value).strip():
#         return None
#     tag_map  = LF_TAG_REGISTRY.get(tag_key, {})
#     resolved = tag_map.get(str(raw_value).lower().strip().replace(" ", "_"))
#     if not resolved:
#         logger.warning(f"Value '{raw_value}' not found for tag '{tag_key}'. Valid: {list(tag_map.values())}")
#     return resolved

# # ================= TAG BUILDER =================

# def build_tags(row_dict, tag_keys):
#     tags = {}
#     for key in tag_keys:
#         raw      = ENVIRONMENT if key == "environment" else row_dict.get(key, "")
#         resolved = resolve_tag_value(key, raw)
#         if resolved:
#             tags[key] = resolved
#     return tags

# # ================= TAG APPLICATION =================

# def apply_tags_to_resource(resource, tags, label):
#     if not tags:
#         return
#     try:
#         lf_client.add_lf_tags_to_resource(
#             Resource=resource,
#             LFTags=[{"TagKey": k, "TagValues": [v]} for k, v in tags.items()],
#         )
#         logger.info(f"Tags applied to {label}: {tags}")
#     except ClientError as e:
#         logger.error(f"Tag failed for {label}: {e}")

# # ================= REMOVE ALL TAGS FROM RESOURCE =================

# def remove_all_database_tags(database):
#     """Remove ALL directly assigned tags from database before reapplying."""
#     try:
#         resp     = lf_client.get_resource_lf_tags(
#             Resource={"Database": {"CatalogId": S3_TABLES_CATALOG_ID, "Name": database}}
#         )
#         existing = {t["TagKey"]: t.get("TagValues", []) for t in resp.get("LFTagsOnDatabase", [])}
#         if not existing:
#             return
#         lf_client.remove_lf_tags_from_resource(
#             Resource={"Database": {"CatalogId": S3_TABLES_CATALOG_ID, "Name": database}},
#             LFTags=[{"TagKey": k, "TagValues": v} for k, v in existing.items()],
#         )
#         logger.info(f"Removed all tags from database {database}: {list(existing.keys())}")
#     except ClientError as e:
#         logger.warning(f"Failed to remove database tags: {e}")


# def remove_all_table_tags(database, table):
#     """Remove ALL directly assigned tags from table before reapplying."""
#     try:
#         resp     = lf_client.get_resource_lf_tags(
#             Resource={"Table": {
#                 "CatalogId": S3_TABLES_CATALOG_ID,
#                 "DatabaseName": database,
#                 "Name": table
#             }}
#         )
#         existing = {t["TagKey"]: t.get("TagValues", []) for t in resp.get("LFTagsOnTable", [])}
#         if not existing:
#             return
#         lf_client.remove_lf_tags_from_resource(
#             Resource={"Table": {
#                 "CatalogId": S3_TABLES_CATALOG_ID,
#                 "DatabaseName": database,
#                 "Name": table
#             }},
#             LFTags=[{"TagKey": k, "TagValues": v} for k, v in existing.items()],
#         )
#         logger.info(f"Removed all tags from table {table}: {list(existing.keys())}")
#     except ClientError as e:
#         logger.warning(f"Failed to remove table tags for {table}: {e}")

# # ================= COLUMN TAG HELPERS =================

# def get_existing_column_tags(database, table, column):
#     try:
#         resp     = lf_client.get_resource_lf_tags(
#             Resource={
#                 "TableWithColumns": {
#                     "CatalogId": S3_TABLES_CATALOG_ID,
#                     "DatabaseName": database,
#                     "Name": table,
#                     "ColumnNames": [column],
#                 }
#             }
#         )
#         existing = {}
#         for t in resp.get("LFTagOnColumns", []):
#             for x in t.get("LFTags", []):
#                 existing[x["TagKey"]] = x.get("TagValues", [])
#         return existing
#     except Exception as e:
#         logger.warning(f"Could not fetch tags for {table}.{column}: {e}")
#         return {}


# def remove_column_tags(database, table, column, tags_to_remove):
#     if not tags_to_remove:
#         return
#     try:
#         lf_client.remove_lf_tags_from_resource(
#             Resource={
#                 "TableWithColumns": {
#                     "CatalogId": S3_TABLES_CATALOG_ID,
#                     "DatabaseName": database,
#                     "Name": table,
#                     "ColumnNames": [column],
#                 }
#             },
#             LFTags=tags_to_remove,
#         )
#         logger.info(f"Removed tags from {column}: {[t['TagKey'] for t in tags_to_remove]}")
#     except ClientError as e:
#         logger.warning(f"Tag removal failed for {column}: {e}")

# # ================= COLUMN TAG CLEANUP =================

# def cleanup_column_tags(database, table, mapping_rows, table_tags):
#     """Remove ALL directly assigned column-level tags before reapplying.
#     Covers ETL metadata columns, table-level tags on columns,
#     and stale sensitivity/DataClassification from previous runs.
#     """
#     # Step 1 - remove ALL tags from ETL metadata columns
#     for column in ETL_METADATA_COLUMNS:
#         existing  = get_existing_column_tags(database, table, column)
#         to_remove = [{"TagKey": k, "TagValues": v} for k, v in existing.items()]
#         if to_remove:
#             logger.info(f"Cleaning ETL metadata column: {column}")
#             remove_column_tags(database, table, column, to_remove)

#     # Step 2 - for every mapped column remove ALL directly assigned tags
#     # This wipes stale tags from previous runs before fresh ones are applied
#     for row in mapping_rows:
#         column = row.get("target_field", "").strip()
#         if not row.get("source_field", "").strip():
#             continue

#         existing  = get_existing_column_tags(database, table, column)
#         if not existing:
#             continue

#         # Remove everything that is directly assigned on this column
#         to_remove = [{"TagKey": k, "TagValues": v} for k, v in existing.items()]
#         logger.info(f"Wiping all existing column tags from {column}: {list(existing.keys())}")
#         remove_column_tags(database, table, column, to_remove)

# # ================= COLUMN TAG APPLICATION =================

# def apply_column_tags(database, table, mapping_rows, table_tags):
#     """Apply column-level tags only where value differs from table default."""
#     for row in mapping_rows:
#         column = row.get("target_field", "").strip()
#         if not row.get("source_field", "").strip():
#             continue

#         col_tags      = build_tags(row, COLUMN_TAG_KEYS)
#         tags_to_apply = {k: v for k, v in col_tags.items() if table_tags.get(k) != v}

#         if not tags_to_apply:
#             logger.info(f"No column tag override needed for: {column}")
#             continue

#         resource = {
#             "TableWithColumns": {
#                 "CatalogId": S3_TABLES_CATALOG_ID,
#                 "DatabaseName": database,
#                 "Name": table,
#                 "ColumnNames": [column],
#             }
#         }
#         apply_tags_to_resource(resource, tags_to_apply, f"column:{column}")

# # ================= DATABASE TAG APPLICATION =================

# def apply_database_tags(database, mapping_rows):
#     """Remove all existing database tags then apply fresh ones."""
#     remove_all_database_tags(database)
#     tags     = build_tags(mapping_rows[0], DATABASE_TAG_KEYS)
#     resource = {"Database": {"CatalogId": S3_TABLES_CATALOG_ID, "Name": database}}
#     apply_tags_to_resource(resource, tags, f"database:{database}")

# # ================= TABLE TAG APPLICATION =================

# def apply_table_tags(database, table, mapping_rows):
#     """Remove all existing table tags then apply fresh ones.
#     Returns applied tags so column tagging can compare against them.
#     """
#     remove_all_table_tags(database, table)
#     tags     = build_tags(mapping_rows[0], TABLE_TAG_KEYS)
#     resource = {
#         "Table": {
#             "CatalogId": S3_TABLES_CATALOG_ID,
#             "DatabaseName": database,
#             "Name": table,
#         }
#     }
#     apply_tags_to_resource(resource, tags, f"table:{table}")
#     return tags

# # ================= LF TAGGING ORCHESTRATOR =================

# def run_lf_tagging(database, table, mapping_rows):
#     """Full tagging cycle per table:
#     1. Remove all database tags  -> apply fresh database tags
#     2. Remove all table tags     -> apply fresh table tags
#     3. Wipe all column tags      -> apply only PII overrides
#     """
#     logger.info(f"Starting LF tagging: {database}.{table}")
#     apply_database_tags(database, mapping_rows)
#     table_tags = apply_table_tags(database, table, mapping_rows)
#     cleanup_column_tags(database, table, mapping_rows, table_tags)
#     apply_column_tags(database, table, mapping_rows, table_tags)
#     logger.info(f"Completed LF tagging: {database}.{table}")

# # ================= MAPPING =================

# def read_mapping(base_object):
#     bucket  = CONFIG_BUCKET
#     key     = f"{SOURCE}/mappings/contracts_mapping_updated.csv"

#     resp    = s3_client.get_object(Bucket=bucket, Key=key)
#     content = resp["Body"].read().decode("utf-8")

#     df = pd.read_csv(
#         StringIO(content),
#         quotechar='"',
#         skipinitialspace=True,
#         dtype=str,
#     )

#     df.columns   = df.columns.str.strip()
#     str_cols     = df.select_dtypes(include="object").columns
#     df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())

#     logger.info(f"CSV columns: {list(df.columns)}")
#     logger.info(f"CSV shape: {df.shape}")

#     required = ["source_system", "source_object", "is_active", IS_LAYER_COL, "data_layer"]
#     missing  = [c for c in required if c not in df.columns]
#     if missing:
#         raise Exception(f"Missing columns in CSV: {missing}. Available: {list(df.columns)}")

#     filtered = df[
#         (df["source_system"]   == SOURCE)
#         & (df["source_object"] == base_object)
#         & (df["is_active"]     == "TRUE")
#         & (df[IS_LAYER_COL]    == "TRUE")
#         & (df["data_layer"]    == LAYER)
#     ]

#     if filtered.empty:
#         logger.warning(f"No rows matched. data_layer values in CSV: {df['data_layer'].unique().tolist()}")
#         logger.warning(f"{IS_LAYER_COL} values in CSV: {df[IS_LAYER_COL].unique().tolist()}")
#         raise Exception(
#             f"No mapping found for source={SOURCE}, object={base_object}, "
#             f"layer={LAYER}, {IS_LAYER_COL}=TRUE"
#         )

#     rows = filtered.to_dict("records")
#     logger.info(f"Loaded {len(rows)} mapping rows for {base_object} [{LAYER}]")
#     return rows

# # ================= MAIN =================

# def main():
#     logger.info(f"Governance job started")
#     logger.info(f"LAYER:    {LAYER}")
#     logger.info(f"Catalog:  {S3_TABLES_CATALOG_ID}")
#     logger.info(f"Database: {DATABASE}")
#     logger.info(f"Tables:   {TABLES}")
#     logger.info(f"Filter:   {IS_LAYER_COL} = TRUE, data_layer = {LAYER}")

#     for table in TABLES:
#         try:
#             mapping = read_mapping(table)
#             run_lf_tagging(DATABASE, table, mapping)
#         except Exception as e:
#             logger.error(f"Governance failed for table '{table}': {e}")
#             raise

#     job.commit()
#     logger.info("Governance job complete")

# # ================= ENTRY =================

# if __name__ == "__main__":
#     main()