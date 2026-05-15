"""
Glue Job: edp-salesforce-gold-account-demo

Business Logic:
- Only one active record per account
- If account data changes, close old record and create new one
- If no change, do nothing
- New accounts are inserted
"""

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# ==================================================
# READ JOB PARAMETERS
# ==================================================
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_DATABASE",
        "SILVER_TABLE",
        "GOLD_DATABASE",
        "GOLD_TABLE",
        "ICEBERG_WAREHOUSE"
    ]
)

SILVER_FQN = f"glue_catalog.{args['SILVER_DATABASE']}.{args['SILVER_TABLE']}"
GOLD_FQN   = f"glue_catalog.{args['GOLD_DATABASE']}.{args['GOLD_TABLE']}"
ICEBERG_WAREHOUSE = args["ICEBERG_WAREHOUSE"]

# ==================================================
# SPARK SESSION – ICEBERG CONFIG
# ==================================================
spark = (
    SparkSession.builder
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)
    .config("spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .getOrCreate()
)

# ==================================================
# INIT GLUE JOB
# ==================================================
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ==================================================
# READ SILVER
# ==================================================
silver_df = spark.read.table(SILVER_FQN)

if silver_df.rdd.isEmpty():
    job.commit()
    sys.exit(0)

# ==================================================
# LATEST RECORD PER ID
# ==================================================
window_spec = (
    Window.partitionBy("id")
    .orderBy(
        col("ingestion_ts").desc(),
        col("last_modified_date").desc()
    )
)

silver_latest = (
    silver_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
    .dropDuplicates(["id"])
)

silver_latest.createOrReplaceTempView("silver_src")

# ==================================================
# BUSINESS COLUMNS
# ==================================================
business_columns = [
"id","is_deleted","name","parent_id","type","record_type_id","status",
"start_date","end_date","currency_iso_code","expected_revenue",
"budgeted_cost","actual_cost","expected_response","number_sent","is_active",
"description","campaign_image_id","tenant_id","number_of_leads",
"number_of_converted_leads","number_of_contacts","number_of_responses",
"number_of_opportunities","number_of_won_opportunities",
"amount_all_opportunities","amount_won_opportunities",
"hierarchy_number_of_leads","hierarchy_number_of_converted_leads",
"hierarchy_number_of_contacts","hierarchy_number_of_responses",
"hierarchy_number_of_opportunities","hierarchy_number_of_won_opportunities",
"hierarchy_amount_all_opportunities","hierarchy_amount_won_opportunities",
"hierarchy_number_sent","hierarchy_expected_revenue",
"hierarchy_budgeted_cost","hierarchy_actual_cost","owner_id","created_date",
"created_by_id","last_modified_date","last_modified_by_id","system_modstamp",
"last_activity_date","last_viewed_date","last_referenced_date",
"campaign_member_record_type_id","business_unit_c",
"cr_legacy_campaign_id_c","db_campaign_tactic_old_c","dm_cam_external_id_c",
"data_loader_id_c","data_quality_description_c","data_quality_score_c",
"external_id_c","ls_legacy_campaign_id_c","ms_legacy_campaign_id_c",
"business_unit_epiq_c","epiq_id_c","approved_cle_topic_c","owner_profile_c",
"cle_application_completed_c","cle_attendance_certificates_created_c",
"cle_attendance_filed_with_state_bar_c","cle_attendee_list_received_c",
"cle_attendee_template_to_ad_c","cle_attendees_added_to_campaign_c",
"cle_backup_presenter_c","cle_catalog_category_c","cle_client_c",
"cle_event_actual_total_cost_c","cle_event_budget_c","region_c",
"cle_fee_cost_c","cle_jurisdiction_c","cle_marketing_invite_created_c",
"cle_material_submitted_c","cle_name_c","cle_presentation_sent_to_sme_c",
"cle_primary_presenter_c","cle_requested_date_c","cle_stage_c",
"cle_state_bar_approval_c","cle_timing_for_approval_c",
"confirmed_number_of_cle_attendees_c","final_cle_presenter_s_c",
"cle_event_location_c","proposed_local_c","requested_course_not_in_catalog_c",
"sme_is_ready_for_cle_event_c","approved_cle_course_not_in_catalog_c",
"owner_s_manager_c","ls_sales_team_c","state_is_self_reporting_c",
"additional_cle_details_c","is_there_a_partner_involved_c","involved_partner_c",
"cle_event_date_c","cle_subcategory_c","who_will_host_virtual_cle_event_c",
"who_will_be_filling_admin_cle_paperwork_c","requested_cle_presenter_c",
"email_campaign_subtype_c","campaign_influence_total_revenue_c",
"litigation_c","account_c","service_line_c"
]

change_detection_condition = " AND ".join(
    [f"tgt.{c} <=> src.{c}" for c in business_columns if c != "id"]
)

# ==================================================
# STEP 1 – EXPIRE CHANGED
# ==================================================
spark.sql(f"""
MERGE INTO {GOLD_FQN} tgt
USING silver_src src
ON tgt.id = src.id
AND tgt.is_current = true

WHEN MATCHED AND NOT ({change_detection_condition})
THEN UPDATE SET
  tgt.is_current = false,
  tgt.effective_end_ts = src.last_modified_date
""")

# ==================================================
# STEP 2 – INSERT NEW / UPDATED
# ==================================================
insert_cols = ",".join([f"src.{c}" for c in business_columns])

spark.sql(f"""
INSERT INTO {GOLD_FQN}
SELECT
 {insert_cols},
 src.ingestion_dt,
 src.ingestion_ts,
 src.source_system,
 src.last_modified_date,
 TIMESTAMP '9999-12-31 23:59:59',
 true
FROM silver_src src
LEFT JOIN {GOLD_FQN} tgt
  ON src.id = tgt.id
 AND tgt.is_current = true
WHERE tgt.id IS NULL
""")

# ==================================================
# COMMIT
# ==================================================
job.commit()
print("Gold job completed successfully")
