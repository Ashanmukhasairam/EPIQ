from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("employee-iceberg-to-s3tables")
    # ---------------------------------------------------
    # ICEBERG CORE
    # ---------------------------------------------------
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2"
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # ---------------------------------------------------
    # DESTINATION → S3 TABLE BUCKET (REST)
    # ---------------------------------------------------
    .config("spark.sql.catalog.s3_rest_catalog",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.s3_rest_catalog.type", "rest")
    .config("spark.sql.catalog.s3_rest_catalog.uri",
            "https://s3tables.us-east-1.amazonaws.com/iceberg")
    .config("spark.sql.catalog.s3_rest_catalog.warehouse",
            "arn:aws:s3tables:us-east-1:730878889077:bucket/test")
    .config("spark.sql.catalog.s3_rest_catalog.rest.sigv4-enabled", "true")
    .config("spark.sql.catalog.s3_rest_catalog.rest.signing-name", "s3tables")
    .config("spark.sql.catalog.s3_rest_catalog.rest.signing-region", "us-east-1")
    .config("spark.sql.catalog.s3_rest_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")

    # ---------------------------------------------------
    # SOURCE → GLUE ICEBERG
    # ---------------------------------------------------
    .config("spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.warehouse",
            "s3://epiq-edp-dl-dev-bronze/fivetran/salesforce/")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

# ---------------------------------------------------
# SOURCE TABLE
# ---------------------------------------------------
source_db = "salesforce"
source_table = "contact"

# ---------------------------------------------------
# DESTINATION TABLE
# ---------------------------------------------------
dest_db = "stage_db"
dest_table = "stg_salesforce_contact"

# ---------------------------------------------------
# STEP 1 — CREATE DESTINATION TABLE (SQL ONLY)
# (NO CTAS, NO LOCATION)
# ---------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS
s3_rest_catalog.{dest_db}.{dest_table} (

  id STRING,
  is_deleted BOOLEAN,
  master_record_id STRING,
  account_id STRING,
  last_name STRING,
  first_name STRING,
  salutation STRING,
  name STRING,
  record_type_id STRING,

  other_street STRING,
  other_city STRING,
  other_state STRING,
  other_postal_code STRING,
  other_country STRING,
  other_state_code STRING,
  other_country_code STRING,
  other_latitude DOUBLE,
  other_longitude DOUBLE,
  other_geocode_accuracy STRING,

  mailing_street STRING,
  mailing_city STRING,
  mailing_state STRING,
  mailing_postal_code STRING,
  mailing_country STRING,
  mailing_state_code STRING,
  mailing_country_code STRING,
  mailing_latitude DOUBLE,
  mailing_longitude DOUBLE,
  mailing_geocode_accuracy STRING,

  phone STRING,
  fax STRING,
  mobile_phone STRING,
  home_phone STRING,
  other_phone STRING,
  assistant_phone STRING,
  reports_to_id STRING,
  email STRING,
  title STRING,
  department STRING,
  assistant_name STRING,
  lead_source STRING,

  birthdate DATE,
  description STRING,
  currency_iso_code STRING,
  owner_id STRING,

  has_opted_out_of_email BOOLEAN,
  has_opted_out_of_fax BOOLEAN,
  do_not_call BOOLEAN,
  can_allow_portal_self_reg BOOLEAN,

  created_date TIMESTAMP,
  created_by_id STRING,
  last_modified_date TIMESTAMP,
  last_modified_by_id STRING,
  system_modstamp TIMESTAMP,

  last_activity_date DATE,
  last_curequest_date TIMESTAMP,
  last_cuupdate_date TIMESTAMP,
  last_viewed_date TIMESTAMP,
  last_referenced_date TIMESTAMP,

  email_bounced_reason STRING,
  email_bounced_date TIMESTAMP,
  is_email_bounced BOOLEAN,
  photo_url STRING,
  jigsaw STRING,
  jigsaw_contact_id STRING,
  connection_received_id STRING,
  connection_sent_id STRING,
  individual_id STRING,

  first_call_date_time TIMESTAMP,
  first_email_date_time TIMESTAMP,
  activity_metric_id STRING,
  is_priority_record BOOLEAN,
  contact_source STRING,
  business_unit_text_c STRING,
  region_based_on_countries_c STRING,
  converted_from_lead_c BOOLEAN,
  rfp_form_fill_date_c DATE,

  mkt_ops_enrichment_c STRING,
  bas_tier_c STRING,
  bas_minimum_salary_c DECIMAL(38,10),
  bas_minimum_hourly_c DECIMAL(38,10),
  bas_status_c STRING,
  referred_by_c STRING,
  file_number_c STRING,
  legacy_database_c STRING,
  legacy_created_by_c STRING,
  legacy_created_date_c DATE,
  legacy_invalid_email_c STRING,

  k_c BOOLEAN,
  dh_c BOOLEAN,
  kdh_c STRING,
  currency_c STRING,
  direct_dial_c STRING,
  legacy_people_soft_id_c STRING,
  other_email_c STRING,
  email_address_type_c STRING,
  preferred_first_name_c STRING,
  legacy_owner_c STRING,

  cloudingo_agent_mar_c STRING,
  cloudingo_agent_mas_c STRING,
  cloudingo_agent_mav_c STRING,
  cloudingo_agent_mrdi_c STRING,
  cloudingo_agent_mtz_c STRING,
  cloudingo_agent_oar_c STRING,
  cloudingo_agent_oas_c STRING,
  cloudingo_agent_oav_c STRING,
  cloudingo_agent_ordi_c STRING,
  cloudingo_agent_otz_c STRING,

  tl_training_lvl_c STRING,
  add_a_note_c STRING,
  assistant_name_c STRING,
  contact_role_c STRING,
  created_date_c TIMESTAMP,
  on_24_attended_live_c STRING,
  dear_c STRING,
  on_24_attended_on_demand_c STRING,
  direct_line_c STRING,
  on_24_registered_c STRING,

  rfp_reason_for_inquiry_c STRING,
  laptop_id_c STRING,
  on_24_event_name_c STRING,
  client_preferred_c STRING,
  no_centralized_marketing_c STRING,
  tl_trainings_completed_c STRING,
  nickname_c STRING,
  case_name_c STRING,
  owner_id_c STRING,
  position_c STRING,

  preferred_time_to_receive_call_c STRING,
  preferred_vendor_c STRING,
  record_status_c STRING,
  referral_notes_c STRING,
  tl_rate_range_c STRING,
  attorney_ranking_c STRING,
  gift_sent_c STRING,
  inactive_c STRING,
  mtmp_email_blast_c STRING,
  mt_protected_contact_c STRING,

  external_referral_name_c STRING,
  related_to_campaign_c STRING,
  designated_tl_c STRING,
  relationship_status_c STRING,
  pentagon_role_c STRING,
  ext_c STRING,
  focus_c STRING,
  initiative_c STRING,
  interests_c STRING,
  no_longer_w_account_c BOOLEAN,

  protected_contact_c STRING,
  suffix_c STRING,
  type_of_contact_c STRING,
  business_lines_c STRING,
  linkedin_profile_c STRING,
  inside_sales_connect_c STRING,
  year_admitted_c STRING,
  legacy_jobscience_id_c STRING,
  primary_practice_area_c STRING,
  ls_sales_play_role_c STRING,

  lid_languages_c STRING,
  contact_status_c STRING,
  prospect_type_c STRING,
  business_unit_c STRING,
  initial_sequence_date_c DATE,
  outreach_actively_being_sequenced_c BOOLEAN,
  outreach_current_sequence_id_c STRING,
  outreach_current_sequence_name_c STRING,
  outreach_current_sequence_status_c STRING,
  outreach_current_sequence_step_number_c DOUBLE,

  outreach_current_sequence_step_type_c STRING,
  outreach_current_sequence_task_due_date_c TIMESTAMP,
  outreach_current_sequence_user_id_c STRING,
  outreach_date_added_to_sequence_c DATE,
  outreach_finished_sequences_c STRING,
  outreach_initial_sequence_id_c STRING,
  outreach_initial_sequence_name_c STRING,
  outreach_number_of_active_sequences_c STRING,
  outreach_number_of_active_tasks_c DOUBLE,
  current_active_campaign_c STRING,

  contact_department_c STRING,
  contact_title_c STRING,
  lid_level_c STRING,
  lid_linked_in_company_id_c STRING,
  lid_linked_in_member_token_c STRING,
  lid_no_longer_at_company_c STRING,
  persona_c STRING,
  contact_marketing_cta_c STRING,
  management_level_c STRING,
  sic_c STRING,

  naics_c STRING,
  microsoft_role_c STRING,
  tl_training_completed_c BOOLEAN,
  workday_id_c STRING,
  campaignlead_c BOOLEAN,
  date_of_campaignlead_c DATE,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP
)

PARTITIONED BY (_fivetran_synced)

""")

# ---------------------------------------------------
# STEP 2 — READ SOURCE ICEBERG
# ---------------------------------------------------

# df = spark.read.format("iceberg").load(
#     "glue_catalog.demo_db.employee$snapshots"
# )

df = spark.table(f"glue_catalog.{source_db}.{source_table}")
df.show(truncate=False)

df.show()

# ---------------------------------------------------
# STEP 3 — APPEND DATA
# ---------------------------------------------------
df.writeTo(f"s3_rest_catalog.{dest_db}.{dest_table}").append()

# ---------------------------------------------------
# VERIFY
# ---------------------------------------------------
spark.sql(f"""
SELECT COUNT(*)
FROM s3_rest_catalog.{dest_db}.{dest_table}
""").show()
