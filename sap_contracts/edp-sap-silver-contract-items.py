import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date
from pyspark.sql.types import DecimalType

# =========================================================
# 1. PARAMETERS
# =========================================================
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BRONZE_BUCKET",
        "CONFIG_BUCKET",
        "SOURCE",
        "ENTITY",
        "SILVER_DATABASE",
        "ACCOUNT_ID",
        "TABLE_BUCKET_NAME"
    ],
)

JOB_NAME = args["JOB_NAME"]
BRONZE_BUCKET = args["BRONZE_BUCKET"]
SOURCE = args["SOURCE"]
ENTITY = args["ENTITY"]
SILVER_DB = args["SILVER_DATABASE"]
ACCOUNT_ID = args["ACCOUNT_ID"]
TABLE_BUCKET_NAME = args["TABLE_BUCKET_NAME"]
CONFIG_BUCKET=args["CONFIG_BUCKET"]

# =========================================================
# 2. Spark Session (Iceberg Enabled)
# =========================================================
spark = (
    SparkSession.builder.appName(JOB_NAME)
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog","s3tables")
    .config("spark.sql.catalog.s3tables","org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.s3tables.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.s3tables.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}")
    .config("spark.sql.catalog.s3tables.warehouse", f"s3://{TABLE_BUCKET_NAME}/bucket/{TABLE_BUCKET_NAME}")
    .getOrCreate()
)

glue_context = GlueContext(spark.sparkContext)
job = Job(glue_context)
job.init(JOB_NAME, args)

# =========================================================
# 3. READ BRONZE JSON
# =========================================================
from pyspark.sql.functions import col

# ==========================================================
# Read Bronze JSON (DEV Safe Version)
# ==========================================================
bronze_path = f"s3://{BRONZE_BUCKET}/{SOURCE.lower()}/{ENTITY}/"
bronze_df = spark.read \
    .format("json") \
    .option("recursiveFileLookup", "true") \
    .load(bronze_path)

bronze_df = bronze_df.cache()   # Avoid Spark raw file restriction

total_count = bronze_df.count()
print("Total Records Read:", total_count)


# ==========================================================
# Simple Corrupt Record Check (DEV Only)
# ==========================================================
if "_corrupt_record" in bronze_df.columns:

    corrupt_count = bronze_df.filter(
        col("_corrupt_record").isNotNull()
    ).count()

    print("Total Corrupt Records:", corrupt_count)

    # Just remove corrupt records in DEV
    bronze_df = bronze_df.filter(
        col("_corrupt_record").isNull()
    ).drop("_corrupt_record")

clean_count = bronze_df.count()
print("Valid Records:", clean_count)

# =========================================================
# 4. FLATTEN HEADER
# =========================================================
header_df = bronze_df.select(
    "ContractNumber",
    "ContractDescription",
    to_date("ContractStartDate").alias("ContractStartDate"),
    "SalesOrganization",
    "DocCurrency",
    "SoldToParty",
    "SoldToPartyText",
    "BillToParty",
    "BillToPartyText",
    "Payer",
    "ShipToParty",
    "PrimaryProjectManager",
    "SalesOffice",
    "SalesGroup",
    "ClientMatter",
    "CustomerReference",
    "LSProjectCode",
    to_date("PricingDate").alias("PricingDate"),
    "HeaderBillingBlock",
    "MaterialContributionFlag",
    col("AnnualPriceEscOverridePercent").cast(DecimalType(5,2)).alias("AnnualPriceEscOverridePercent"),
    "AnnualPriceEscContractLanguage",
    "ProjectNumber"
)

# =========================================================
# 5. FLATTEN ITEMS
# =========================================================
item_df = bronze_df \
    .withColumn("item", explode("_ContractItems")) \
    .select(
        col("ContractNumber"),
        col("item.ItemId").alias("ItemId"),
        col("item.Material").alias("Material"),
        col("item.ItemDescription").alias("ItemDescription"),
        col("item.ItemCategory").alias("ItemCategory"),
        col("item.HighLevelItem").alias("HighLevelItem"),
        col("item.OrderQuantity").cast(DecimalType(18,3)).alias("OrderQuantity"),
        col("item.SalesUnit").alias("SalesUnit"),
        col("item.NetValue").cast(DecimalType(18,2)).alias("NetValue"),
        col("item.CustomerMaterial").alias("CustomerMaterial"),
        to_date(col("item.DismantlingDate")).alias("DismantlingDate"),
        col("item.RevenueClass").alias("RevenueClass"),
        col("item.ItemBillingBlock").alias("ItemBillingBlock"),
        col("item.ReasonForRejection").alias("ReasonForRejection"),
        col("item.ProfitCenter").alias("ProfitCenter"),
        col("item.MaterialGroup3").alias("MaterialGroup3"),
        col("item.GeneratedMaterialCode").alias("GeneratedMaterialCode"),
        col("item.ContributionRatioPercentage").cast(DecimalType(5,2)).alias("ContributionRatioPercentage"),
        col("item.AnnualPriceEscOverride").alias("AnnualPriceEscOverride")
    )

# =========================================================
# 6. DEFINE TABLE NAMES
# =========================================================
HEADER_TABLE = f"s3tables.{SILVER_DB}.contractheader"
ITEM_TABLE   = f"s3tables.{SILVER_DB}.contractitems"

# =========================================================
# 7. CREATE ICEBERG TABLES IF NOT EXISTS
# =========================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {HEADER_TABLE} (
    ContractNumber STRING,
    ContractDescription STRING,
    ContractStartDate DATE,
    SalesOrganization STRING,
    DocCurrency STRING,
    SoldToParty STRING,
    SoldToPartyText STRING,
    BillToParty STRING,
    BillToPartyText STRING,
    Payer STRING,
    ShipToParty STRING,
    PrimaryProjectManager STRING,
    SalesOffice STRING,
    SalesGroup STRING,
    ClientMatter STRING,
    CustomerReference STRING,
    LSProjectCode STRING,
    PricingDate DATE,
    HeaderBillingBlock STRING,
    MaterialContributionFlag STRING,
    AnnualPriceEscOverridePercent DECIMAL(5,2),
    AnnualPriceEscContractLanguage STRING,
    ProjectNumber STRING
)
USING iceberg
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ITEM_TABLE} (
    ContractNumber STRING,
    ItemId STRING,
    Material STRING,
    ItemDescription STRING,
    ItemCategory STRING,
    HighLevelItem STRING,
    OrderQuantity DECIMAL(18,3),
    SalesUnit STRING,
    NetValue DECIMAL(18,2),
    CustomerMaterial STRING,
    DismantlingDate DATE,
    RevenueClass STRING,
    ItemBillingBlock STRING,
    ReasonForRejection STRING,
    ProfitCenter STRING,
    MaterialGroup3 STRING,
    GeneratedMaterialCode STRING,
    ContributionRatioPercentage DECIMAL(5,2),
    AnnualPriceEscOverride STRING
)
USING iceberg
""")

# =========================================================
# 8. WRITE INTO S3 TABLES
# =========================================================
print("Writing Header Table...")
header_df = header_df.toDF(*[c.lower().strip() for c in header_df.columns])
item_df   = item_df.toDF(*[c.lower().strip() for c in item_df.columns])

header_df.printSchema()
header_df.writeTo(HEADER_TABLE).append()


print("Writing Item Table...")
item_df.printSchema()
item_df.writeTo(ITEM_TABLE).append()



print("Iceberg Load Completed Successfully")

job.commit()
