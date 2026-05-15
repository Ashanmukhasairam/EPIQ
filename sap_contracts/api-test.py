import sys
import requests
from datetime import datetime
from calendar import monthrange
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# =========================================================
# 1. GLUE INIT
# =========================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
print("========== SAP ODATA → BRONZE LOAD STARTED ==========")
print("Start Time:", datetime.utcnow())

# =========================================================
# 2. CONFIG
# =========================================================
BASE_URL = "https://saphubdev.epiqglobal.com/sap/opu/odata4/sap/zsb_contracts_api/srvd_a2x/sap/zsd_contracts_api/0001/ZSD_API_CONTRACT_LIST"
USERNAME = "EDP_SAP_API"
PASSWORD = "FaeXxqEYlvtKAxtSwZTbcbFtQoLymaBs4YSb<KKm"
BRONZE_BUCKET = "s3://epiq-edp-dl-dev-bronze/sap/"
START_YEAR = 2020
END_YEAR = 2020
PAGE_SIZE = 2000

# =========================================================
# 3. FETCH FUNCTION
# =========================================================

def fetch_month_data(year, month):
    last_day = monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day}"
    all_records = []
    skip = 0
    print(f"\nFetching data for {year}-{month:02d}")
    while True:
        params = {
            "$filter": f"ContractDate ge {start_date} and ContractDate le {end_date} and attribute eq 'CREATE'",
            "$select": "ContractNumber,SalesOrganization,CreatedDate,ChangedDate",
            "$top": str(PAGE_SIZE),
            "$skip": str(skip),
            "$format": "json"
        }
        response = requests.get(
            BASE_URL,
            params=params,
            auth=(USERNAME, PASSWORD),
            headers={"Accept": "application/json"},
            timeout=300
        )
        if response.status_code != 200:
            print("SAP ERROR STATUS:", response.status_code)
            print("SAP ERROR RESPONSE:", response.text)
            break
        data = response.json()
        records = data.get("value", [])
        if not records:
            break
        all_records.extend(records)
        print(f"Fetched {len(records)} records (skip={skip})")
        if len(records) < PAGE_SIZE:
            skip += PAGE_SIZE
            break
        skip += PAGE_SIZE
    print(f"Total records for {year}-{month:02d}: {len(all_records)}")
    return all_records

# =========================================================
# 4. MAIN LOOP — Collect ALL records first
# =========================================================
all_records_combined = []

for year in range(START_YEAR, END_YEAR + 1):
    for month in range(1, 13):
        records = fetch_month_data(year, month)
        if records:
            all_records_combined.extend(records)
            print(f"Accumulated total so far: {len(all_records_combined)} records")

print(f"\nTotal records fetched across all months: {len(all_records_combined)}")

# =========================================================
# 5. WRITE SINGLE CSV FILE
# =========================================================
if all_records_combined:
    spark_df = spark.createDataFrame(all_records_combined)

    output_path = f"{BRONZE_BUCKET}all_contracts/create2020_folder"

    spark_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

    print(f"All records written to: {output_path}")
else:
    print("No records found across the entire date range. Nothing written.")

# =========================================================
# 6. JOB COMMIT
# =========================================================
job.commit()
print("========== JOB COMPLETED SUCCESSFULLY ==========")
print("End Time:", datetime.utcnow())