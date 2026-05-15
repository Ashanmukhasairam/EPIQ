import sys
import json
import boto3
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import time

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# ==========================================================
# GLUE INIT
# ==========================================================
print("\n========== GLUE JOB START ==========")

job_start = datetime.now()

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("Spark Version:", spark.version)
print("Start Time:", job_start)

# ==========================================================
# 1️⃣ READ GLUE PARAMETERS
# ==========================================================
args = getResolvedOptions(sys.argv, ['start_date', 'end_date', 'secret_name'])

FROM_DATE = args['start_date']
TO_DATE   = args['end_date']
secret_name = args['secret_name']

print("Start Date:", FROM_DATE)
print("End Date:", TO_DATE)
print("Secret Name:", secret_name)

# ==========================================================
# 2️⃣ GET CREDENTIALS FROM SECRETS MANAGER
# ==========================================================

client = boto3.client("secretsmanager")
secret_response = client.get_secret_value(SecretId=secret_name)
secret = json.loads(secret_response["SecretString"])

USERNAME = secret["username"]
PASSWORD = secret["password"]

print("Successfully retrieved SAP credentials")

# ======================================================
# CONFIG
# ======================================================
HOST = "https://saphubdev.epiqglobal.com"

SERVICE_ROOT = (
    "/sap/opu/odata4/sap/zsb_contracts_api/srvd_a2x/"
    "sap/zsd_contracts_api/0001"
)

ENTITY_SET = "ZSD_API_CONTRACT_LIST"
METADATA_URL = HOST + SERVICE_ROOT + "/$metadata"
SERVICE_URL = HOST + SERVICE_ROOT + f"/{ENTITY_SET}"

S3_BASE_PATH = "s3://epiq-edp-dl-dev-bronze/sap/contracts_list/"
NUM_OUTPUT_FILES = 10

# ======================================================
# HTTP RETRY
# ======================================================
def request_with_retry(url, params=None, retries=4, headers=None):

    if headers is None:
        headers = {"Accept": "application/json"}

    for attempt in range(retries):
        try:
            print("\nCalling:", url)

            response = requests.get(
                url,
                params=params,
                auth=HTTPBasicAuth(USERNAME, PASSWORD),
                headers=headers,
                timeout=120
            )

            print("Status:", response.status_code)

            if response.status_code == 200:
                return response

            if response.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue

            raise Exception(f"SAP Error {response.status_code}: {response.text[:300]}")

        except requests.exceptions.RequestException:
            time.sleep(2 ** attempt)

    raise Exception("Max retries reached contacting SAP")

# ======================================================
# GET METADATA FIELDS
# ======================================================
def get_entity_fields():

    print("\nReading metadata...")

    response = request_with_retry(METADATA_URL, headers={"Accept": "application/xml"})

    ns = {"edmx":"http://docs.oasis-open.org/odata/ns/edmx","edm":"http://docs.oasis-open.org/odata/ns/edm"}
    root = ET.fromstring(response.text)

    entity_type_name = None
    for container in root.findall(".//edm:EntityContainer", ns):
        for entityset in container.findall("edm:EntitySet", ns):
            if entityset.attrib.get("Name") == ENTITY_SET:
                entity_type_name = entityset.attrib.get("EntityType")

    entity_type_name = entity_type_name.split(".")[-1]

    fields=[]
    for schema in root.findall(".//edm:Schema", ns):
        for entity in schema.findall("edm:EntityType", ns):
            if entity.attrib.get("Name")==entity_type_name:
                for prop in entity.findall("edm:Property", ns):
                    fields.append(prop.attrib.get("Name"))

    print("Total Fields:", len(fields))
    return ",".join(fields)

# ======================================================
# MONTH RANGE GENERATOR
# ======================================================
def generate_month_ranges(start_date, end_date):

    start = datetime.strptime(start_date,"%Y-%m-%d")
    end   = datetime.strptime(end_date,"%Y-%m-%d")

    ranges=[]
    current=start

    while current<=end:
        next_month=(current.replace(day=28)+timedelta(days=4)).replace(day=1)
        range_end=min(next_month-timedelta(days=1),end)
        ranges.append((current.strftime("%Y-%m-%d"),range_end.strftime("%Y-%m-%d")))
        current=next_month

    print("Total Monthly Batches:",len(ranges))
    return ranges

def to_odata_datetime(date_str, end_of_day=False):
    if end_of_day:
        return f"datetimeoffset'{date_str}T23:59:59Z'"
    return f"datetimeoffset'{date_str}T00:00:00Z'"

# ======================================================
# FETCH SAP DATA
# ======================================================
def fetch_all_data():

    select_fields=get_entity_fields()
    all_records=[]

    for start,end in generate_month_ranges(FROM_DATE,TO_DATE):

        print(f"\nProcessing {start} -> {end}")
        
        start_dt = to_odata_datetime(start)
        end_dt   = to_odata_datetime(end, end_of_day=True)
        
        filter_query = (
            f"(ChangedDate ge {start_dt} and ChangedDate le {end_dt}) "
            f"or (ChangedDate eq null and CreatedDate ge {start_dt} and CreatedDate le {end_dt})"
        )

        url=SERVICE_URL
        params={
            "$filter": filter_query,
            "$select": select_fields,
            "$format": "json"
        }

        while url:
            response=request_with_retry(url,params=params)
            data=response.json()

            records=data.get("value",[])
            print("Records:",len(records))
            all_records.extend(records)

            next_link=data.get("@odata.nextLink")
            if next_link:
                url=urljoin(HOST,next_link)
                params=None
            else:
                url=None

    print("Total Records Pulled:",len(all_records))
    return all_records

# ======================================================
# MAIN
# ======================================================
records=fetch_all_data()

if not records:
    print("No data returned")
    sys.exit(0)

df=spark.createDataFrame(records)

print("Final Row Count:",df.count())

# ======================================================
# BUILD OUTPUT PATH (YYYY/MM/DD/HH)
# ======================================================
ingest_time = datetime.now()

YEAR  = ingest_time.strftime("%Y")
MONTH = ingest_time.strftime("%m")
DAY   = ingest_time.strftime("%d")
HOUR  = ingest_time.strftime("%H")

output_path = f"{S3_BASE_PATH}{YEAR}/{MONTH}/{DAY}/{HOUR}/"

print("Writing Bronze Data to:",output_path)

# ======================================================
# WRITE CHUNKED FILES
# ======================================================
print("Writing", NUM_OUTPUT_FILES, "chunk files...")

(
    df.repartition(NUM_OUTPUT_FILES)
      .write
      .mode("overwrite")
      .json(output_path)
)

print("\n========== JOB COMPLETED ==========")
print("Runtime:",datetime.now()-job_start)
