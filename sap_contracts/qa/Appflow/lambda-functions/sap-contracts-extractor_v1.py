import json
import boto3
import urllib.request
import urllib.parse
import base64
import time
import os
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================================
# CONFIG
# ================================
MAX_WORKERS    = 10
RETRY_COUNT    = 3
BACKOFF_FACTOR = 2
API_TIMEOUT    = 60

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ================================
# CREDENTIALS
# ================================
def get_sap_credentials(env):
    client = boto3.client('secretsmanager', region_name='us-east-1')
    if env == "qa":
        secret_id = "sap/qa/contracts_api/credentials"
    else:
        secret_id = "sap/dev/odata/credentials"
    print(f"Using secret: {secret_id}")
    secret = client.get_secret_value(SecretId=secret_id)
    return json.loads(secret['SecretString'])


# ================================
# SAP API CALL
# ================================
def call_sap_api(url, username, password):
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    req = urllib.request.Request(url)
    req.add_header('Authorization', f'Basic {credentials}')
    req.add_header('Accept', 'application/json')
    with urllib.request.urlopen(req, timeout=API_TIMEOUT) as response:
        return json.loads(response.read().decode())


# ================================
# WATERMARK
# ================================
def get_watermark_date():
    athena = boto3.client('athena', region_name='us-east-1')
    query  = """
        SELECT MAX(last_successful_watermark)
        FROM edp_configs_dev.edp_incremental_control
        WHERE table_name = 'contracts'
    """
    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': 's3://aws-athena-query-results-us-east-1-730878889077/'
        }
    )
    query_execution_id = response['QueryExecutionId']
    print(f"Athena query started: {query_execution_id}")

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state  = status['QueryExecution']['Status']['State']
        print(f"Athena query state: {state}")
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if state != 'SUCCEEDED':
        raise Exception("Athena query failed")

    result          = athena.get_query_results(QueryExecutionId=query_execution_id)
    rows            = result['ResultSet']['Rows']
    watermark_value = rows[1]['Data'][0].get('VarCharValue', None)
    print(f"Watermark: {watermark_value}")

    if watermark_value and watermark_value != '':
        return (
            datetime.strptime(watermark_value[:10], '%Y-%m-%d') + timedelta(days=1)
        ).strftime('%Y-%m-%d')
    else:
        return (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')


# ================================
# 90-DAY CHUNKS
# ================================
def build_date_chunks(start_date_str, end_date_str):
    start  = datetime.strptime(start_date_str, "%Y-%m-%d")
    end    = datetime.strptime(end_date_str,   "%Y-%m-%d")
    chunks = []
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=89), end)
        chunks.append((
            chunk_start.strftime('%Y-%m-%d'),
            chunk_end.strftime('%Y-%m-%d')
        ))
        chunk_start = chunk_end + timedelta(days=1)
    print(f"Total 90-day chunks: {len(chunks)}")
    return chunks


# ================================
# CONTRACT LIST — returns full list
# ================================
def get_contract_list(creds, start_date, end_date):
    base_url      = creds['sap_base_url']
    sap_path      = creds['sap_path']
    chunks        = build_date_chunks(start_date, end_date)
    all_contracts = []

    for (chunk_start, chunk_end) in chunks:
        print(f"Processing chunk: {chunk_start} → {chunk_end}")
        for attribute in ['CREATE', 'CHANGE']:
            filter_str = (
                f"ContractDate ge {chunk_start} and "
                f"ContractDate le {chunk_end} and "
                # f"ContractTime ge 00:00:00 and "
                # f"ContractTime le 23:59:59 and "
                f"attribute eq '{attribute}'"
            )
            url = (
                f"{base_url}{sap_path}ZSD_API_CONTRACT_LIST"
                f"?$filter={urllib.parse.quote(filter_str)}"
                f"&$select=ContractNumber,SalesOrganization,ContractDate,CreatedDate,ChangedDate,attribute"
                f"&$top=5000&$format=json"
            )
            try:
                response = call_sap_api(url, creds['username'], creds['password'])
                records  = response.get('value', [])
                print(f"{chunk_start}→{chunk_end} | {attribute} | {len(records)} records")
                for record in records:
                    record['attribute'] = attribute
                all_contracts.extend(records)
            except Exception as e:
                print(f"Error {chunk_start}→{chunk_end} {attribute}: {e}")

    print(f"Total contracts: {len(all_contracts)}")
    return all_contracts


# ================================
# CONTRACT DETAILS — single
# ================================
def get_contract_details(contract_number, creds):
    base_url = creds['sap_base_url']
    url = (
        f"{base_url}/sap/opu/odata4/sap/zsb_contracts_data_api/srvd_a2x/sap/zsd_contracts_data_api/0001/"
        f"ContractHeader('{contract_number}')?$expand=_ContractItems&$format=json"
    )
    return call_sap_api(url, creds['username'], creds['password'])


def get_contract_details_with_retry(contract_number, creds):
    delay = 2
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            return get_contract_details(contract_number, creds)
        except Exception as e:
            print(f"Retry {attempt} for {contract_number}: {e}")
            if attempt == RETRY_COUNT:
                return None
            time.sleep(delay)
            delay *= BACKOFF_FACTOR


# ================================
# PARALLEL FETCH — 10 at a time
# with 1 second gap between batches
# ================================
def fetch_details_parallel(contracts, creds):
    contract_numbers = list(set([
        c.get('ContractNumber') for c in contracts
        if c.get('ContractNumber')
    ]))

    total = len(contract_numbers)
    print(f"====================================")
    print(f"Fetching details for {total} contracts")
    print(f"====================================")

    all_details  = []
    batch_number = 0

    for i in range(0, total, 10):
        batch        = contract_numbers[i:i + 10]
        batch_number += 1

        print(f"Batch {batch_number} started: {batch}")

        batch_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(get_contract_details_with_retry, cn, creds): cn
                for cn in batch
            }
            for future in as_completed(futures):
                cn     = futures[future]
                result = future.result()
                if result:
                    batch_results.append(result)
                    print(f"Fetched: {cn}")
                else:
                    print(f"Failed:  {cn}")

        all_details.extend(batch_results)
        print(f"Batch {batch_number} done — {len(batch_results)}/{len(batch)} successful")
        print(f"Total fetched so far: {len(all_details)}/{total}")

        if i + 10 < total:
            print(f"Waiting 1 second before next batch...")
            time.sleep(1)

    print(f"====================================")
    print(f"ALL DONE — Total: {len(all_details)}")
    print(f"====================================")

    return all_details


# ================================
# LAMBDA HANDLER
# ================================
def lambda_handler(event, context):
    print("==== EXTRACTOR START ====")
    print("EVENT:", json.dumps(event))

    try:
        env    = event.get('env', 'dev')
        entity = event.get('entity', '')
        offset = int(event.get('offset', 0))
        limit  = int(event.get('limit', 50))      # ← default 50
        creds  = get_sap_credentials(env)

        # ── Date range ──
        load_type = os.environ.get("LOAD_TYPE", "INCR").upper()
        if load_type == "FULL":
            start_date = os.environ.get("START_DATE")
            end_date   = os.environ.get("END_DATE")
            if not start_date or not end_date:
                raise Exception("START_DATE and END_DATE required for FULL load")
            print(f"[FULL LOAD] {start_date} to {end_date}")
        else:
            start_date = get_watermark_date()
            end_date   = datetime.now().strftime('%Y-%m-%d')
            print(f"[INCR LOAD] {start_date} to {end_date}")

        print(f"ENV={env} | ENTITY={entity} | OFFSET={offset} | LIMIT={limit}")

        # ── ContractList ──
        if entity == "ContractList":
            print(f"Fetching ContractList page offset={offset} limit={limit}")
            all_contracts = get_contract_list(creds, start_date, end_date)

            page     = all_contracts[offset:offset + limit]
            has_more = (offset + limit) < len(all_contracts)

            print(f"Page size: {len(page)} | has_more: {has_more}")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'contract_list': {'value': page},
                    'has_more':      has_more
                })
            }

        # ── ContractDetails ──
        elif entity == "ContractDetails":
            print(f"Fetching ContractDetails page offset={offset} limit={limit}")
            all_contracts = get_contract_list(creds, start_date, end_date)

            page_contracts = all_contracts[offset:offset + limit]
            has_more       = (offset + limit) < len(all_contracts)

            print(f"Contracts in this page: {len(page_contracts)} | has_more: {has_more}")

            all_details = fetch_details_parallel(page_contracts, creds)

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'contract_details': all_details,
                    'has_more':         has_more
                })
            }

        else:
            raise Exception(f"Unknown entity: {entity}")

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }