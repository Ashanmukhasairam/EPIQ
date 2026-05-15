import json
import boto3
import urllib.request
import urllib.parse
import base64
import uuid
from datetime import datetime, timedelta


# def get_sap_credentials(env):
#    client = boto3.client('secretsmanager', region_name='us-east-1')
#    if env == "qa":
#        secret_id = "sap/qa/contracts_api/credentials"
#    else:
#        secret_id = "sap/dev/odata/credentials"
#    print(f"Using secret: {secret_id}")
#    secret = client.get_secret_value(SecretId=secret_id)
#    return json.loads(secret['SecretString'])

def get_sap_credentials(env):
    client = boto3.client('secretsmanager', region_name='us-east-1')
    
    secret_id = "sap/qa/contracts_api/credentials"
    
    print(f"Using secret: {secret_id}")
    
    secret = client.get_secret_value(SecretId=secret_id)
    return json.loads(secret['SecretString'])

def call_sap_api(url, username, password):
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    req = urllib.request.Request(url)
    req.add_header('Authorization', f'Basic {credentials}')
    req.add_header('Accept', 'application/json')
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.loads(response.read().decode())

def get_watermark_date():
    athena = boto3.client('athena', region_name='us-east-1')  # change region if needed
    
    query = """
        SELECT MAX(last_successful_watermark)
        FROM edp_configs_dev.edp_incremental_control
        WHERE table_name = 'contracts'
    """
    
    # Run the query
    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': 's3://aws-athena-query-results-us-east-1-730878889077/'  # <-- change this
        }
    )
    
    query_execution_id = response['QueryExecutionId']
    print(f"Athena query started: {query_execution_id}")
    
    # Wait for query to complete
    import time
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        print(f"Athena query state: {state}")
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)
    
    if state != 'SUCCEEDED':
        error_reason = status['QueryExecution']['Status'].get('StateChangeReason', 'No reason provided')
        print(f"Athena failure reason: {error_reason}")
        raise Exception(f"Athena query failed with state: {state}. Reason: {error_reason}")
    
    # Get the result
    result = athena.get_query_results(QueryExecutionId=query_execution_id)
    rows = result['ResultSet']['Rows']
    
    # Row 0 is header, Row 1 is actual value
    watermark_value = rows[1]['Data'][0].get('VarCharValue', None)
    print(f"Watermark from Athena: {watermark_value}")
    
    from datetime import datetime, timedelta

    if watermark_value and watermark_value != '':
        # Add 1 day so we start from the next day after the last watermark
        start_date = (datetime.strptime(watermark_value[:10], '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        print("No watermark found, falling back to last 30 days")
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    print(f"Using start_date: {start_date}")
    return start_date


def get_contract_list(creds):
    base_url = creds['sap_base_url']
    sap_path = creds['sap_path']
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = get_watermark_date()

    # NEW: time filters
    start_time = "00:00:00"
    end_time = "23:59:59"

    all_contracts = []

    for attribute in ['CREATE', 'CHANGE']:

        # UPDATED FILTER
        filter_str = (
            f"ContractDate ge {start_date} and "
            f"ContractDate le {end_date} and "
            f"ContractTime ge {start_time} and "
            f"ContractTime le {end_time} and "
            f"attribute eq '{attribute}'"
        )

        encoded_filter = urllib.parse.quote(filter_str)

        url = (
            f"{base_url}{sap_path}"
            f"ZSD_API_CONTRACT_LIST"
            f"?$filter={encoded_filter}"
            f"&$select=ContractNumber,SalesOrganization,ContractDate,CreatedDate,ChangedDate,ContractTime,attribute"
            f"&$top=5000&$format=json"
        )

        print(f"Calling ContractList for attribute={attribute}: {url}")

        response = call_sap_api(url, creds['username'], creds['password'])
        records = response.get('value', [])

        print(f"attribute={attribute} returned {len(records)} records")

        for record in records:
            record['attribute'] = attribute

        all_contracts.extend(records)

    print(f"Total contracts combined: {len(all_contracts)}")

    return {'value': all_contracts}


def get_contract_details(contract_number, creds):
    base_url = creds['sap_base_url']
    url = (
        f"{base_url}/sap/opu/odata4/sap/zsb_contracts_data_api/srvd_a2x/sap/zsd_contracts_data_api/0001/"
        f"ContractHeader('{contract_number}')?$expand=_ContractItems&$format=json"
    )
    print(f"ContractDetails URL: {url}")
    return call_sap_api(url, creds['username'], creds['password'])


def lambda_handler(event, context):
    print("==== DEBUG START ====")
    print("EVENT:", json.dumps(event)) 
    try:
        env = event.get('env', 'dev')   # default dev
        print(f"Running for environment: {env}")
        creds = get_sap_credentials(env)

        print("Fetching Contract List...")
        contract_list_data = get_contract_list(creds)

        contracts = contract_list_data.get('value', [])
        print(f"Found {len(contracts)} contracts. Fetching details...")

        all_details = []

        for contract in contracts:
            contract_number = contract.get('ContractNumber')

            if contract_number:
                try:
                    details = get_contract_details(contract_number, creds)
                    all_details.append(details)
                except Exception as e:
                    print(f"Error for {contract_number}: {e}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'contracts_fetched': len(contracts),
                'details_fetched': len(all_details),
                'contract_list': contract_list_data,
                'contract_details': all_details
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }