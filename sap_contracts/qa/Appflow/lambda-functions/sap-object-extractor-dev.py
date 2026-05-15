import json
import boto3
import urllib.request
import urllib.parse
import base64
import time
import uuid
import os
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
MAX_WORKERS     = int(os.environ.get("MAX_WORKERS", 20))
RETRY_COUNT     = int(os.environ.get("RETRY_COUNT", 3))
BACKOFF_FACTOR  = int(os.environ.get("BACKOFF_FACTOR", 2))
API_TIMEOUT     = int(os.environ.get("API_TIMEOUT", 60))

BRONZE_BUCKET   = os.environ.get("BRONZE_BUCKET")
ATHENA_DB       = os.environ.get("ATHENA_DB")
AUDIT_TABLE     = os.environ.get("AUDIT_TABLE")
ATHENA_OUTPUT   = os.environ.get("ATHENA_OUTPUT")
ATHENA_REGION   = os.environ.get("AWS_REGION", "us-east-1") # Native Lambda variable

# Populated once at handler entry — used by all audit functions
RUN_ID          = None   # unique per Lambda invocation
LAMBDA_REQUEST_ID = None # context.aws_request_id

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ─────────────────────────────────────────────
# ATHENA HELPER
# ─────────────────────────────────────────────

def run_athena_query(sql: str, timeout_seconds: int = 120) -> dict:
    """
    Execute a DDL/DML statement in Athena and wait for completion.
    Returns the final execution status dict.
    Raises on failure so callers can decide whether to swallow or propagate.

    Example SQL executed:
        INSERT INTO edp_configs_dev.pipeline_execution_summary
        VALUES ('abc-123', 'SAP_Contracts_Bronze', ...)
    """
    athena  = boto3.client('athena', region_name=ATHENA_REGION)
    resp    = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': ATHENA_DB},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT},
    )
    qid     = resp['QueryExecutionId']
    elapsed = 0
    poll    = 2

    while elapsed < timeout_seconds:
        result = athena.get_query_execution(QueryExecutionId=qid)
        state  = result['QueryExecution']['Status']['State']

        if state == 'SUCCEEDED':
            logger.info(f"[ATHENA] Query succeeded | qid={qid}")
            return result['QueryExecution']

        if state in ('FAILED', 'CANCELLED'):
            reason = (
                result['QueryExecution']['Status']
                .get('StateChangeReason', 'No reason provided')
            )
            raise RuntimeError(
                f"Athena query {state} | qid={qid} | reason={reason} "
                f"| sql_preview={sql[:300]}"
            )

        time.sleep(poll)
        elapsed += poll

    raise TimeoutError(
        f"Athena query did not complete within {timeout_seconds}s | qid={qid}"
    )


# ─────────────────────────────────────────────
# SQL VALUE FORMATTERS
# ─────────────────────────────────────────────

def _str(value) -> str:
    """
    Safely format a Python string for Athena SQL.
    None  → NULL
    str   → 'escaped_value'   (single quotes inside value are doubled)

    Example:
        _str("SAP's API")  →  'SAP''s API'
        _str(None)         →  NULL
    """
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _ts(value) -> str:
    """
    Format a datetime or ISO string for Athena TIMESTAMP literal.
    None      → NULL
    datetime  → TIMESTAMP '2024-01-15 09:30:00.000'
    str       → TIMESTAMP '2024-01-15 09:30:00.000'  (parsed first)

    Example:
        _ts(datetime(2024,1,15,9,30))  →  TIMESTAMP '2024-01-15 09:30:00.000'
        _ts(None)                      →  NULL
    """
    if value is None:
        return "NULL"
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"


def _int(value) -> str:
    """
    Format an integer for Athena SQL.
    None → NULL
    int  → numeric literal (no quotes)

    Example:
        _int(1500)  →  1500
        _int(None)  →  NULL
    """
    if value is None:
        return "NULL"
    return str(int(value))


def _date(value) -> str:
    """
    Format a date or datetime for Athena DATE literal.
    None     → NULL
    date     → DATE '2024-01-15'
    datetime → DATE '2024-01-15'

    Example:
        _date(datetime(2024,1,15))  →  DATE '2024-01-15'
        _date(None)                 →  NULL
    """
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        value = value.date()
    return f"DATE '{value.strftime('%Y-%m-%d')}'"


# ─────────────────────────────────────────────
# AUDIT: INSERT (status = RUNNING)
# ─────────────────────────────────────────────

def insert_audit_start(
    pipeline_name: str,
    table_name: str,
    source_system: str,
    environment: str,
    load_type: str,
    start_time: datetime,
) -> bool:
    """
    Write a RUNNING audit row at the very beginning of Lambda execution.
    All metric fields (records_*, duration_*) are intentionally NULL —
    they are populated only on SUCCESS or FAILURE update.

    Returns True on success, False if Athena write fails (non-fatal for pipeline).

    Example SQL generated:
        INSERT INTO edp_configs_dev.pipeline_execution_summary
          (run_id, pipeline_name, source_system, table_name, layer,
           environment, load_type, status, pipeline_start_time,
           created_at, etl_run_date, glue_job_name, glue_job_run_id,
           records_read, records_written, records_inserted,
           records_updated, records_deleted, records_rejected,
           retry_count, etl_run_id)
        VALUES
          ('uuid-here', 'SAP_Contracts_Bronze', 'SAP', 'contractdetails',
           'BRONZE', 'qa', 'INCR', 'RUNNING',
           TIMESTAMP '2024-01-15 09:30:00.000',
           TIMESTAMP '2024-01-15 09:30:00.000',
           DATE '2024-01-15',
           'Lambda_SAP_Contracts_Bronze', 'lambda-request-id-here',
           0, 0, 0, 0, 0, 0, 0, NULL)
    """
    if RUN_ID is None:
        logger.warning("[AUDIT] insert_audit_start called before RUN_ID was set — skipping")
        return False

    sql = f"""
        INSERT INTO {AUDIT_TABLE}
          (run_id, pipeline_name, source_system, table_name, layer,
           environment, load_type, status, pipeline_start_time,
           created_at, etl_run_date, glue_job_name, glue_job_run_id,
           records_read, records_written, records_inserted,
           records_updated, records_deleted, records_rejected,
           retry_count, etl_run_id)
        VALUES
          (
            {_str(RUN_ID)},
            {_str(pipeline_name)},
            {_str(source_system)},
            {_str(table_name)},
            'BRONZE',
            {_str(environment)},
            {_str(load_type)},
            'RUNNING',
            {_ts(start_time)},
            {_ts(start_time)},
            {_date(start_time)},
            {_str(f'Lambda_{pipeline_name}')},
            {_str(LAMBDA_REQUEST_ID)},
            0, 0, 0, 0, 0, 0, 0,
            NULL
          )
    """

    try:
        run_athena_query(sql)
        logger.info(f"[AUDIT] RUNNING row inserted | run_id={RUN_ID}")
        return True
    except Exception as e:
        # Audit failure must never crash the pipeline
        logger.error(f"[AUDIT] insert_audit_start failed: {e}")
        return False


# ─────────────────────────────────────────────
# AUDIT: UPDATE (status = SUCCESS)
# ─────────────────────────────────────────────

def update_audit_success(
    records_read: int,
    records_written: int,
    end_time: datetime,
    start_time: datetime,
    partial_error_message: str = None,
) -> bool:
    """
    Update the RUNNING row to SUCCESS once the pipeline completes.
    duration_seconds is computed from start_time and end_time.
    partial_error_message is for soft failures (e.g. some contracts
    failed after retries but the run overall succeeded).

    Returns True on success, False if Athena write fails (non-fatal).

    Example SQL generated:
        UPDATE edp_configs_dev.pipeline_execution_summary
        SET
          status             = 'SUCCESS',
          records_read       = 4500,
          records_written    = 4487,
          records_inserted   = 4487,
          pipeline_end_time  = TIMESTAMP '2024-01-15 09:45:00.000',
          duration_seconds   = 900,
          error_message      = '13 contracts failed after 3 retries',
          current_max_watermark = NULL,
          watermark_end      = TIMESTAMP '2024-01-15 00:00:00.000'
        WHERE run_id = 'uuid-here'
          AND layer  = 'BRONZE'
    """
    if RUN_ID is None:
        logger.warning("[AUDIT] update_audit_success called before RUN_ID was set — skipping")
        return False

    duration = int((end_time - start_time).total_seconds())

    sql = f"""
        UPDATE {AUDIT_TABLE}
        SET
            status                = 'SUCCESS',
            records_read          = {_int(records_read)},
            records_written       = {_int(records_written)},
            records_inserted      = {_int(records_written)},
            records_updated       = 0,
            records_deleted       = 0,
            records_rejected      = {_int(records_read - records_written)},
            pipeline_end_time     = {_ts(end_time)},
            duration_seconds      = {_int(duration)},
            error_message         = {_str(partial_error_message)}
        WHERE run_id = {_str(RUN_ID)}
          AND layer  = 'BRONZE'
    """

    try:
        run_athena_query(sql)
        logger.info(
            f"[AUDIT] SUCCESS updated | run_id={RUN_ID} "
            f"| read={records_read} | written={records_written} "
            f"| duration={duration}s"
        )
        return True
    except Exception as e:
        logger.error(f"[AUDIT] update_audit_success failed: {e}")
        return False


# ─────────────────────────────────────────────
# AUDIT: UPDATE (status = FAILED)
# ─────────────────────────────────────────────

def update_audit_failure(
    error_message: str,
    end_time: datetime,
    start_time: datetime,
    records_read: int = 0,
    records_written: int = 0,
) -> bool:
    if RUN_ID is None:
        logger.warning("[AUDIT] update_audit_failure called before RUN_ID was set — skipping")
        return False

    duration = int((end_time - start_time).total_seconds())

    # Truncate error messages that exceed column capacity
    safe_error = (error_message or "Unknown error")[:1900]

    sql = f"""
        UPDATE {AUDIT_TABLE}
        SET
            status            = 'FAILED',
            error_message     = {_str(safe_error)},
            records_read      = {_int(records_read)},
            records_written   = {_int(records_written)},
            records_rejected  = {_int(records_read - records_written)},
            pipeline_end_time = {_ts(end_time)},
            duration_seconds  = {_int(duration)}
        WHERE run_id = {_str(RUN_ID)}
          AND layer  = 'BRONZE'
    """

    try:
        run_athena_query(sql)
        logger.info(f"[AUDIT] FAILED updated | run_id={RUN_ID} | error={safe_error[:120]}")
        return True
    except Exception as e:
        logger.error(f"[AUDIT] update_audit_failure itself failed: {e}")
        return False


# ─────────────────────────────────────────────
# CREDENTIALS / SAP  (unchanged)
# ─────────────────────────────────────────────

# def get_sap_credentials(env):
#     client    = boto3.client('secretsmanager', region_name='us-east-1')
#     secret_id = (
#         "sap/qa/contracts_api/credentials"
#         if env == "qa"
#         else "sap/dev/odata/credentials"
#     )
#     logger.info(f"Using secret: {secret_id}")
#     secret = client.get_secret_value(SecretId=secret_id)
#     return json.loads(secret['SecretString'])

def get_sap_credentials(secret_arn):
    
    if not secret_arn:
        raise ValueError("secret_arn is missing from the event payload")

    client = boto3.client(
        'secretsmanager',
        region_name=os.environ.get('AWS_REGION', 'us-east-1')
    )

    logger.info(f"Using secret ARN: {secret_arn}")

    secret = client.get_secret_value(
        SecretId=secret_arn
    )

    return json.loads(secret['SecretString'])


def call_sap_api(url, username, password):
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    req = urllib.request.Request(url)
    req.add_header('Authorization', f'Basic {credentials}')
    req.add_header('Accept', 'application/json')
    with urllib.request.urlopen(req, timeout=API_TIMEOUT) as response:
        return json.loads(response.read().decode())


# ─────────────────────────────────────────────
# CACHE HELPERS  (unchanged)
# ─────────────────────────────────────────────

def get_cache_key(env, start_date, end_date):
    return f"sap/cache/contract_list_{env}_{start_date}_{end_date}.json"


def cache_exists(env, start_date, end_date):
    s3 = boto3.client('s3', region_name='us-east-1')
    try:
        s3.head_object(Bucket=BRONZE_BUCKET, Key=get_cache_key(env, start_date, end_date))
        return True
    except Exception:
        return False


def save_cache(contracts, env, start_date, end_date):
    s3  = boto3.client('s3', region_name='us-east-1')
    key = get_cache_key(env, start_date, end_date)
    s3.put_object(
        Bucket=BRONZE_BUCKET, Key=key,
        Body=json.dumps(contracts), ContentType='application/json'
    )
    logger.info(f"Cached: {key}")


def load_cache(env, start_date, end_date):
    s3  = boto3.client('s3', region_name='us-east-1')
    key = get_cache_key(env, start_date, end_date)
    obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
    logger.info(f"Loaded cache: {key}")
    return json.loads(obj['Body'].read().decode())


def delete_cache(env, start_date, end_date):
    s3  = boto3.client('s3', region_name='us-east-1')
    key = get_cache_key(env, start_date, end_date)
    s3.delete_object(Bucket=BRONZE_BUCKET, Key=key)
    logger.info(f"Cache deleted: {key}")


def write_to_s3_temp(data, env, entity, offset):
    s3  = boto3.client('s3', region_name='us-east-1')
    key = f"sap/temp/{entity}_{env}_{offset}.json"
    s3.put_object(
        Bucket=BRONZE_BUCKET, Key=key,
        Body=json.dumps(data), ContentType='application/json'
    )
    logger.info(f"Written to S3 temp: {key}")
    return key


def log_failed_contracts(failed_list, env, start_date, end_date):
    if not failed_list:
        return
    s3  = boto3.client('s3', region_name='us-east-1')
    key = f"sap/logs/failed_contracts_{env}_{start_date}_{end_date}.json"
    s3.put_object(
        Bucket=BRONZE_BUCKET, Key=key,
        Body=json.dumps({"failed_contracts": failed_list}),
        ContentType='application/json'
    )
    logger.info(f"Failed contracts logged: {key}")


# ─────────────────────────────────────────────
# WATERMARK / DATE HELPERS  (unchanged)
# ─────────────────────────────────────────────

def get_watermark_date():
    athena = boto3.client('athena', region_name='us-east-1')
    query  = """
        SELECT MAX(last_successful_watermark)
        FROM edp_configs_dev.edp_incremental_control
        WHERE table_name = 'contracts'
    """
    resp   = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    qid    = resp['QueryExecutionId']
    while True:
        state = athena.get_query_execution(
            QueryExecutionId=qid
        )['QueryExecution']['Status']['State']
        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(2)
    if state != 'SUCCEEDED':
        raise RuntimeError("Athena watermark query failed")
    result    = athena.get_query_results(QueryExecutionId=qid)
    watermark = result['ResultSet']['Rows'][1]['Data'][0].get('VarCharValue')
    if watermark:
        return (
            datetime.strptime(watermark[:10], '%Y-%m-%d') + timedelta(days=1)
        ).strftime('%Y-%m-%d')
    return (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')


def build_date_chunks(start_str, end_str):
    start, end = (
        datetime.strptime(start_str, "%Y-%m-%d"),
        datetime.strptime(end_str,   "%Y-%m-%d"),
    )
    chunks, cs = [], start
    while cs <= end:
        ce = min(cs + timedelta(days=89), end)
        chunks.append((cs.strftime('%Y-%m-%d'), ce.strftime('%Y-%m-%d')))
        cs = ce + timedelta(days=1)
    return chunks


# ─────────────────────────────────────────────
# CONTRACT LIST  (unchanged)
# ─────────────────────────────────────────────

def get_contract_list(creds, start_date, end_date):
    base_url, sap_path = creds['sap_base_url'], creds['sap_path']
    chunks             = build_date_chunks(start_date, end_date)
    all_contracts      = []
    for (cs, ce) in chunks:
        for attr in ['CREATE', 'CHANGE']:
            fstr = (
                f"ContractDate ge {cs} and ContractDate le {ce}"
                f" and attribute eq '{attr}'"
            )
            url = (
                f"{base_url}{sap_path}ZSD_API_CONTRACT_LIST"
                f"?$filter={urllib.parse.quote(fstr)}"
                f"&$select=ContractNumber,SalesOrganization,ContractDate,"
                f"CreatedDate,ChangedDate,attribute"
                f"&$top=5000&$format=json"
            )
            try:
                resp = call_sap_api(url, creds['username'], creds['password'])
                recs = resp.get('value', [])
                logger.info(f"{cs}→{ce} | {attr} | {len(recs)} records")
                for r in recs:
                    r['attribute'] = attr
                all_contracts.extend(recs)
            except Exception as e:
                logger.error(f"Contract list error {cs}→{ce} {attr}: {e}")
    logger.info(f"Total contracts: {len(all_contracts)}")
    return all_contracts


# ─────────────────────────────────────────────
# CONTRACT DETAILS  (unchanged)
# ─────────────────────────────────────────────

def get_contract_details(cn, creds):
    url = (
        f"{creds['sap_base_url']}/sap/opu/odata4/sap/zsb_contracts_data_api"
        f"/srvd_a2x/sap/zsd_contracts_data_api/0001/"
        f"ContractHeader('{cn}')?$expand=_ContractItems&$format=json"
    )
    return call_sap_api(url, creds['username'], creds['password'])


def get_contract_details_retry(cn, creds):
    delay = 2
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            return get_contract_details(cn, creds)
        except Exception as e:
            logger.warning(f"Retry {attempt}/{RETRY_COUNT} for {cn}: {e}")
            if attempt == RETRY_COUNT:
                return None
            delay *= BACKOFF_FACTOR


def fetch_details_parallel(contracts, creds, env, start_date, end_date):
    numbers     = list(set(c['ContractNumber'] for c in contracts if c.get('ContractNumber')))
    total       = len(numbers)
    all_details = []
    all_failed  = []
    logger.info(f"Fetching details for {total} unique contracts")

    for i in range(0, total, 10):
        batch = numbers[i:i + 10]
        br, bf = [], []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(get_contract_details_retry, cn, creds): cn for cn in batch}
            for f in as_completed(futures):
                cn = futures[f]
                r  = f.result()
                if r:
                    br.append(r)
                else:
                    bf.append(cn)
                    logger.warning(f"FAIL after retries: {cn}")
        all_details.extend(br)
        all_failed.extend(bf)
        logger.info(f"Progress: {len(all_details)}/{total} ok | {len(all_failed)} failed")
        if i + 10 < total:
            time.sleep(0.5)

    if all_failed:
        log_failed_contracts(all_failed, env, start_date, end_date)

    logger.info(f"Fetch complete: {len(all_details)} ok | {len(all_failed)} failed")
    return all_details, len(all_failed)


# ─────────────────────────────────────────────
# CACHE ORCHESTRATION  (unchanged)
# ─────────────────────────────────────────────

def get_cached_contracts(creds, env, start_date, end_date):
    if cache_exists(env, start_date, end_date):
        return load_cache(env, start_date, end_date)
    contracts = get_contract_list(creds, start_date, end_date)
    save_cache(contracts, env, start_date, end_date)
    return contracts


def smart_return(data, env, entity, offset):
    body = json.dumps(data)
    size = len(body.encode('utf-8'))
    if size > 5_000_000:
        key = write_to_s3_temp(data, env, entity, offset)
        return {
            'statusCode': 200,
            'body': json.dumps({'s3_key': key, 'has_more': data.get('has_more', False)})
        }
    return {'statusCode': 200, 'body': body}


# ─────────────────────────────────────────────
# HANDLER
# ─────────────────────────────────────────────

def lambda_handler(event, context):
    global RUN_ID, LAMBDA_REQUEST_ID

    # ── Identity ──────────────────────────────────────────────────────────
    RUN_ID            = str(uuid.uuid4())
    LAMBDA_REQUEST_ID = context.aws_request_id
    start_time        = datetime.utcnow()

    logger.info("==== BRONZE EXTRACTOR START ====")
    logger.info(f"EVENT: {json.dumps(event)}")
    logger.info(f"RUN_ID={RUN_ID} | LAMBDA_REQUEST_ID={LAMBDA_REQUEST_ID}")

    # ── Runtime config ────────────────────────────────────────────────────
    env       = event.get('env', 'dev')
    entity    = event.get('entity', '')
    offset    = int(event.get('offset', 0))
    limit     = int(event.get('limit', int(os.environ.get("PAGE_SIZE", "50"))))
    load_type = os.environ.get("LOAD_TYPE", "INCR").upper()

    # ── AUDIT: INSERT RUNNING ─────────────────────────────────────────────
    # Written before any pipeline work begins so even a mid-run crash
    # leaves a RUNNING row (detectable by Airflow reconciliation).
    insert_audit_start(
        pipeline_name  = "SAP_Contracts_Bronze",
        table_name     = entity or "contracts",
        source_system  = "SAP",
        environment    = env,
        load_type      = load_type,
        start_time     = start_time,
    )

    # ── Counters tracked throughout for accurate audit on partial failure ─
    records_read    = 0
    records_written = 0

    try:
        # creds = get_sap_credentials(env)
        creds = get_sap_credentials(event.get("secret_arn"))

        if load_type == "FULL":
            start_date = os.environ.get("START_DATE")
            end_date   = os.environ.get("END_DATE")
            if not start_date or not end_date:
                raise ValueError("START_DATE and END_DATE env vars required for FULL load")
            logger.info(f"[FULL LOAD] {start_date} → {end_date}")
        else:
            start_date = get_watermark_date()
            end_date   = datetime.utcnow().strftime('%Y-%m-%d')
            logger.info(f"[INCR LOAD] {start_date} → {end_date}")

        logger.info(f"ENV={env} | ENTITY={entity} | OFFSET={offset} | LIMIT={limit}")

        if offset == 0:
            try:
                delete_cache(env, start_date, end_date)
            except Exception:
                pass

        all_contracts  = get_cached_contracts(creds, env, start_date, end_date)
        records_read   = len(all_contracts)

        # ── ContractList ──────────────────────────────────────────────────
        if entity == "ContractList":
            page     = all_contracts[offset:offset + limit]
            has_more = (offset + limit) < len(all_contracts)
            records_written = len(page)
            logger.info(f"Page: {records_written} records | has_more={has_more}")

            if not has_more:
                delete_cache(env, start_date, end_date)

            end_time = datetime.utcnow()

            #  AUDIT: SUCCESS
            update_audit_success(
                records_read    = records_read,
                records_written = records_written,
                end_time        = end_time,
                start_time      = start_time,
            )

            return smart_return(
                {'contract_list': {'value': page}, 'has_more': has_more},
                env, entity, offset
            )

        # ── ContractDetails ───────────────────────────────────────────────
        elif entity == "ContractDetails":
            page_contracts  = all_contracts[offset:offset + limit]
            has_more        = (offset + limit) < len(all_contracts)
            records_read    = len(page_contracts)
            logger.info(f"Contracts in page: {records_read} | has_more={has_more}")

            details, failed_count = fetch_details_parallel(
                page_contracts, creds, env, start_date, end_date
            )
            records_written = len(details)

            if not has_more:
                delete_cache(env, start_date, end_date)

            end_time = datetime.utcnow()

            # Build partial-failure message for audit visibility
            partial_error = (
                f"{failed_count} contract(s) failed after {RETRY_COUNT} retries — "
                f"see sap/logs/failed_contracts_{env}_{start_date}_{end_date}.json"
            ) if failed_count > 0 else None

            # ✅ AUDIT: SUCCESS (with soft-failure note when applicable)
            # update_audit_success(
            #     records_read         = records_read,
            #     records_written      = records_written,
            #     end_time             = end_time,
            #     start_time           = start_time,
            #     partial_error_message= partial_error,
            # )

            return smart_return(
                {'contract_details': details, 'has_more': has_more},
                env, entity, offset
            )

        else:
            raise ValueError(f"Unknown entity: '{entity}'")

    except Exception as e:
        error_msg = str(e)
        end_time  = datetime.utcnow()
        logger.error(f"Lambda failed: {error_msg}", exc_info=True)

        # AUDIT: FAILED — passes partial counts if failure was mid-run
        update_audit_failure(
            error_message   = error_msg,
            end_time        = end_time,
            start_time      = start_time,
            records_read    = records_read,
            records_written = records_written,
        )

        return {'statusCode': 500, 'body': json.dumps({'error': error_msg})}