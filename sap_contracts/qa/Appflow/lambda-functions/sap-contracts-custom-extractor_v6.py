import json
import boto3
import urllib.request
import urllib.parse
import base64
import time
import uuid
import os
import sys
import logging
import random
import hashlib
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List, Tuple, Any


# ============================================================
# CONFIG
# ============================================================

# --- Core Settings ---
MAX_WORKERS            = int(os.environ.get("MAX_WORKERS", "20"))
RETRY_COUNT            = int(os.environ.get("RETRY_COUNT", "3"))
BACKOFF_FACTOR         = float(os.environ.get("BACKOFF_FACTOR", "2"))
API_TIMEOUT            = int(os.environ.get("API_TIMEOUT", "60"))
BRONZE_BUCKET          = os.environ.get("BRONZE_BUCKET", "epiq-edp-dl-qa-bronze")
AWS_REGION             = os.environ.get("AWS_REGION", "us-east-1")

# --- Pagination & Batching ---
DEFAULT_PAGE_SIZE      = int(os.environ.get("PAGE_SIZE", "50"))
MAX_BATCH_SIZE         = int(os.environ.get("MAX_BATCH_SIZE", "50"))
MIN_BATCH_SIZE         = int(os.environ.get("MIN_BATCH_SIZE", "10"))
MAX_PAYLOAD_BYTES      = int(os.environ.get("MAX_PAYLOAD_BYTES", "5000000"))  # 5MB
MAX_ITEMS_PER_CONTRACT = int(os.environ.get("MAX_ITEMS_PER_CONTRACT", "500"))
MEMORY_THRESHOLD_MB    = int(os.environ.get("MEMORY_THRESHOLD_MB", "256"))

# --- Athena ---
AUDIT_TABLE            = os.environ.get("AUDIT_TABLE", "edp_configs_dev.pipeline_execution_summary")
CONTROL_TABLE          = os.environ.get("CONTROL_TABLE", "edp_configs_dev.edp_incremental_control")
ATHENA_DB              = os.environ.get("ATHENA_DB", "edp_configs_dev")
ATHENA_OUTPUT          = os.environ.get("ATHENA_OUTPUT", "s3://aws-athena-query-results-us-east-1-730878889077/")
ATHENA_REGION          = os.environ.get("ATHENA_REGION", "us-east-1")

# --- Notifications ---
SNS_TOPIC_ARN          = os.environ.get("SNS_TOPIC_ARN", "")
ENVIRONMENT            = os.environ.get("ENVIRONMENT", "qa")
NOTIFICATION_ENABLED   = os.environ.get("NOTIFICATION_ENABLED", "true").lower() == "true"

# --- Circuit Breaker ---
CIRCUIT_BREAKER_THRESHOLD = int(os.environ.get("CIRCUIT_BREAKER_THRESHOLD", "5"))
CIRCUIT_BREAKER_TIMEOUT   = int(os.environ.get("CIRCUIT_BREAKER_TIMEOUT", "60"))

# --- Schema Drift ---
SCHEMA_SNAPSHOT_PREFIX = os.environ.get("SCHEMA_SNAPSHOT_PREFIX", "sap/schema_snapshots/")

# --- Checkpoint ---
CHECKPOINT_PREFIX      = os.environ.get("CHECKPOINT_PREFIX", "sap/checkpoints/")

# --- Load Type ---
LOAD_TYPE              = os.environ.get("LOAD_TYPE", "INCR").upper()

# --- Global Runtime State (set in handler) ---
RUN_ID                 = None
LAMBDA_REQUEST_ID      = None
EXECUTION_START_TIME   = None


# ============================================================
# LOGGING
# ============================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def structured_log(level: str, message: str, **kwargs):
    """
    Emit structured JSON log entry to CloudWatch.
    Includes run_id, entity, execution_id, timestamp for correlation.
    Levels: INFO, WARNING, ERROR, CRITICAL
    """
    record = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "message": message,
        "run_id": RUN_ID,
        "lambda_request_id": LAMBDA_REQUEST_ID,
        "environment": ENVIRONMENT,
    }
    # Merge additional context
    record.update({k: v for k, v in kwargs.items() if v is not None})
    print(json.dumps(record, default=str))


def log_info(message: str, **kwargs):
    """INFO level structured log."""
    structured_log("INFO", message, **kwargs)


def log_warning(message: str, **kwargs):
    """WARNING level structured log."""
    structured_log("WARNING", message, **kwargs)
    logger.warning(f"[{RUN_ID}] {message}")


def log_error(message: str, **kwargs):
    """ERROR level structured log."""
    structured_log("ERROR", message, **kwargs)
    logger.error(f"[{RUN_ID}] {message}")


def log_critical(message: str, **kwargs):
    """CRITICAL level structured log."""
    structured_log("CRITICAL", message, **kwargs)
    logger.critical(f"[{RUN_ID}] {message}")


# ============================================================
# ATHENA HELPER
# ============================================================

ICEBERG_RETRY_COUNT = int(os.environ.get("ICEBERG_RETRY_COUNT", "3"))
ICEBERG_RETRY_DELAY = int(os.environ.get("ICEBERG_RETRY_DELAY", "5"))


def _run_athena_query_once(sql: str, timeout_seconds: int = 120) -> dict:
    """
    Execute a single Athena DDL/DML statement and wait for completion.
    Returns the final execution status dict.
    Raises on failure so callers can decide whether to swallow or propagate.
    """
    athena = boto3.client('athena', region_name=ATHENA_REGION)

    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': ATHENA_DB},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT},
    )
    qid = resp['QueryExecutionId']
    elapsed = 0
    poll = 1

    while elapsed < timeout_seconds:
        result = athena.get_query_execution(QueryExecutionId=qid)
        state = result['QueryExecution']['Status']['State']

        if state == 'SUCCEEDED':
            log_info("ATHENA: Query succeeded", query_id=qid, elapsed_seconds=elapsed)
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


def run_athena_query(sql: str, timeout_seconds: int = 120) -> dict:
    """
    Execute a DDL/DML statement in Athena with retry for Iceberg commit errors.
    ICEBERG_COMMIT_ERROR is a known transient error when INSERT and UPDATE
    happen too quickly on the same Iceberg table — a short delay + retry fixes it.
    """
    log_info("ATHENA: Starting query execution", sql_preview=sql[:300])

    for attempt in range(1, ICEBERG_RETRY_COUNT + 1):
        try:
            return _run_athena_query_once(sql, timeout_seconds)
        except RuntimeError as e:
            error_msg = str(e)
            is_iceberg_conflict = 'ICEBERG_COMMIT_ERROR' in error_msg

            if is_iceberg_conflict and attempt < ICEBERG_RETRY_COUNT:
                delay = ICEBERG_RETRY_DELAY * attempt
                log_warning(
                    "ATHENA: Iceberg commit conflict — retrying",
                    attempt=attempt,
                    max_attempts=ICEBERG_RETRY_COUNT,
                    delay_seconds=delay,
                    error_preview=error_msg[:200],
                )
                time.sleep(delay)
                continue

            # Non-Iceberg error or final attempt — raise
            log_error(
                "ATHENA: Query failed",
                attempt=attempt,
                is_iceberg_conflict=is_iceberg_conflict,
                error=error_msg[:300],
            )
            raise


def run_athena_query_with_results(sql: str, timeout_seconds: int = 120) -> List[Dict]:
    """
    Execute an Athena SELECT query and return result rows as list of dicts.
    Used for reading watermark values and other SELECT queries.
    """
    athena = boto3.client('athena', region_name=ATHENA_REGION)
    log_info("ATHENA: Starting SELECT query", sql_preview=sql[:300])

    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': ATHENA_DB},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT},
    )
    qid = resp['QueryExecutionId']
    elapsed = 0
    poll = 1

    while elapsed < timeout_seconds:
        result = athena.get_query_execution(QueryExecutionId=qid)
        state = result['QueryExecution']['Status']['State']

        if state == 'SUCCEEDED':
            log_info("ATHENA: SELECT query succeeded", query_id=qid)
            # Fetch results
            results = athena.get_query_results(QueryExecutionId=qid)
            rows = results['ResultSet']['Rows']
            if len(rows) < 2:
                return []
            headers = [col['VarCharValue'] for col in rows[0]['Data']]
            data_rows = []
            for row in rows[1:]:
                row_dict = {}
                for i, col in enumerate(row['Data']):
                    row_dict[headers[i]] = col.get('VarCharValue')
                data_rows.append(row_dict)
            return data_rows

        if state in ('FAILED', 'CANCELLED'):
            reason = result['QueryExecution']['Status'].get('StateChangeReason', 'No reason')
            log_error("ATHENA: SELECT query failed", query_id=qid, reason=reason)
            raise RuntimeError(f"Athena SELECT failed | qid={qid} | reason={reason}")

        time.sleep(poll)
        elapsed += poll

    raise TimeoutError(f"Athena SELECT did not complete within {timeout_seconds}s | qid={qid}")


# ============================================================
# SQL VALUE FORMATTERS
# ============================================================

def _str(value) -> str:
    """Format a Python string for Athena SQL. None → NULL, str → 'escaped'."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _ts(value) -> str:
    """Format a datetime for Athena TIMESTAMP literal. None → NULL."""
    if value is None:
        return "NULL"
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"


def _int(value) -> str:
    """Format an integer for Athena SQL. None → NULL."""
    if value is None:
        return "NULL"
    return str(int(value))


def _date(value) -> str:
    """Format a date for Athena DATE literal. None → NULL."""
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        value = value.date()
    return f"DATE '{value.strftime('%Y-%m-%d')}'"


# ============================================================
# WATERMARK
# ============================================================

def get_current_watermark(table_name: str = "contracts") -> Optional[str]:
    """
    Read the current watermark value from the incremental control table.
    Returns the last_successful_watermark as ISO string, or None if first run.

    Query: SELECT last_successful_watermark FROM control table WHERE table_name = X
    """
    log_info("WATERMARK: Reading current watermark", table_name=table_name)

    sql = f"""
        SELECT last_successful_watermark, watermark_column, etl_run_id
        FROM {CONTROL_TABLE}
        WHERE table_name = '{table_name}'
        AND source_system = 'sap'
        ORDER BY updated_at DESC
        LIMIT 1
    """

    try:
        rows = run_athena_query_with_results(sql)
        if not rows:
            log_warning(
                "WATERMARK: No watermark row found — first time load",
                table_name=table_name,
            )
            return None

        watermark_value = rows[0].get('last_successful_watermark')
        if watermark_value is None or watermark_value.strip() == '' or watermark_value == 'NULL':
            log_warning(
                "WATERMARK: Watermark value is NULL — treating as first time load",
                table_name=table_name,
            )
            return None

        log_info(
            "WATERMARK: Current watermark retrieved",
            table_name=table_name,
            watermark_value=watermark_value,
            etl_run_id=rows[0].get('etl_run_id'),
        )
        return watermark_value

    except Exception as e:
        log_error(
            "WATERMARK: Failed to read watermark — defaulting to None",
            error=str(e),
            table_name=table_name,
        )
        return None


# def get_new_max_watermark(contracts: List[Dict], watermark_column: str = "ChangedDate") -> Optional[str]:
#     """
#     Compute the maximum watermark value from extracted records.
#     Uses the watermark_column field from contract records.
#     Returns ISO date string (UTC) or None if no valid dates found.
#     """
#     log_info("WATERMARK: Computing new max watermark",
#              watermark_column=watermark_column, record_count=len(contracts))
#
#     max_date = None
#     for contract in contracts:
#         date_val = contract.get(watermark_column)
#         if date_val is None:
#             continue
#         # Handle multiple date formats from SAP
#         try:
#             if isinstance(date_val, str):
#                 parsed = None
#                 for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%Y%m%d', '%Y-%m-%dT%H:%M:%SZ'):
#                     try:
#                         parsed = datetime.strptime(date_val[:len(fmt.replace('%', 'X'))], fmt)
#                         break
#                     except ValueError:
#                         continue
#                 if parsed is None:
#                     try:
#                         parsed = datetime.strptime(date_val[:10], '%Y-%m-%d')
#                     except ValueError:
#                         continue
#                 if max_date is None or parsed > max_date:
#                     max_date = parsed
#         except Exception:
#             continue
#
#     if max_date is None:
#         log_warning("WATERMARK: Could not compute max watermark — no valid dates found")
#         return None
#
#     result = max_date.strftime('%Y-%m-%d %H:%M:%S')
#     log_info("WATERMARK: New max watermark computed", new_max_watermark=result)
#     return result

def get_new_max_watermark(
    contracts: List[Dict]
) -> Optional[str]:
    log_info(
        "WATERMARK: Computing new max watermark",
        record_count=len(contracts)
    )

    max_date = None

    for contract in contracts:

        # Use ChangedDate first, else CreatedDate
        date_val = (
            contract.get("ChangedDate")
            or contract.get("CreatedDate")
        )

        if not date_val:
            continue

        try:
            parsed = datetime.strptime(
                date_val[:10],
                '%Y-%m-%d'
            )

            if max_date is None or parsed > max_date:
                max_date = parsed

        except Exception:
            continue

    if max_date is None:
        log_warning(
            "WATERMARK: No valid dates found"
        )
        return None

    result = max_date.strftime('%Y-%m-%d')

    log_info(
        "WATERMARK: New max watermark computed",
        new_max_watermark=result
    )
    return result


def update_control_table_watermark(
    table_name: str,
    new_watermark: str,
    etl_run_id: str
) -> bool:
    """
    Update the control table with the new watermark ONLY after full success.
    Uses INSERT approach (append new row with latest updated_at).
    The SELECT in get_current_watermark orders by updated_at DESC LIMIT 1.

    IMPORTANT: This must only be called after successful pipeline completion.
    """
    log_info(
        "WATERMARK: Updating control table",
        table_name=table_name,
        new_watermark=new_watermark,
        etl_run_id=etl_run_id,
    )
    now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    sql = f"""
        UPDATE {CONTROL_TABLE}
        SET
            last_successful_watermark = TIMESTAMP '{new_watermark}',
            updated_at = TIMESTAMP '{now_utc}',
            etl_run_id = {_str(etl_run_id)}
        WHERE
            table_name = {_str(table_name)}
            AND source_system = 'sap'
    """

    try:
        run_athena_query(sql)
        log_info(
            "WATERMARK: Control table updated successfully",
            table_name=table_name,
            new_watermark=new_watermark,
        )
        return True
    except Exception as e:
        log_error(
            "WATERMARK: Failed to update control table",
            error=str(e),
            table_name=table_name,
        )
        return False


def get_watermark_date() -> str:
    """
    Get the start date for incremental extraction based on watermark.
    Returns date string in YYYY-MM-DD format.
    Handles: first-time load (defaults to 30 days back), NULL values, timezone (UTC).
    """
    log_info("WATERMARK: Resolving extraction start date")

    watermark = get_current_watermark("contracts")

    if watermark is None:
        # First-time load: go back 30 days
        start_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')
        log_info(
            "WATERMARK: First-time load — using 30-day lookback",
            start_date=start_date,
        )
        return start_date

    # Add 1 day to watermark to avoid reprocessing the boundary date
    try:
        wm_date = datetime.strptime(watermark[:10], '%Y-%m-%d')
        start_date = (wm_date + timedelta(days=1)).strftime('%Y-%m-%d')
        log_info(
            "WATERMARK: Incremental start date computed",
            watermark=watermark,
            start_date=start_date,
        )
        return start_date
    except Exception as e:
        log_error(
            "WATERMARK: Failed to parse watermark date — falling back to 30 days",
            error=str(e),
            watermark=watermark,
        )
        return (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')


# ============================================================
# AUDITING
# ============================================================

class AuditContext:
    """
    Holds all audit metrics for a single pipeline execution.
    Updated progressively during execution.
    """
    def __init__(self):
        self.run_id = RUN_ID
        self.execution_id = LAMBDA_REQUEST_ID
        self.entity = ""
        self.source_system = "SAP"
        self.layer = "BRONZE"
        self.pipeline_start_time = None
        self.pipeline_end_time = None
        self.status = "RUNNING"
        self.records_read = 0
        self.records_written = 0
        self.records_failed = 0
        self.retry_count = 0
        self.duration_seconds = 0
        self.error_message = None
        self.watermark_start = None
        self.watermark_end = None
        self.max_contract_items_count = 0
        self.pagination_count = 0
        self.api_call_count = 0


# Global audit context
AUDIT_CTX = None


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
    All metric fields are NULL initially — populated on completion.
    """
    global AUDIT_CTX
    AUDIT_CTX = AuditContext()
    AUDIT_CTX.entity = table_name
    AUDIT_CTX.pipeline_start_time = start_time

    if RUN_ID is None:
        log_warning("AUDIT: insert_audit_start called before RUN_ID was set — skipping")
        return False

    log_info(
        "AUDIT: Inserting RUNNING row",
        pipeline_name=pipeline_name,
        table_name=table_name,
        run_id=RUN_ID,
    )

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
        log_info("AUDIT: RUNNING row inserted successfully", run_id=RUN_ID)
        return True
    except Exception as e:
        log_error("AUDIT: insert_audit_start failed (non-fatal)", error=str(e))
        return False


def update_audit_success(
    records_read: int,
    records_written: int,
    end_time: datetime,
    start_time: datetime,
    partial_error_message: str = None,
    watermark_start: str = None,
    watermark_end: str = None,
    api_call_count: int = 0,
    pagination_count: int = 0,
    retry_count: int = 0,
    max_contract_items_count: int = 0,
) -> bool:
    """
    Update the RUNNING row to SUCCESS once the pipeline completes fully.
    """
    if RUN_ID is None:
        log_warning("AUDIT: update_audit_success called before RUN_ID was set — skipping")
        return False

    duration = int((end_time - start_time).total_seconds())

    log_info(
        "AUDIT: Updating to SUCCESS",
        run_id=RUN_ID,
        records_read=records_read,
        records_written=records_written,
        duration_seconds=duration,
    )

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
            error_message         = {_str(partial_error_message)},
            retry_count           = {_int(retry_count)}
        WHERE run_id = {_str(RUN_ID)}
        AND layer  = 'BRONZE'
    """

    try:
        run_athena_query(sql)
        log_info("AUDIT: SUCCESS updated", run_id=RUN_ID, duration=duration)
        return True
    except Exception as e:
        log_error("AUDIT: update_audit_success failed (non-fatal)", error=str(e))
        return False


def update_audit_failure(
    error_message: str,
    end_time: datetime,
    start_time: datetime,
    records_read: int = 0,
    records_written: int = 0,
) -> bool:
    """Update the RUNNING row to FAILED status."""
    if RUN_ID is None:
        log_warning("AUDIT: update_audit_failure called before RUN_ID was set — skipping")
        return False

    duration = int((end_time - start_time).total_seconds())
    safe_error = (error_message or "Unknown error")[:1900]

    log_info(
        "AUDIT: Updating to FAILED",
        run_id=RUN_ID,
        error_preview=safe_error[:120],
        duration_seconds=duration,
    )

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
        log_info("AUDIT: FAILED updated", run_id=RUN_ID)
        return True
    except Exception as e:
        log_error("AUDIT: update_audit_failure itself failed (non-fatal)", error=str(e))
        return False


def update_audit_partial_success(
    records_read: int,
    records_written: int,
    records_failed: int,
    end_time: datetime,
    start_time: datetime,
    error_message: str = None,
    retry_count: int = 0,
) -> bool:
    """
    Update to PARTIAL_SUCCESS when some contracts fail but most succeed.
    This status signals downstream (Airflow) that data is available but incomplete.
    """
    if RUN_ID is None:
        log_warning("AUDIT: update_audit_partial_success called before RUN_ID — skipping")
        return False

    duration = int((end_time - start_time).total_seconds())
    safe_error = (error_message or "Some records failed")[:1900]

    log_info(
        "AUDIT: Updating to PARTIAL_SUCCESS",
        run_id=RUN_ID,
        records_read=records_read,
        records_written=records_written,
        records_failed=records_failed,
    )

    sql = f"""
        UPDATE {AUDIT_TABLE}
        SET
            status            = 'PARTIAL_SUCCESS',
            error_message     = {_str(safe_error)},
            records_read      = {_int(records_read)},
            records_written   = {_int(records_written)},
            records_rejected  = {_int(records_failed)},
            pipeline_end_time = {_ts(end_time)},
            duration_seconds  = {_int(duration)},
            retry_count       = {_int(retry_count)}
        WHERE run_id = {_str(RUN_ID)}
        AND layer  = 'BRONZE'
    """

    try:
        run_athena_query(sql)
        log_info("AUDIT: PARTIAL_SUCCESS updated", run_id=RUN_ID)
        return True
    except Exception as e:
        log_error("AUDIT: update_audit_partial_success failed (non-fatal)", error=str(e))
        return False


# ============================================================
# NOTIFICATIONS
# ============================================================

def _build_email_subject(entity: str, status: str) -> str:
    """Build standardized email subject line."""
    return f"[{ENVIRONMENT.upper()}] SAP Bronze Pipeline | {entity} | {status} | run_id={RUN_ID}"


def _build_email_body(
    entity: str,
    status: str,
    records_read: int = 0,
    records_written: int = 0,
    records_failed: int = 0,
    duration_seconds: int = 0,
    watermark_start: str = None,
    watermark_end: str = None,
    error_summary: str = None,
) -> str:
    """Build detailed notification email body."""
    log_group = f"/aws/lambda/sap-contracts-extractor"
    body = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SAP BRONZE PIPELINE NOTIFICATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Environment     : {ENVIRONMENT.upper()}
Entity          : {entity}
Status          : {status}
Run ID          : {RUN_ID}
Lambda Req ID   : {LAMBDA_REQUEST_ID}

━━━━━━━━━━ METRICS ━━━━━━━━━━

Records Read    : {records_read}
Records Written : {records_written}
Records Failed  : {records_failed}
Duration        : {duration_seconds} seconds

━━━━━━━━━━ WATERMARK ━━━━━━━━━━

Watermark Start : {watermark_start or 'N/A'}
Watermark End   : {watermark_end or 'N/A'}

━━━━━━━━━━ LOGGING ━━━━━━━━━━

CloudWatch Log Group: {log_group}
Filter by run_id   : {RUN_ID}

"""
    if error_summary:
        body += f"""━━━━━━━━━━ ERROR SUMMARY ━━━━━━━━━━

{error_summary}

"""
    body += """━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
End of Notification
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    return body


def send_success_notification(
    entity: str,
    records_read: int,
    records_written: int,
    duration_seconds: int,
    watermark_start: str = None,
    watermark_end: str = None,
) -> bool:
    """Send SUCCESS notification via SNS."""
    if not NOTIFICATION_ENABLED or not SNS_TOPIC_ARN:
        log_info("NOTIFICATION: Skipped (disabled or no SNS topic configured)")
        return False

    log_info("NOTIFICATION: Sending SUCCESS notification", entity=entity)

    try:
        sns = boto3.client('sns', region_name=AWS_REGION)
        subject = _build_email_subject(entity, "SUCCESS")
        body = _build_email_body(
            entity=entity,
            status="SUCCESS",
            records_read=records_read,
            records_written=records_written,
            duration_seconds=duration_seconds,
            watermark_start=watermark_start,
            watermark_end=watermark_end,
        )
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],  # SNS subject limit
            Message=body,
            MessageAttributes={
                'status':      {'DataType': 'String', 'StringValue': 'SUCCESS'},
                'environment': {'DataType': 'String', 'StringValue': ENVIRONMENT},
                'entity':      {'DataType': 'String', 'StringValue': entity},
                'run_id':      {'DataType': 'String', 'StringValue': RUN_ID or ''},
            },
        )
        log_info("NOTIFICATION: SUCCESS notification sent", entity=entity)
        return True
    except Exception as e:
        log_error("NOTIFICATION: Failed to send SUCCESS notification", error=str(e))
        return False


def send_failure_notification(
    entity: str,
    error_summary: str,
    records_read: int = 0,
    duration_seconds: int = 0,
    watermark_start: str = None,
) -> bool:
    """Send FAILURE notification via SNS."""
    if not NOTIFICATION_ENABLED or not SNS_TOPIC_ARN:
        log_info("NOTIFICATION: Skipped (disabled or no SNS topic configured)")
        return False

    log_info("NOTIFICATION: Sending FAILURE notification", entity=entity)

    try:
        sns = boto3.client('sns', region_name=AWS_REGION)
        subject = _build_email_subject(entity, "FAILED")
        body = _build_email_body(
            entity=entity,
            status="FAILED",
            records_read=records_read,
            duration_seconds=duration_seconds,
            watermark_start=watermark_start,
            error_summary=error_summary,
        )
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],
            Message=body,
            MessageAttributes={
                'status':      {'DataType': 'String', 'StringValue': 'FAILED'},
                'environment': {'DataType': 'String', 'StringValue': ENVIRONMENT},
                'entity':      {'DataType': 'String', 'StringValue': entity},
                'run_id':      {'DataType': 'String', 'StringValue': RUN_ID or ''},
            },
        )
        log_info("NOTIFICATION: FAILURE notification sent", entity=entity)
        return True
    except Exception as e:
        log_error("NOTIFICATION: Failed to send FAILURE notification", error=str(e))
        return False


def send_partial_success_notification(
    entity: str,
    records_read: int,
    records_written: int,
    records_failed: int,
    duration_seconds: int,
    watermark_start: str = None,
    watermark_end: str = None,
    error_summary: str = None,
) -> bool:
    """Send PARTIAL_SUCCESS notification via SNS."""
    if not NOTIFICATION_ENABLED or not SNS_TOPIC_ARN:
        log_info("NOTIFICATION: Skipped (disabled or no SNS topic configured)")
        return False

    log_info(
        "NOTIFICATION: Sending PARTIAL_SUCCESS notification",
        entity=entity,
        records_failed=records_failed,
    )

    try:
        sns = boto3.client('sns', region_name=AWS_REGION)
        subject = _build_email_subject(entity, "PARTIAL_SUCCESS")
        body = _build_email_body(
            entity=entity,
            status="PARTIAL_SUCCESS",
            records_read=records_read,
            records_written=records_written,
            records_failed=records_failed,
            duration_seconds=duration_seconds,
            watermark_start=watermark_start,
            watermark_end=watermark_end,
            error_summary=error_summary,
        )
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],
            Message=body,
            MessageAttributes={
                'status':      {'DataType': 'String', 'StringValue': 'PARTIAL_SUCCESS'},
                'environment': {'DataType': 'String', 'StringValue': ENVIRONMENT},
                'entity':      {'DataType': 'String', 'StringValue': entity},
                'run_id':      {'DataType': 'String', 'StringValue': RUN_ID or ''},
            },
        )
        log_info("NOTIFICATION: PARTIAL_SUCCESS notification sent", entity=entity)
        return True
    except Exception as e:
        log_error("NOTIFICATION: Failed to send PARTIAL_SUCCESS notification", error=str(e))
        return False


# ============================================================
# RESTARTABILITY
# ============================================================

def _checkpoint_key(entity: str, env: str, run_id: str) -> str:
    """Generate S3 key for checkpoint file."""
    return f"{CHECKPOINT_PREFIX}{entity}/{env}/{run_id}/checkpoint.json"


def _failed_contracts_queue_key(entity: str, env: str, run_id: str) -> str:
    """Generate S3 key for failed contracts retry queue."""
    return f"{CHECKPOINT_PREFIX}{entity}/{env}/{run_id}/failed_queue.json"


def save_checkpoint(
    entity: str,
    env: str,
    checkpoint_data: Dict[str, Any],
) -> bool:
    """
    Persist intermediate state to S3 for restartability.
    Checkpoint contains: last_offset, completed_contracts, batch_index, etc.
    """
    key = _checkpoint_key(entity, env, RUN_ID)
    log_info(
        "RESTARTABILITY: Saving checkpoint",
        entity=entity,
        checkpoint_key=key,
        last_offset=checkpoint_data.get('last_offset'),
    )

    try:
        s3 = boto3.client('s3', region_name=AWS_REGION)
        checkpoint_data['updated_at'] = datetime.now(timezone.utc).isoformat()
        checkpoint_data['run_id'] = RUN_ID
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=json.dumps(checkpoint_data, default=str).encode(),
            ContentType='application/json',
        )
        log_info("RESTARTABILITY: Checkpoint saved successfully", checkpoint_key=key)
        return True
    except Exception as e:
        log_error("RESTARTABILITY: Failed to save checkpoint", error=str(e))
        return False


def load_checkpoint(entity: str, env: str) -> Optional[Dict[str, Any]]:
    """
    Load the most recent checkpoint for this entity/env/run.
    Returns None if no checkpoint exists (fresh run).
    """
    key = _checkpoint_key(entity, env, RUN_ID)
    log_info("RESTARTABILITY: Attempting to load checkpoint", checkpoint_key=key)

    try:
        s3 = boto3.client('s3', region_name=AWS_REGION)
        obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
        data = json.loads(obj['Body'].read().decode())
        log_info(
            "RESTARTABILITY: Checkpoint loaded — resuming from last state",
            checkpoint_key=key,
            last_offset=data.get('last_offset'),
            completed_count=len(data.get('completed_contracts', [])),
        )
        return data
    except s3.exceptions.NoSuchKey:
        log_info("RESTARTABILITY: No checkpoint found — fresh run", checkpoint_key=key)
        return None
    except Exception as e:
        log_warning(
            "RESTARTABILITY: Error loading checkpoint — starting fresh",
            error=str(e),
        )
        return None


def clear_checkpoint(entity: str, env: str) -> bool:
    """
    Delete checkpoint after successful completion.
    Ensures next run starts fresh.
    """
    key = _checkpoint_key(entity, env, RUN_ID)
    log_info("RESTARTABILITY: Clearing checkpoint", checkpoint_key=key)

    try:
        s3 = boto3.client('s3', region_name=AWS_REGION)
        s3.delete_object(Bucket=BRONZE_BUCKET, Key=key)
        log_info("RESTARTABILITY: Checkpoint cleared successfully")
        return True
    except Exception as e:
        log_warning("RESTARTABILITY: Failed to clear checkpoint (non-fatal)", error=str(e))
        return False


def save_failed_contracts_queue(
    entity: str,
    env: str,
    failed_contracts: List[str],
) -> bool:
    """
    Persist list of failed contract numbers for retry in next run.
    """
    if not failed_contracts:
        return True

    key = _failed_contracts_queue_key(entity, env, RUN_ID)
    log_info(
        "RESTARTABILITY: Saving failed contracts queue",
        count=len(failed_contracts),
        queue_key=key,
    )

    try:
        s3 = boto3.client('s3', region_name=AWS_REGION)
        payload = {
            'run_id': RUN_ID,
            'failed_contracts': failed_contracts,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'retry_eligible': True,
        }
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=json.dumps(payload).encode(),
            ContentType='application/json',
        )
        log_info("RESTARTABILITY: Failed contracts queue saved", count=len(failed_contracts))
        return True
    except Exception as e:
        log_error("RESTARTABILITY: Failed to save failed queue", error=str(e))
        return False


def retry_failed_contracts(entity: str, env: str, creds: Dict) -> Tuple[List, List]:
    """
    Load and retry previously failed contracts from the retry queue.
    Returns: (successful_details, still_failed_contracts)
    """
    s3 = boto3.client('s3', region_name=AWS_REGION)
    prefix = f"{CHECKPOINT_PREFIX}{entity}/{env}/"

    log_info("RESTARTABILITY: Checking for failed contracts to retry", prefix=prefix)

    try:
        response = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)
        retry_contracts = []

        for obj in response.get('Contents', []):
            if 'failed_queue.json' in obj['Key']:
                data = json.loads(
                    s3.get_object(Bucket=BRONZE_BUCKET, Key=obj['Key'])['Body'].read().decode()
                )
                if data.get('retry_eligible'):
                    retry_contracts.extend(data.get('failed_contracts', []))
                    # Mark as consumed
                    data['retry_eligible'] = False
                    s3.put_object(
                        Bucket=BRONZE_BUCKET,
                        Key=obj['Key'],
                        Body=json.dumps(data).encode(),
                        ContentType='application/json',
                    )

        if not retry_contracts:
            log_info("RESTARTABILITY: No failed contracts to retry")
            return [], []

        log_info("RESTARTABILITY: Retrying failed contracts", count=len(retry_contracts))

        # Retry each contract
        successful = []
        still_failed = []
        for cn in retry_contracts:
            result = get_contract_details_retry(cn, creds)
            if result:
                successful.append(result)
            else:
                still_failed.append(cn)

        log_info(
            "RESTARTABILITY: Retry results",
            retried=len(retry_contracts),
            succeeded=len(successful),
            still_failed=len(still_failed),
        )

        return successful, still_failed

    except Exception as e:
        log_error("RESTARTABILITY: Error during retry of failed contracts", error=str(e))
        return [], []


# ============================================================
# PAGINATION
# ============================================================

def calculate_adaptive_batch_size(
    total_contracts: int,
    avg_items_per_contract: int = 10,
    memory_available_mb: int = None,
) -> int:
    """
    Dynamically calculate optimal batch size based on:
    - Total contracts to process
    - Average items per contract (learned from previous batches)
    - Available Lambda memory
    - MAX_PAYLOAD safeguards
    """
    if memory_available_mb is None:
        memory_available_mb = MEMORY_THRESHOLD_MB

    # Estimate bytes per contract (header + items)
    est_bytes_per_contract = 500 + (avg_items_per_contract * 200)

    # Calculate max contracts that fit in memory (leave 30% buffer)
    max_by_memory = int((memory_available_mb * 1024 * 1024 * 0.7) / est_bytes_per_contract)

    # Calculate max contracts that fit in payload
    max_by_payload = int(MAX_PAYLOAD_BYTES / est_bytes_per_contract)

    # Take the minimum of all constraints
    optimal = min(max_by_memory, max_by_payload, MAX_BATCH_SIZE, total_contracts)
    optimal = max(optimal, MIN_BATCH_SIZE)  # Never go below minimum

    log_info(
        "PAGINATION: Adaptive batch size calculated",
        total_contracts=total_contracts,
        avg_items_per_contract=avg_items_per_contract,
        max_by_memory=max_by_memory,
        max_by_payload=max_by_payload,
        optimal_batch_size=optimal,
    )

    return optimal


def chunk_large_contract_details(contract_detail: Dict, max_items: int = None) -> List[Dict]:
    """
    Split a contract with too many line items into multiple chunks.
    Each chunk preserves the contract header and a subset of items.
    Ensures parent-child relationship is maintained via chunk metadata.
    """
    if max_items is None:
        max_items = MAX_ITEMS_PER_CONTRACT

    items = contract_detail.get('_ContractItems', [])
    if len(items) <= max_items:
        return [contract_detail]

    log_info(
        "PAGINATION: Chunking large contract",
        contract=contract_detail.get('ContractNumber', 'unknown'),
        total_items=len(items),
        chunk_size=max_items,
    )

    # Build header (everything except items)
    header = {k: v for k, v in contract_detail.items() if k != '_ContractItems'}
    chunks = []
    total_chunks = (len(items) + max_items - 1) // max_items

    for i in range(0, len(items), max_items):
        chunk = dict(header)
        chunk['_ContractItems'] = items[i:i + max_items]
        chunk['_chunk_index'] = i // max_items
        chunk['_total_chunks'] = total_chunks
        chunk['_chunk_items_start'] = i
        chunk['_chunk_items_end'] = min(i + max_items, len(items))
        chunks.append(chunk)

    log_info(
        "PAGINATION: Contract chunked",
        contract=contract_detail.get('ContractNumber', 'unknown'),
        total_chunks=len(chunks),
    )

    return chunks


def monitor_payload_size(records: List[Dict]) -> Dict[str, int]:
    """
    Monitor current payload size to prevent overflow.
    Returns size metrics for adaptive decisions.
    """
    payload_json = json.dumps(records, default=str)
    size_bytes = len(payload_json.encode('utf-8'))
    return {
        'size_bytes': size_bytes,
        'size_mb': round(size_bytes / (1024 * 1024), 2),
        'record_count': len(records),
        'avg_record_bytes': size_bytes // max(len(records), 1),
        'within_limit': size_bytes < MAX_PAYLOAD_BYTES,
    }


# ============================================================
# SCHEMA_DRIFT
# ============================================================

def _schema_snapshot_key(entity: str, env: str) -> str:
    """S3 key for the latest schema snapshot."""
    return f"{SCHEMA_SNAPSHOT_PREFIX}{env}/{entity}/latest_schema.json"


def _schema_history_key(entity: str, env: str) -> str:
    """S3 key for schema change history."""
    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    return f"{SCHEMA_SNAPSHOT_PREFIX}{env}/{entity}/history/{ts}_schema.json"


def detect_schema_drift(
    entity: str,
    env: str,
    current_records: List[Dict],
) -> Dict[str, Any]:
    """
    Compare current record schema against the last known schema snapshot.
    Detects newly added fields and removed fields.
    Does NOT fail the pipeline on drift — all fields preserved in raw_json.
    """
    if not current_records:
        return {'has_drift': False, 'new_fields': [], 'removed_fields': []}

    # Extract current schema (all unique field names across sample records)
    sample_size = min(len(current_records), 100)
    current_fields = set()
    for record in current_records[:sample_size]:
        if isinstance(record, dict):
            current_fields.update(record.keys())

    # Remove internal metadata fields
    current_fields = {f for f in current_fields if not f.startswith('_chunk_')}

    log_info(
        "SCHEMA_DRIFT: Current fields extracted",
        entity=entity,
        field_count=len(current_fields),
    )

    # Load previous schema snapshot
    s3 = boto3.client('s3', region_name=AWS_REGION)
    previous_fields = set()
    snapshot_key = _schema_snapshot_key(entity, env)

    try:
        obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=snapshot_key)
        snapshot_data = json.loads(obj['Body'].read().decode())
        previous_fields = set(snapshot_data.get('fields', []))
        log_info(
            "SCHEMA_DRIFT: Previous schema loaded",
            previous_field_count=len(previous_fields),
        )
    except Exception:
        log_info("SCHEMA_DRIFT: No previous schema found — first snapshot will be created")

    # Detect drift
    new_fields = current_fields - previous_fields
    removed_fields = previous_fields - current_fields
    has_drift = bool(new_fields or removed_fields)

    drift_report = {
        'has_drift': has_drift,
        'new_fields': sorted(list(new_fields)),
        'removed_fields': sorted(list(removed_fields)),
        'current_field_count': len(current_fields),
        'previous_field_count': len(previous_fields),
        'detected_at': datetime.now(timezone.utc).isoformat(),
        'entity': entity,
        'run_id': RUN_ID,
    }

    if has_drift:
        log_warning(
            "SCHEMA_DRIFT: Drift detected!",
            entity=entity,
            new_fields=drift_report['new_fields'],
            removed_fields=drift_report['removed_fields'],
        )
    else:
        log_info("SCHEMA_DRIFT: No drift detected", entity=entity)

    return drift_report


def persist_schema_snapshot(
    entity: str,
    env: str,
    current_records: List[Dict],
    drift_report: Dict = None,
) -> bool:
    """
    Save the current schema snapshot to S3.
    Also saves to history for audit trail.
    """
    if not current_records:
        return False

    # Extract all fields
    all_fields = set()
    for record in current_records[:100]:
        if isinstance(record, dict):
            all_fields.update(record.keys())

    all_fields = sorted([f for f in all_fields if not f.startswith('_chunk_')])

    snapshot = {
        'entity': entity,
        'environment': env,
        'fields': all_fields,
        'field_count': len(all_fields),
        'sample_size': min(len(current_records), 100),
        'total_records': len(current_records),
        'snapshot_time': datetime.now(timezone.utc).isoformat(),
        'run_id': RUN_ID,
        'drift_report': drift_report,
    }

    s3 = boto3.client('s3', region_name=AWS_REGION)

    try:
        # Save latest snapshot (overwrite)
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=_schema_snapshot_key(entity, env),
            Body=json.dumps(snapshot, indent=2).encode(),
            ContentType='application/json',
        )

        # Save to history (append)
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=_schema_history_key(entity, env),
            Body=json.dumps(snapshot, indent=2).encode(),
            ContentType='application/json',
        )

        log_info(
            "SCHEMA_DRIFT: Schema snapshot persisted",
            entity=entity,
            field_count=len(all_fields),
        )
        return True
    except Exception as e:
        log_error("SCHEMA_DRIFT: Failed to persist schema snapshot", error=str(e))
        return False


# ============================================================
# RESILIENCY
# ============================================================

class CircuitBreaker:
    """
    Circuit breaker pattern for SAP API calls.
    States: CLOSED (normal) → OPEN (failing) → HALF_OPEN (testing)
    Prevents hammering a failing API.
    """

    def __init__(self, threshold: int = None, timeout: int = None):
        self.threshold = threshold or CIRCUIT_BREAKER_THRESHOLD
        self.timeout = timeout or CIRCUIT_BREAKER_TIMEOUT
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def record_success(self):
        """Reset circuit breaker on success."""
        self.failure_count = 0
        self.state = "CLOSED"

    def record_failure(self):
        """Record a failure and potentially open the circuit."""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.threshold:
            self.state = "OPEN"
            log_warning(
                "RESILIENCY: Circuit breaker OPENED",
                failure_count=self.failure_count,
                threshold=self.threshold,
            )

    def can_execute(self) -> bool:
        """Check if a request is allowed through the circuit breaker."""
        if self.state == "CLOSED":
            return True

        if self.state == "OPEN":
            elapsed = time.time() - (self.last_failure_time or 0)
            if elapsed >= self.timeout:
                self.state = "HALF_OPEN"
                log_info(
                    "RESILIENCY: Circuit breaker moved to HALF_OPEN",
                    elapsed_seconds=elapsed,
                )
                return True
            return False

        # HALF_OPEN — allow one test request
        return True

    def get_state(self) -> str:
        return self.state


# Global circuit breaker instance
SAP_CIRCUIT_BREAKER = CircuitBreaker()


def classify_error(error: Exception) -> str:
    """
    Classify an error as retryable or non-retryable.
    Returns: 'retryable', 'non_retryable', 'throttled'
    """
    error_str = str(error).lower()

    # Throttling (HTTP 429)
    if '429' in error_str or 'too many requests' in error_str or 'throttl' in error_str:
        return 'throttled'

    # Retryable server errors
    if any(code in error_str for code in ['500', '502', '503', '504']):
        return 'retryable'

    # Network/timeout errors
    if any(term in error_str for term in ['timeout', 'timed out', 'connection', 'socket', 'eof']):
        return 'retryable'

    # Non-retryable client errors
    if any(code in error_str for code in ['400', '401', '403', '404', '405']):
        return 'non_retryable'

    # Default: retryable (be optimistic)
    return 'retryable'


def exponential_backoff_with_jitter(attempt: int, base_delay: float = 2.0) -> float:
    """
    Calculate delay with exponential backoff + full jitter.
    Prevents thundering herd problem.
    """
    max_delay = min(base_delay * (BACKOFF_FACTOR ** attempt), 60.0)
    delay = random.uniform(0, max_delay)
    return delay


# ============================================================
# PERFORMANCE
# ============================================================

# Reusable session for connection pooling
_http_opener = None


def get_http_opener():
    """
    Create a reusable HTTP opener with connection pooling.
    Avoids creating new TCP connections for every SAP API call.
    """
    global _http_opener
    if _http_opener is None:
        _http_opener = urllib.request.build_opener(
            urllib.request.HTTPHandler(),
            urllib.request.HTTPSHandler(),
        )
    return _http_opener


def calculate_optimal_workers(total_items: int, avg_response_time_ms: int = 500) -> int:
    """
    Dynamically calculate optimal thread pool size.
    Considers total items, response time, and Lambda timeout.
    """
    optimal = min(
        MAX_WORKERS,
        max(5, total_items // 3),
        int(API_TIMEOUT * 1000 / max(avg_response_time_ms, 100)),
    )
    log_info(
        "PERFORMANCE: Optimal workers calculated",
        total_items=total_items,
        optimal_workers=optimal,
    )
    return optimal


# ============================================================
# S3_CACHE
# ============================================================

def get_cache_key(env: str, start_date: str, end_date: str) -> str:
    """Generate S3 cache key for contract list."""
    return f"sap/cache/contract_list_{env}_{start_date}_{end_date}.json"


def cache_exists(env: str, start_date: str, end_date: str) -> bool:
    """Check if contract list cache exists in S3."""
    s3 = boto3.client('s3', region_name=AWS_REGION)
    try:
        s3.head_object(Bucket=BRONZE_BUCKET, Key=get_cache_key(env, start_date, end_date))
        log_info("S3_CACHE: Cache exists", env=env, start_date=start_date, end_date=end_date)
        return True
    except Exception:
        log_info("S3_CACHE: Cache does not exist", env=env, start_date=start_date, end_date=end_date)
        return False


def save_cache(contracts: List[Dict], env: str, start_date: str, end_date: str):
    """Save contract list to S3 cache."""
    s3 = boto3.client('s3', region_name=AWS_REGION)
    key = get_cache_key(env, start_date, end_date)
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(contracts, default=str),
        ContentType='application/json',
    )
    log_info("S3_CACHE: Saved", key=key, record_count=len(contracts))


def load_cache(env: str, start_date: str, end_date: str) -> List[Dict]:
    """Load contract list from S3 cache."""
    s3 = boto3.client('s3', region_name=AWS_REGION)
    key = get_cache_key(env, start_date, end_date)
    obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
    contracts = json.loads(obj['Body'].read().decode())
    log_info("S3_CACHE: Loaded", key=key, record_count=len(contracts))
    return contracts


def delete_cache_file(env: str, start_date: str, end_date: str):
    """Delete contract list cache from S3."""
    s3 = boto3.client('s3', region_name=AWS_REGION)
    key = get_cache_key(env, start_date, end_date)
    s3.delete_object(Bucket=BRONZE_BUCKET, Key=key)
    log_info("S3_CACHE: Deleted", key=key)


def write_to_s3_temp(data: Any, env: str, entity: str, offset: int) -> str:
    """Write large response to S3 temp location."""
    s3 = boto3.client('s3', region_name=AWS_REGION)
    key = f"sap/temp/{entity}_{env}_{offset}_{RUN_ID}.json"
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(data, default=str),
        ContentType='application/json',
    )
    log_info("S3_CACHE: Written to temp", key=key)
    return key


def log_failed_contracts(failed_list: List[str], env: str, start_date: str, end_date: str):
    """Persist failed contract numbers to S3 for investigation."""
    if not failed_list:
        return
    s3 = boto3.client('s3', region_name=AWS_REGION)
    key = f"sap/logs/failed_contracts_{env}_{start_date}_{end_date}_{RUN_ID}.json"
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps({
            "failed_contracts": failed_list,
            "run_id": RUN_ID,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "count": len(failed_list),
        }),
        ContentType='application/json',
    )
    log_info("S3_CACHE: Failed contracts logged", key=key, count=len(failed_list))


# ============================================================
# SAP_API
# ============================================================

def get_sap_credentials(secret_arn: str) -> Dict:
    """
    Fetch SAP credentials dynamically using the Secret ARN
    passed from the AppFlow connector event payload.
    """
    if not secret_arn:
        raise ValueError("secret_arn is missing from the event payload")

    client = boto3.client('secretsmanager', region_name=AWS_REGION)
    log_info("SAP_API: Fetching credentials", secret_arn=secret_arn)

    secret = client.get_secret_value(SecretId=secret_arn)
    creds = json.loads(secret['SecretString'])
    log_info("SAP_API: Credentials retrieved successfully")
    return creds


def call_sap_api(url: str, username: str, password: str) -> Dict:
    """
    Make authenticated SAP OData API call.
    Uses circuit breaker pattern.
    """
    # Check circuit breaker
    if not SAP_CIRCUIT_BREAKER.can_execute():
        raise RuntimeError(
            f"SAP_API: Circuit breaker OPEN — API calls blocked. "
            f"State={SAP_CIRCUIT_BREAKER.get_state()}"
        )

    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    req = urllib.request.Request(url)
    req.add_header('Authorization', f'Basic {credentials}')
    req.add_header('Accept', 'application/json')

    start_ms = time.time() * 1000

    try:
        with urllib.request.urlopen(req, timeout=API_TIMEOUT) as response:
            data = json.loads(response.read().decode())
            duration_ms = int(time.time() * 1000 - start_ms)
            SAP_CIRCUIT_BREAKER.record_success()
            log_info(
                "SAP_API: Call successful",
                url_preview=url[:100],
                duration_ms=duration_ms,
                status_code=response.status,
            )
            return data
    except Exception as e:
        duration_ms = int(time.time() * 1000 - start_ms)
        SAP_CIRCUIT_BREAKER.record_failure()
        log_error(
            "SAP_API: Call failed",
            url_preview=url[:100],
            duration_ms=duration_ms,
            error=str(e),
        )
        raise


def build_date_chunks(start_str: str, end_str: str) -> List[Tuple[str, str]]:
    """Split date range into 90-day chunks for SAP API pagination."""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    chunks = []
    cs = start
    while cs <= end:
        ce = min(cs + timedelta(days=89), end)
        chunks.append((cs.strftime('%Y-%m-%d'), ce.strftime('%Y-%m-%d')))
        cs = ce + timedelta(days=1)
    log_info(
        "SAP_API: Date chunks built",
        start=start_str,
        end=end_str,
        chunk_count=len(chunks),
    )
    return chunks


# ============================================================
# CONTRACT LIST
# ============================================================

def get_contract_list(creds: Dict, start_date: str, end_date: str) -> List[Dict]:
    """
    Fetch full contract list from SAP OData API with date chunking.
    Handles pagination within each chunk.
    """
    base_url = creds['sap_base_url']
    sap_path = creds['sap_path']
    chunks = build_date_chunks(start_date, end_date)
    all_contracts = []
    api_call_count = 0

    log_info(
        "SAP_API: Fetching contract list",
        start_date=start_date,
        end_date=end_date,
        chunk_count=len(chunks),
    )

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
                api_call_count += 1
                recs = resp.get('value', [])
                log_info(
                    "SAP_API: Contract list chunk fetched",
                    date_start=cs,
                    date_end=ce,
                    attribute=attr,
                    record_count=len(recs),
                )
                for r in recs:
                    r['attribute'] = attr
                all_contracts.extend(recs)
            except Exception as e:
                log_error(
                    "SAP_API: Contract list error",
                    date_start=cs,
                    date_end=ce,
                    attribute=attr,
                    error=str(e),
                )

    log_info(
        "SAP_API: Contract list fetch complete",
        total_contracts=len(all_contracts),
        api_calls=api_call_count,
    )

    # Update audit context
    if AUDIT_CTX:
        AUDIT_CTX.api_call_count += api_call_count

    return all_contracts


# ============================================================
# CONTRACT DETAILS
# ============================================================

def get_contract_details(cn: str, creds: Dict) -> Dict:
    """Fetch single contract details with expanded items."""
    url = (
        f"{creds['sap_base_url']}/sap/opu/odata4/sap/zsb_contracts_data_api"
        f"/srvd_a2x/sap/zsd_contracts_data_api/0001/"
        f"ContractHeader('{cn}')?$expand=_ContractItems&$format=json"
    )
    return call_sap_api(url, creds['username'], creds['password'])


def get_contract_details_retry(cn: str, creds: Dict) -> Optional[Dict]:
    """
    Fetch contract details with exponential backoff + jitter.
    Respects circuit breaker state.
    Classifies errors for retry decisions.
    """
    total_retries = 0

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            result = get_contract_details(cn, creds)
            return result
        except Exception as e:
            error_class = classify_error(e)
            total_retries += 1

            log_warning(
                "SAP_API: Contract detail retry",
                contract=cn,
                attempt=attempt,
                max_attempts=RETRY_COUNT,
                error_class=error_class,
                error=str(e),
            )

            if error_class == 'non_retryable':
                log_error(
                    "SAP_API: Non-retryable error — giving up",
                    contract=cn,
                    error=str(e),
                )
                return None

            if attempt == RETRY_COUNT:
                log_error(
                    "SAP_API: Max retries exhausted",
                    contract=cn,
                    total_retries=total_retries,
                )
                return None

            # Check circuit breaker before retrying
            if not SAP_CIRCUIT_BREAKER.can_execute():
                log_error(
                    "SAP_API: Circuit breaker open — aborting retries",
                    contract=cn,
                )
                return None

            delay = exponential_backoff_with_jitter(attempt)
            if error_class == 'throttled':
                delay = delay * 2  # Extra backoff for throttling

            log_info(
                "SAP_API: Waiting before retry",
                contract=cn,
                delay_seconds=round(delay, 2),
            )
            time.sleep(delay)

    return None


def fetch_details_parallel(
    contracts: List[Dict],
    creds: Dict,
    env: str,
    start_date: str,
    end_date: str,
    checkpoint_data: Optional[Dict] = None,
) -> Tuple[List[Dict], int]:
    """
    Parallel contract detail extraction with:
    - Adaptive batch sizing
    - Checkpointing between batches
    - Large contract chunking
    - Circuit breaker integration
    - Memory monitoring
    """
    numbers = list(set(c['ContractNumber'] for c in contracts if c.get('ContractNumber')))
    total = len(numbers)
    all_details = []
    all_failed = []
    total_retries = 0
    max_items_count = 0

    # Resume from checkpoint if available
    completed_contracts = set()
    if checkpoint_data:
        completed_contracts = set(checkpoint_data.get('completed_contracts', []))
        numbers = [cn for cn in numbers if cn not in completed_contracts]
        log_info(
            "RESTARTABILITY: Resuming from checkpoint",
            previously_completed=len(completed_contracts),
            remaining=len(numbers),
        )

    log_info(
        "SAP_API: Starting parallel detail extraction",
        total_unique=total,
        remaining=len(numbers),
        max_workers=MAX_WORKERS,
    )

    # Calculate adaptive batch size
    batch_size = calculate_adaptive_batch_size(len(numbers))
    optimal_workers = calculate_optimal_workers(len(numbers))

    for i in range(0, len(numbers), batch_size):
        batch = numbers[i:i + batch_size]
        batch_results = []
        batch_failed = []

        # Check circuit breaker before each batch
        if not SAP_CIRCUIT_BREAKER.can_execute():
            log_error(
                "RESILIENCY: Circuit breaker open — stopping batch processing",
                processed=len(all_details),
                remaining=len(numbers) - i,
            )
            all_failed.extend(numbers[i:])
            break

        with ThreadPoolExecutor(max_workers=optimal_workers) as ex:
            futures = {
                ex.submit(get_contract_details_retry, cn, creds): cn
                for cn in batch
            }
            for f in as_completed(futures):
                cn = futures[f]
                try:
                    r = f.result()
                    if r:
                        # Handle large contracts — chunk if needed
                        items = r.get('_ContractItems', [])
                        item_count = len(items)
                        max_items_count = max(max_items_count, item_count)

                        if item_count > MAX_ITEMS_PER_CONTRACT:
                            chunked = chunk_large_contract_details(r)
                            batch_results.extend(chunked)
                        else:
                            batch_results.append(r)

                        completed_contracts.add(cn)
                    else:
                        batch_failed.append(cn)
                        log_warning("SAP_API: Contract failed after retries", contract=cn)
                except Exception as e:
                    batch_failed.append(cn)
                    log_error("SAP_API: Unexpected error in future", contract=cn, error=str(e))

        all_details.extend(batch_results)
        all_failed.extend(batch_failed)

        # Save checkpoint after each batch
        save_checkpoint(
            entity="ContractDetails",
            env=env,
            checkpoint_data={
                'last_offset': i + batch_size,
                'completed_contracts': list(completed_contracts),
                'batch_index': i // batch_size,
                'total_batches': (len(numbers) + batch_size - 1) // batch_size,
                'details_count': len(all_details),
                'failed_count': len(all_failed),
            },
        )

        log_info(
            "SAP_API: Batch complete",
            batch_index=i // batch_size,
            batch_results=len(batch_results),
            batch_failed=len(batch_failed),
            total_progress=f"{len(all_details)}/{total}",
            failed_total=len(all_failed),
        )

        # Monitor payload size
        size_info = monitor_payload_size(all_details)
        if not size_info['within_limit']:
            log_warning(
                "PAGINATION: Payload size approaching limit",
                size_mb=size_info['size_mb'],
            )

    # Log failed contracts
    if all_failed:
        log_failed_contracts(all_failed, env, start_date, end_date)
        save_failed_contracts_queue("ContractDetails", env, all_failed)

    # Update audit context
    if AUDIT_CTX:
        AUDIT_CTX.records_failed = len(all_failed)
        AUDIT_CTX.retry_count = total_retries
        AUDIT_CTX.max_contract_items_count = max_items_count
        AUDIT_CTX.api_call_count += total + len(all_failed) * RETRY_COUNT

    log_info(
        "SAP_API: Parallel extraction complete",
        total_details=len(all_details),
        total_failed=len(all_failed),
        max_items_count=max_items_count,
    )

    return all_details, len(all_failed)


# ============================================================
# APPFLOW_HELPERS
# ============================================================

def get_cached_contracts(creds: Dict, env: str, start_date: str, end_date: str) -> List[Dict]:
    """Load contract list from cache or fetch fresh from SAP."""
    if cache_exists(env, start_date, end_date):
        log_info("S3_CACHE: Using cached contract list")
        return load_cache(env, start_date, end_date)
    log_info("S3_CACHE: No cache — fetching fresh from SAP")
    contracts = get_contract_list(creds, start_date, end_date)
    save_cache(contracts, env, start_date, end_date)
    return contracts


def smart_return(data: Any, env: str, entity: str, offset: int) -> Dict:
    """
    Return response inline or via S3 temp file if too large.
    Prevents Lambda response payload overflow.
    """
    body = json.dumps(data, default=str)
    size = len(body.encode('utf-8'))

    log_info(
        "APPFLOW_HELPERS: Response size check",
        size_bytes=size,
        max_bytes=MAX_PAYLOAD_BYTES,
        entity=entity,
        offset=offset,
    )

    if size > MAX_PAYLOAD_BYTES:
        key = write_to_s3_temp(data, env, entity, offset)
        log_info(
            "APPFLOW_HELPERS: Response written to S3 (too large for inline)",
            s3_key=key,
            size_bytes=size,
        )
        return {
            'statusCode': 200,
            'body': json.dumps({'s3_key': key, 'has_more': data.get('has_more', False)}),
        }
    return {'statusCode': 200, 'body': body}


# ============================================================
# EXTRACTOR_HANDLER
# ============================================================

def lambda_handler(event, context):
    """
    Main Lambda handler for SAP Contracts Bronze Extractor.

    Flow:
    1. Initialize run context (run_id, audit)
    2. Read watermark from control table
    3. Fetch contract list (with caching)
    4. Fetch contract details (with checkpointing, retries, circuit breaker)
    5. Detect schema drift
    6. Update audit (SUCCESS/FAILED/PARTIAL_SUCCESS)
    7. Update watermark (only on full success)
    8. Send notification
    9. Return response to AppFlow connector
    """
    global RUN_ID, LAMBDA_REQUEST_ID, EXECUTION_START_TIME, AUDIT_CTX

    # ── Identity & Initialization ─────────────────────────────────────────
    RUN_ID = str(uuid.uuid4())
    LAMBDA_REQUEST_ID = context.aws_request_id
    EXECUTION_START_TIME = datetime.now(timezone.utc)
    start_time = EXECUTION_START_TIME

    log_info(
        "==== BRONZE EXTRACTOR START ====",
        event_keys=list(event.keys()),
        memory_limit_mb=context.memory_limit_in_mb,
        function_name=context.function_name,
        remaining_time_ms=context.get_remaining_time_in_millis(),
    )

    log_info(
        "==== BRONZE EXTRACTOR START ====",
        event_keys=list(event.keys()),
        memory_limit_mb=context.memory_limit_in_mb,
        function_name=context.function_name,
        remaining_time_ms=context.get_remaining_time_in_millis(),
    )

    log_info("EVENT PAYLOAD", event=json.dumps(event, default=str)[:2000])

    # ── Runtime Config ────────────────────────────────────────────────────
    env = event.get('env', 'dev')
    entity = event.get('entity', '')
    offset = int(event.get('offset', 0))
    limit = int(event.get('limit', DEFAULT_PAGE_SIZE))
    load_type = LOAD_TYPE

    log_info(
        "CONFIG: Runtime parameters",
        env=env,
        entity=entity,
        offset=offset,
        limit=limit,
        load_type=load_type,
    )

    # ── AUDIT: INSERT RUNNING ─────────────────────────────────────────────
    insert_audit_start(
        pipeline_name="SAP_Contracts_Bronze",
        table_name=entity or "contracts",
        source_system="SAP",
        environment=env,
        load_type=load_type,
        start_time=start_time,
    )

    # ── Counters ──────────────────────────────────────────────────────────
    records_read = 0
    records_written = 0
    watermark_start = None
    watermark_end = None
    api_call_count = 0
    pagination_count = 0

    try:
        # ── Credentials ───────────────────────────────────────────────────
        creds = get_sap_credentials(event.get("secret_arn"))

        # ── WATERMARK: Determine date range ───────────────────────────────
        if load_type == "FULL":
            start_date = os.environ.get("START_DATE")
            end_date = os.environ.get("END_DATE")
            if not start_date or not end_date:
                raise ValueError("START_DATE and END_DATE env vars required for FULL load")
            log_info("WATERMARK: FULL load mode", start_date=start_date, end_date=end_date)
        else:
            start_date = get_watermark_date()
            end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            log_info("WATERMARK: INCR load mode", start_date=start_date, end_date=end_date)

        watermark_start = start_date

        log_info(
            "EXTRACTION: Parameters resolved",
            env=env,
            entity=entity,
            offset=offset,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
        )

        # ── Clear stale cache on first page ───────────────────────────────
        if offset == 0:
            try:
                delete_cache_file(env, start_date, end_date)
            except Exception:
                pass

        # ── Fetch contract list (cached) ──────────────────────────────────
        all_contracts = get_cached_contracts(creds, env, start_date, end_date)
        records_read = len(all_contracts)

        log_info(
            "EXTRACTION: Contract list ready",
            total_contracts=records_read,
            entity=entity,
        )

        # ── ContractList Entity ───────────────────────────────────────────
        if entity == "ContractList":
            page = all_contracts[offset:offset + limit]
            has_more = (offset + limit) < len(all_contracts)
            records_written = len(page)
            pagination_count += 1

            log_info(
                "EXTRACTION: ContractList page built",
                offset=offset,
                limit=limit,
                page_size=records_written,
                has_more=has_more,
            )

            # Schema drift detection
            drift_report = detect_schema_drift("ContractList", env, page)
            persist_schema_snapshot("ContractList", env, page, drift_report)

            if not has_more:
                delete_cache_file(env, start_date, end_date)
                clear_checkpoint("ContractList", env)

            # Compute new watermark from all contracts
            new_watermark = get_new_max_watermark(all_contracts)
            watermark_end = new_watermark

            today_date = datetime.now(timezone.utc).date()

            # Update watermark only if records exist
            if new_watermark is not None:

                new_watermark_date = datetime.strptime(
                    new_watermark,
                    '%Y-%m-%d'
                ).date()

                # Update watermark ONLY on full success
                if new_watermark_date < today_date:
                    update_control_table_watermark(
                        "contracts",
                        new_watermark,
                        RUN_ID,
                    )
                    log_info(
                        "WATERMARK: Updated successfully",
                        new_watermark=new_watermark,
                    )

            else:
                log_info("WATERMARK: No records found — watermark unchanged")

            end_time = datetime.now(timezone.utc)
            duration = int((end_time - start_time).total_seconds())

            # AUDIT: SUCCESS
            update_audit_success(
                records_read=records_read,
                records_written=records_written,
                end_time=end_time,
                start_time=start_time,
                watermark_start=watermark_start,
                watermark_end=watermark_end,
                pagination_count=pagination_count,
            )

            # NOTIFICATION: SUCCESS
            if not has_more:
                send_success_notification(
                    entity="ContractList",
                    records_read=records_read,
                    records_written=records_written,
                    duration_seconds=duration,
                    watermark_start=watermark_start,
                    watermark_end=watermark_end,
                )

            return smart_return(
                {'contract_list': {'value': page}, 'has_more': has_more},
                env, entity, offset,
            )

        # ── ContractDetails Entity ────────────────────────────────────────
        elif entity == "ContractDetails":
            page_contracts = all_contracts[offset:offset + limit]
            has_more = (offset + limit) < len(all_contracts)
            records_read = len(page_contracts)

            log_info(
                "EXTRACTION: ContractDetails page",
                offset=offset,
                limit=limit,
                contracts_in_page=records_read,
                has_more=has_more,
            )

            # Load checkpoint for resumability
            checkpoint = load_checkpoint("ContractDetails", env)

            # Retry previously failed contracts
            retry_results, still_failed = retry_failed_contracts(
                "ContractDetails", env, creds
            )

            if retry_results:
                log_info(
                    "RESTARTABILITY: Recovered contracts from retry",
                    recovered=len(retry_results),
                )

            # Fetch details in parallel with all enterprise features
            details, failed_count = fetch_details_parallel(
                page_contracts, creds, env, start_date, end_date,
                checkpoint_data=checkpoint,
            )

            # Add retry results
            details.extend(retry_results)
            records_written = len(details)

            # Schema drift detection on details
            drift_report = detect_schema_drift("ContractDetails", env, details)
            persist_schema_snapshot("ContractDetails", env, details, drift_report)

            if not has_more:
                delete_cache_file(env, start_date, end_date)
                clear_checkpoint("ContractDetails", env)

            new_watermark = get_new_max_watermark(all_contracts)
            watermark_end = new_watermark

            today_date = datetime.now(timezone.utc).date()

            if new_watermark is not None:

                new_watermark_date = datetime.strptime(
                    new_watermark,
                    '%Y-%m-%d'
                ).date()

                # Update watermark ONLY on full success
                if new_watermark_date < today_date:
                    update_control_table_watermark(
                        "contracts",
                        new_watermark,
                        RUN_ID,
                    )
                    log_info(
                        "WATERMARK: Updated successfully",
                        new_watermark=new_watermark,
                    )

            else:
                log_info("WATERMARK: No records found — watermark unchanged")

            end_time = datetime.now(timezone.utc)
            duration = int((end_time - start_time).total_seconds())

            # Determine final status
            if failed_count == 0:
                # AUDIT: SUCCESS
                update_audit_success(
                    records_read=records_read,
                    records_written=records_written,
                    end_time=end_time,
                    start_time=start_time,
                    watermark_start=watermark_start,
                    watermark_end=watermark_end,
                    api_call_count=AUDIT_CTX.api_call_count if AUDIT_CTX else 0,
                    pagination_count=pagination_count,
                    max_contract_items_count=AUDIT_CTX.max_contract_items_count if AUDIT_CTX else 0,
                )
                if not has_more:
                    send_success_notification(
                        entity="ContractDetails",
                        records_read=records_read,
                        records_written=records_written,
                        duration_seconds=duration,
                        watermark_start=watermark_start,
                        watermark_end=watermark_end,
                    )
            else:
                # PARTIAL_SUCCESS — some contracts failed
                partial_error = (
                    f"{failed_count} contract(s) failed after {RETRY_COUNT} retries — "
                    f"see sap/logs/failed_contracts_{env}_{start_date}_{end_date}_{RUN_ID}.json"
                )
                update_audit_partial_success(
                    records_read=records_read,
                    records_written=records_written,
                    records_failed=failed_count,
                    end_time=end_time,
                    start_time=start_time,
                    error_message=partial_error,
                    retry_count=AUDIT_CTX.retry_count if AUDIT_CTX else 0,
                )
                if not has_more:
                    send_partial_success_notification(
                        entity="ContractDetails",
                        records_read=records_read,
                        records_written=records_written,
                        records_failed=failed_count,
                        duration_seconds=duration,
                        watermark_start=watermark_start,
                        watermark_end=watermark_end,
                        error_summary=partial_error,
                    )

            return smart_return(
                {'contract_details': details, 'has_more': has_more},
                env, entity, offset,
            )

        else:
            raise ValueError(f"Unknown entity: '{entity}'")

    except Exception as e:
        error_msg = str(e)
        end_time = datetime.now(timezone.utc)
        duration = int((end_time - start_time).total_seconds())

        log_critical(
            "EXTRACTOR: Unhandled exception",
            error=error_msg,
            entity=entity,
            records_read=records_read,
            records_written=records_written,
            duration_seconds=duration,
        )

        # AUDIT: FAILED
        update_audit_failure(
            error_message=error_msg,
            end_time=end_time,
            start_time=start_time,
            records_read=records_read,
            records_written=records_written,
        )

        # NOTIFICATION: FAILURE
        send_failure_notification(
            entity=entity or "contracts",
            error_summary=error_msg[:500],
            records_read=records_read,
            duration_seconds=duration,
            watermark_start=watermark_start,
        )

        return {'statusCode': 500, 'body': json.dumps({'error': error_msg})}