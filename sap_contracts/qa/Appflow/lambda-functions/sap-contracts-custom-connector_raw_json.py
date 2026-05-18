import json
import boto3
import os
import logging
import time
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BRONZE_BUCKET       = os.environ.get("BRONZE_BUCKET", "epiq-edp-dl-qa-bronze")
AWS_REGION          = os.environ.get("AWS_REGION", "us-east-1")
RAW_JSON_FIELD_NAME = "raw_json"
DEFAULT_PAGE_SIZE   = 100
MAX_PAGE_SIZE       = 500
MAX_RESPONSE_BYTES  = 5_000_000


# ============================================================
# LOGGING
# ============================================================

def log(message, details=None):
    record = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "message": message
    }
    if details is not None:
        record["details"] = details
    print(json.dumps(record, default=str))


# ============================================================
# HELPERS
# ============================================================

def get_env_from_event(event):
    ctx  = event.get("connectorContext") or {}
    name = ctx.get("connectorProfileLabel") or ""
    return "qa" if "qa" in name.lower() else "dev"


def get_secret_arn_from_event(event):
    ctx   = event.get("connectorContext") or {}
    creds = ctx.get("credentials") or {}
    return creds.get("secretArn")


def get_execution_id(event):
    ctx = event.get("connectorContext") or {}
    eid = (
        ctx.get("executionId")
        or event.get("executionId")
        or event.get("ExecutionId")
    )
    if eid:
        value = str(eid).strip().lstrip("-")
        value = value.replace(":", "-").replace(" ", "_").replace("/", "_")
        return value or f"manual_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    return f"manual_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"


def get_next_token(event):
    return (
        event.get("nextToken")
        or event.get("dataPullNextToken")
        or event.get("NextToken")
    )


def get_page_size(event):
    raw = event.get("maxResults") or event.get("pageSize") or event.get("limit")
    try:
        size = int(raw) if raw else DEFAULT_PAGE_SIZE
    except Exception:
        size = DEFAULT_PAGE_SIZE
    return min(max(size, 1), MAX_PAGE_SIZE)


# ============================================================
# INVOKE EXTRACTOR
# ============================================================

def invoke_extractor(payload):
    client = boto3.client('lambda', region_name=AWS_REGION)
    result = client.invoke(
        FunctionName='sap-contracts-extractor',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    return json.loads(result['Payload'].read())


# ============================================================
# S3 TEMP READ + DELETE  (extractor large-response path)
# ============================================================

def read_and_delete_s3(s3_key):
    s3  = boto3.client('s3', region_name=AWS_REGION)
    obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=s3_key)
    data = json.loads(obj['Body'].read().decode())
    log("Read temp S3 key", {"key": s3_key})
    s3.delete_object(Bucket=BRONZE_BUCKET, Key=s3_key)
    log("Deleted temp S3 key", {"key": s3_key})
    return data


# ============================================================
# PER-EXECUTION S3 CACHE
# ============================================================

def cache_key_for(entity_key, execution_id):
    return f"sap/appflow_cache/{entity_key}/{execution_id}/data.json"


def s3_object_exists(key):
    s3 = boto3.client('s3', region_name=AWS_REGION)
    try:
        s3.head_object(Bucket=BRONZE_BUCKET, Key=key)
        return True
    except Exception:
        return False


def save_records_to_cache(key, records):
    s3 = boto3.client('s3', region_name=AWS_REGION)
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(records).encode(),
        ContentType="application/json"
    )
    log("Saved records to execution cache", {"key": key, "count": len(records)})


def load_records_from_cache(key):
    s3  = boto3.client('s3', region_name=AWS_REGION)
    obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
    records = json.loads(obj['Body'].read().decode())
    log("Loaded records from execution cache", {"key": key, "count": len(records)})
    return records


def delete_cache(key):
    s3 = boto3.client('s3', region_name=AWS_REGION)
    try:
        s3.delete_object(Bucket=BRONZE_BUCKET, Key=key)
        log("Deleted execution cache", {"key": key})
    except Exception as e:
        log("Failed to delete execution cache", {"key": key, "error": str(e)})


# ============================================================
# UNIVERSAL EXTRACTOR DATA PARSER
# Handles every possible structure the extractor might return.
# Logs exactly what it finds so you can see in CloudWatch.
# ============================================================

def extract_records_from_data(entity_key, data):
    """
    Tries every known structure to find the list of records.
    Logs what it finds at each step — check CloudWatch to see
    which branch fires for your extractor.
    """

    log("PARSER: received data", {
        "entity_key":     entity_key,
        "data_type":      type(data).__name__,
        "top_level_keys": list(data.keys()) if isinstance(data, dict) else "IS_LIST_OR_OTHER",
        "preview":        json.dumps(data, default=str)[:1000]
    })

    # ── If data is already a flat list of records ─────────────────────────
    if isinstance(data, list):
        log("PARSER: data is a list directly", {"count": len(data)})
        return data

    if not isinstance(data, dict):
        log("PARSER: data is neither list nor dict — returning empty", {"type": type(data).__name__})
        return []

    # ── ContractList paths ────────────────────────────────────────────────
    if entity_key == "contractlist":

        # Path 1: {"contract_list": {"value": [...]}}
        if "contract_list" in data:
            val = data["contract_list"]
            if isinstance(val, dict) and "value" in val:
                records = val["value"]
                log("PARSER: contractlist → contract_list.value", {"count": len(records)})
                return records
            if isinstance(val, list):
                log("PARSER: contractlist → contract_list (list)", {"count": len(val)})
                return val

        # Path 2: {"value": [...]}
        if "value" in data:
            val = data["value"]
            if isinstance(val, list):
                log("PARSER: contractlist → value", {"count": len(val)})
                return val

        # Path 3: {"data": [...]}
        if "data" in data:
            val = data["data"]
            if isinstance(val, list):
                log("PARSER: contractlist → data", {"count": len(val)})
                return val

        # Path 4: {"contracts": [...]}
        if "contracts" in data:
            val = data["contracts"]
            if isinstance(val, list):
                log("PARSER: contractlist → contracts", {"count": len(val)})
                return val

        # Path 5: {"items": [...]}
        if "items" in data:
            val = data["items"]
            if isinstance(val, list):
                log("PARSER: contractlist → items", {"count": len(val)})
                return val

        # Path 6: look for any key whose value is a non-empty list
        for k, v in data.items():
            if isinstance(v, list) and len(v) > 0:
                log("PARSER: contractlist → first list key found", {"key": k, "count": len(v)})
                return v

    # ── ContractDetails paths ─────────────────────────────────────────────
    elif entity_key == "contractdetails":

        # Path 1: {"contract_details": [...]}
        if "contract_details" in data:
            val = data["contract_details"]
            if isinstance(val, list):
                log("PARSER: contractdetails → contract_details", {"count": len(val)})
                return val

        # Path 2: {"contracts": [...]}
        if "contracts" in data:
            val = data["contracts"]
            if isinstance(val, list):
                log("PARSER: contractdetails → contracts", {"count": len(val)})
                return val

        # Path 3: {"value": [...]}
        if "value" in data:
            val = data["value"]
            if isinstance(val, list):
                log("PARSER: contractdetails → value", {"count": len(val)})
                return val

        # Path 4: {"data": [...]}
        if "data" in data:
            val = data["data"]
            if isinstance(val, list):
                log("PARSER: contractdetails → data", {"count": len(val)})
                return val

        # Path 5: {"items": [...]}
        if "items" in data:
            val = data["items"]
            if isinstance(val, list):
                log("PARSER: contractdetails → items", {"count": len(val)})
                return val

        # Path 6: look for any key whose value is a non-empty list
        for k, v in data.items():
            if isinstance(v, list) and len(v) > 0:
                log("PARSER: contractdetails → first list key found", {"key": k, "count": len(v)})
                return v

    # ── Nothing found — log full data so you can see in CloudWatch ────────
    log("PARSER: WARNING — no records found, full data dump", {
        "entity_key": entity_key,
        "full_data":  json.dumps(data, default=str)[:3000]
    })
    return []


def flatten_records(entity_key, raw_records):
    """
    Takes the raw list from the extractor and normalizes each record:
    - Strips @odata metadata keys
    - For ContractDetails: moves _ContractItems → ContractItems
    """
    result = []

    for r in raw_records:
        if not isinstance(r, dict):
            log("FLATTEN: skipping non-dict record", {"type": type(r).__name__, "value": str(r)[:200]})
            continue

        if entity_key == "contractdetails":
            # Separate header from items
            header = {
                k: v for k, v in r.items()
                if not k.startswith("@") and k != "_ContractItems"
            }
            header["ContractItems"] = r.get("_ContractItems", [])
            result.append(header)

        else:
            # ContractList or anything else — strip OData keys only
            clean = {k: v for k, v in r.items() if not k.startswith("@")}
            result.append(clean)

    log("FLATTEN: done", {"entity_key": entity_key, "input": len(raw_records), "output": len(result)})
    return result


# ============================================================
# FETCH ALL RECORDS FROM EXTRACTOR
# ============================================================

def fetch_all_records(entity, entity_key, env, secret_arn):
    all_records = []
    offset      = 0
    page_size   = int(os.environ.get("PAGE_SIZE", "50"))

    while True:
        log("Calling extractor", {
            "entity":    entity,
            "offset":    offset,
            "limit":     page_size,
            "env":       env
        })

        raw = invoke_extractor({
            "entity":     entity,
            "env":        env,
            "offset":     offset,
            "limit":      page_size,
            "secret_arn": secret_arn
        })

        # ── Check for Lambda-level error ──────────────────────────────────
        if raw.get("statusCode") == 500:
            err = json.loads(raw.get("body", "{}")).get("error", "Extractor error")
            raise RuntimeError(f"Extractor returned 500: {err}")

        # ── Parse body ────────────────────────────────────────────────────
        body_raw = raw.get("body", "{}")
        body     = json.loads(body_raw) if isinstance(body_raw, str) else body_raw
        has_more = body.get("has_more", False)
        s3_key   = body.get("s3_key")

        log("Extractor response meta", {
            "has_more":        has_more,
            "s3_key":          s3_key,
            "body_top_keys":   list(body.keys()) if isinstance(body, dict) else "NOT_DICT",
            "body_preview":    json.dumps(body, default=str)[:500]
        })

        # ── Get actual data (S3 or inline) ────────────────────────────────
        data = read_and_delete_s3(s3_key) if s3_key else body

        # ── Parse records from whatever structure the extractor returns ────
        raw_records = extract_records_from_data(entity_key, data)
        records     = flatten_records(entity_key, raw_records)

        all_records.extend(records)

        log("Extractor page complete", {
            "offset":       offset,
            "page_records": len(records),
            "total":        len(all_records),
            "has_more":     has_more
        })

        if not has_more:
            break

        offset += page_size

    log("All extractor pages fetched", {
        "entity":        entity,
        "total_records": len(all_records)
    })
    return all_records


# ============================================================
# BUILD APPFLOW RECORD — one field: raw_json
# ============================================================

def build_appflow_record(source_record):
    return {
        RAW_JSON_FIELD_NAME: json.dumps(
            source_record,
            ensure_ascii=False,
            default=str
        )
    }


# ============================================================
# PAGINATE RECORDS FOR APPFLOW
# ============================================================

def paginate_records(all_records, event):
    total     = len(all_records)
    start     = int(get_next_token(event) or 0)
    page_size = get_page_size(event)

    output = []
    idx    = start

    while idx < total and len(output) < page_size:
        candidate = json.dumps(
            build_appflow_record(all_records[idx]),
            ensure_ascii=False,
            default=str
        )
        test_size = len(json.dumps({
            "isSuccess": True,
            "records":   output + [candidate],
            "nextToken": None
        }).encode())
        if test_size > MAX_RESPONSE_BYTES:
            log("Response size limit reached", {"idx": idx})
            break
        output.append(candidate)
        idx += 1

    next_token = str(idx) if idx < total else None

    log("Page built", {
        "start":      start,
        "end":        idx,
        "page_count": len(output),
        "total":      total,
        "nextToken":  next_token
    })

    return {
        "isSuccess": True,
        "records":   output,
        "nextToken": next_token
    }


# ============================================================
# SINGLE AppFlow FIELD DEFINITION — only raw_json
# ============================================================

def raw_json_field():
    return {
        "fieldName":     RAW_JSON_FIELD_NAME,
        "dataType":      "String",
        "dataTypeLabel": "String",
        "label":         RAW_JSON_FIELD_NAME,
        "description":   "Complete SAP contract record as stringified JSON",
        "isPrimaryKey":  False,
        "defaultValue":  None,
        "isDeprecated":  False,
        "constraints":   None,
        "readProperties": {
            "isRetrievable":                        True,
            "isNullable":                           True,
            "isQueryable":                          False,
            "isTimestampFieldForIncrementalQueries": False
        },
        "writeProperties":  None,
        "filterOperators":  ["PROJECTION"],
        "customProperties": None
    }


def build_entity_response(identifier, label):
    return {
        "isSuccess":    True,
        "errorDetails": None,
        "entityDefinition": {
            "entity": {
                "entityIdentifier": identifier,
                "hasNestedEntities": False,
                "isWritable":       False,
                "label":            label,
                "description":      label
            },
            "fields": [raw_json_field()]
        }
    }


# ============================================================
# LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context):
    try:
        log("=== CONNECTOR INVOKED ===", {
            "type":   event.get("type"),
            "entity": event.get("entityIdentifier")
        })

        request_type = event.get("type", "")
        entity       = event.get("entityIdentifier", "") or ""
        entity_key   = entity.replace(" ", "").replace("_", "").lower()

        # ── DescribeConnectorConfiguration ─────────────────────────────────
        if request_type == "DescribeConnectorConfigurationRequest":
            return {
                "connectorOwner":   "epiq",
                "connectorName":    "SAPContractsConnector",
                "connectorVersion": "1.0",
                "connectorModes":   ["SOURCE"],
                "authenticationConfig": {
                    "isBasicAuthSupported":  None,
                    "isApiKeyAuthSupported": None,
                    "isOAuth2Supported":     None,
                    "isCustomAuthSupported": True,
                    "oAuth2Defaults":        None,
                    "customAuthConfig": [{"authenticationType": "NO_AUTH", "authParameters": []}]
                },
                "supportedApiVersions":        ["v1"],
                "isSuccess":                   True,
                "connectorRuntimeSetting":     None,
                "logoURL":                     None,
                "errorDetails":                None,
                "operatorsSupported":          ["PROJECTION", "NO_OP"],
                "triggerFrequenciesSupported": ["BYMINUTE", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "ONCE"],
                "supportedWriteOperations":    [],
                "supportedTriggerTypes":       ["SCHEDULED", "ONDEMAND"]
            }

        # ── ValidateCredentials ────────────────────────────────────────────
        elif request_type == "ValidateCredentialsRequest":
            return {"isSuccess": True, "errorDetails": None}

        # ── ValidateConnectorRuntimeSettings ──────────────────────────────
        elif request_type == "ValidateConnectorRuntimeSettingsRequest":
            return {"isSuccess": True, "errorsByInputField": None, "errorDetails": None}

        # ── ListEntities ───────────────────────────────────────────────────
        elif request_type == "ListEntitiesRequest":
            return {
                "isSuccess":    True,
                "errorDetails": None,
                "entities": [
                    {
                        "entityIdentifier": "ContractList",
                        "hasNestedEntities": False,
                        "isWritable":       False,
                        "label":            "Contract List",
                        "description":      "Contract List"
                    },
                    {
                        "entityIdentifier": "ContractDetails",
                        "hasNestedEntities": False,
                        "isWritable":       False,
                        "label":            "Contract Details",
                        "description":      "Contract Details"
                    }
                ],
                "nextToken":    None,
                "cacheControl": {"timeToLive": 1}
            }

        # ── DescribeEntity ─────────────────────────────────────────────────
        elif request_type == "DescribeEntityRequest":
            log("DescribeEntityRequest", {"entity": entity})

            if entity_key == "contractlist":
                return build_entity_response("ContractList", "Contract List")
            elif entity_key == "contractdetails":
                return build_entity_response("ContractDetails", "Contract Details")
            else:
                return {
                    "isSuccess": False,
                    "errorDetails": {
                        "errorCode":    "INVALID_ENTITY",
                        "errorMessage": f"Unknown entity: {entity}"
                    }
                }

        # ── QueryData ──────────────────────────────────────────────────────
        elif request_type == "QueryDataRequest":
            env          = get_env_from_event(event)
            secret_arn   = get_secret_arn_from_event(event)
            execution_id = get_execution_id(event)
            cache_key    = cache_key_for(entity_key, execution_id)
            next_token   = get_next_token(event)

            log("QueryDataRequest", {
                "env":          env,
                "entity":       entity,
                "execution_id": execution_id,
                "next_token":   next_token
            })

            if not secret_arn:
                return {
                    "isSuccess":    False,
                    "errorDetails": {
                        "errorCode":    "SERVER_ERROR",
                        "errorMessage": "secret_arn missing from AppFlow context"
                    },
                    "records": []
                }

            # First page → fetch all records from extractor and cache them
            # Later pages → load from cache (don't call extractor again)
            if next_token is None:
                log("First page — fetching all records from extractor")
                all_records = fetch_all_records(entity, entity_key, env, secret_arn)
                save_records_to_cache(cache_key, all_records)
            else:
                log("Subsequent page — loading from cache", {"next_token": next_token})
                all_records = load_records_from_cache(cache_key)

            log("Records ready for pagination", {"total": len(all_records)})

            response = paginate_records(all_records, event)

            # Delete cache only after the very last page
            if response["nextToken"] is None:
                delete_cache(cache_key)
            else:
                log("Cache retained for next page", {"nextToken": response["nextToken"]})

            return response

        # ── Unknown ────────────────────────────────────────────────────────
        else:
            log("Unknown request_type", {"type": request_type})
            return {
                "isSuccess":    False,
                "errorDetails": {
                    "errorCode":    "UNKNOWN_REQUEST",
                    "errorMessage": f"No handler for: {request_type}"
                }
            }

    except Exception as e:
        logger.error(f"UNHANDLED EXCEPTION: {str(e)}", exc_info=True)
        return {
            "isSuccess":    False,
            "errorDetails": {
                "errorCode":    "SERVER_ERROR",
                "errorMessage": f"Connector error: {str(e)}"
            }
        }