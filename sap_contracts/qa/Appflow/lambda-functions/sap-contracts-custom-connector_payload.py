import json
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BRONZE_BUCKET = "epiq-edp-dl-qa-bronze"
MAX_RESPONSE_SIZE = 5 * 1024 * 1024

# Single AppFlow field — full record stored as stringified JSON.
# AppFlow mapping stays stable regardless of SAP schema changes.
RAW_JSON_FIELD_NAME = "raw_json"


def invoke_extractor(payload):
    client = boto3.client('lambda', region_name='us-east-1')
    result = client.invoke(
        FunctionName='sap-contracts-extractor',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    return json.loads(result['Payload'].read())


def read_from_s3(s3_key):
    s3  = boto3.client('s3', region_name='us-east-1')
    obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=s3_key)
    data = json.loads(obj['Body'].read().decode())
    logger.info(f"Read from S3: {s3_key}")
    s3.delete_object(Bucket=BRONZE_BUCKET, Key=s3_key)
    logger.info(f"Deleted temp: {s3_key}")
    return data


def build_field_definition(field_name):
    """
    Returns a single AppFlow field definition for raw_json.
    The full source record is stored as a stringified JSON string in this field.
    This removes any dependency on hardcoded SAP field lists.
    """
    return {
        "fieldName": field_name,
        "dataType": "String",
        "dataTypeLabel": "String",
        "label": field_name,
        "description": "Complete SAP source record as stringified JSON",
        "isPrimaryKey": False,
        "defaultValue": None,
        "isDeprecated": None,
        "constraints": None,
        "readProperties": {
            "isRetrievable": True,
            "isNullable": True,
            "isQueryable": False,
            "isTimestampFieldForIncrementalQueries": False
        },
        "writeProperties": None,
        "filterOperators": ["PROJECTION"],
        "customProperties": None
    }


# Single field list shared by all entities.
RAW_JSON_FIELDS = [build_field_definition(RAW_JSON_FIELD_NAME)]


def build_appflow_record(source_record):
    """
    Wraps any SAP record dict into the single raw_json AppFlow field.

    Example output:
      {
        "raw_json": "{\"ContractNumber\": \"12345\", \"SalesOrganization\": \"1000\", ...}"
      }

    This means downstream consumers parse raw_json rather than relying
    on individual AppFlow field mappings that would break on SAP schema changes.
    """
    return {
        RAW_JSON_FIELD_NAME: json.dumps(source_record, ensure_ascii=False, default=str)
    }


def lambda_handler(event, context):
    logger.info("=== CONNECTOR RUNNING ===")

    request_type = event.get("type", "")
    entity       = event.get("entityIdentifier", "")
    entity_key   = (entity or "").replace(" ", "").replace("_", "").lower()

    logger.info(f"[TYPE] {request_type} | [ENTITY] {entity}")

    if request_type == "DescribeConnectorConfigurationRequest":
        return {
            "connectorOwner": "CUSTOM", "connectorName": "SAPODataConnector", "connectorVersion": "1.0",
            "connectorModes": ["SOURCE"],
            "authenticationConfig": {
                "isBasicAuthSupported": None, "isApiKeyAuthSupported": None, "isOAuth2Supported": None,
                "isCustomAuthSupported": True, "oAuth2Defaults": None,
                "customAuthConfig": [{"authenticationType": "No_Auth", "authParameters": []}]
            },
            "supportedApiVersions": ["v1"], "isSuccess": True,
            "connectorRuntimeSetting": None, "logoURL": None, "errorDetails": None,
            "operatorsSupported": [
                "PROJECTION", "LESS_THAN", "GREATER_THAN", "BETWEEN",
                "LESS_THAN_OR_EQUAL_TO", "GREATER_THAN_OR_EQUAL_TO",
                "EQUAL_TO", "CONTAINS", "NOT_EQUAL_TO", "ADDITION",
                "SUBTRACTION", "MULTIPLICATION", "DIVISION", "MASK_ALL",
                "MASK_FIRST_N", "MASK_LAST_N", "VALIDATE_NON_NULL",
                "VALIDATE_NON_ZERO", "VALIDATE_NON_NEGATIVE", "VALIDATE_NUMERIC", "NO_OP"
            ],
            "triggerFrequenciesSupported": ["BYMINUTE", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "ONCE"],
            "supportedWriteOperations": ["INSERT", "UPDATE", "UPSERT", "DELETE"],
            "supportedTriggerTypes": ["SCHEDULED", "ONDEMAND"]
        }

    elif request_type == "ValidateCredentialsRequest":
        return {"isSuccess": True, "errorDetails": None}

    elif request_type == "ValidateConnectorRuntimeSettingsRequest":
        return {"isSuccess": True, "errorsByInputField": None, "errorDetails": None}

    elif request_type == "ListEntitiesRequest":
        return {
            "isSuccess": True, "errorDetails": None,
            "entities": [
                {"entityIdentifier": "ContractList",    "hasNestedEntities": False, "isWritable": False, "label": "Contract List",    "description": "Contract List"},
                {"entityIdentifier": "ContractDetails", "hasNestedEntities": False, "isWritable": False, "label": "Contract Details", "description": "Contract Details"}
            ],
            "nextToken": None, "cacheControl": None
        }

    elif request_type == "DescribeEntityRequest":
        # Both entities now share the same single raw_json field.
        # No hardcoded field lists needed — add/remove SAP fields freely
        # without touching this connector.
        if entity_key == "contractlist":
            eid, label = "ContractList", "Contract List"
        elif entity_key == "contractdetails":
            eid, label = "ContractDetails", "Contract Details"
        else:
            return {"isSuccess": False, "errorDetails": None, "entityDefinition": None, "cacheControl": None}

        return {
            "isSuccess": True, "errorDetails": None,
            "entityDefinition": {
                "entity": {
                    "entityIdentifier": eid,
                    "hasNestedEntities": False,
                    "isWritable": False,
                    "label": label,
                    "description": label
                },
                "fields": RAW_JSON_FIELDS,
                "customProperties": None
            },
            "cacheControl": None
        }

    elif request_type == "QueryDataRequest":
        connector_context = event.get("connectorContext", {})
        connection_name   = connector_context.get("connectorProfileLabel", "")
        env               = "qa" if "qa" in connection_name.lower() else "dev"

        logger.info(f"FULL EVENT: {json.dumps(event)}")
        logger.info(f"CONNECTOR_CONTEXT: {json.dumps(connector_context)}")

        credentials = connector_context.get("credentials", {})
        secret_arn  = credentials.get("secretArn")

        next_token = event.get("nextToken", None)
        page_size  = int(os.environ.get("PAGE_SIZE", "50"))
        offset     = int(next_token) if next_token else 0

        logger.info(f"[ENV] {env} | [ENTITY] {entity} | [OFFSET] {offset}")

        try:
            sap_response = invoke_extractor({
                "entity": entity, "env": env,
                "offset": offset, "limit": page_size,
                "secret_arn": secret_arn
            })

            if sap_response.get("statusCode") == 500:
                err = json.loads(sap_response.get("body", "{}")).get("error", "Extractor failed")
                logger.error(f"Extractor error: {err}")
                return {
                    "isSuccess": False,
                    "errorDetails": {"errorCode": "SERVER_ERROR", "errorMessage": err, "retryAfterSeconds": 30},
                    "records": []
                }

            body     = json.loads(sap_response.get("body", "{}"))
            has_more = body.get("has_more", False)
            s3_key   = body.get("s3_key", None)

            if s3_key:
                logger.info("Large response — reading from S3")
                actual_data = read_from_s3(s3_key)
            else:
                logger.info("Small response — direct")
                actual_data = body

            records      = []
            current_size = 0

            if entity_key == "contractlist":
                # Each SAP record is wrapped as-is into raw_json.
                # No field picking — the full record travels through unchanged.
                for r in actual_data.get("contract_list", {}).get("value", []):
                    appflow_record = build_appflow_record(r)
                    record_str     = json.dumps(appflow_record)
                    record_size    = len(record_str.encode("utf-8"))

                    if current_size + record_size > MAX_RESPONSE_SIZE:
                        logger.info("Size limit hit in ContractList")
                        return {"isSuccess": True, "records": records, "nextToken": str(offset)}

                    records.append(record_str)
                    current_size += record_size

            elif entity_key == "contractdetails":
                MAX_ITEMS_PER_CHUNK = 1000

                contract_list = actual_data.get("contract_details", [])
                logger.info(f"Processing {len(contract_list)} contracts")

                for c in contract_list:
                    cn    = str(c.get("ContractNumber", ""))
                    items = c.get("_ContractItems", [])
                    total_items = len(items)

                    logger.info(f"Contract {cn}: {total_items} items")

                    if total_items == 0:
                        # Emit one record with an empty ContractItems list.
                        record_data = {**c, "_ContractItems": []}
                        appflow_record = build_appflow_record(record_data)
                        record_str     = json.dumps(appflow_record)
                        record_size    = len(record_str.encode("utf-8"))

                        if current_size + record_size > MAX_RESPONSE_SIZE:
                            logger.info(f"Size limit hit at contract {cn}")
                            return {"isSuccess": True, "records": records, "nextToken": str(offset)}

                        records.append(record_str)
                        current_size += record_size

                    else:
                        # Chunk large item lists to stay within response size limits.
                        for i in range(0, total_items, MAX_ITEMS_PER_CHUNK):
                            chunk_items = items[i:i + MAX_ITEMS_PER_CHUNK]
                            logger.info(f"Contract {cn}: chunk {i}-{i + len(chunk_items)}")

                            # Spread header fields + chunked items into one record per chunk.
                            record_data = {**c, "_ContractItems": chunk_items}
                            appflow_record = build_appflow_record(record_data)
                            record_str     = json.dumps(appflow_record)
                            record_size    = len(record_str.encode("utf-8"))

                            if current_size + record_size > MAX_RESPONSE_SIZE:
                                logger.info(f"Size limit hit at contract {cn} chunk {i}")
                                return {"isSuccess": True, "records": records, "nextToken": str(offset)}

                            records.append(record_str)
                            current_size += record_size

            logger.info(f"Records: {len(records)} | Size: {current_size} bytes")

            return {
                "isSuccess": True, "errorDetails": None,
                "records": records,
                "nextToken": str(offset + page_size) if has_more else None
            }

        except Exception as e:
            logger.error(f"ERROR: {str(e)}", exc_info=True)
            return {
                "isSuccess": False,
                "errorDetails": {"errorCode": "SERVER_ERROR", "errorMessage": str(e), "retryAfterSeconds": 30},
                "records": []
            }

    return {"isSuccess": False, "errorMessage": f"Unknown: {request_type}"}