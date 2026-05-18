import json
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET")
EXTRACTOR_ARN = os.environ.get("EXTRACTOR_LAMBDA_ARN")
LOAD_TYPE     = os.environ.get("LOAD_TYPE", "FULL") # Default to FULL if missing
MAX_RESPONSE_SIZE = 5 * 1024 * 1024
RAW_JSON_FIELD_NAME = "raw_json"

def invoke_extractor(payload):
    payload['LOAD_TYPE'] = LOAD_TYPE

    client = boto3.client('lambda', region_name='us-east-1')
    result = client.invoke(
        FunctionName=EXTRACTOR_ARN,
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


def make_field(name, data_type):
    if data_type in ("Integer", "Float", "Double", "Long", "Short", "BigInteger", "BigDecimal"):
        filter_ops = ["NOT_EQUAL_TO", "EQUAL_TO", "LESS_THAN", "LESS_THAN_OR_EQUAL_TO", "GREATER_THAN", "GREATER_THAN_OR_EQUAL_TO"]
    elif data_type in ("Date", "DateTime"):
        filter_ops = ["EQUAL_TO", "LESS_THAN", "LESS_THAN_OR_EQUAL_TO", "GREATER_THAN", "GREATER_THAN_OR_EQUAL_TO", "BETWEEN"]
    elif data_type == "Boolean":
        filter_ops = ["EQUAL_TO", "NOT_EQUAL_TO"]
    else:
        filter_ops = ["CONTAINS", "EQUAL_TO", "NOT_EQUAL_TO"]
    return {
        "fieldName": name, "dataType": data_type, "dataTypeLabel": data_type,
        "label": name, "description": name, "isPrimaryKey": False,
        "defaultValue": None, "isDeprecated": None, "constraints": None,
        "readProperties": {"isRetrievable": True, "isNullable": True, "isQueryable": True, "isTimestampFieldForIncrementalQueries": False},
        "writeProperties": None, "filterOperators": filter_ops, "customProperties": None
    }


def fetch_remote_schema(entity, env, secret_arn=None):
    """
    Ask the extractor Lambda for a schema definition for `entity`.
    Expected extractor response body JSON shape (recommended):
      {"fields": [{"name": "ContractNumber", "type": "String"}, ...]}

    Returns a list of AppFlow field dicts (from `make_field`) or None on failure.
    """
    try:
        payload = {"action": "describe_entity", "entity": entity, "env": env, "secret_arn": secret_arn}
        resp = invoke_extractor(payload)

        # Support both direct body and S3 temp pattern
        body = {}
        if isinstance(resp, dict) and resp.get("statusCode") in (200, "200"):
            body = json.loads(resp.get("body") or "{}")
        elif isinstance(resp, dict) and resp.get("body"):
            body = json.loads(resp.get("body") or "{}")
        elif isinstance(resp, dict):
            body = resp

        fields = body.get("fields")
        if not fields or not isinstance(fields, list):
            logger.info("No remote schema fields returned by extractor")
            return None

        appflow_fields = []
        for f in fields:
            name = f.get("name") or f.get("fieldName")
            dtype = f.get("type") or f.get("dataType") or "String"
            if not name:
                continue
            appflow_fields.append(make_field(name, dtype))

        logger.info(f"Fetched remote schema for {entity}: {len(appflow_fields)} fields")
        return appflow_fields

    except Exception as e:
        logger.warning(f"fetch_remote_schema failed for {entity}: {e}")
        return None


def fetch_and_convert_schema(entity, env, secret_arn=None):
    """
    Fetch schema from the extractor and convert it to AppFlow fields with fallback.
    Final fallback is the single `raw_json` field.
    """
    appflow_fields = fetch_remote_schema(entity, env, secret_arn)
    if appflow_fields is None:
        logger.info(f"Falling back to single raw_json field schema for {entity}")
        return get_source_fields()
    return appflow_fields


def build_field_definition(field_name):
    return {
        "fieldName": field_name,
        "dataType": "String",
        "dataTypeLabel": "String",
        "label": field_name,
        "description": "Complete source record as stringified JSON",
        "constraints": {
            "isRequired": False,
            "isUnique": False
        },
        "readProperties": {
            "isRetrievable": True,
            "isQueryable": False,
            "isTimestampFieldForIncrementalQueries": False,
            "isNullable": True
        },
        "writeProperties": {
            "isCreatable": False,
            "isUpdatable": False,
            "isUpsertable": False,
            "isDefaultedOnCreate": False,
            "isNullable": True,
            "supportedWriteOperations": []
        },
        "filterOperators": [
            "PROJECTION"
        ],
        "supportedFieldTypeDetails": {
            "fieldType": "string",
            "filterOperators": [
                "PROJECTION"
            ]
        }
    }


def get_source_fields():
    fields = [build_field_definition(RAW_JSON_FIELD_NAME)]
    logger.info(f"Returning single source field for AppFlow: {RAW_JSON_FIELD_NAME}")
    return fields



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
                "customAuthConfig": [{"authenticationType": "NO_AUTH", "authParameters": []}]
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
                {"entityIdentifier": "ContractList", "hasNestedEntities": False, "isWritable": False, "label": "Contract List", "description": "Contract List"},
                {"entityIdentifier": "ContractDetails", "hasNestedEntities": False, "isWritable": False, "label": "Contract Details", "description": "Contract Details"}
            ],
            "nextToken": None, "cacheControl": None
        }

    elif request_type == "DescribeEntityRequest":
        # Try to fetch a dynamic schema from the extractor; fall back to single `raw_json` field.
        connector_context = event.get("connectorContext", {})
        connection_name = connector_context.get("connectorProfileLabel", "")
        env = "qa" if "qa" in connection_name.lower() else "dev"
        credentials = connector_context.get("credentials", {})
        secret_arn = credentials.get("secretArn")

        if entity_key == "contractlist":
            eid, label = "ContractList", "Contract List"
        elif entity_key == "contractdetails":
            eid, label = "ContractDetails", "Contract Details"
        else:
            return {"isSuccess": False, "errorDetails": None, "entityDefinition": None, "cacheControl": None}

        fields = fetch_and_convert_schema(eid, env, secret_arn)

        return {
            "isSuccess": True, "errorDetails": None,
            "entityDefinition": {
                "entity": {"entityIdentifier": eid, "hasNestedEntities": False, "isWritable": False, "label": label, "description": label},
                "fields": fields, "customProperties": None
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
        secret_arn = credentials.get("secretArn")
        
        next_token = event.get("nextToken", None)
        page_size  = int(os.environ.get("PAGE_SIZE", "50"))
        offset     = int(next_token) if next_token else 0
        
        

        logger.info(f"[ENV] {env} | [ENTITY] {entity} | [OFFSET] {offset}")

        try:
            sap_response = invoke_extractor({"entity": entity, "env": env, "offset": offset, "limit": page_size, "secret_arn": secret_arn})

            if sap_response.get("statusCode") == 500:
                err = json.loads(sap_response.get("body", "{}")).get("error", "Extractor failed")
                logger.error(f"Extractor error: {err}")
                return {"isSuccess": False, "errorDetails": {"errorCode": "SERVER_ERROR", "errorMessage": err, "retryAfterSeconds": 30}, "records": []}

            body     = json.loads(sap_response.get("body", "{}"))
            has_more = body.get("has_more", False)
            s3_key   = body.get("s3_key", None)

            if s3_key:
                logger.info("Large response — reading from S3")
                actual_data = read_from_s3(s3_key)
            else:
                logger.info("Small response — direct")
                actual_data = body

            records = []
            current_size = 0

            if entity_key == "contractlist":
                for r in actual_data.get("contract_list", {}).get("value", []):
                    source_record = {
                        "ContractNumber":    str(r.get("ContractNumber", "")),
                        "SalesOrganization": str(r.get("SalesOrganization", "")),
                        "ContractDate":      str(r.get("ContractDate", "")),
                        "CreatedDate":       str(r.get("CreatedDate", "")),
                        "ChangedDate":       str(r.get("ChangedDate", "")),
                        "attribute":         str(r.get("attribute", ""))
                    }
                    appflow_record = {RAW_JSON_FIELD_NAME: json.dumps(source_record, ensure_ascii=False, default=str)}
                    record = json.dumps(appflow_record, ensure_ascii=False)
                    record_size = len(record.encode("utf-8"))
                    if current_size + record_size > MAX_RESPONSE_SIZE:
                        logger.info("Size limit hit while building contract list page")
                        return {"isSuccess": True, "records": records, "nextToken": str(offset)}
                    records.append(record)
                    current_size += record_size

            elif entity_key == "contractdetails":
                MAX_ITEMS_PER_CHUNK = 1000

                logger.info(f"Processing {len(actual_data.get('contract_details', []))} contracts")

                for idx, c in enumerate(actual_data.get("contract_details", [])):
                    cn    = str(c.get("ContractNumber", ""))
                    items = c.get("_ContractItems", [])
                    total_items = len(items)

                    logger.info(f"Contract {cn}: {total_items} items")

                    if total_items == 0:
                        source_record = {
                            "ContractNumber":                 cn,
                            "ContractDescription":            str(c.get("ContractDescription", "")),
                            "ContractStartDate":              str(c.get("ContractStartDate", "")),
                            "SalesOrganization":              str(c.get("SalesOrganization", "")),
                            "DocCurrency":                    str(c.get("DocCurrency", "")),
                            "SoldToParty":                    str(c.get("SoldToParty", "")),
                            "SoldToPartyText":                str(c.get("SoldToPartyText", "")),
                            "BillToParty":                    str(c.get("BillToParty", "")),
                            "BillToPartyText":                str(c.get("BillToPartyText", "")),
                            "Payer":                          str(c.get("Payer", "")),
                            "ShipToParty":                    str(c.get("ShipToParty", "")),
                            "PrimaryProjectManager":          str(c.get("PrimaryProjectManager", "")),
                            "SalesOffice":                    str(c.get("SalesOffice", "")),
                            "SalesGroup":                     str(c.get("SalesGroup", "")),
                            "ClientMatter":                   str(c.get("ClientMatter", "")),
                            "CustomerReference":              str(c.get("CustomerReference", "")),
                            "LSProjectCode":                  str(c.get("LSProjectCode", "")),
                            "PricingDate":                    str(c.get("PricingDate", "")),
                            "HeaderBillingBlock":             str(c.get("HeaderBillingBlock", "")),
                            "MaterialContributionFlag":       str(c.get("MaterialContributionFlag", "")),
                            "AnnualPriceEscOverridePercent":  c.get("AnnualPriceEscOverridePercent", 0.0),
                            "AnnualPriceEscContractLanguage": str(c.get("AnnualPriceEscContractLanguage", "")),
                            "ProjectNumber":                  str(c.get("ProjectNumber", "")),
                            "CustomerGroup":                  str(c.get("CustomerGroup", "")),
                            "zzotcontract":                   str(c.get("zzotcontract", "")),
                            "Deactivated":                    str(c.get("Deactivated", "")),
                            "InternalUse":                    str(c.get("InternalUse", "")),
                            "ClientFacing":                   str(c.get("ClientFacing", "")),
                            "ContractItems":                  []
                        }
                        appflow_record = {RAW_JSON_FIELD_NAME: json.dumps(source_record, ensure_ascii=False, default=str)}
                        record = json.dumps(appflow_record, ensure_ascii=False)
                        record_size = len(record.encode("utf-8"))
                        if current_size + record_size > MAX_RESPONSE_SIZE:
                            logger.info(f"Size limit hit at contract {cn}")
                            return {"isSuccess": True, "records": records, "nextToken": str(offset)}
                        records.append(record)
                        current_size += record_size
                    else:
                        for i in range(0, total_items, MAX_ITEMS_PER_CHUNK):
                            chunk_items = items[i:i + MAX_ITEMS_PER_CHUNK]
                            logger.info(f"Contract {cn}: chunk {i}-{i+len(chunk_items)}")

                            record = json.dumps({
                                "ContractNumber":                 cn,
                                "ContractDescription":            str(c.get("ContractDescription", "")),
                                "ContractStartDate":              str(c.get("ContractStartDate", "")),
                                "SalesOrganization":              str(c.get("SalesOrganization", "")),
                                "DocCurrency":                    str(c.get("DocCurrency", "")),
                                "SoldToParty":                    str(c.get("SoldToParty", "")),
                                "SoldToPartyText":                str(c.get("SoldToPartyText", "")),
                                "BillToParty":                    str(c.get("BillToParty", "")),
                                "BillToPartyText":                str(c.get("BillToPartyText", "")),
                                "Payer":                          str(c.get("Payer", "")),
                                "ShipToParty":                    str(c.get("ShipToParty", "")),
                                "PrimaryProjectManager":          str(c.get("PrimaryProjectManager", "")),
                                "SalesOffice":                    str(c.get("SalesOffice", "")),
                                "SalesGroup":                     str(c.get("SalesGroup", "")),
                                "ClientMatter":                   str(c.get("ClientMatter", "")),
                                "CustomerReference":              str(c.get("CustomerReference", "")),
                                "LSProjectCode":                  str(c.get("LSProjectCode", "")),
                                "PricingDate":                    str(c.get("PricingDate", "")),
                                "HeaderBillingBlock":             str(c.get("HeaderBillingBlock", "")),
                                "MaterialContributionFlag":       str(c.get("MaterialContributionFlag", "")),
                                "AnnualPriceEscOverridePercent":  c.get("AnnualPriceEscOverridePercent", 0.0),
                                "AnnualPriceEscContractLanguage": str(c.get("AnnualPriceEscContractLanguage", "")),
                                "ProjectNumber":                  str(c.get("ProjectNumber", "")),
                                "CustomerGroup":                  str(c.get("CustomerGroup", "")),
                                "zzotcontract":                   str(c.get("zzotcontract", "")),
                                "Deactivated":                   str(c.get("zzotcontract", "")),
                                "InternalUse":                   str(c.get("InternalUse", "")),
                                "ClientFacing":                   str(c.get("ClientFacing", "")),
                                "ContractItems":                  chunk_items
                            })

                            source_record = {
                                "ContractNumber":                 cn,
                                "ContractDescription":            str(c.get("ContractDescription", "")),
                                "ContractStartDate":              str(c.get("ContractStartDate", "")),
                                "SalesOrganization":              str(c.get("SalesOrganization", "")),
                                "DocCurrency":                    str(c.get("DocCurrency", "")),
                                "SoldToParty":                    str(c.get("SoldToParty", "")),
                                "SoldToPartyText":                str(c.get("SoldToPartyText", "")),
                                "BillToParty":                    str(c.get("BillToParty", "")),
                                "BillToPartyText":                str(c.get("BillToPartyText", "")),
                                "Payer":                          str(c.get("Payer", "")),
                                "ShipToParty":                    str(c.get("ShipToParty", "")),
                                "PrimaryProjectManager":          str(c.get("PrimaryProjectManager", "")),
                                "SalesOffice":                    str(c.get("SalesOffice", "")),
                                "SalesGroup":                     str(c.get("SalesGroup", "")),
                                "ClientMatter":                   str(c.get("ClientMatter", "")),
                                "CustomerReference":              str(c.get("CustomerReference", "")),
                                "LSProjectCode":                  str(c.get("LSProjectCode", "")),
                                "PricingDate":                    str(c.get("PricingDate", "")),
                                "HeaderBillingBlock":             str(c.get("HeaderBillingBlock", "")),
                                "MaterialContributionFlag":       str(c.get("MaterialContributionFlag", "")),
                                "AnnualPriceEscOverridePercent":  c.get("AnnualPriceEscOverridePercent", 0.0),
                                "AnnualPriceEscContractLanguage": str(c.get("AnnualPriceEscContractLanguage", "")),
                                "ProjectNumber":                  str(c.get("ProjectNumber", "")),
                                "CustomerGroup":                  str(c.get("CustomerGroup", "")),
                                "zzotcontract":                   str(c.get("zzotcontract", "")),
                                "Deactivated":                    str(c.get("Deactivated", "")),
                                "InternalUse":                    str(c.get("InternalUse", "")),
                                "ClientFacing":                   str(c.get("ClientFacing", "")),
                                "ContractItems":                  chunk_items
                            }

                            appflow_record = {RAW_JSON_FIELD_NAME: json.dumps(source_record, ensure_ascii=False, default=str)}
                            record = json.dumps(appflow_record, ensure_ascii=False)
                            record_size = len(record.encode("utf-8"))

                            if current_size + record_size > MAX_RESPONSE_SIZE:
                                logger.info(f"Size limit hit at contract {cn} chunk {i}")
                                return {"isSuccess": True, "records": records, "nextToken": str(offset)}

                            records.append(record)
                            current_size += record_size

            logger.info(f"Records: {len(records)} | Size: {current_size} bytes")

            return {
                "isSuccess": True, "errorDetails": None,
                "records": records,
                "nextToken": str(offset + page_size) if has_more else None
            }

        except Exception as e:
            logger.error(f"ERROR: {str(e)}", exc_info=True)
            return {"isSuccess": False, "errorDetails": {"errorCode": "SERVER_ERROR", "errorMessage": str(e), "retryAfterSeconds": 30}, "records": []}

    return {"isSuccess": False, "errorMessage": f"Unknown: {request_type}"}