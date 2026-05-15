import json
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BRONZE_BUCKET = "epiq-edp-dl-qa-bronze"


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


CONTRACT_LIST_FIELDS = [
    make_field("ContractNumber", "String"),
    make_field("SalesOrganization", "String"),
    make_field("ContractDate", "String"),
    make_field("CreatedDate", "String"),
    make_field("ChangedDate", "String"),
    make_field("attribute", "String"),
]

CONTRACT_DETAILS_FIELDS = [
    make_field("ContractNumber", "String"),
    make_field("ContractDescription", "String"),
    make_field("ContractStartDate", "String"),
    make_field("SalesOrganization", "String"),
    make_field("DocCurrency", "String"),
    make_field("SoldToParty", "String"),
    make_field("SoldToPartyText", "String"),
    make_field("BillToParty", "String"),
    make_field("BillToPartyText", "String"),
    make_field("Payer", "String"),
    make_field("ShipToParty", "String"),
    make_field("PrimaryProjectManager", "String"),
    make_field("SalesOffice", "String"),
    make_field("SalesGroup", "String"),
    make_field("ClientMatter", "String"),
    make_field("CustomerReference", "String"),
    make_field("LSProjectCode", "String"),
    make_field("PricingDate", "String"),
    make_field("HeaderBillingBlock", "String"),
    make_field("MaterialContributionFlag", "String"),
    make_field("AnnualPriceEscOverridePercent", "Double"),
    make_field("AnnualPriceEscContractLanguage", "String"),
    make_field("ProjectNumber", "String"),
    make_field("CustomerGroup", "String"),
    make_field("ContractItems", "String"),
]


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
        if entity_key == "contractlist":
            fields, eid, label = CONTRACT_LIST_FIELDS, "ContractList", "Contract List"
        elif entity_key == "contractdetails":
            fields, eid, label = CONTRACT_DETAILS_FIELDS, "ContractDetails", "Contract Details"
        else:
            return {"isSuccess": False, "errorDetails": None, "entityDefinition": None, "cacheControl": None}
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

        next_token = event.get("nextToken", None)
        page_size  = int(os.environ.get("PAGE_SIZE", "50"))
        offset     = int(next_token) if next_token else 0

        logger.info(f"[ENV] {env} | [ENTITY] {entity} | [OFFSET] {offset}")

        try:
            sap_response = invoke_extractor({"entity": entity, "env": env, "offset": offset, "limit": page_size})

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

            if entity_key == "contractlist":
                for r in actual_data.get("contract_list", {}).get("value", []):
                    records.append(json.dumps({
                        "ContractNumber":    str(r.get("ContractNumber", "")),
                        "SalesOrganization": str(r.get("SalesOrganization", "")),
                        "ContractDate":      str(r.get("ContractDate", "")),
                        "CreatedDate":       str(r.get("CreatedDate", "")),
                        "ChangedDate":       str(r.get("ChangedDate", "")),
                        "attribute":         str(r.get("attribute", ""))
                    }))

            elif entity_key == "contractdetails":
                for c in actual_data.get("contract_details", []):
                    records.append(json.dumps({
                        "ContractNumber":                 str(c.get("ContractNumber", "")),
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
                        "ContractItems":                  c.get("_ContractItems", [])
                    }))

            logger.info(f"Records: {len(records)} | next_token: {str(offset+page_size) if has_more else None}")

            return {
                "isSuccess": True, "errorDetails": None,
                "records": records,
                "nextToken": str(offset + page_size) if has_more else None
            }

        except Exception as e:
            logger.error(f"ERROR: {str(e)}", exc_info=True)
            return {"isSuccess": False, "errorDetails": {"errorCode": "SERVER_ERROR", "errorMessage": str(e), "retryAfterSeconds": 30}, "records": []}

    return {"isSuccess": False, "errorMessage": f"Unknown: {request_type}"}