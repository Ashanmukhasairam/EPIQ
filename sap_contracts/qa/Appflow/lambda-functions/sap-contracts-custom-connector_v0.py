import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ============================================================
# Invoke SAP extractor Lambda (Lambda 1)
# ============================================================
def invoke_sap_lambda(payload):
    client = boto3.client('lambda', region_name='us-east-1')
    result = client.invoke(
        FunctionName='sap-contracts-extractor',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    return json.loads(result['Payload'].read())

# ============================================================
# Field helper (AppFlow REQUIRED format)
# ============================================================
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
        "fieldName": name,
        "dataType": data_type,
        "dataTypeLabel": data_type,
        "label": name,
        "description": name,
        "isPrimaryKey": False,
        "defaultValue": None,
        "isDeprecated": None,
        "constraints": None,
        "readProperties": {
            "isRetrievable": True,
            "isNullable": True,
            "isQueryable": True,
            "isTimestampFieldForIncrementalQueries": False
        },
        "writeProperties": None,
        "filterOperators": filter_ops,
        "customProperties": None
    }

# ============================================================
# ENTITY FIELD DEFINITIONS
# ============================================================

CONTRACT_LIST_FIELDS = [
    make_field("ContractNumber", "String"),
    make_field("SalesOrganization", "String"),
    make_field("ContractDate", "String"),
    make_field("CreatedDate", "String"),
    make_field("ChangedDate", "String"),
    make_field("ContractTime", "String"),
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


# ============================================================
# MAIN HANDLER
# ============================================================
def lambda_handler(event, context):

    logger.info("=== SAP ODATA CUSTOM CONNECTOR RUNNING ===")
    logger.info(json.dumps(event))

    request_type = event.get("type", "")
    entity = event.get("entityIdentifier", "")

    # 🔥 Strong normalization
    entity_key = (entity or "").replace(" ", "").replace("_", "").lower()

    logger.info(f"[REQUEST TYPE] {request_type}")
    logger.info(f"[ENTITY RAW] {entity}")
    logger.info(f"[ENTITY NORMALIZED] {entity_key}")

    # ========================================================
    # 1. Describe Connector
    # ========================================================
    if request_type == "DescribeConnectorConfigurationRequest":
        return {
            "connectorOwner": "CUSTOM",
            "connectorName": "SAPODataConnector",
            "connectorVersion": "1.0",
            "connectorModes": ["SOURCE"],
            "authenticationConfig": {
                "isBasicAuthSupported": None,
                "isApiKeyAuthSupported": None,
                "isOAuth2Supported": None,
                "isCustomAuthSupported": True,
                "oAuth2Defaults": None,
                "customAuthConfig": [
                    {
                        "authenticationType": "NO_AUTH",
                        "authParameters": []
                    }
                ]
            },
            "supportedApiVersions": ["v1"],
            "isSuccess": True,
            "connectorRuntimeSetting": None,
            "logoURL": None,
            "errorDetails": None,
            "operatorsSupported": [
                "PROJECTION",
                "LESS_THAN",
                "GREATER_THAN",
                "BETWEEN",
                "LESS_THAN_OR_EQUAL_TO",
                "GREATER_THAN_OR_EQUAL_TO",
                "EQUAL_TO",
                "CONTAINS",
                "NOT_EQUAL_TO",
                "ADDITION",
                "SUBTRACTION",
                "MULTIPLICATION",
                "DIVISION",
                "MASK_ALL",
                "MASK_FIRST_N",
                "MASK_LAST_N",
                "VALIDATE_NON_NULL",
                "VALIDATE_NON_ZERO",
                "VALIDATE_NON_NEGATIVE",
                "VALIDATE_NUMERIC",
                "NO_OP"
            ],
            "triggerFrequenciesSupported": [
                "BYMINUTE", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "ONCE"
            ],
            "supportedWriteOperations": [
                "INSERT", "UPDATE", "UPSERT", "DELETE"
            ],
            "supportedTriggerTypes": [
                "SCHEDULED", "ONDEMAND"
            ]
        }

    # ========================================================
    # 2. Validate Credentials
    # ========================================================
    elif request_type == "ValidateCredentialsRequest":
        return {
            "isSuccess": True,
            "errorDetails": None
        }

    # ========================================================
    # 2b. Validate Connector Runtime Settings
    # ========================================================
    elif request_type == "ValidateConnectorRuntimeSettingsRequest":
        return {
            "isSuccess": True,
            "errorsByInputField": None,
            "errorDetails": None
        }

    # ========================================================
    # 3. List Entities
    # ========================================================
    elif request_type == "ListEntitiesRequest":
        return {
            "isSuccess": True,
            "errorDetails": None,
            "entities": [
                {
                    "entityIdentifier": "ContractList",
                    "hasNestedEntities": False,
                    "isWritable": False,
                    "label": "Contract List",
                    "description": "Contract List"
                },
                {
                    "entityIdentifier": "ContractDetails",
                    "hasNestedEntities": False,
                    "isWritable": False,
                    "label": "Contract Details",
                    "description": "Contract Details"
                }
            ],
            "nextToken": None,
            "cacheControl": None
        }

    # ========================================================
    # 4. Describe Entity
    # ========================================================
    elif request_type == "DescribeEntityRequest":
        logger.info("=== ENTERED DescribeEntityRequest ===")

        if entity_key == "contractlist":
            logger.info(f"Returning ContractList fields — {len(CONTRACT_LIST_FIELDS)} fields")

            return {
                "isSuccess": True,
                "errorDetails": None,
                "entityDefinition": {
                    "entity": {
                        "entityIdentifier": "ContractList",
                        "hasNestedEntities": False,
                        "isWritable": False,
                        "label": "Contract List",
                        "description": "Contract List"
                    },
                    "fields": CONTRACT_LIST_FIELDS,
                    "customProperties": None
                },
                "cacheControl": None
            }

        elif entity_key == "contractdetails":
            logger.info(f"Returning ContractDetails fields — {len(CONTRACT_DETAILS_FIELDS)} fields")

            return {
                "isSuccess": True,
                "errorDetails": None,
                "entityDefinition": {
                    "entity": {
                        "entityIdentifier": "ContractDetails",
                        "hasNestedEntities": False,
                        "isWritable": False,
                        "label": "Contract Details",
                        "description": "Contract Details"
                    },
                    "fields": CONTRACT_DETAILS_FIELDS,
                    "customProperties": None
                },
                "cacheControl": None
            }

        else:
            logger.error(f"Unknown entity received: {entity}")

            return {
                "isSuccess": False,
                "errorDetails": None,
                "entityDefinition": None,
                "cacheControl": None
            }

    # ========================================================
    # 5. Query Data
    # ========================================================
    elif request_type == "QueryDataRequest":

        logger.info("=== ENTERED QueryDataRequest ===")
        connector_context = event.get("connectorContext", {})

        connection_name = connector_context.get("connectorProfileLabel", "")

        env = "qa" if "qa" in connection_name.lower() else "dev"

        logger.info(f"[CONNECTION NAME] {connection_name}")
        
        logger.info(f"[ENV DETECTED] {env}")

        try:
            logger.info(f"Invoking Lambda 1 with env={env} and entity={entity}")
            sap_response = invoke_sap_lambda({"entity": entity, "env": env})
            body = json.loads(sap_response.get("body", "{}"))

            records = []

            if entity_key == "contractlist":
                raw_records = body.get("contract_list", {}).get("value", [])

                for r in raw_records:
                    records.append(json.dumps({
                        "ContractNumber": str(r.get("ContractNumber", "")),
                        "SalesOrganization": str(r.get("SalesOrganization", "")),
                        "ContractDate": str(r.get("ContractDate", "")),
                        "CreatedDate": str(r.get("CreatedDate", "")),
                        "ChangedDate": str(r.get("ChangedDate", "")),
                        "ContractTime": str(r.get("ContractTime", "")),
                        "attribute": str(r.get("attribute", ""))
                    }))

            elif entity_key == "contractdetails":
                raw_records = body.get("contract_details", [])

                for contract in raw_records:
                    record = {
                        "ContractNumber": str(contract.get("ContractNumber", "")),
                        "ContractDescription": str(contract.get("ContractDescription", "")),
                        "ContractStartDate": str(contract.get("ContractStartDate", "")),
                        "SalesOrganization": str(contract.get("SalesOrganization", "")),
                        "DocCurrency": str(contract.get("DocCurrency", "")),
                        "SoldToParty": str(contract.get("SoldToParty", "")),
                        "SoldToPartyText": str(contract.get("SoldToPartyText", "")),
                        "BillToParty": str(contract.get("BillToParty", "")),
                        "BillToPartyText": str(contract.get("BillToPartyText", "")),
                        "Payer": str(contract.get("Payer", "")),
                        "ShipToParty": str(contract.get("ShipToParty", "")),
                        "PrimaryProjectManager": str(contract.get("PrimaryProjectManager", "")),
                        "SalesOffice": str(contract.get("SalesOffice", "")),
                        "SalesGroup": str(contract.get("SalesGroup", "")),
                        "ClientMatter": str(contract.get("ClientMatter", "")),
                        "CustomerReference": str(contract.get("CustomerReference", "")),
                        "LSProjectCode": str(contract.get("LSProjectCode", "")),
                        "PricingDate": str(contract.get("PricingDate", "")),
                        "HeaderBillingBlock": str(contract.get("HeaderBillingBlock", "")),
                        "MaterialContributionFlag": str(contract.get("MaterialContributionFlag", "")),
                        "AnnualPriceEscOverridePercent": contract.get("AnnualPriceEscOverridePercent", 0.0),
                        "AnnualPriceEscContractLanguage": str(contract.get("AnnualPriceEscContractLanguage", "")),
                        "ProjectNumber": str(contract.get("ProjectNumber", "")),
                        "CustomerGroup": str(contract.get("CustomerGroup", "")),
                        "ContractItems": contract.get("_ContractItems", [])  # ✅ native list — no json.dumps()
                    }
                    records.append(json.dumps(record))

            logger.info(f"Returning {len(records)} records")

            return {
                "isSuccess": True,
                "errorDetails": None,
                "records": records
            }

        except Exception as e:
            logger.error(f"[ERROR] {str(e)}", exc_info=True)
            return {
                "isSuccess": False,
                "errorDetails": {
                    "errorCode": "SERVER_ERROR",
                    "errorMessage": str(e),
                    "retryAfterSeconds": None
                },
                "records": []
            }

    # ========================================================
    # Fallback
    # ========================================================
    return {
        "isSuccess": False,
        "errorMessage": f"Unknown request type: {request_type}"
    }