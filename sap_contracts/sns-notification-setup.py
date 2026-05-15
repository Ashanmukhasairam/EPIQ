import boto3
import time

sns = boto3.client("sns")
athena = boto3.client("athena")

SNS_TOPIC = "arn:aws:sns:us-east-1:730878889077:monetoring-alerting-test-topic"

DATABASE = "edp_configs_dev"
OUTPUT = "s3://edp-athena-dl-results/"


def run_query(query):

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT}
    )

    query_id = response["QueryExecutionId"]

    # Wait until query finishes
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            break

        if state in ["FAILED", "CANCELLED"]:
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise Exception(f"Athena query failed: {reason}")

        time.sleep(3)

    result = athena.get_query_results(QueryExecutionId = query_id)

    rows = result["ResultSet"]["Rows"]

    table_lines = []

    for r in rows[1:]:
        value = r["Data"][0].get("VarCharValue", "")
        table_lines.append(value)

    return "\n".join(table_lines)


def lambda_handler(event, context):

    try:

        # Get parameter from Airflow
        etl_run_id = event.get("etl_run_id")

        if not etl_run_id:
            raise Exception("etl_run_id not provided in event payload")

        query = f"""
        SELECT
        format('%-25s %-10s %-10s %-15s',
               'pipeline_name',
               'layer',
               'status',
               'records_read')

        UNION ALL

        SELECT
        format('%-25s %-10s %-10s %-15s',
               pipeline_name,
               layer,
               status,
               CAST(records_read AS VARCHAR))

        FROM edp_configs_dev.pipeline_execution_summary
        WHERE etl_run_id = '{etl_run_id}'

        ORDER BY
        CASE layer
            WHEN 'BRONZE' THEN 1
            WHEN 'SILVER' THEN 2
            WHEN 'GOLD' THEN 3
        END
        """

        table_data = run_query(query)

        message = f"""
Pipeline Execution Report

ETL Run ID : {etl_run_id}

Pipeline Summary

{table_data}

Regards,
EDP Data Platform
"""

        sns.publish(
            TopicArn=SNS_TOPIC,
            Subject=f"Pipeline Execution Report - {etl_run_id}",
            Message=message
        )

        return {
            "statusCode": 200,
            "body": "Notification sent successfully"
        }

    except Exception as e:

        return {
            "statusCode": 500,
            "body": str(e)
        }

















# import boto3
# import time

# sns = boto3.client("sns")
# athena = boto3.client("athena")

# SNS_TOPIC = "arn:aws:sns:us-east-1:730878889077:monetoring-alerting-test-topic"

# DATABASE = "edp_configs_dev"
# OUTPUT = "s3://edp-athena-dl-results/"

# def run_query(query):

#     response = athena.start_query_execution(
#         QueryString=query,
#         QueryExecutionContext={"Database": DATABASE},
#         ResultConfiguration={"OutputLocation": OUTPUT}
#     )

#     query_id = response["QueryExecutionId"]

#     # Wait for Athena query to complete
#     while True:
#         status = athena.get_query_execution(QueryExecutionId=query_id)
#         state = status["QueryExecution"]["Status"]["State"]

#         if state == "SUCCEEDED":
#             break

#         if state in ["FAILED", "CANCELLED"]:
#             reason = status["QueryExecution"]["Status"].get(
#                 "StateChangeReason", "Unknown error"
#             )
#             raise Exception(f"Athena query failed: {reason}")

#         time.sleep(3)

#     result = athena.get_query_results(QueryExecutionId=query_id)

#     rows = result["ResultSet"]["Rows"]

#     table_lines = []

#     for r in rows[1:]:
#         value = r["Data"][0].get("VarCharValue", "")
#         table_lines.append(value)

#     return "\n".join(table_lines)


# def lambda_handler(event, context):

#     try:
#         job_name = event["detail"]["jobName"]
#         state = event["detail"]["state"]

#         query = """
#         SELECT
#         format('%-25s %-10s %-10s %-15s %-40s',
#                'pipeline_name',
#                'layer',
#                'status',
#                'records_read',
#                'run_id')

#         UNION ALL

#         SELECT
#         format('%-25s %-10s %-10s %-15s %-40s',
#                pipeline_name,
#                layer,
#                status,
#                CAST(records_read AS VARCHAR),
#                run_id)

#         FROM (
#             SELECT
#                 pipeline_name,
#                 layer,
#                 status,
#                 records_read,
#                 run_id,
#                 ROW_NUMBER() OVER (
#                     PARTITION BY pipeline_name, layer
#                     ORDER BY pipeline_end_time DESC
#                 ) rn
#             FROM edp_configs_dev.pipeline_execution_summary
#         )
#         WHERE rn = 1
#         ORDER BY pipeline_name
#         """
        
#         table_data = run_query(query)

#         message = f"""
# Glue Job Execution Report

# Job Name : {job_name}
# Status   : {state}

# Pipeline Execution Summary

# {table_data}

# Regards,
# EDP Data Platform
# """

#         sns.publish(
#             TopicArn=SNS_TOPIC,
#             Subject=f"Glue Job {state} - {job_name}",
#             Message=message
#         )

#         return {
#             "statusCode": 200,
#             "body": "SNS notification sent successfully"
#         }

#     except Exception as e:

#         return {
#             "statusCode": 500,
#             "body": str(e)
#         }