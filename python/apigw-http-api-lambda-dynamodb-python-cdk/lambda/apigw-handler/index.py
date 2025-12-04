# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries
patch_all()

import boto3
import os
import json
import logging
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    table = os.environ.get("TABLE_NAME")
    
    # Extract security context
    request_context = event.get("requestContext", {})
    source_ip = request_context.get("identity", {}).get("sourceIp", "unknown")
    user_agent = request_context.get("identity", {}).get("userAgent", "unknown")
    request_id = request_context.get("requestId", "unknown")
    
    # Structured logging with security context
    logger.info(json.dumps({
        "event": "request_received",
        "request_id": request_id,
        "source_ip": source_ip,
        "user_agent": user_agent,
        "table_name": table
    }))
    
    try:
        if event.get("body"):
            item = json.loads(event["body"])
            logger.info(json.dumps({
                "event": "processing_payload",
                "request_id": request_id,
                "item_id": item.get("id")
            }))
            
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            
            logger.info(json.dumps({
                "event": "dynamodb_write_success",
                "request_id": request_id,
                "item_id": id
            }))
            
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": "Successfully inserted data!"}),
            }
        else:
            logger.info(json.dumps({
                "event": "processing_default_payload",
                "request_id": request_id
            }))
            
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": str(uuid.uuid4())},
                },
            )
            
            logger.info(json.dumps({
                "event": "dynamodb_write_success",
                "request_id": request_id
            }))
            
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": "Successfully inserted data!"}),
            }
    except Exception as e:
        logger.error(json.dumps({
            "event": "error",
            "request_id": request_id,
            "error_type": type(e).__name__,
            "error_message": str(e),
            "source_ip": source_ip
        }))
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "Internal server error"}),
        }
