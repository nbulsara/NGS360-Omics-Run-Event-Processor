''' Omics Run Event Processor Lambda Function '''
import json
from datetime import datetime
import uuid
import os
import logging

import boto3
import requests


def flatten(event):
    ''' Flattens a nested JSON object into a single-level dictionary.'''
    flat_event = {}
    for key, value in event.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flat_event[f"{sub_key}"] = sub_value
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    for sub_key, sub_value in item.items():
                        flat_event[f"{sub_key}_{i}"] = sub_value
                else:
                    flat_event[f"{key}_{i}"] = item
        else:
            flat_event[key] = value
    return flat_event


def setup_logging(event=None):
    ''' Sets up logging configuration '''
    VERBOSE_LOGGING = os.environ.get(
        'VERBOSE_LOGGING', 'false'
    ).lower() == 'true'
    log_level = logging.DEBUG if VERBOSE_LOGGING else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Reduce boto3 logging noise
    logging.getLogger('boto3').setLevel(logging.INFO)
    logging.getLogger('botocore').setLevel(logging.INFO)
    if event:
        logger.info("Received event: %s", json.dumps(event))
    return logger


def lambda_handler(event, context):
    '''
    Main Entry Point for Lambda function

    Calls NGS360 API Service with run event information
    '''
    logger = setup_logging(event)
    s3 = boto3.client('s3')

    API_SERVER = os.environ['API_SERVER']
    DATA_LAKE_BUCKET = os.environ['DATA_LAKE_BUCKET']
    S3_PREFIX = os.environ.get('S3_PREFIX', 'omics-run-events')
    if not API_SERVER or not DATA_LAKE_BUCKET:
        raise ValueError(
            'API_SERVER and/or DATA_LAKE_BUCKET environment variables not set'
        )

    # Generate unique filename using timestamp and UUID
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f'event_{timestamp}_{str(uuid.uuid4())}.json'

    # Flatten the event JSON
    flat_event = flatten(event)

    # Convert flattened dict to JSON string
    json_data = json.dumps(flat_event)

    # Upload to S3
    s3.put_object(
        Bucket=DATA_LAKE_BUCKET,
        Key=f'{S3_PREFIX}/{file_name}',
        Body=json_data,
        ContentType='application/json',
        ServerSideEncryption='AES256'
    )

    # Call GA4GH WES API Server
    api_url = f'{API_SERVER}/internal/callbacks/omics-state-change'
    headers = {'Content-Type': 'application/json'}
    requests.post(api_url, headers=headers, data=json_data, timeout=10)

    msg = f'Event processed, {json_data} -> s3://{DATA_LAKE_BUCKET}/{S3_PREFIX}/{file_name}'
    logger.info(msg)
    return {
        'statusCode': 200,
        'body': msg
    }
