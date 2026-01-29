''' Omics Run Event Processor Lambda Function '''
import json
from datetime import datetime
import uuid
import os
import logging

import boto3
import requests

secrets_client = boto3.client('secretsmanager')
s3 = boto3.client('s3')
omics_client = boto3.client('omics')


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


def get_auth_token():
    if os.environ.get("AUTH_TOKEN"):
        return os.environ.get("AUTH_TOKEN")

    # Retrieve API Server Auth Token from Secrets Manager
    secret_name = os.environ.get('ENV_SECRETS')
    if secret_name:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=secret_name
        )
        secret_string = get_secret_value_response['SecretString']
        secret_dict = json.loads(secret_string)
        AUTH_TOKEN = secret_dict.get('AUTH_TOKEN')
        return AUTH_TOKEN

    return None


def get_log_urls(run_id, region, logger):
    """
    Get CloudWatch log URLs for an AWS HealthOmics run.

    Args:
        run_id: AWS HealthOmics run ID
        region: AWS region
        logger: Logger instance

    Returns:
        Dictionary containing log URLs or empty dict if not available
    """
    try:
        # Get run details from AWS HealthOmics
        response = omics_client.get_run(id=run_id)
        logger.debug(f"Got run details for {run_id}: {response}")

        # Check if logLocation exists in the response
        if 'logLocation' not in response:
            logger.warning(f"No logLocation found in response for run {run_id}")
            return {}

        log_location = response.get('logLocation', {})

        # Check if runLogStream exists
        if 'runLogStream' not in log_location:
            logger.warning(f"No runLogStream found in logLocation for run {run_id}")
            return {}

        run_log_stream = log_location['runLogStream']

        # CloudWatch logs format: arn:aws:logs:region:account:log-group:name:log-stream:name
        if not run_log_stream.startswith('arn:aws:logs:'):
            logger.warning(f"runLogStream doesn't match expected CloudWatch ARN format: {run_log_stream}")
            return {}

        # Extract log group and log stream
        parts = run_log_stream.split(':')
        if len(parts) < 8:
            logger.warning(f"Invalid CloudWatch ARN format: {run_log_stream}")
            return {}

        log_group = parts[6]

        # Extract the log stream
        arn_parts = run_log_stream.split(':log-stream:')
        if len(arn_parts) != 2:
            logger.warning(f"Cannot extract log stream from ARN: {run_log_stream}")
            return {}

        log_stream = arn_parts[1]  # This should be "run/{run_id}"

        # Construct CloudWatch log URL with proper URL encoding
        run_log_url = (
            f"https://{region}.console.aws.amazon.com/cloudwatch/home"
            f"?region={region}#logsV2:log-groups/log-group/"
            f"{log_group.replace('/', '%2F')}"
            f"/log-events/{log_stream.replace('/', '%2F')}"
        )

        # Extract run ID from log stream for task logs
        run_id_parts = log_stream.split('/')
        if len(run_id_parts) < 2 or run_id_parts[0] != 'run':
            logger.warning(f"Cannot extract run ID from log stream: {log_stream}")
            return {'run_log': run_log_url}

        actual_run_id = run_id_parts[1]

        # Initialize result with run log URL
        result = {'run_log': run_log_url}

        # Try to get task IDs for this run
        try:
            # List tasks for this run
            tasks_response = omics_client.list_run_tasks(
                id=run_id,
                maxResults=10  # Adjust as needed
            )
            
            # Process task information
            if 'items' in tasks_response and tasks_response['items']:
                task_logs = {}
                for task in tasks_response['items']:
                    task_id = task.get('id')
                    task_name = task.get('name', 'unnamed')
                    if task_id:
                        # Create direct link to task log
                        task_log_stream = f"run/{actual_run_id}/task/{task_id}"
                        task_log_url = (
                            f"https://{region}.console.aws.amazon.com/cloudwatch/home"
                            f"?region={region}#logsV2:log-groups/log-group/"
                            f"{log_group.replace('/', '%2F')}"
                            f"/log-events/{task_log_stream.replace('/', '%2F')}"
                        )
                        task_logs[task_name] = task_log_url
                
                if task_logs:
                    result['task_logs'] = task_logs
                    logger.info(f"Added {len(task_logs)} task log URLs for run {run_id}")
            else:
                # Fallback to base URL if no tasks found
                task_logs_base_url = (
                    f"https://{region}.console.aws.amazon.com/cloudwatch/home"
                    f"?region={region}#logsV2:log-groups/log-group/"
                    f"{log_group.replace('/', '%2F')}"
                )
                result['task_logs_base_url'] = task_logs_base_url
                logger.info(f"No tasks found, added task logs base URL for run {run_id}")
        except Exception as e:
            logger.warning(f"Error retrieving task logs for run {run_id}: {str(e)}")
            # Fallback to base URL
            task_logs_base_url = (
                f"https://{region}.console.aws.amazon.com/cloudwatch/home"
                f"?region={region}#logsV2:log-groups/log-group/"
                f"{log_group.replace('/', '%2F')}"
            )
            result['task_logs_base_url'] = task_logs_base_url
            
        # Try to find manifest log
        try:
            # For manifest log, the format is manifest/run/{run_id}/{uuid}
            # We need to list log streams to find the exact UUID
            # For now, we'll include a base URL that users can navigate from
            manifest_log_base_url = (
                f"https://{region}.console.aws.amazon.com/cloudwatch/home"
                f"?region={region}#logsV2:log-groups/log-group/"
                f"{log_group.replace('/', '%2F')}/log-events/manifest$252Frun$252F{actual_run_id}"
            )
            result['manifest_log_base_url'] = manifest_log_base_url
            logger.info(f"Added manifest log base URL for run {run_id}")
        except Exception as e:
            logger.warning(f"Error creating manifest log URL for run {run_id}: {str(e)}")

        # Return all log URLs
        return result

    except Exception as e:
        logger.error(f"Error getting log URLs for run {run_id}: {str(e)}")
        return {}


def get_run_tags(run_id, logger):
    """
    Get tags for an AWS HealthOmics run.

    Args:
        run_id: AWS HealthOmics run ID
        logger: Logger instance

    Returns:
        Dictionary containing tags or empty dict if not available
    """
    try:
        response = omics_client.get_run(id=run_id)
        tags = response.get('tags', {})

        if tags:
            logger.info(f"Retrieved {len(tags)} tags for run {run_id}")
        else:
            logger.info(f"No tags found for run {run_id}")
        return tags

    except Exception as e:
        logger.error(f"Error getting tags for run {run_id}: {str(e)}")
        return {}

def fetch_output_mapping(output_uri, run_id, logger):
    """
    Fetch output mapping from S3.

    Args:
        output_uri: S3 URI of the output directory
        run_id: AWS HealthOmics run ID
        logger: Logger instance

    Returns:
        Dictionary mapping output names to S3 URIs or empty dict if not available
    """
    try:
        # Parse S3 URI
        if not output_uri.startswith('s3://'):
            logger.warning(f"Output URI {output_uri} is not an S3 URI")
            return {}

        # Remove s3:// prefix and split into bucket and key
        path = output_uri[5:]
        parts = path.split('/', 1)
        if len(parts) < 2:
            logger.warning(f"Invalid S3 URI format: {output_uri}")
            return {}

        bucket = parts[0]
        key_prefix = parts[1]

        # Ensure key prefix ends with a slash
        if not key_prefix.endswith('/'):
            key_prefix += '/'

        # The specific path to the outputs.json file
        output_json_key = f"{key_prefix}logs/outputs.json"

        # Try to fetch the output mapping file
        try:
            logger.info(f"Attempting to fetch output mapping from s3://{bucket}/{output_json_key}")
            response = s3.get_object(Bucket=bucket, Key=output_json_key)
            content = response['Body'].read().decode('utf-8')
            mapping = json.loads(content)

            # Validate mapping format
            if isinstance(mapping, dict):
                # Convert CWL-style output format to a simpler key-value mapping
                result = {}
                for key, value in mapping.items():
                    if isinstance(value, dict) and 'location' in value:
                        # Extract the S3 URI from the location field
                        result[key] = value['location']
                    elif (isinstance(value, list) and
                          all(isinstance(item, dict) and 'location' in item for item in value)):
                        # For array outputs, extract all locations
                        result[key] = [item['location'] for item in value]
                    else:
                        # For other types, just convert to string
                        result[key] = str(value)

                logger.info(f"Successfully loaded output mapping with {len(result)} entries")
                return result
            else:
                logger.warning(f"Output mapping file s3://{bucket}/{output_json_key} is not a dictionary")

        except s3.exceptions.NoSuchKey:
            logger.info(f"Output mapping file s3://{bucket}/{output_json_key} not found")
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse output mapping file s3://{bucket}/{output_json_key} as JSON")
        except Exception as e:
            logger.warning(f"Error accessing s3://{bucket}/{output_json_key}: {str(e)}")

        # If we get here, we couldn't find a valid output mapping file
        logger.warning(f"No valid output mapping file found for run {run_id}")
        return {}

    except Exception as e:
        logger.error(f"Error fetching output mapping for run {run_id}: {str(e)}")
        return {}


def ensure_json_serializable(obj):
    """
    Ensure an object is JSON serializable by converting non-serializable types.

    Args:
        obj: Any Python object

    Returns:
        JSON serializable version of the object
    """
    if isinstance(obj, dict):
        return {k: ensure_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [ensure_json_serializable(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat() if hasattr(obj, 'isoformat') else str(obj)
    elif isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    else:
        return str(obj)


def lambda_handler(event, context):
    '''
    Main Entry Point for Lambda function

    Calls NGS360 API Service with run event information
    '''
    data = {}
    logger = setup_logging(event)

    API_SERVER = os.environ['API_SERVER']
    AUTH_TOKEN = get_auth_token()

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
    data['omics_run_id'] = flat_event.get('runId')
    data['status'] = flat_event.get('status')
    data['event_time'] = flat_event.get('time')
    data['event_id'] = flat_event.get('id')

    # Get the run status and ID
    status = flat_event.get('status')
    run_id = flat_event.get('runId')
    region = flat_event.get('region', 'us-east-1')

    if run_id:
        try:
            tags = get_run_tags(run_id, logger)
            if tags and 'WESRunId' in tags:
                data['wes_run_id'] = tags['WESRunId']
                logger.info(f"Added wes_run_id from WESRunId tag: {tags['WESRunId']}")
        except Exception as e:
            logger.error(f"Error getting tags for run {run_id}: {str(e)}")

    logging.info('checkpoint1')

    # For finishing events (COMPLETED, FAILED, CANCELLED), add additional information
    if status in ['COMPLETED', 'FAILED', 'CANCELLED'] and run_id:
        logger.info(f"Processing {status} event for run {run_id}")

        # Add log URLs for all finishing events
        try:
            log_urls = get_log_urls(run_id, region, logger)
            if log_urls:
                data['log_urls'] = log_urls
                logger.info(f"Added log URLs for run {run_id}")
        except Exception as e:
            logger.error(f"Error getting log URLs for run {run_id}: {str(e)}")

        # For COMPLETED events only, add output mapping
        if status == 'COMPLETED':
            output_uri = flat_event.get('runOutputUri')
            if output_uri:
                try:
                    output_mapping = fetch_output_mapping(output_uri, run_id, logger)
                    if output_mapping:
                        data['output_mapping'] = output_mapping
                        logger.info(f"Added output mapping for run {run_id}")
                except Exception as e:
                    logger.error(f"Error fetching output mapping for run {run_id}: {str(e)}")

    # Ensure all values are JSON serializable
    data = ensure_json_serializable(data)

    # Convert flattened dict to JSON string
    json_data = json.dumps(data)

    logging.info('checkpoint2')
    logging.info(json_data)

    # Upload to S3
    s3.put_object(
        Bucket=DATA_LAKE_BUCKET,
        Key=f'{S3_PREFIX}/{file_name}',
        Body=json_data,
        ContentType='application/json',
        ServerSideEncryption='AES256'
    )

    logging.info('checkpoint3')
    logging.info(json_data)

    # Call GA4GH WES API Server
    api_url = f'{API_SERVER}/internal/callbacks/omics-state-change'
    headers = {'Content-Type': 'application/json'}
    if AUTH_TOKEN:
        headers['Authorization'] = f'Bearer {AUTH_TOKEN}'
    #else: # TODO: Remove this line - testing purpose
    #    headers['X-Internal-API-Key'] = 'testapikey' # TODO: Remove this line - testing purpose
    logging.info(headers)

    try:
        response = requests.post(api_url, headers=headers, data=json_data, timeout=10)
        response.raise_for_status()
        logger.info(f"Successfully sent event to API server: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending event to API server: {str(e)}")
        # We don't want to fail the Lambda function if the API call fails
        # The event is already archived in S3

    msg = f'Event processed, status: {status} -> s3://{DATA_LAKE_BUCKET}/{S3_PREFIX}/{file_name}'
    logger.info(msg)
    return {
        'statusCode': 200,
        'body': msg
    }
