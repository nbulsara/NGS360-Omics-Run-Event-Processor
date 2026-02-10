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
                    # The field is 'taskId', not 'id'
                    task_id = task.get('taskId')
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
            # For manifest log, we need to check if there's a UUID suffix
            # For now, we'll provide a link to the CloudWatch console where users can search for the manifest log
            # The format is typically manifest/run/{run_id}/{uuid}, but the UUID part varies

            # Link to the CloudWatch log group with a filter for this run's manifest logs
            manifest_log_base_url = (
                f"https://{region}.console.aws.amazon.com/cloudwatch/home"
                f"?region={region}#logsV2:log-groups/log-group/"
                f"{log_group.replace('/', '%2F')}"
                f"?logStreamNameFilter=manifest%2Frun%2F{actual_run_id}"
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


def convert_file_path_to_s3(file_path):
    """
    Convert file path to S3 URI if needed.

    Args:
        file_path: File path (can be file://, s3://, or NGS360: format)

    Returns:
        S3 URI
    """
    if file_path.startswith('s3://'):
        return file_path
    elif file_path.startswith('file://'):
        # Remove file:// prefix and convert to S3 path
        # This is a simplified conversion - in production you'd need proper path mapping
        local_path = file_path[7:]  # Remove 'file://'
        # For now, assume it's already in the right bucket structure
        return f"s3://bucket-name{local_path}"
    elif file_path.startswith('NGS360:'):
        # Handle NGS360 file IDs - convert to actual S3 paths
        # This would need integration with NGS360 file service
        file_id = file_path[7:]  # Remove 'NGS360:'
        return f"s3://ngs360-files/{file_id}"
    else:
        # Assume it's already a valid S3 URI or local path
        return file_path


def convert_wes_params_to_omics(wes_params, workflow_type):
    """
    Convert WES workflow parameters to Omics format.

    Args:
        wes_params: Dictionary of WES workflow parameters
        workflow_type: Workflow type (e.g., 'CWL', 'WDL')

    Returns:
        Dictionary of Omics-formatted parameters
    """
    omics_params = {}

    for key, value in wes_params.items():
        if key == 'workflow_id':
            continue  # Exclude workflow_id from parameters

        if isinstance(value, dict) and 'class' in value:
            # CWL File/Directory object
            converted_value = value.copy()
            if 'path' in converted_value:
                converted_value['path'] = convert_file_path_to_s3(converted_value['path'])
            omics_params[key] = converted_value

        elif isinstance(value, list):
            # Array of files/values
            processed_list = []
            for item in value:
                if isinstance(item, dict) and 'path' in item:
                    converted_item = item.copy()
                    converted_item['path'] = convert_file_path_to_s3(converted_item['path'])
                    processed_list.append(converted_item)
                elif isinstance(item, str) and (item.startswith('file://') or
                                                item.startswith('NGS360:') or
                                                item.startswith('s3://')):
                    processed_list.append(convert_file_path_to_s3(item))
                else:
                    processed_list.append(item)
            omics_params[key] = processed_list

        elif isinstance(value, str) and (value.startswith('file://') or
                                         value.startswith('NGS360:') or
                                         value.startswith('s3://')):
            # Simple file path
            omics_params[key] = convert_file_path_to_s3(value)

        else:
            # Other parameters (strings, numbers, booleans)
            omics_params[key] = value

    return omics_params


def validate_submission_request(event):
    """
    Validate workflow submission request.

    Args:
        event: Lambda event containing submission request

    Returns:
        tuple: (is_valid, error_message)
    """
    required_fields = ['action', 'wes_run_id', 'workflow_id']

    for field in required_fields:
        if field not in event:
            return False, f"Missing required field: {field}"

    if event['action'] != 'submit_workflow':
        return False, f"Invalid action: {event['action']}"

    workflow_id = event.get('workflow_id', '')
    if not workflow_id or len(workflow_id) < 1:
        return False, f"Invalid workflow_id: {workflow_id}. Workflow ID is required"

    wes_run_id = event.get('wes_run_id', '')
    if len(wes_run_id) != 36:
        return False, f"Invalid wes_run_id format: {wes_run_id}. Must be 36 characters"

    return True, None


def submit_omics_run(event, context):
    """
    Handle workflow submission requests from GA4GH WES API.
    Submits new workflows to AWS Omics using the same logic as the working omics.py.
    """
    logger = setup_logging(event)

    try:
        # Validate input parameters
        is_valid, error_msg = validate_submission_request(event)
        if not is_valid:
            logger.error(f"Validation error: {error_msg}")
            return {
                'statusCode': 400,
                'error': 'ValidationError',
                'message': error_msg
            }

        # Extract parameters
        wes_run_id = event['wes_run_id']
        workflow_id = event['workflow_id']
        workflow_version = event.get('workflow_version')  # Optional workflow version
        workflow_type = event.get('workflow_type', 'CWL')
        wes_params = event.get('parameters', {})
        workflow_engine_params = event.get('workflow_engine_parameters', {})
        tags = event.get('tags', {})

        logger.info(f"Processing workflow submission: wes_run_id={wes_run_id}, workflow_id={workflow_id}, workflow_version={workflow_version}")

        # Convert WES parameters to Omics format using the same logic as omics.py
        omics_params = convert_wes_params_to_omics(wes_params, workflow_type)
        logger.info(f"Converted parameters for Omics: {json.dumps(omics_params, default=str)}")

        # Set default output URI if not provided in workflow_engine_parameters
        # Following the same logic as omics.py execute method
        output_uri = None
        output_bucket = os.environ.get('DATA_LAKE_BUCKET', 's3://ngs360-omics-data-lake')

        if workflow_engine_params and 'outputUri' in workflow_engine_params:
            output_uri = workflow_engine_params['outputUri']
            logger.info(f"Using output URI from workflow_engine_parameters: {output_uri}")
        else:
            output_uri = f"{output_bucket}/runs/{wes_run_id}/output/"
            logger.info(f"Using default output URI: {output_uri}")

        # Prepare Omics start_run parameters following omics.py format
        kwargs = {
            'workflowId': workflow_id,
            'roleArn': os.environ.get('OMICS_ROLE_ARN'),
            'parameters': omics_params,
            'outputUri': output_uri,
            'name': f"wes-run-{wes_run_id}",
            'retentionMode': 'REMOVE',
            'storageType': 'DYNAMIC'
        }

        # Add workflow version if specified
        if workflow_version:
            kwargs['workflowVersionName'] = workflow_version
            logger.info(f"Using workflow version: {workflow_version}")

        # Add tags from the event, ensuring WESRunId is included
        if tags and len(tags) > 0:
            # Ensure WESRunId is in tags
            if 'WESRunId' not in tags:
                tags['WESRunId'] = wes_run_id
            kwargs['tags'] = tags
            logger.info(f"Adding tags to Omics run: {tags}")

            # Override name if provided in tags
            if "Name" in tags:
                kwargs['name'] = tags.get("Name")
        else:
            # Ensure WESRunId tag is always present
            kwargs['tags'] = {'WESRunId': wes_run_id}

        # Extract and add Omics-specific parameters from workflow_engine_parameters
        # Following the same logic as omics.py execute method
        if workflow_engine_params:
            engine_params = workflow_engine_params

            # Override name if provided
            if 'name' in engine_params:
                kwargs['name'] = engine_params['name']

            # Add run group ID if specified
            if 'runGroupId' in engine_params:
                kwargs['runGroupId'] = engine_params['runGroupId']
                logger.info(f"Using run group ID: {engine_params['runGroupId']}")

            # Add cache ID for reusing previous runs
            if 'cacheId' in engine_params:
                kwargs['cacheId'] = engine_params['cacheId']
                logger.info(f"Using cached run ID: {engine_params['cacheId']}")

            # Add tags if provided (merge with existing tags)
            if 'tags' in engine_params:
                kwargs['tags'].update(engine_params['tags'])

            # Add other supported parameters from omics.py
            omics_engine_params = [
                'priority', 'storageCapacity', 'accelerators', 'logLevel', 'storageType'
            ]
            for param in omics_engine_params:
                if param in engine_params:
                    kwargs[param] = engine_params[param]

        # Validate that we have required parameters
        if not kwargs['roleArn']:
            raise ValueError("OMICS_ROLE_ARN environment variable is required")

        # Log the API call parameters (same as omics.py)
        logger.info(f"Starting Omics run with parameters: {kwargs}")

        # Make the API call to start the run (same as omics.py)
        logging.info(kwargs)
        response = omics_client.start_run(**kwargs)

        # Store the Omics run ID
        omics_run_id = response['id']
        log_msg = f"Started AWS Omics run: {omics_run_id}, output will be in: {output_uri}"
        logger.info(f"WES run {wes_run_id}: {log_msg}")

        return {
            'statusCode': 200,
            'omics_run_id': omics_run_id,
            'output_uri': output_uri,
            'message': 'Workflow submitted successfully',
            'wes_run_id': wes_run_id
        }

    except Exception as e:
        logger.error(f"Error in workflow submission: {str(e)}")
        return {
            'statusCode': 500,
            'error': 'OmicsSubmissionError',
            'message': f'Failed to submit workflow to Omics: {str(e)}'
        }


def update_status(event, context):
    """
    Handle EventBridge state change events (existing functionality).
    This contains all the original lambda_handler logic.
    """
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
    headers['X-Internal-API-Key'] = AUTH_TOKEN
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


def lambda_handler(event, context):
    """
    Main entry point for Lambda function.
    Routes requests to appropriate handler based on event type.
    """
    logger = setup_logging(event)

    try:
        # Method 1: Check for explicit workflow submission action
        if event.get('action') == 'submit_workflow':
            logger.info("Routing to workflow submission handler")
            return submit_omics_run(event, context)

        # Method 2: Check for EventBridge characteristics
        elif (event.get('source') == 'aws.omics' and
              event.get('detail-type') == 'Run Status Change'):
            logger.info("Routing to status update handler")
            return update_status(event, context)

        # Method 3: Fallback for existing EventBridge events (backward compatibility)
        elif 'detail' in event and 'runId' in event.get('detail', {}):
            logger.info("Routing to status update handler (legacy format)")
            return update_status(event, context)

        # Method 4: Direct fields check (fallback for submission)
        elif 'wes_run_id' in event and 'workflow_id' in event:
            logger.info("Routing to workflow submission handler (legacy format)")
            if 'action' not in event:
                event['action'] = 'submit_workflow'  # Add action for consistency
            return submit_omics_run(event, context)

        # Unknown event type
        else:
            error_msg = f"Unknown event type. Event structure: {json.dumps(event, default=str)[:500]}..."
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'error': 'UnknownEventType',
                'message': 'Unable to determine event type - neither EventBridge nor workflow submission'
            }

    except Exception as e:
        logger.error(f"Error in main handler: {str(e)}")
        return {
            'statusCode': 500,
            'error': 'InternalError',
            'message': f'Lambda handler error: {str(e)}'
        }
