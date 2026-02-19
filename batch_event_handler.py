import requests
import json
import os
from logger import get_logger

logger = get_logger()


def post_job(job_id, job_status, log_stream_name):
    """ Post Batch job status to NGS360 REST API """
    message_body = {
        'job_id': job_id,
        'job_status': job_status,
        'log_stream_name': log_stream_name
    }

    headers = {'Content-Type': 'application/json'}
    url = "%s/api/v1/jobs/%s" % (os.environ['NGS360_API_SERVER'], job_id)

    try:
        logger.info("Sending %s to %s", message_body, url)
        res = requests.put(url, headers=headers, data=json.dumps(message_body))
        if res.status_code != 200:
            logger.error("%s returned %s", url, res.status_code)
    except requests.exceptions.RequestException as e:
        logger.error(str(e))
        return False
    return True


def batch_event_handler(event):
    """
    AWS Batch Event Handler
    """
    # Extract relevant information from the Batch event
    job_id = event.get('detail', {}).get('jobId')
    job_name = event.get('detail', {}).get('jobName')
    job_status = event.get('detail', {}).get('status')
    log_stream_name = ''

    logger.info(
        f"Processing Batch job - ID: {job_id}, "
        f"Name: {job_name}, Status: {job_status}"
    )

    # Implement your logic to handle different job statuses
    if job_status in ('STARTING', 'RUNNING', 'SUCCEEDED', 'FAILED'):
        if 'logStreamName' in event['detail']['container']:
            log_stream_name = event['detail']['container']['logStreamName']
            logger.info(f"Log Stream Name: {log_stream_name}")
        else:
            logger.error("Unable to determine logStreamName")

    post_job(job_id, job_status, log_stream_name)

    return {
        'statusCode': 200,
        'message': 'AWS Batch Event processed.',
    }
