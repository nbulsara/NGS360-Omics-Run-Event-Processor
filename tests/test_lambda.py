#!/usr/bin/env python3
"""
Test script for the AWS HealthOmics Run Event Processor Lambda function.

This script tests the Lambda function with sample events to verify the enhanced functionality.
"""

import json
import os
import sys
from unittest.mock import MagicMock, patch

# Add the parent directory to the path so we can import the lambda module
sys.path.append('..')

# Import the lambda module with a different name to avoid keyword conflict
import sys
sys.path.append('..')
# Use importlib to import a module with a Python keyword name
import importlib
lambda_func = importlib.import_module('lambda')

# Set up mock environment variables
os.environ['API_SERVER'] = 'https://api.example.com'
os.environ['DATA_LAKE_BUCKET'] = 'test-bucket'
os.environ['S3_PREFIX'] = 'omics-run-events'
os.environ['VERBOSE_LOGGING'] = 'true'

# Create mock objects for boto3 clients
mock_s3 = MagicMock()
mock_omics = MagicMock()
mock_secrets_client = MagicMock()

# Sample output mapping for completed events
sample_output_mapping = {
    "RecalibratedBAM": "s3://bucket/path/to/output.bam",
    "VariantCalls": "s3://bucket/path/to/variants.vcf"
}

# Sample log URLs for completed/failed/cancelled events
sample_log_urls = {
    'run_log': 'https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/aws%2Fomics%2FWorkflowLog/log-events/run%2F8567247',
    'task_logs_base_url': 'https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/aws%2Fomics%2FWorkflowLog',
    'manifest_log_base_url': 'https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/aws%2Fomics%2FWorkflowLog/log-events/manifest$252Frun$252F8567247'
}

# Configure mock responses
def setup_mocks():
    # Mock S3 get_object for output mapping
    mock_s3.get_object.return_value = {
        'Body': MagicMock(
            read=MagicMock(
                return_value=json.dumps(sample_output_mapping).encode('utf-8')
            )
        )
    }
    
    # Mock Omics get_run for log URLs and tags
    mock_omics.get_run.return_value = {
        'logLocation': {
            'runLogStream': 'arn:aws:logs:us-east-1:example_account:log-group:/aws/omics/WorkflowLog:log-stream:run/8567247'
        },
        'tags': {
            'WESRunId': 'test-wes-run-123',
            'Project': 'TestProject',
            'Owner': 'TestUser'
        }
    }
    
    # Mock Omics list_run_tasks
    mock_omics.list_run_tasks.return_value = {
        'items': [
            {
                'id': '3974135',
                'name': 'main',
                'status': 'COMPLETED'
            }
        ]
    }
    
    # Replace the boto3 clients with our mocks
    lambda_func.s3 = mock_s3
    lambda_func.omics_client = mock_omics
    lambda_func.secrets_client = mock_secrets_client

def test_event(event_file, expected_status, should_have_log_urls=False, should_have_output_mapping=False):
    """Test the Lambda function with a sample event."""
    print(f"\nTesting {event_file} (status: {expected_status})...")
    
    # Load the sample event
    with open(event_file, 'r') as f:
        event = json.load(f)
    
    # Reset mock call counts
    mock_s3.reset_mock()
    mock_omics.reset_mock()
    
    # Patch the requests.post method to avoid making actual HTTP requests
    with patch('requests.post') as mock_post:
        # Set up the mock to return a successful response
        mock_post.return_value.status_code = 200
        
        # Call the Lambda function
        response = lambda_func.lambda_handler(event, None)
        
        # Verify the response
        assert response['statusCode'] == 200, f"Expected status code 200, got {response['statusCode']}"
        
        # Get the captured JSON data
        captured_data = None
        for call in mock_post.call_args_list:
            args, kwargs = call
            captured_data = json.loads(kwargs.get('data', '{}'))
            break
        
        # Verify the status
        assert captured_data['status'] == expected_status, f"Expected status {expected_status}, got {captured_data['status']}"
        
        # Verify log URLs for completed/failed/cancelled events
        if should_have_log_urls:
            assert 'log_urls' in captured_data, f"Expected log_urls in event data for {expected_status} event"
            print(f"✓ Log URLs included for {expected_status} event")
            # We now call get_run twice (once for tags, once for logs), so we don't assert call count here
        else:
            assert 'log_urls' not in captured_data, f"Did not expect log_urls in event data for {expected_status} event"
            print(f"✓ No log URLs for {expected_status} event (as expected)")
            # For non-finishing events, we still call get_run once for tags
            assert mock_omics.get_run.call_count == 1, f"Expected get_run to be called once for tags, called {mock_omics.get_run.call_count} times"
        
        # Verify output mapping for completed events
        if should_have_output_mapping:
            assert 'output_mapping' in captured_data, f"Expected output_mapping in event data for {expected_status} event"
            print(f"✓ Output mapping included for {expected_status} event")
            mock_s3.get_object.assert_called_once()
        else:
            assert 'output_mapping' not in captured_data, f"Did not expect output_mapping in event data for {expected_status} event"
            print(f"✓ No output mapping for {expected_status} event (as expected)")
        
        # Verify WESRunId tag is included in all events
        assert 'wes_run_id' in captured_data, f"Expected wes_run_id in event data for {expected_status} event"
        assert captured_data['wes_run_id'] == 'test-wes-run-123', f"Expected wes_run_id to be 'test-wes-run-123', got {captured_data['wes_run_id']}"
        print(f"✓ WESRunId tag correctly included for {expected_status} event")
            
        print(f"✓ Test passed for {event_file}")
        return captured_data

def main():
    """Run the tests."""
    print("Setting up mocks...")
    setup_mocks()
    
    print("\nTesting Lambda function with sample events...")
    
    # Test completed event (should have log URLs and output mapping)
    completed_data = test_event(
        'example_jsons/event_completed_example.json',
        'COMPLETED',
        should_have_log_urls=True,
        should_have_output_mapping=True
    )
    
    # Test failed event (should have log URLs but no output mapping)
    failed_data = test_event(
        'example_jsons/event_failed_example.json',
        'FAILED',
        should_have_log_urls=True,
        should_have_output_mapping=False
    )
    
    # Test cancelled event (should have log URLs but no output mapping)
    cancelled_data = test_event(
        'example_jsons/event_cancelled_example.json',
        'CANCELLED',
        should_have_log_urls=True,
        should_have_output_mapping=False
    )
    
    # Test running event (should have neither log URLs nor output mapping)
    running_data = test_event(
        'example_jsons/event_running_example.json',
        'RUNNING',
        should_have_log_urls=False,
        should_have_output_mapping=False
    )
    
    # Test starting event (should have neither log URLs nor output mapping)
    starting_data = test_event(
        'example_jsons/event_starting_example.json',
        'STARTING',
        should_have_log_urls=False,
        should_have_output_mapping=False
    )
    
    # Test pending event (should have neither log URLs nor output mapping)
    pending_data = test_event(
        'example_jsons/event_pending_example.json',
        'PENDING',
        should_have_log_urls=False,
        should_have_output_mapping=False
    )
    
    # Test stopping event (should have neither log URLs nor output mapping)
    stopping_data = test_event(
        'example_jsons/event_stopping_example.json',
        'STOPPING',
        should_have_log_urls=False,
        should_have_output_mapping=False
    )
    
    print("\nAll tests passed!")

if __name__ == '__main__':
    main()