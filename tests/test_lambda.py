#!/usr/bin/env python3
"""
Mock-based unit tests for the AWS HealthOmics Run Event Processor Lambda function.

This script tests all Lambda function components using mocks to avoid real AWS API calls.
"""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime

# Add the parent directory to the path so we can import the lambda module
sys.path.append('..')

# Set up required environment variables before importing lambda module
os.environ['API_SERVER'] = 'https://api.example.com'
os.environ['DATA_LAKE_BUCKET'] = 'test-bucket'
os.environ['S3_PREFIX'] = 'omics-run-events'
os.environ['VERBOSE_LOGGING'] = 'true'
os.environ['OMICS_ROLE_ARN'] = 'arn:aws:iam::123456789012:role/test-omics-role'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'  # Set default region for boto3

# Mock boto3 clients before importing lambda module to avoid AWS credential issues
with patch('boto3.client') as mock_boto3_client:
    # Configure mock clients
    mock_boto3_client.return_value = MagicMock()

    # Import the lambda module with a different name to avoid keyword conflict
    import importlib
    lambda_func = importlib.import_module('lambda')


class TestValidation(unittest.TestCase):
    """Test validation functions."""

    def test_validate_submission_request_valid(self):
        """Test validation with valid submission request."""
        valid_event = {
            'action': 'submit_workflow',
            'wes_run_id': 'test-wes-run-123',
            'workflow_id': '6287203',
            'workflow_engine_parameters': {
                'outputUri': 's3://test-bucket/outputs/'
            }
        }

        is_valid, error_msg = lambda_func.validate_submission_request(valid_event)
        self.assertTrue(is_valid)
        self.assertIsNone(error_msg)

    def test_validate_submission_request_missing_fields(self):
        """Test validation with missing required fields."""
        test_cases = [
            # Missing action
            ({'wes_run_id': 'test-run', 'workflow_id': '123', 'workflow_engine_parameters': {'outputUri': 's3://test/'}}, 'action'),
            # Missing wes_run_id
            ({'action': 'submit_workflow', 'workflow_id': '123', 'workflow_engine_parameters': {'outputUri': 's3://test/'}}, 'wes_run_id'),
            # Missing workflow_id
            ({'action': 'submit_workflow', 'wes_run_id': 'test-run', 'workflow_engine_parameters': {'outputUri': 's3://test/'}}, 'workflow_id'),
            # Missing workflow_engine_parameters
            ({'action': 'submit_workflow', 'wes_run_id': 'test-run', 'workflow_id': '123'}, 'workflow_engine_parameters'),
            # Missing outputUri in workflow_engine_parameters
            ({'action': 'submit_workflow', 'wes_run_id': 'test-run', 'workflow_id': '123', 'workflow_engine_parameters': {}}, 'outputUri'),
        ]

        for event, expected_field in test_cases:
            with self.subTest(missing_field=expected_field):
                is_valid, error_msg = lambda_func.validate_submission_request(event)
                self.assertFalse(is_valid)
                self.assertIn(expected_field, error_msg)

    def test_validate_submission_request_invalid_action(self):
        """Test validation with invalid action."""
        invalid_event = {
            'action': 'invalid_action',
            'wes_run_id': 'test-run',
            'workflow_id': '123',
            'workflow_engine_parameters': {'outputUri': 's3://test/'}
        }

        is_valid, error_msg = lambda_func.validate_submission_request(invalid_event)
        self.assertFalse(is_valid)
        self.assertIn('invalid_action', error_msg)

    def test_validate_submission_request_invalid_workflow_id(self):
        """Test validation with invalid workflow_id."""
        invalid_events = [
            {'action': 'submit_workflow', 'wes_run_id': 'test-run', 'workflow_id': '', 'workflow_engine_parameters': {'outputUri': 's3://test/'}},
            {'action': 'submit_workflow', 'wes_run_id': 'test-run', 'workflow_id': None, 'workflow_engine_parameters': {'outputUri': 's3://test/'}},
        ]

        for event in invalid_events:
            with self.subTest(workflow_id=event['workflow_id']):
                is_valid, error_msg = lambda_func.validate_submission_request(event)
                self.assertFalse(is_valid)
                self.assertIn('workflow_id', error_msg)


class TestWorkflowSubmission(unittest.TestCase):
    """Test workflow submission functionality."""

    @patch('lambda.omics_client')
    def test_submit_omics_run_success(self, mock_omics_client):
        """Test successful workflow submission."""
        # Set up mock
        mock_omics_client.start_run.return_value = {
            'id': '1234567',
            'arn': 'arn:aws:omics:us-east-1:123456789012:run/1234567'
        }

        event = {
            'action': 'submit_workflow',
            'wes_run_id': 'test-wes-run-123',
            'workflow_id': '6287203',
            'workflow_version': '1.0',
            'workflow_type': 'CWL',
            'parameters': {
                'input_file': 'file:///data/input.txt'
            },
            'workflow_engine_parameters': {
                'name': 'test-workflow',
                'outputUri': 's3://test-bucket/outputs/',
                'cacheId': 'cache-123'
            },
            'tags': {
                'Project': 'TestProject'
            }
        }

        response = lambda_func.submit_omics_run(event, None)

        # Verify response
        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(response['omics_run_id'], '1234567')
        self.assertIn('output_uri', response)

        # Verify start_run was called with correct parameters
        mock_omics_client.start_run.assert_called_once()
        call_kwargs = mock_omics_client.start_run.call_args[1]

        self.assertEqual(call_kwargs['workflowId'], '6287203')
        self.assertEqual(call_kwargs['roleArn'], 'arn:aws:iam::123456789012:role/test-omics-role')
        self.assertEqual(call_kwargs['name'], 'test-workflow')
        self.assertEqual(call_kwargs['outputUri'], 's3://test-bucket/outputs/')
        self.assertEqual(call_kwargs['cacheId'], 'cache-123')
        self.assertEqual(call_kwargs['workflowVersionName'], '1.0')
        self.assertIn('WESRunId', call_kwargs['tags'])
        self.assertEqual(call_kwargs['tags']['WESRunId'], 'test-wes-run-123')

    def test_submit_omics_run_validation_error(self):
        """Test workflow submission with validation error."""
        invalid_event = {
            'action': 'submit_workflow',
            'wes_run_id': 'test-run',
            'workflow_id': '123',
            # Missing workflow_engine_parameters
        }

        response = lambda_func.submit_omics_run(invalid_event, None)

        self.assertEqual(response['statusCode'], 400)
        self.assertEqual(response['error'], 'ValidationError')
        self.assertIn('workflow_engine_parameters', response['message'])

    @patch('lambda.omics_client')
    def test_submit_omics_run_api_error(self, mock_omics_client):
        """Test workflow submission with AWS API error."""
        # Set up mock to raise exception
        mock_omics_client.start_run.side_effect = Exception("AWS API Error")

        event = {
            'action': 'submit_workflow',
            'wes_run_id': 'test-run-123',
            'workflow_id': '6287203',
            'workflow_engine_parameters': {
                'outputUri': 's3://test-bucket/outputs/'
            }
        }

        response = lambda_func.submit_omics_run(event, None)

        self.assertEqual(response['statusCode'], 500)
        self.assertEqual(response['error'], 'OmicsSubmissionError')
        self.assertIn('AWS API Error', response['message'])


class TestEventProcessing(unittest.TestCase):
    """Test EventBridge event processing functionality."""

    @patch('lambda.s3.put_object')  # Mock S3 upload
    @patch('lambda.requests.post')
    @patch('lambda.s3.get_object')  # Mock S3 get_object for output mapping
    @patch('lambda.omics_client')
    def test_update_status_completed_event(self, mock_omics_client, mock_s3_get, mock_requests, mock_s3_put):
        """Test processing of completed EventBridge event."""
        # Set up mocks
        mock_requests.return_value.status_code = 200
        mock_s3_put.return_value = {}  # Mock S3 upload
        mock_omics_client.get_run.return_value = {
            'logLocation': {
                'runLogStream': 'arn:aws:logs:us-east-1:example:log-group:/aws/omics/WorkflowLog:log-stream:run/8567247'
            },
            'tags': {
                'WESRunId': 'test-wes-run-123',
                'Project': 'TestProject'
            }
        }
        mock_omics_client.list_run_tasks.return_value = {
            'items': [
                {'taskId': '3974135', 'name': 'main', 'status': 'COMPLETED'}
            ]
        }
        mock_s3_get.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=json.dumps({'output1': 's3://bucket/output.txt'}).encode()))
        }

        event = {
            'source': 'aws.omics',
            'detail-type': 'Run Status Change',
            'detail': {
                'status': 'COMPLETED',
                'runId': '8567247',
                'runOutputUri': 's3://bucket/outputs/'
            },
            'region': 'us-east-1',
            'time': '2023-01-01T00:00:00Z',
            'id': 'test-event-123'
        }

        response = lambda_func.update_status(event, None)

        # Verify response
        self.assertEqual(response['statusCode'], 200)

        # Verify API calls were made
        self.assertEqual(mock_omics_client.get_run.call_count, 2)  # Once for tags, once for logs
        mock_omics_client.list_run_tasks.assert_called_once()
        mock_s3_get.assert_called_once()  # S3 get_object for output mapping
        mock_s3_put.assert_called_once()  # S3 put_object for event storage

        # Verify GA4GH API was called
        mock_requests.assert_called_once()
        call_kwargs = mock_requests.call_args[1]
        sent_data = json.loads(call_kwargs['data'])

        # Verify event data structure
        self.assertEqual(sent_data['status'], 'COMPLETED')
        self.assertEqual(sent_data['omics_run_id'], '8567247')
        self.assertEqual(sent_data['wes_run_id'], 'test-wes-run-123')
        self.assertIn('log_urls', sent_data)
        self.assertIn('output_mapping', sent_data)

    @patch('lambda.s3.put_object')  # Mock S3 upload
    @patch('lambda.requests.post')
    @patch('lambda.omics_client')
    def test_update_status_running_event(self, mock_omics_client, mock_requests, mock_s3_put):
        """Test processing of running EventBridge event."""
        # Set up mocks
        mock_requests.return_value.status_code = 200
        mock_s3_put.return_value = {}  # Mock S3 upload
        mock_omics_client.get_run.return_value = {
            'tags': {
                'WESRunId': 'test-wes-run-456'
            }
        }

        event = {
            'source': 'aws.omics',
            'detail-type': 'Run Status Change',
            'detail': {
                'status': 'RUNNING',
                'runId': '8567248'
            },
            'region': 'us-east-1',
            'time': '2023-01-01T00:00:00Z',
            'id': 'test-event-456'
        }

        response = lambda_func.update_status(event, None)

        # Verify response
        self.assertEqual(response['statusCode'], 200)

        # Verify only tags were fetched (no logs or output mapping for running events)
        mock_omics_client.get_run.assert_called_once()

        # Verify GA4GH API was called
        mock_requests.assert_called_once()
        call_kwargs = mock_requests.call_args[1]
        sent_data = json.loads(call_kwargs['data'])

        # Verify event data structure
        self.assertEqual(sent_data['status'], 'RUNNING')
        self.assertEqual(sent_data['omics_run_id'], '8567248')
        self.assertEqual(sent_data['wes_run_id'], 'test-wes-run-456')
        self.assertNotIn('log_urls', sent_data)
        self.assertNotIn('output_mapping', sent_data)


class TestEventRouting(unittest.TestCase):
    """Test event routing in main lambda handler."""

    @patch('lambda.submit_omics_run')
    def test_lambda_handler_submission_routing(self, mock_submit_omics_run):
        """Test routing of workflow submission events."""
        mock_submit_omics_run.return_value = {'statusCode': 200, 'omics_run_id': '123'}

        event = {
            'source': 'ga4ghwes',
            'action': 'submit_workflow',
            'wes_run_id': 'test-run',
            'workflow_id': '123'
        }

        response = lambda_func.lambda_handler(event, None)

        mock_submit_omics_run.assert_called_once_with(event, None)
        self.assertEqual(response['statusCode'], 200)

    @patch('lambda.update_status')
    def test_lambda_handler_eventbridge_routing(self, mock_update_status):
        """Test routing of EventBridge events."""
        mock_update_status.return_value = {'statusCode': 200}

        event = {
            'source': 'aws.omics',
            'detail-type': 'Run Status Change',
            'detail': {'status': 'RUNNING', 'runId': '123'}
        }

        response = lambda_func.lambda_handler(event, None)

        mock_update_status.assert_called_once_with(event, None)
        self.assertEqual(response['statusCode'], 200)

    def test_lambda_handler_unknown_event(self):
        """Test handling of unknown event types."""
        event = {
            'unknown_field': 'unknown_value'
        }

        response = lambda_func.lambda_handler(event, None)

        self.assertEqual(response['statusCode'], 400)
        self.assertIn('Unable to determine event type - neither EventBridge nor workflow submission', response['message'])


class TestHelperFunctions(unittest.TestCase):
    """Test helper functions."""

    def test_ensure_json_serializable(self):
        """Test JSON serialization helper."""
        test_data = {
            'string': 'test',
            'number': 42,
            'datetime': datetime(2023, 1, 1, 12, 0, 0),
            'nested': {
                'datetime': datetime(2023, 1, 1, 13, 0, 0),
                'list': [datetime(2023, 1, 1, 14, 0, 0), 'string']
            }
        }

        result = lambda_func.ensure_json_serializable(test_data)

        # Verify datetimes are converted to strings
        self.assertEqual(result['string'], 'test')
        self.assertEqual(result['number'], 42)
        self.assertIsInstance(result['datetime'], str)
        self.assertIsInstance(result['nested']['datetime'], str)
        self.assertIsInstance(result['nested']['list'][0], str)
        self.assertEqual(result['nested']['list'][1], 'string')

    def test_flatten(self):
        """Test event flattening function."""
        nested_event = {
            'detail': {
                'status': 'COMPLETED',
                'runId': '123',
                'nested': {
                    'value': 'test'
                }
            },
            'source': 'aws.omics'
        }

        flattened = lambda_func.flatten(nested_event)

        # The flatten function only flattens one level deep
        self.assertEqual(flattened['status'], 'COMPLETED')
        self.assertEqual(flattened['runId'], '123')
        self.assertEqual(flattened['nested'], {'value': 'test'})  # nested dict stays as nested dict
        self.assertEqual(flattened['source'], 'aws.omics')


if __name__ == '__main__':
    unittest.main()
