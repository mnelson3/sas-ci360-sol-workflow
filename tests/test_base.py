#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for SAS CI360 Workflow Module

Comprehensive test suite for the CI360WorkflowBase class and its APIs.
"""

import asyncio
import unittest
from unittest.mock import Mock, patch
from typing import Dict, Any

import jwt

from sasci360solworkflow.base import CI360WorkflowBase, CI360WorkflowConfig, CI360WorkflowError


class TestCI360WorkflowConfig(unittest.TestCase):
    """Test cases for CI360WorkflowConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = CI360WorkflowConfig()
        self.assertEqual(config.algorithm, "HS256")
        self.assertEqual(config.api_base, "/marketingWorkflow")
        self.assertEqual(config.encoding, "utf-8")
        self.assertIsNone(config.host)
        self.assertIsNone(config.secret_key)
        self.assertIsNone(config.tenant_id)
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.max_retries, 3)
        self.assertEqual(config.retry_backoff, 0.5)
        self.assertTrue(config.enable_compression)
        self.assertEqual(config.max_concurrent_workflows, 50)
        self.assertEqual(config.workflow_timeout, 7200)

    def test_custom_config(self):
        """Test custom configuration values."""
        config = CI360WorkflowConfig(
            host="https://api.example.com",
            secret_key="test-secret",
            tenant_id="test-tenant",
            timeout=60,
            max_concurrent_workflows=25
        )
        self.assertEqual(config.host, "https://api.example.com")
        self.assertEqual(config.secret_key, "test-secret")
        self.assertEqual(config.tenant_id, "test-tenant")
        self.assertEqual(config.timeout, 60)
        self.assertEqual(config.max_concurrent_workflows, 25)


class TestCI360WorkflowBase(unittest.TestCase):
    """Test cases for CI360WorkflowBase class."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = CI360WorkflowConfig(
            host="https://api.example.com",
            secret_key="test-secret-key",
            tenant_id="test-tenant-id"
        )

    @patch('sasci360solworkflow.base.requests.Session')
    def test_initialization_success(self, mock_session_class):
        """Test successful initialization."""
        mock_session = Mock()
        mock_session_class.return_value = mock_session

        client = CI360WorkflowBase(self.config)

        self.assertEqual(client.config, self.config)
        decoded = jwt.decode(client.token, self.config.secret_key, algorithms=[self.config.algorithm])
        self.assertEqual(decoded["tenant_id"], self.config.tenant_id)

    # Workflow Management Tests

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_get_workflows_async(self, mock_request, mock_session_class):
        """Test async workflow retrieval."""
        mock_request.return_value = {"workflows": [], "total": 0}

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.get_workflows_async(limit=20, offset=40))

        self.assertEqual(result, {"workflows": [], "total": 0})
        mock_request.assert_called_once_with(
            "GET", "/workflows",
            params={"limit": 20, "offset": 40}
        )

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_get_workflow_async(self, mock_request, mock_session_class):
        """Test async single workflow retrieval."""
        workflow_data = {
            "id": "wf-123",
            "name": "Customer Onboarding Flow",
            "status": "active",
            "steps": ["welcome", "profile", "activation"]
        }
        mock_request.return_value = workflow_data

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.get_workflow_async("wf-123"))

        self.assertEqual(result["name"], "Customer Onboarding Flow")
        mock_request.assert_called_once_with("GET", "/workflows/wf-123")

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_create_workflow_async(self, mock_request, mock_session_class):
        """Test async workflow creation."""
        workflow_data = {
            "name": "New Customer Workflow",
            "description": "Automated customer onboarding",
            "steps": [
                {"name": "send_welcome", "type": "email", "config": {"templateId": "welcome-1"}},
                {"name": "wait_24h", "type": "delay", "config": {"hours": 24}},
                {"name": "send_followup", "type": "email", "config": {"templateId": "followup-1"}}
            ]
        }
        mock_request.return_value = {"id": "wf-456", **workflow_data}

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.create_workflow_async(workflow_data))

        self.assertEqual(result["id"], "wf-456")
        mock_request.assert_called_once_with("POST", "/workflows", data=workflow_data)

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_update_workflow_async(self, mock_request, mock_session_class):
        """Test async workflow update."""
        update_data = {"name": "Updated Workflow Name", "status": "inactive"}
        mock_request.return_value = {"id": "wf-123", **update_data}

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.update_workflow_async("wf-123", update_data))

        self.assertEqual(result["name"], "Updated Workflow Name")
        mock_request.assert_called_once_with("PUT", "/workflows/wf-123", data=update_data)

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_delete_workflow_async(self, mock_request, mock_session_class):
        """Test async workflow deletion."""
        mock_request.return_value = None

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.delete_workflow_async("wf-123"))

        self.assertTrue(result)
        mock_request.assert_called_once_with("DELETE", "/workflows/wf-123")

    # Process Execution Tests

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_start_process_async(self, mock_request, mock_session_class):
        """Test async process start."""
        input_data = {"customerId": "cust-123", "email": "customer@example.com"}
        mock_request.return_value = {
            "processId": "proc-789",
            "workflowId": "wf-123",
            "status": "running",
            "startedAt": "2025-12-13T10:00:00Z"
        }

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.start_process_async("wf-123", input_data))

        self.assertEqual(result["processId"], "proc-789")
        expected_payload = {"workflowId": "wf-123", "inputData": input_data}
        mock_request.assert_called_once_with("POST", "/processes/start", data=expected_payload)

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_get_process_status_async(self, mock_request, mock_session_class):
        """Test async process status retrieval."""
        mock_request.return_value = {
            "processId": "proc-789",
            "status": "completed",
            "currentStep": "send_followup",
            "progress": 100,
            "outputData": {"emailSent": True}
        }

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.get_process_status_async("proc-789"))

        self.assertEqual(result["status"], "completed")
        mock_request.assert_called_once_with("GET", "/processes/proc-789")

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_cancel_process_async(self, mock_request, mock_session_class):
        """Test async process cancellation."""
        mock_request.return_value = None

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.cancel_process_async("proc-789"))

        self.assertTrue(result)
        mock_request.assert_called_once_with("POST", "/processes/proc-789/cancel")

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_get_processes_async(self, mock_request, mock_session_class):
        """Test async process listing."""
        mock_request.return_value = {"processes": [], "total": 0}

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.get_processes_async(limit=30, offset=60, status_filter="running"))

        self.assertEqual(result, {"processes": [], "total": 0})
        expected_params = {"limit": 30, "offset": 60, "status": "running"}
        mock_request.assert_called_once_with("GET", "/processes", params=expected_params)

    # Trigger Management Tests

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_get_triggers_async(self, mock_request, mock_session_class):
        """Test async trigger retrieval."""
        mock_request.return_value = {"triggers": [], "total": 0}

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.get_triggers_async(limit=25, offset=50))

        self.assertEqual(result, {"triggers": [], "total": 0})
        mock_request.assert_called_once_with(
            "GET", "/triggers",
            params={"limit": 25, "offset": 50}
        )

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_create_trigger_async(self, mock_request, mock_session_class):
        """Test async trigger creation."""
        trigger_data = {
            "name": "New Customer Trigger",
            "event": "customer.created",
            "workflowId": "wf-123",
            "conditions": {"source": "website"}
        }
        mock_request.return_value = {"id": "trig-999", **trigger_data}

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.create_trigger_async(trigger_data))

        self.assertEqual(result["name"], "New Customer Trigger")
        mock_request.assert_called_once_with("POST", "/triggers", data=trigger_data)

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_delete_trigger_async(self, mock_request, mock_session_class):
        """Test async trigger deletion."""
        mock_request.return_value = None

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.delete_trigger_async("trig-999"))

        self.assertTrue(result)
        mock_request.assert_called_once_with("DELETE", "/triggers/trig-999")

    # Template Management Tests

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_get_workflow_templates_async(self, mock_request, mock_session_class):
        """Test async workflow template retrieval."""
        mock_request.return_value = {"templates": [], "total": 0}

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.get_workflow_templates_async(category="onboarding", limit=10))

        self.assertEqual(result, {"templates": [], "total": 0})
        expected_params = {"category": "onboarding", "limit": 10, "offset": 0}
        mock_request.assert_called_once_with("GET", "/templates/workflows", params=expected_params)

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_create_workflow_from_template_async(self, mock_request, mock_session_class):
        """Test async workflow creation from template."""
        workflow_data = {"name": "My Custom Workflow", "customizations": {"emailTemplate": "custom-1"}}
        mock_request.return_value = {
            "id": "wf-888",
            "name": "My Custom Workflow",
            "templateId": "tmpl-777",
            "status": "draft"
        }

        client = CI360WorkflowBase(self.config)
        result = asyncio.run(client.create_workflow_from_template_async("tmpl-777", workflow_data))

        self.assertEqual(result["id"], "wf-888")
        expected_payload = {"templateId": "tmpl-777", "workflowData": workflow_data}
        mock_request.assert_called_once_with("POST", "/workflows/from-template", data=expected_payload)

    # Synchronous method tests

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_get_workflows_sync(self, mock_request, mock_session_class):
        """Test synchronous workflow retrieval."""
        mock_request.return_value = {"workflows": [], "total": 0}

        client = CI360WorkflowBase(self.config)
        result = client.get_workflows(limit=20)

        self.assertEqual(result["total"], 0)

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_create_workflow_sync(self, mock_request, mock_session_class):
        """Test synchronous workflow creation."""
        workflow_data = {"name": "Test Workflow"}
        mock_request.return_value = {"id": "wf-123", **workflow_data}

        client = CI360WorkflowBase(self.config)
        result = client.create_workflow(workflow_data)

        self.assertEqual(result["id"], "wf-123")

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_start_process_sync(self, mock_request, mock_session_class):
        """Test synchronous process start."""
        mock_request.return_value = {"processId": "proc-123", "status": "running"}

        client = CI360WorkflowBase(self.config)
        result = client.start_process("wf-123")

        self.assertEqual(result["processId"], "proc-123")


class TestCI360WorkflowErrorHandling(unittest.TestCase):
    """Test error handling scenarios."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = CI360WorkflowConfig(
            host="https://api.example.com",
            secret_key="test-secret-key",
            tenant_id="test-tenant-id"
        )

    @patch('sasci360solworkflow.base.requests.Session')
    @patch('sasci360solworkflow.base.CI360WorkflowBase._make_request_async')
    def test_connection_error_handling(self, mock_request, mock_session_class):
        """Test connection error handling."""
        from sasci360solworkflow.base import CI360WorkflowConnectionError
        mock_request.side_effect = CI360WorkflowConnectionError("Workflow service unavailable")

        client = CI360WorkflowBase(self.config)

        with self.assertRaises(CI360WorkflowConnectionError):
            asyncio.run(client.get_workflows_async())


if __name__ == '__main__':
    unittest.main()