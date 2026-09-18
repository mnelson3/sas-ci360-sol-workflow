#!/usr/bin/env python3
# Business Source License 1.1
#
# Parameters
#
# Licensor:             Nelson Grey LLC
# Licensed Work:        SAS CI360 Workflow Solution
# Additional Use Grant: None
# Change Date:          2029-12-13
# Change License:       Apache License 2.0
#
# Terms
#
# The Licensor hereby grants you the right to copy, modify, create derivative
# works, redistribute, and make non-production use of the Licensed Work. The
# Licensor may make an Additional Use Grant, above, permitting limited
# production use.
#
# Effective on the Change Date, or the fourth anniversary of the first publicly
# available distribution of a specific version of the Licensed Work under this
# License, whichever comes first, the Licensor hereby grants you rights under
# the terms of the Change License, and the rights granted in the paragraph
# above terminate.
#
# If your use of the Licensed Work does not comply with the requirements
# currently in effect as described in this License, you must purchase a
# commercial license from the Licensor, its affiliated entities, or authorized
# resellers, or you must refrain from using the Licensed Work.
#
# All copies of the original and modified Licensed Work, and derivative works
# of the Licensed Work, are subject to this License. This License applies
# separately for each version of the Licensed Work and the Change Date may vary
# for each version of the Licensed Work released by Licensor.
#
# You must conspicuously display this License on each original or modified copy
# of the Licensed Work. If you receive the Licensed Work in original or
# modified form from a third party, the terms and conditions set forth in this
# License apply to your use of that work.
#
# Any use of the Licensed Work in violation of this License will automatically
# terminate your rights under this License for the current and all other
# versions of the Licensed Work.
#
# This License does not grant you any right in any trademark or logo of
# Licensor or its affiliates (provided that you may use a trademark or logo of
# Licensor as expressly required by this License).
#
# TO THE EXTENT PERMITTED BY APPLICABLE LAW, THE LICENSED WORK IS PROVIDED ON
# AN "AS IS" BASIS. LICENSOR HEREBY DISCLAIMS ALL WARRANTIES AND CONDITIONS,
# EXPRESS OR IMPLIED, INCLUDING (WITHOUT LIMITATION) WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NON-INFRINGEMENT, AND
# TITLE.
#
# MariaDB hereby grants you permission to use this License's text to license
# your works, and to refer to it using the trademark "Business Source License",
# as long as you comply with the Covenants of Licensor below.
#
# Covenants of Licensor
#
# In consideration of the right to use this License's text and the "Business
# Source License" name and trademark, Licensor covenants to MariaDB, a Delaware
# corporation, for the benefit of MariaDB and any other party that has
# contributed to the Licensed Work, to use best efforts to provide the Change
# License on the Change Date for each version of the Licensed Work, and to
# designate the Change License as "Apache License 2.0" or a later version of
# the Apache License.

#
# Copyright (c) 2025 Nelson Grey LLC
# Author: Nelson Grey LLC
#
# Licensed under the Business Source License 1.1 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# https://github.com/mnelson3/sas-ci360-sol-workflow/blob/main/LICENSE
#
# -*- coding: utf-8 -*-
"""
SAS CI360 Workflow Module Base Class

Provides foundational functionality for SAS Customer Intelligence 360
workflow operations, including connection management, authentication,
and workflow automation capabilities.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import jwt
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class CI360WorkflowConfig:
    """Configuration for CI360 Workflow operations."""

    algorithm: str = "HS256"
    api_base: str = "/marketingWorkflow"
    encoding: str = "utf-8"
    host: Optional[str] = None
    secret_key: Optional[str] = None
    tenant_id: Optional[str] = None
    timeout: int = 30
    max_retries: int = 3
    retry_backoff: float = 0.5
    enable_compression: bool = True
    max_concurrent_workflows: int = 50
    workflow_timeout: int = 7200
    token_ttl: int = 3600


class CI360WorkflowError(Exception):
    """Base exception for CI360 Workflow operations."""
    pass


class CI360WorkflowAuthError(CI360WorkflowError):
    """Authentication-related errors."""
    pass


class CI360WorkflowConnectionError(CI360WorkflowError):
    """Connection and network-related errors."""
    pass


class CI360WorkflowValidationError(CI360WorkflowError):
    """Data validation errors."""
    pass


class CI360WorkflowBase:
    """
    Base class for SAS CI360 Workflow operations.

    Provides authentication, connection management, and common functionality
    for workflow automation and process management API interactions with async support.
    """

    def __init__(self, config: Optional[CI360WorkflowConfig] = None) -> None:
        """
        Initialize the CI360 Workflow base client.

        Args:
            config: Configuration object for CI360 Workflow operations

        Raises:
            CI360WorkflowValidationError: If required configuration is missing
        """
        self.config = config or CI360WorkflowConfig()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Validate configuration
        self._validate_config()

        # Initialize HTTP session with retry strategy
        self.session = self._create_session()

        # Generate authentication token
        self.token = self._generate_token()

        # Connection state
        self._connected = False

        self.logger.info("CI360 Workflow Base initialized successfully")

    def _validate_config(self) -> None:
        """Validate configuration parameters."""
        required_fields = ['host', 'secret_key', 'tenant_id']
        missing = [field for field in required_fields if not getattr(self.config, field)]

        if missing:
            raise CI360WorkflowValidationError(f"Missing required configuration: {', '.join(missing)}")

        # Validate algorithm
        supported_algorithms = ['HS256', 'HS384', 'HS512', 'RS256', 'RS384', 'RS512']
        if self.config.algorithm not in supported_algorithms:
            raise CI360WorkflowValidationError(f"Unsupported algorithm: {self.config.algorithm}")

        # Validate workflow concurrency
        if self.config.max_concurrent_workflows <= 0:
            raise CI360WorkflowValidationError("max_concurrent_workflows must be positive")

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry strategy."""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _generate_token(self) -> str:
        """Generate JWT authentication token."""
        try:
            now = datetime.now(timezone.utc)
            payload = {
                "tenant_id": self.config.tenant_id,
                "iat": now,
                "exp": now + timedelta(seconds=self.config.token_ttl),
            }
            return jwt.encode(
                payload,
                self.config.secret_key,
                algorithm=self.config.algorithm,
            )
        except Exception as e:
            raise CI360WorkflowAuthError(f"Failed to generate authentication token: {e}")

    def get_auth_headers(self) -> Dict[str, str]:
        """
        Get authentication headers for API requests.

        Returns:
            Dict[str, str]: Headers dictionary with authorization token
        """
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Tenant-ID": str(self.config.tenant_id)
        }

    async def validate_connection_async(self) -> bool:
        """
        Asynchronously validate connection to CI360 service.

        Returns:
            bool: True if connection is valid
        """
        try:
            # Basic health check endpoint
            health_url = urljoin(self.config.host, "/health")
            headers = self.get_auth_headers()

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.session.get(
                    health_url,
                    headers=headers,
                    timeout=self.config.timeout
                )
            )

            self._connected = response.status_code == 200
            return self._connected

        except Exception as e:
            self.logger.error(f"Connection validation failed: {e}")
            self._connected = False
            return False

    def validate_connection(self) -> bool:
        """
        Validate connection to CI360 service.

        Returns:
            bool: True if connection is valid
        """
        try:
            # Run async validation in sync context
            return asyncio.run(self.validate_connection_async())
        except Exception as e:
            self.logger.error(f"Sync connection validation failed: {e}")
            return False

    async def _make_request_async(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make asynchronous HTTP request to CI360 API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint path
            data: Request body data
            params: Query parameters

        Returns:
            Dict[str, Any]: Response data

        Raises:
            CI360WorkflowConnectionError: For network/connection errors
            CI360WorkflowAuthError: For authentication errors
        """
        if not self._connected:
            await self.validate_connection_async()
            if not self._connected:
                raise CI360WorkflowConnectionError("No active connection to CI360 service")

        url = urljoin(self.config.host + self.config.api_base, endpoint.lstrip('/'))
        headers = self.get_auth_headers()

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=data,
                    params=params,
                    timeout=self.config.timeout
                )
            )

            response.raise_for_status()
            return response.json() if response.content else {}

        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                raise CI360WorkflowAuthError(f"Authentication failed: {e}")
            elif response.status_code >= 500:
                raise CI360WorkflowConnectionError(f"Server error: {e}")
            else:
                raise CI360WorkflowError(f"API request failed: {e}")
        except requests.exceptions.RequestException as e:
            raise CI360WorkflowConnectionError(f"Network error: {e}")

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make synchronous HTTP request to CI360 API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint path
            data: Request body data
            params: Query parameters

        Returns:
            Dict[str, Any]: Response data
        """
        try:
            return asyncio.run(self._make_request_async(method, endpoint, data, params))
        except Exception as e:
            self.logger.error(f"Request failed: {e}")
            raise

    # Workflow Management APIs

    async def get_workflows_async(
        self,
        limit: int = 50,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Retrieve workflows asynchronously.

        Args:
            limit: Maximum number of workflows to return
            offset: Number of workflows to skip
            filters: Optional filters for workflows

        Returns:
            Dict containing workflow data and metadata
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        if filters:
            params.update(filters)

        return await self._make_request_async("GET", "/workflows", params=params)

    def get_workflows(
        self,
        limit: int = 50,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Retrieve workflows synchronously."""
        params = {
            "limit": limit,
            "offset": offset
        }
        if filters:
            params.update(filters)

        return self._make_request("GET", "/workflows", params=params)

    async def get_workflow_async(self, workflow_id: str) -> Dict[str, Any]:
        """
        Retrieve specific workflow data asynchronously.

        Args:
            workflow_id: Unique workflow identifier

        Returns:
            Dict containing workflow data
        """
        return await self._make_request_async("GET", f"/workflows/{workflow_id}")

    def get_workflow(self, workflow_id: str) -> Dict[str, Any]:
        """Retrieve specific workflow data synchronously."""
        return self._make_request("GET", f"/workflows/{workflow_id}")

    async def create_workflow_async(self, workflow_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create new workflow asynchronously.

        Args:
            workflow_data: Workflow definition data

        Returns:
            Dict containing created workflow data
        """
        return await self._make_request_async("POST", "/workflows", data=workflow_data)

    def create_workflow(self, workflow_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create new workflow synchronously."""
        return self._make_request("POST", "/workflows", data=workflow_data)

    async def update_workflow_async(self, workflow_id: str, workflow_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update workflow definition asynchronously.

        Args:
            workflow_id: Unique workflow identifier
            workflow_data: Updated workflow data

        Returns:
            Dict containing updated workflow data
        """
        return await self._make_request_async("PUT", f"/workflows/{workflow_id}", data=workflow_data)

    def update_workflow(self, workflow_id: str, workflow_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update workflow definition synchronously."""
        return self._make_request("PUT", f"/workflows/{workflow_id}", data=workflow_data)

    async def delete_workflow_async(self, workflow_id: str) -> bool:
        """
        Delete workflow asynchronously.

        Args:
            workflow_id: Unique workflow identifier

        Returns:
            True if deletion successful
        """
        await self._make_request_async("DELETE", f"/workflows/{workflow_id}")
        return True

    def delete_workflow(self, workflow_id: str) -> bool:
        """Delete workflow synchronously."""
        self._make_request("DELETE", f"/workflows/{workflow_id}")
        return True

    # Process Execution APIs

    async def start_process_async(self, workflow_id: str, input_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Start workflow process execution asynchronously.

        Args:
            workflow_id: Unique workflow identifier
            input_data: Optional input data for the process

        Returns:
            Dict containing process execution results
        """
        payload = {
            "workflowId": workflow_id,
            "inputData": input_data or {}
        }
        return await self._make_request_async("POST", "/processes/start", data=payload)

    def start_process(self, workflow_id: str, input_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Start workflow process execution synchronously."""
        payload = {
            "workflowId": workflow_id,
            "inputData": input_data or {}
        }
        return self._make_request("POST", "/processes/start", data=payload)

    async def get_process_status_async(self, process_id: str) -> Dict[str, Any]:
        """
        Get process execution status asynchronously.

        Args:
            process_id: Unique process identifier

        Returns:
            Dict containing process status and details
        """
        return await self._make_request_async("GET", f"/processes/{process_id}")

    def get_process_status(self, process_id: str) -> Dict[str, Any]:
        """Get process execution status synchronously."""
        return self._make_request("GET", f"/processes/{process_id}")

    async def cancel_process_async(self, process_id: str) -> bool:
        """
        Cancel running process asynchronously.

        Args:
            process_id: Unique process identifier

        Returns:
            True if cancellation successful
        """
        await self._make_request_async("POST", f"/processes/{process_id}/cancel")
        return True

    def cancel_process(self, process_id: str) -> bool:
        """Cancel running process synchronously."""
        self._make_request("POST", f"/processes/{process_id}/cancel")
        return True

    async def get_processes_async(
        self,
        limit: int = 50,
        offset: int = 0,
        status_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List process executions asynchronously.

        Args:
            limit: Maximum number of processes to return
            offset: Number of processes to skip
            status_filter: Optional status filter (running, completed, failed, cancelled)

        Returns:
            Dict containing list of process executions
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        if status_filter:
            params["status"] = status_filter

        return await self._make_request_async("GET", "/processes", params=params)

    def get_processes(
        self,
        limit: int = 50,
        offset: int = 0,
        status_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """List process executions synchronously."""
        params = {
            "limit": limit,
            "offset": offset
        }
        if status_filter:
            params["status"] = status_filter

        return self._make_request("GET", "/processes", params=params)

    # Trigger Management APIs

    async def get_triggers_async(
        self,
        limit: int = 50,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Retrieve workflow triggers asynchronously.

        Args:
            limit: Maximum number of triggers to return
            offset: Number of triggers to skip
            filters: Optional filters for triggers

        Returns:
            Dict containing trigger data and metadata
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        if filters:
            params.update(filters)

        return await self._make_request_async("GET", "/triggers", params=params)

    def get_triggers(
        self,
        limit: int = 50,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Retrieve workflow triggers synchronously."""
        params = {
            "limit": limit,
            "offset": offset
        }
        if filters:
            params.update(filters)

        return self._make_request("GET", "/triggers", params=params)

    async def create_trigger_async(self, trigger_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create new workflow trigger asynchronously.

        Args:
            trigger_data: Trigger configuration data

        Returns:
            Dict containing created trigger data
        """
        return await self._make_request_async("POST", "/triggers", data=trigger_data)

    def create_trigger(self, trigger_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create new workflow trigger synchronously."""
        return self._make_request("POST", "/triggers", data=trigger_data)

    async def update_trigger_async(self, trigger_id: str, trigger_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update trigger configuration asynchronously.

        Args:
            trigger_id: Unique trigger identifier
            trigger_data: Updated trigger data

        Returns:
            Dict containing updated trigger data
        """
        return await self._make_request_async("PUT", f"/triggers/{trigger_id}", data=trigger_data)

    def update_trigger(self, trigger_id: str, trigger_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update trigger configuration synchronously."""
        return self._make_request("PUT", f"/triggers/{trigger_id}", data=trigger_data)

    async def delete_trigger_async(self, trigger_id: str) -> bool:
        """
        Delete workflow trigger asynchronously.

        Args:
            trigger_id: Unique trigger identifier

        Returns:
            True if deletion successful
        """
        await self._make_request_async("DELETE", f"/triggers/{trigger_id}")
        return True

    def delete_trigger(self, trigger_id: str) -> bool:
        """Delete workflow trigger synchronously."""
        self._make_request("DELETE", f"/triggers/{trigger_id}")
        return True

    # Workflow Templates APIs

    async def get_workflow_templates_async(
        self,
        category: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Retrieve workflow templates asynchronously.

        Args:
            category: Optional template category filter
            limit: Maximum number of templates to return
            offset: Number of templates to skip

        Returns:
            Dict containing workflow templates
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        if category:
            params["category"] = category

        return await self._make_request_async("GET", "/templates/workflows", params=params)

    def get_workflow_templates(
        self,
        category: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Retrieve workflow templates synchronously."""
        params = {
            "limit": limit,
            "offset": offset
        }
        if category:
            params["category"] = category

        return self._make_request("GET", "/templates/workflows", params=params)

    async def create_workflow_from_template_async(self, template_id: str, workflow_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create workflow from template asynchronously.

        Args:
            template_id: Unique template identifier
            workflow_data: Workflow-specific configuration

        Returns:
            Dict containing created workflow data
        """
        payload = {
            "templateId": template_id,
            "workflowData": workflow_data
        }
        return await self._make_request_async("POST", "/workflows/from-template", data=payload)

    def create_workflow_from_template(self, template_id: str, workflow_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create workflow from template synchronously."""
        payload = {
            "templateId": template_id,
            "workflowData": workflow_data
        }
        return self._make_request("POST", "/workflows/from-template", data=payload)

    def __enter__(self):
        """Context manager entry."""
        if not self.validate_connection():
            raise CI360WorkflowConnectionError("Failed to establish connection")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.session.close()

    async def __aenter__(self):
        """Async context manager entry."""
        if not await self.validate_connection_async():
            raise CI360WorkflowConnectionError("Failed to establish connection")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.session.close()
