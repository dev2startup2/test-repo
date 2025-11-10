"""
Unit tests for APIOperator
"""

import pytest
import requests
from plugins.operators.api_operator import (
    APIOperator,
    PaginatedAPIOperator
)


class TestAPIOperator:
    """
    Test APIOperator functionality.
    """

    def test_initialization(self):
        """Test operator initialization."""
        op = APIOperator(
            task_id='test_api',
            endpoint='https://api.example.com/data',
            method='GET'
        )

        assert op.task_id == 'test_api'
        assert op.endpoint == 'https://api.example.com/data'
        assert op.method == 'GET'

    def test_authentication_bearer(self):
        """Test Bearer token authentication."""
        op = APIOperator(
            task_id='test',
            endpoint='https://api.example.com/data',
            auth_type='bearer',
            auth_token='test_token_123'
        )

        session = requests.Session()
        op._setup_auth(session)

        assert 'Authorization' in op.headers
        assert op.headers['Authorization'] == 'Bearer test_token_123'

    def test_authentication_api_key(self):
        """Test API Key authentication."""
        op = APIOperator(
            task_id='test',
            endpoint='https://api.example.com/data',
            auth_type='api_key',
            auth_token='test_api_key',
            api_key_header='X-API-Key'
        )

        session = requests.Session()
        op._setup_auth(session)

        assert 'X-API-Key' in op.headers
        assert op.headers['X-API-Key'] == 'test_api_key'

    def test_validate_response_success(self, mock_api_response):
        """Test response validation with success."""
        mock_api_response.status_code = 200

        op = APIOperator(
            task_id='test',
            endpoint='https://api.example.com/data',
            expected_status_codes=[200, 201]
        )

        # Should not raise exception
        op._validate_response(mock_api_response)

    def test_validate_response_failure(self, mock_api_response):
        """Test response validation with failure."""
        mock_api_response.status_code = 404
        mock_api_response.text = 'Not Found'

        op = APIOperator(
            task_id='test',
            endpoint='https://api.example.com/data',
            expected_status_codes=[200]
        )

        with pytest.raises(ValueError, match="Unexpected status code"):
            op._validate_response(mock_api_response)

    def test_parse_response_json(self, mock_api_response):
        """Test parsing JSON response."""
        mock_api_response.headers = {'Content-Type': 'application/json'}
        mock_api_response.json.return_value = {'key': 'value'}

        op = APIOperator(
            task_id='test',
            endpoint='https://api.example.com/data'
        )

        result = op._parse_response(mock_api_response)

        assert result == {'key': 'value'}

    def test_parse_response_text(self, mock_api_response):
        """Test parsing text response."""
        mock_api_response.headers = {'Content-Type': 'text/plain'}
        mock_api_response.text = 'Hello World'

        op = APIOperator(
            task_id='test',
            endpoint='https://api.example.com/data'
        )

        result = op._parse_response(mock_api_response)

        assert result == 'Hello World'

    def test_execute(self, mock_api_response, airflow_context):
        """Test execute method."""
        mock_api_response.status_code = 200
        mock_api_response.json.return_value = {'data': [1, 2, 3]}
        mock_api_response.headers = {'Content-Type': 'application/json'}

        op = APIOperator(
            task_id='test',
            endpoint='https://api.example.com/data'
        )

        result = op.execute(airflow_context)

        assert result == {'data': [1, 2, 3]}


class TestPaginatedAPIOperator:
    """
    Test PaginatedAPIOperator functionality.
    """

    def test_initialization(self):
        """Test operator initialization."""
        op = PaginatedAPIOperator(
            task_id='test_paginated',
            endpoint='https://api.example.com/data',
            pagination_type='page',
            page_size=50
        )

        assert op.task_id == 'test_paginated'
        assert op.pagination_type == 'page'
        assert op.page_size == 50

    def test_update_pagination_params_page(self):
        """Test updating pagination parameters (page-based)."""
        op = PaginatedAPIOperator(
            task_id='test',
            endpoint='https://api.example.com/data',
            pagination_type='page',
            page_size=100,
            page_param='page',
            size_param='per_page'
        )

        op.params = {}
        op._update_pagination_params(2)

        assert op.params['page'] == 2
        assert op.params['per_page'] == 100

    def test_update_pagination_params_offset(self):
        """Test updating pagination parameters (offset-based)."""
        op = PaginatedAPIOperator(
            task_id='test',
            endpoint='https://api.example.com/data',
            pagination_type='offset',
            page_size=50
        )

        op.params = {}
        op._update_pagination_params(3)

        assert op.params['offset'] == 100  # (3-1) * 50
        assert op.params['limit'] == 50

    def test_has_more_pages_true(self):
        """Test has_more_pages returns True."""
        op = PaginatedAPIOperator(
            task_id='test',
            endpoint='https://api.example.com/data',
            page_size=100,
            total_key='total',
            results_key='items'
        )

        response = {
            'total': 500,
            'items': [1, 2, 3] * 34  # 102 items (more than page_size)
        }

        assert op._has_more_pages(response, 1) is True

    def test_has_more_pages_false(self):
        """Test has_more_pages returns False."""
        op = PaginatedAPIOperator(
            task_id='test',
            endpoint='https://api.example.com/data',
            page_size=100,
            total_key='total',
            results_key='items'
        )

        response = {
            'total': 50,
            'items': [1, 2, 3] * 10  # 30 items (less than page_size)
        }

        assert op._has_more_pages(response, 1) is False
