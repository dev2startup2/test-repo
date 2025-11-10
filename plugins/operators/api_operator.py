"""
Custom API Operator
===================

Enhanced API operator for REST API integration:
- Authentication support (Bearer, Basic, API Key)
- Rate limiting
- Retry with exponential backoff
- Response parsing and validation
- Error handling
"""

from typing import Any, Dict, List, Optional, Union, Callable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import logging
import time
import json
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class APIOperator(BaseOperator):
    """
    Custom API operator for REST API calls with advanced features.

    Features:
    - Multiple authentication methods
    - Automatic retries with backoff
    - Rate limiting
    - Response validation
    - Pagination support

    :param endpoint: API endpoint URL
    :param method: HTTP method (GET, POST, PUT, DELETE, PATCH)
    :param headers: HTTP headers
    :param data: Request body data
    :param params: URL parameters
    :param auth_type: Authentication type ('bearer', 'basic', 'api_key', None)
    :param auth_token: Authentication token
    :param timeout: Request timeout in seconds
    :param retries: Number of retry attempts
    :param retry_delay: Initial delay between retries (exponential backoff)
    :param validate_response: Validate response
    :param expected_status_codes: Expected HTTP status codes
    """

    template_fields = ('endpoint', 'data', 'params', 'headers')
    ui_color = '#fff4e6'

    @apply_defaults
    def __init__(
        self,
        endpoint: str,
        method: str = 'GET',
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Union[Dict, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        auth_type: Optional[str] = None,
        auth_token: Optional[str] = None,
        auth_username: Optional[str] = None,
        auth_password: Optional[str] = None,
        api_key_header: str = 'X-API-Key',
        timeout: int = 30,
        retries: int = 3,
        retry_delay: int = 2,
        validate_response: bool = True,
        expected_status_codes: Optional[List[int]] = None,
        response_filter: Optional[Callable] = None,
        rate_limit: Optional[int] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.method = method.upper()
        self.headers = headers or {}
        self.data = data
        self.params = params
        self.auth_type = auth_type
        self.auth_token = auth_token
        self.auth_username = auth_username
        self.auth_password = auth_password
        self.api_key_header = api_key_header
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self.validate_response = validate_response
        self.expected_status_codes = expected_status_codes or [200, 201, 204]
        self.response_filter = response_filter
        self.rate_limit = rate_limit
        self._last_request_time = 0

    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Execute API request.

        :param context: Airflow context
        :return: API response
        """
        self.log.info(f"Calling API: {self.method} {self.endpoint}")

        # Setup session with retry
        session = self._create_session()

        # Setup authentication
        self._setup_auth(session)

        # Rate limiting
        self._apply_rate_limit()

        # Make request with retries
        response = self._make_request(session, context)

        # Validate response
        if self.validate_response:
            self._validate_response(response)

        # Parse response
        result = self._parse_response(response)

        # Apply filter if provided
        if self.response_filter:
            result = self.response_filter(result)

        self.log.info(f"API call successful. Status: {response.status_code}")

        return result

    def _create_session(self) -> requests.Session:
        """
        Create requests session with retry configuration.

        :return: Configured session
        """
        session = requests.Session()

        # Configure retries
        retry_strategy = Retry(
            total=self.retries,
            backoff_factor=self.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _setup_auth(self, session: requests.Session) -> None:
        """
        Setup authentication for the session.

        :param session: Requests session
        """
        if self.auth_type == 'bearer':
            # Bearer token authentication
            self.headers['Authorization'] = f'Bearer {self.auth_token}'
            self.log.info("Using Bearer token authentication")

        elif self.auth_type == 'basic':
            # Basic authentication
            from requests.auth import HTTPBasicAuth
            session.auth = HTTPBasicAuth(self.auth_username, self.auth_password)
            self.log.info("Using Basic authentication")

        elif self.auth_type == 'api_key':
            # API key authentication
            self.headers[self.api_key_header] = self.auth_token
            self.log.info(f"Using API Key authentication ({self.api_key_header})")

    def _apply_rate_limit(self) -> None:
        """
        Apply rate limiting between requests.
        """
        if self.rate_limit:
            time_since_last = time.time() - self._last_request_time
            min_interval = 1.0 / self.rate_limit

            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                self.log.info(f"Rate limiting: sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time)

            self._last_request_time = time.time()

    def _make_request(
        self,
        session: requests.Session,
        context: Dict[str, Any]
    ) -> requests.Response:
        """
        Make HTTP request.

        :param session: Requests session
        :param context: Airflow context
        :return: Response object
        """
        start_time = datetime.now()

        try:
            response = session.request(
                method=self.method,
                url=self.endpoint,
                headers=self.headers,
                params=self.params,
                json=self.data if isinstance(self.data, dict) else None,
                data=self.data if isinstance(self.data, str) else None,
                timeout=self.timeout
            )

            execution_time = (datetime.now() - start_time).total_seconds()
            self.log.info(f"Request completed in {execution_time:.2f}s")

            return response

        except requests.exceptions.RequestException as e:
            self.log.error(f"API request failed: {str(e)}")
            raise

    def _validate_response(self, response: requests.Response) -> None:
        """
        Validate API response.

        :param response: Response object
        :raises ValueError: If response is invalid
        """
        if response.status_code not in self.expected_status_codes:
            error_msg = (
                f"Unexpected status code: {response.status_code}. "
                f"Expected: {self.expected_status_codes}. "
                f"Response: {response.text[:500]}"
            )
            self.log.error(error_msg)
            raise ValueError(error_msg)

    def _parse_response(self, response: requests.Response) -> Any:
        """
        Parse API response.

        :param response: Response object
        :return: Parsed response
        """
        if response.status_code == 204:
            return None

        content_type = response.headers.get('Content-Type', '')

        if 'application/json' in content_type:
            return response.json()
        elif 'text/' in content_type:
            return response.text
        else:
            return response.content


class PaginatedAPIOperator(APIOperator):
    """
    API operator with pagination support.

    Automatically handles paginated API responses.

    :param pagination_type: Type of pagination ('offset', 'page', 'cursor')
    :param page_size: Number of items per page
    :param max_pages: Maximum number of pages to fetch
    :param page_param: Name of page parameter
    :param size_param: Name of size parameter
    """

    @apply_defaults
    def __init__(
        self,
        pagination_type: str = 'page',
        page_size: int = 100,
        max_pages: Optional[int] = None,
        page_param: str = 'page',
        size_param: str = 'size',
        total_key: str = 'total',
        results_key: str = 'results',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.pagination_type = pagination_type
        self.page_size = page_size
        self.max_pages = max_pages
        self.page_param = page_param
        self.size_param = size_param
        self.total_key = total_key
        self.results_key = results_key

    def execute(self, context: Dict[str, Any]) -> List[Any]:
        """
        Execute paginated API requests.

        :param context: Airflow context
        :return: Combined results from all pages
        """
        self.log.info(f"Fetching paginated data from: {self.endpoint}")

        all_results = []
        page = 1
        has_more = True

        while has_more:
            # Update pagination parameters
            self.params = self.params or {}
            self._update_pagination_params(page)

            # Fetch page
            self.log.info(f"Fetching page {page}...")
            response = super().execute(context)

            # Extract results
            if isinstance(response, dict) and self.results_key in response:
                page_results = response[self.results_key]
            else:
                page_results = response

            all_results.extend(page_results)

            # Check if more pages exist
            has_more = self._has_more_pages(response, page)
            page += 1

            # Check max pages limit
            if self.max_pages and page > self.max_pages:
                self.log.warning(f"Reached max pages limit: {self.max_pages}")
                break

        self.log.info(f"Fetched total of {len(all_results)} items across {page-1} pages")

        return all_results

    def _update_pagination_params(self, page: int) -> None:
        """
        Update pagination parameters.

        :param page: Current page number
        """
        if self.pagination_type == 'page':
            self.params[self.page_param] = page
            self.params[self.size_param] = self.page_size
        elif self.pagination_type == 'offset':
            self.params['offset'] = (page - 1) * self.page_size
            self.params['limit'] = self.page_size

    def _has_more_pages(self, response: Any, current_page: int) -> bool:
        """
        Check if more pages exist.

        :param response: API response
        :param current_page: Current page number
        :return: True if more pages exist
        """
        if isinstance(response, dict):
            # Check total count
            if self.total_key in response:
                total = response[self.total_key]
                return (current_page * self.page_size) < total

            # Check results
            if self.results_key in response:
                results = response[self.results_key]
                return len(results) >= self.page_size

        # If list, check if full page
        if isinstance(response, list):
            return len(response) >= self.page_size

        return False


class APIToDatabaseOperator(BaseOperator):
    """
    Fetch data from API and load directly to database.

    Combines API call and database insert in one operator.

    :param api_endpoint: API endpoint
    :param table: Target database table
    :param database_conn_id: Database connection ID
    :param transform_func: Optional transformation function
    """

    template_fields = ('api_endpoint', 'table')
    ui_color = '#e6f7ff'

    @apply_defaults
    def __init__(
        self,
        api_endpoint: str,
        table: str,
        database_conn_id: str = 'postgres_default',
        method: str = 'GET',
        headers: Optional[Dict] = None,
        params: Optional[Dict] = None,
        transform_func: Optional[Callable] = None,
        batch_size: int = 1000,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.api_endpoint = api_endpoint
        self.table = table
        self.database_conn_id = database_conn_id
        self.method = method
        self.headers = headers
        self.params = params
        self.transform_func = transform_func
        self.batch_size = batch_size

    def execute(self, context: Dict[str, Any]) -> int:
        """
        Execute API to database operation.

        :param context: Airflow context
        :return: Number of rows inserted
        """
        self.log.info(f"Fetching from API: {self.api_endpoint}")

        # Fetch from API
        api_op = APIOperator(
            task_id='temp_api',
            endpoint=self.api_endpoint,
            method=self.method,
            headers=self.headers,
            params=self.params
        )

        data = api_op.execute(context)

        # Transform if needed
        if self.transform_func:
            self.log.info("Applying transformation function")
            data = self.transform_func(data)

        # Load to database
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import pandas as pd

        df = pd.DataFrame(data)
        hook = PostgresHook(postgres_conn_id=self.database_conn_id)
        engine = hook.get_sqlalchemy_engine()

        df.to_sql(
            self.table,
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=self.batch_size
        )

        self.log.info(f"Loaded {len(df)} rows to table: {self.table}")

        return len(df)
