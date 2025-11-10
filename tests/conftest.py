"""
Pytest Configuration and Fixtures
==================================

Shared fixtures and configuration for all tests.
"""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import MagicMock


@pytest.fixture
def sample_dataframe():
    """
    Create a sample DataFrame for testing.
    """
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, None, 45],
        'city': ['New York', 'London', 'Paris', 'Tokyo', 'Berlin'],
        'value': [100, 200, 300, 400, 500]
    })


@pytest.fixture
def sample_dict_data():
    """
    Create sample dictionary data for testing.
    """
    return [
        {'id': 1, 'name': 'Alice', 'value': 100},
        {'id': 2, 'name': 'Bob', 'value': 200},
        {'id': 3, 'name': 'Charlie', 'value': 300}
    ]


@pytest.fixture
def airflow_context():
    """
    Create mock Airflow context for testing.
    """
    ti = MagicMock()
    ti.xcom_push = MagicMock()
    ti.xcom_pull = MagicMock(return_value=None)

    return {
        'task_instance': ti,
        'execution_date': datetime(2024, 1, 1),
        'dag_run': MagicMock(),
        'task': MagicMock(),
        'ds': '2024-01-01',
        'tomorrow_ds': '2024-01-02',
        'yesterday_ds': '2023-12-31'
    }


@pytest.fixture
def mock_postgres_hook(mocker):
    """
    Create mock PostgresHook for testing.
    """
    mock_hook = MagicMock()
    mock_hook.get_records = MagicMock(return_value=[])
    mock_hook.run = MagicMock()
    mock_hook.get_pandas_df = MagicMock(return_value=pd.DataFrame())
    mock_hook.get_sqlalchemy_engine = MagicMock()

    mocker.patch(
        'airflow.providers.postgres.hooks.postgres.PostgresHook',
        return_value=mock_hook
    )

    return mock_hook


@pytest.fixture
def mock_api_response(mocker):
    """
    Create mock API response for testing.
    """
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json = MagicMock(return_value={'data': []})
    mock_response.text = 'OK'
    mock_response.content = b'OK'
    mock_response.headers = {'Content-Type': 'application/json'}

    mocker.patch('requests.request', return_value=mock_response)

    return mock_response


@pytest.fixture
def temp_test_file(tmp_path):
    """
    Create temporary test file.
    """
    test_file = tmp_path / "test_data.csv"
    df = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })
    df.to_csv(test_file, index=False)
    return str(test_file)


# Pytest hooks for custom behavior

def pytest_configure(config):
    """
    Configure pytest with custom settings.
    """
    config.addinivalue_line(
        "markers", "unit: Unit tests"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests"
    )


def pytest_collection_modifyitems(config, items):
    """
    Modify test collection.
    """
    # Add markers based on test location
    for item in items:
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
