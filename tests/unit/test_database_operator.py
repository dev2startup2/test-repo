"""
Unit tests for DatabaseOperator
"""

import pytest
import pandas as pd
from plugins.operators.database_operator import (
    DatabaseOperator,
    BulkInsertOperator,
    DatabaseToXComOperator
)


class TestDatabaseOperator:
    """
    Test DatabaseOperator functionality.
    """

    def test_initialization(self):
        """Test operator initialization."""
        op = DatabaseOperator(
            task_id='test_db_op',
            sql='SELECT 1',
            database_conn_id='test_conn'
        )

        assert op.task_id == 'test_db_op'
        assert op.sql == 'SELECT 1'
        assert op.database_conn_id == 'test_conn'

    def test_validate_sql_success(self):
        """Test SQL validation with valid query."""
        op = DatabaseOperator(
            task_id='test',
            sql='SELECT * FROM table'
        )

        # Should not raise exception
        op._validate_sql('SELECT * FROM table')

    def test_validate_sql_empty(self):
        """Test SQL validation with empty query."""
        op = DatabaseOperator(
            task_id='test',
            sql=''
        )

        with pytest.raises(ValueError, match="SQL query cannot be empty"):
            op._validate_sql('')

    def test_execute_single_query(self, mock_postgres_hook, airflow_context):
        """Test executing single query."""
        mock_postgres_hook.get_records.return_value = [(1,), (2,), (3,)]

        op = DatabaseOperator(
            task_id='test',
            sql='SELECT id FROM users'
        )

        result = op.execute(airflow_context)

        # Verify hook was called
        assert mock_postgres_hook.get_records.called

    def test_execute_multiple_queries(self, mock_postgres_hook, airflow_context):
        """Test executing multiple queries."""
        op = DatabaseOperator(
            task_id='test',
            sql=['SELECT 1', 'SELECT 2', 'SELECT 3']
        )

        result = op.execute(airflow_context)

        # Verify hook was called multiple times
        assert mock_postgres_hook.run.call_count >= 1


class TestBulkInsertOperator:
    """
    Test BulkInsertOperator functionality.
    """

    def test_initialization(self):
        """Test operator initialization."""
        df = pd.DataFrame({'col1': [1, 2, 3]})

        op = BulkInsertOperator(
            task_id='test_bulk',
            table='test_table',
            data=df
        )

        assert op.task_id == 'test_bulk'
        assert op.table == 'test_table'

    def test_execute_with_dataframe(self, mock_postgres_hook, airflow_context):
        """Test bulk insert with DataFrame."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [100, 200, 300]
        })

        # Mock engine
        mock_engine = pytest.mock.MagicMock()
        mock_postgres_hook.get_sqlalchemy_engine.return_value = mock_engine

        op = BulkInsertOperator(
            task_id='test',
            table='test_table',
            data=df
        )

        result = op.execute(airflow_context)

        assert result == 3  # 3 rows inserted

    def test_execute_with_list(self, mock_postgres_hook, airflow_context):
        """Test bulk insert with list of dicts."""
        data = [
            {'id': 1, 'value': 100},
            {'id': 2, 'value': 200}
        ]

        mock_engine = pytest.mock.MagicMock()
        mock_postgres_hook.get_sqlalchemy_engine.return_value = mock_engine

        op = BulkInsertOperator(
            task_id='test',
            table='test_table',
            data=data
        )

        result = op.execute(airflow_context)

        assert result == 2


class TestDatabaseToXComOperator:
    """
    Test DatabaseToXComOperator functionality.
    """

    def test_initialization(self):
        """Test operator initialization."""
        op = DatabaseToXComOperator(
            task_id='test_xcom',
            sql='SELECT * FROM table',
            xcom_key='my_data'
        )

        assert op.task_id == 'test_xcom'
        assert op.sql == 'SELECT * FROM table'
        assert op.xcom_key == 'my_data'

    def test_execute(self, mock_postgres_hook, airflow_context):
        """Test execute method."""
        # Mock data
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C']
        })
        mock_postgres_hook.get_pandas_df.return_value = df

        op = DatabaseToXComOperator(
            task_id='test',
            sql='SELECT * FROM table',
            xcom_key='test_data'
        )

        result = op.execute(airflow_context)

        # Verify XCom push was called
        assert airflow_context['task_instance'].xcom_push.called

        # Verify result
        assert len(result) == 3

    def test_max_rows_limit(self, mock_postgres_hook, airflow_context):
        """Test max rows limit."""
        # Create large DataFrame
        df = pd.DataFrame({
            'id': range(20000),
            'value': range(20000)
        })
        mock_postgres_hook.get_pandas_df.return_value = df

        op = DatabaseToXComOperator(
            task_id='test',
            sql='SELECT * FROM large_table',
            max_rows=10000
        )

        result = op.execute(airflow_context)

        # Should be limited to max_rows
        assert len(result) == 10000
