"""
Unit tests for ValidationOperator
"""

import pytest
import pandas as pd
from plugins.operators.validation_operator import (
    ValidationOperator,
    DataQualityCheckOperator
)


class TestValidationOperator:
    """
    Test ValidationOperator functionality.
    """

    def test_initialization(self):
        """Test operator initialization."""
        df = pd.DataFrame({'col1': [1, 2, 3]})

        op = ValidationOperator(
            task_id='test_validation',
            data=df,
            validation_rules={}
        )

        assert op.task_id == 'test_validation'
        assert isinstance(op.data, pd.DataFrame)

    def test_load_data_dataframe(self):
        """Test loading DataFrame data."""
        df = pd.DataFrame({'col1': [1, 2, 3]})

        op = ValidationOperator(
            task_id='test',
            data=df
        )

        loaded_df = op._load_data()

        assert isinstance(loaded_df, pd.DataFrame)
        assert len(loaded_df) == 3

    def test_load_data_list(self):
        """Test loading list data."""
        data = [
            {'id': 1, 'value': 100},
            {'id': 2, 'value': 200}
        ]

        op = ValidationOperator(
            task_id='test',
            data=data
        )

        loaded_df = op._load_data()

        assert isinstance(loaded_df, pd.DataFrame)
        assert len(loaded_df) == 2

    def test_validate_schema_success(self):
        """Test schema validation with valid schema."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'value': [100, 200, 300]
        })

        op = ValidationOperator(
            task_id='test',
            data=df
        )

        rules = {
            'required_columns': ['id', 'name', 'value']
        }

        result = op._validate_schema(df, rules)

        assert result['passed'] is True
        assert len(result['errors']) == 0

    def test_validate_schema_missing_columns(self):
        """Test schema validation with missing columns."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C']
        })

        op = ValidationOperator(
            task_id='test',
            data=df
        )

        rules = {
            'required_columns': ['id', 'name', 'value']  # 'value' missing
        }

        result = op._validate_schema(df, rules)

        assert result['passed'] is False
        assert len(result['errors']) > 0
        assert 'value' in result['errors'][0]

    def test_validate_nulls_pass(self):
        """Test null validation with passing data."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [100, 200, 300, 400, 500]
        })

        op = ValidationOperator(
            task_id='test',
            data=df
        )

        rules = {
            'columns': ['id', 'value'],
            'threshold': 0
        }

        result = op._validate_nulls(df, rules)

        assert result['passed'] is True

    def test_validate_nulls_fail(self):
        """Test null validation with failing data."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [100, None, 300, None, 500]  # 40% nulls
        })

        op = ValidationOperator(
            task_id='test',
            data=df
        )

        rules = {
            'columns': ['value'],
            'threshold': 10  # 10% threshold
        }

        result = op._validate_nulls(df, rules)

        assert result['passed'] is False
        assert result['details']['value']['null_percent'] == 40.0

    def test_validate_duplicates_none(self):
        """Test duplicate validation with no duplicates."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [100, 200, 300, 400, 500]
        })

        op = ValidationOperator(
            task_id='test',
            data=df
        )

        rules = {
            'columns': ['id'],
            'threshold': 0
        }

        result = op._validate_duplicates(df, rules)

        assert result['passed'] is True
        assert result['duplicate_count'] == 0

    def test_validate_duplicates_found(self):
        """Test duplicate validation with duplicates."""
        df = pd.DataFrame({
            'id': [1, 2, 2, 3, 3],  # Duplicates
            'value': [100, 200, 200, 300, 300]
        })

        op = ValidationOperator(
            task_id='test',
            data=df
        )

        rules = {
            'columns': ['id'],
            'threshold': 0
        }

        result = op._validate_duplicates(df, rules)

        assert result['passed'] is False
        assert result['duplicate_count'] > 0

    def test_validate_ranges_pass(self):
        """Test range validation with valid ranges."""
        df = pd.DataFrame({
            'age': [25, 30, 35, 40, 45],
            'score': [70, 80, 90, 85, 95]
        })

        op = ValidationOperator(
            task_id='test',
            data=df
        )

        rules = {
            'columns': {
                'age': {'min': 18, 'max': 65},
                'score': {'min': 0, 'max': 100}
            }
        }

        result = op._validate_ranges(df, rules)

        assert result['passed'] is True

    def test_validate_ranges_violations(self):
        """Test range validation with violations."""
        df = pd.DataFrame({
            'age': [15, 30, 35, 40, 70],  # 15 < 18, 70 > 65
            'score': [70, 80, 110, 85, -5]  # 110 > 100, -5 < 0
        })

        op = ValidationOperator(
            task_id='test',
            data=df
        )

        rules = {
            'columns': {
                'age': {'min': 18, 'max': 65},
                'score': {'min': 0, 'max': 100}
            }
        }

        result = op._validate_ranges(df, rules)

        assert result['passed'] is False
        assert len(result['errors']) > 0

    def test_execute_full_validation(self, airflow_context):
        """Test full validation execution."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['A', 'B', 'C', 'D', 'E'],
            'age': [25, 30, 35, 40, 45],
            'value': [100, 200, 300, 400, 500]
        })

        op = ValidationOperator(
            task_id='test',
            data=df,
            validation_rules={
                'schema': {
                    'required_columns': ['id', 'name', 'age', 'value']
                },
                'null_check': {
                    'columns': ['id', 'name'],
                    'threshold': 0
                },
                'duplicate_check': {
                    'columns': ['id'],
                    'threshold': 0
                },
                'range_check': {
                    'columns': {
                        'age': {'min': 18, 'max': 65}
                    }
                }
            },
            fail_on_error=False,
            write_report=False
        )

        report = op.execute(airflow_context)

        assert 'validations' in report
        assert report['passed'] is True


class TestDataQualityCheckOperator:
    """
    Test DataQualityCheckOperator functionality.
    """

    def test_initialization(self):
        """Test operator initialization."""
        df = pd.DataFrame({'col1': [1, 2, 3]})

        op = DataQualityCheckOperator(
            task_id='test_quality',
            data=df
        )

        assert op.task_id == 'test_quality'
        assert op.check_nulls is True
        assert op.check_duplicates is True

    def test_execute(self, airflow_context):
        """Test execute method."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 3, 5],  # Has duplicate
            'value': [100, None, 300, 400, 500]  # Has null
        })

        op = DataQualityCheckOperator(
            task_id='test',
            data=df,
            check_nulls=True,
            check_duplicates=True,
            check_types=True
        )

        report = op.execute(airflow_context)

        assert report['total_rows'] == 5
        assert report['total_columns'] == 2
        assert 'checks' in report
        assert 'nulls' in report['checks']
        assert 'duplicates' in report['checks']
        assert 'types' in report['checks']

    def test_null_detection(self, airflow_context):
        """Test null detection."""
        df = pd.DataFrame({
            'col1': [1, None, 3],
            'col2': ['a', 'b', None]
        })

        op = DataQualityCheckOperator(
            task_id='test',
            data=df,
            check_nulls=True
        )

        report = op.execute(airflow_context)

        nulls = report['checks']['nulls']
        assert 'col1' in nulls['columns_with_nulls']
        assert 'col2' in nulls['columns_with_nulls']

    def test_duplicate_detection(self, airflow_context):
        """Test duplicate detection."""
        df = pd.DataFrame({
            'id': [1, 2, 2, 3, 3, 3],  # 3 duplicates
            'value': [100, 200, 200, 300, 300, 300]
        })

        op = DataQualityCheckOperator(
            task_id='test',
            data=df,
            check_duplicates=True
        )

        report = op.execute(airflow_context)

        dups = report['checks']['duplicates']
        assert dups['count'] > 0
