"""
Data Validation Operator
=========================

Comprehensive data validation operator:
- Schema validation
- Data quality checks
- Business rule validation
- Great Expectations integration
- Custom validators
"""

from typing import Any, Dict, List, Optional, Callable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import logging
from datetime import datetime


class ValidationOperator(BaseOperator):
    """
    Data validation operator with comprehensive checks.

    Validates:
    - Schema (columns, data types)
    - Data quality (nulls, duplicates, ranges)
    - Business rules (custom logic)
    - Statistical properties

    :param data: Data to validate (DataFrame or path to file)
    :param validation_rules: Dictionary of validation rules
    :param fail_on_error: Raise exception on validation failure
    :param write_report: Write validation report to file
    """

    template_fields = ('data',)
    ui_color = '#fff1f0'

    @apply_defaults
    def __init__(
        self,
        data: Any,
        validation_rules: Optional[Dict[str, Any]] = None,
        fail_on_error: bool = True,
        write_report: bool = True,
        report_path: str = '/home/user/test-repo/data/output/validation_report.json',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.data = data
        self.validation_rules = validation_rules or {}
        self.fail_on_error = fail_on_error
        self.write_report = write_report
        self.report_path = report_path

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute validation checks.

        :param context: Airflow context
        :return: Validation report
        """
        self.log.info("Starting data validation...")

        # Load data
        df = self._load_data()

        # Run validations
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'validations': {},
            'passed': True,
            'errors': []
        }

        # Schema validation
        if 'schema' in self.validation_rules:
            schema_result = self._validate_schema(df, self.validation_rules['schema'])
            report['validations']['schema'] = schema_result
            if not schema_result['passed']:
                report['passed'] = False
                report['errors'].extend(schema_result['errors'])

        # Null check
        if 'null_check' in self.validation_rules:
            null_result = self._validate_nulls(df, self.validation_rules['null_check'])
            report['validations']['null_check'] = null_result
            if not null_result['passed']:
                report['passed'] = False
                report['errors'].extend(null_result['errors'])

        # Duplicate check
        if 'duplicate_check' in self.validation_rules:
            dup_result = self._validate_duplicates(df, self.validation_rules['duplicate_check'])
            report['validations']['duplicate_check'] = dup_result
            if not dup_result['passed']:
                report['passed'] = False
                report['errors'].extend(dup_result['errors'])

        # Range check
        if 'range_check' in self.validation_rules:
            range_result = self._validate_ranges(df, self.validation_rules['range_check'])
            report['validations']['range_check'] = range_result
            if not range_result['passed']:
                report['passed'] = False
                report['errors'].extend(range_result['errors'])

        # Custom validators
        if 'custom' in self.validation_rules:
            custom_result = self._validate_custom(df, self.validation_rules['custom'])
            report['validations']['custom'] = custom_result
            if not custom_result['passed']:
                report['passed'] = False
                report['errors'].extend(custom_result['errors'])

        # Write report
        if self.write_report:
            self._write_report(report)

        # Log summary
        self._log_summary(report)

        # Fail if needed
        if not report['passed'] and self.fail_on_error:
            raise ValueError(f"Validation failed: {len(report['errors'])} errors found")

        return report

    def _load_data(self) -> pd.DataFrame:
        """
        Load data to validate.

        :return: DataFrame
        """
        if isinstance(self.data, pd.DataFrame):
            return self.data
        elif isinstance(self.data, str):
            # Assume file path
            if self.data.endswith('.csv'):
                return pd.read_csv(self.data)
            elif self.data.endswith('.json'):
                return pd.read_json(self.data)
            elif self.data.endswith('.parquet'):
                return pd.read_parquet(self.data)
            else:
                raise ValueError(f"Unsupported file format: {self.data}")
        elif isinstance(self.data, list):
            return pd.DataFrame(self.data)
        else:
            raise ValueError(f"Unsupported data type: {type(self.data)}")

    def _validate_schema(self, df: pd.DataFrame, rules: Dict) -> Dict:
        """
        Validate schema (columns and data types).

        :param df: DataFrame
        :param rules: Schema rules
        :return: Validation result
        """
        result = {'passed': True, 'errors': []}

        # Check required columns
        if 'required_columns' in rules:
            required = set(rules['required_columns'])
            actual = set(df.columns)
            missing = required - actual

            if missing:
                result['passed'] = False
                result['errors'].append(f"Missing required columns: {missing}")

        # Check data types
        if 'dtypes' in rules:
            for col, expected_dtype in rules['dtypes'].items():
                if col in df.columns:
                    actual_dtype = str(df[col].dtype)
                    if actual_dtype != expected_dtype:
                        result['passed'] = False
                        result['errors'].append(
                            f"Column {col}: expected type {expected_dtype}, "
                            f"got {actual_dtype}"
                        )

        return result

    def _validate_nulls(self, df: pd.DataFrame, rules: Dict) -> Dict:
        """
        Validate null values.

        :param df: DataFrame
        :param rules: Null check rules
        :return: Validation result
        """
        result = {'passed': True, 'errors': [], 'details': {}}

        # Check specific columns
        if 'columns' in rules:
            for col in rules['columns']:
                if col in df.columns:
                    null_count = df[col].isnull().sum()
                    null_percent = (null_count / len(df)) * 100

                    result['details'][col] = {
                        'null_count': int(null_count),
                        'null_percent': round(null_percent, 2)
                    }

                    # Check threshold
                    threshold = rules.get('threshold', 0)
                    if null_percent > threshold:
                        result['passed'] = False
                        result['errors'].append(
                            f"Column {col}: {null_percent:.2f}% nulls "
                            f"(threshold: {threshold}%)"
                        )

        return result

    def _validate_duplicates(self, df: pd.DataFrame, rules: Dict) -> Dict:
        """
        Validate duplicate records.

        :param df: DataFrame
        :param rules: Duplicate check rules
        :return: Validation result
        """
        result = {'passed': True, 'errors': []}

        # Check for duplicates based on subset of columns
        if 'columns' in rules:
            duplicates = df.duplicated(subset=rules['columns'], keep=False)
            dup_count = duplicates.sum()
            dup_percent = (dup_count / len(df)) * 100

            result['duplicate_count'] = int(dup_count)
            result['duplicate_percent'] = round(dup_percent, 2)

            # Check threshold
            threshold = rules.get('threshold', 0)
            if dup_percent > threshold:
                result['passed'] = False
                result['errors'].append(
                    f"Found {dup_count} duplicates ({dup_percent:.2f}%) "
                    f"(threshold: {threshold}%)"
                )

        return result

    def _validate_ranges(self, df: pd.DataFrame, rules: Dict) -> Dict:
        """
        Validate value ranges.

        :param df: DataFrame
        :param rules: Range check rules
        :return: Validation result
        """
        result = {'passed': True, 'errors': [], 'details': {}}

        # Check ranges for each column
        if 'columns' in rules:
            for col, range_spec in rules['columns'].items():
                if col in df.columns:
                    violations = 0

                    # Min value check
                    if 'min' in range_spec:
                        below_min = (df[col] < range_spec['min']).sum()
                        if below_min > 0:
                            violations += below_min
                            result['errors'].append(
                                f"Column {col}: {below_min} values below minimum "
                                f"({range_spec['min']})"
                            )

                    # Max value check
                    if 'max' in range_spec:
                        above_max = (df[col] > range_spec['max']).sum()
                        if above_max > 0:
                            violations += above_max
                            result['errors'].append(
                                f"Column {col}: {above_max} values above maximum "
                                f"({range_spec['max']})"
                            )

                    result['details'][col] = {'violations': violations}

                    if violations > 0:
                        result['passed'] = False

        return result

    def _validate_custom(self, df: pd.DataFrame, rules: List[Callable]) -> Dict:
        """
        Run custom validation functions.

        :param df: DataFrame
        :param rules: List of validation functions
        :return: Validation result
        """
        result = {'passed': True, 'errors': []}

        for i, validator_func in enumerate(rules):
            try:
                is_valid, message = validator_func(df)
                if not is_valid:
                    result['passed'] = False
                    result['errors'].append(f"Custom validator {i+1}: {message}")
            except Exception as e:
                result['passed'] = False
                result['errors'].append(f"Custom validator {i+1} failed: {str(e)}")

        return result

    def _write_report(self, report: Dict) -> None:
        """
        Write validation report to file.

        :param report: Validation report
        """
        import json

        with open(self.report_path, 'w') as f:
            json.dump(report, f, indent=2)

        self.log.info(f"Validation report written to: {self.report_path}")

    def _log_summary(self, report: Dict) -> None:
        """
        Log validation summary.

        :param report: Validation report
        """
        self.log.info("=" * 50)
        self.log.info("VALIDATION SUMMARY")
        self.log.info("=" * 50)
        self.log.info(f"Status: {'PASSED ✓' if report['passed'] else 'FAILED ✗'}")
        self.log.info(f"Total Records: {report['total_records']}")
        self.log.info(f"Total Errors: {len(report['errors'])}")

        if report['errors']:
            self.log.info("\nErrors:")
            for error in report['errors']:
                self.log.error(f"  - {error}")

        self.log.info("=" * 50)


class GreatExpectationsOperator(BaseOperator):
    """
    Great Expectations integration operator.

    Uses Great Expectations library for advanced data validation.

    :param data: Data to validate
    :param expectation_suite_name: Great Expectations suite name
    :param data_context_root_dir: Root directory for GE context
    """

    template_fields = ('data',)
    ui_color = '#f0f5ff'

    @apply_defaults
    def __init__(
        self,
        data: Any,
        expectation_suite_name: str,
        data_context_root_dir: Optional[str] = None,
        fail_on_error: bool = True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.data = data
        self.expectation_suite_name = expectation_suite_name
        self.data_context_root_dir = data_context_root_dir
        self.fail_on_error = fail_on_error

    def execute(self, context: Dict[str, Any]) -> Dict:
        """
        Execute Great Expectations validation.

        :param context: Airflow context
        :return: Validation results
        """
        try:
            from great_expectations.data_context import DataContext
            from great_expectations.dataset import PandasDataset
        except ImportError:
            raise ImportError(
                "Great Expectations not installed. "
                "Install with: pip install great-expectations"
            )

        self.log.info("Running Great Expectations validation...")

        # Load data
        if isinstance(self.data, pd.DataFrame):
            df = self.data
        else:
            df = pd.DataFrame(self.data)

        # Get data context
        if self.data_context_root_dir:
            data_context = DataContext(self.data_context_root_dir)
        else:
            data_context = DataContext()

        # Create GE dataset
        ge_df = PandasDataset(df)

        # Get expectation suite
        suite = data_context.get_expectation_suite(self.expectation_suite_name)

        # Validate
        results = ge_df.validate(expectation_suite=suite)

        # Check results
        if not results.success and self.fail_on_error:
            self.log.error(f"Validation failed: {results.statistics}")
            raise ValueError("Great Expectations validation failed")

        self.log.info(f"Validation passed: {results.statistics}")

        return results.to_json_dict()


class DataQualityCheckOperator(BaseOperator):
    """
    Quick data quality check operator.

    Performs common quality checks without complex configuration.

    :param data: Data to check
    :param check_nulls: Check for null values
    :param check_duplicates: Check for duplicates
    :param check_types: Check data types
    """

    template_fields = ('data',)
    ui_color = '#fffbe6'

    @apply_defaults
    def __init__(
        self,
        data: Any,
        check_nulls: bool = True,
        check_duplicates: bool = True,
        check_types: bool = True,
        fail_on_error: bool = False,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.data = data
        self.check_nulls = check_nulls
        self.check_duplicates = check_duplicates
        self.check_types = check_types
        self.fail_on_error = fail_on_error

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute quality checks.

        :param context: Airflow context
        :return: Check results
        """
        if isinstance(self.data, pd.DataFrame):
            df = self.data
        else:
            df = pd.DataFrame(self.data)

        report = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'checks': {}
        }

        # Null check
        if self.check_nulls:
            null_counts = df.isnull().sum()
            null_percent = (null_counts / len(df) * 100).round(2)
            report['checks']['nulls'] = {
                'columns_with_nulls': null_counts[null_counts > 0].to_dict(),
                'null_percentages': null_percent[null_counts > 0].to_dict()
            }

        # Duplicate check
        if self.check_duplicates:
            dup_count = df.duplicated().sum()
            report['checks']['duplicates'] = {
                'count': int(dup_count),
                'percent': round((dup_count / len(df) * 100), 2)
            }

        # Type check
        if self.check_types:
            report['checks']['types'] = {
                col: str(dtype) for col, dtype in df.dtypes.items()
            }

        # Log report
        self.log.info("Data Quality Report:")
        self.log.info(f"  Total Rows: {report['total_rows']}")
        self.log.info(f"  Total Columns: {report['total_columns']}")

        if self.check_nulls:
            null_cols = len(report['checks']['nulls']['columns_with_nulls'])
            if null_cols > 0:
                self.log.warning(f"  Columns with nulls: {null_cols}")

        if self.check_duplicates:
            dup_count = report['checks']['duplicates']['count']
            if dup_count > 0:
                self.log.warning(f"  Duplicate rows: {dup_count}")

        return report
