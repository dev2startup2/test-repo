"""
Custom Database Operator
=========================

Enhanced database operator with additional features:
- Batch processing
- Transaction management
- Error handling with retry
- Data validation
- Performance optimization
"""

from typing import Any, Dict, List, Optional, Union
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
import logging
import pandas as pd
from datetime import datetime


class DatabaseOperator(BaseOperator):
    """
    Custom database operator for enhanced ETL operations.

    Supports:
    - Multiple database types (PostgreSQL, MySQL, etc.)
    - Batch processing
    - Transaction management
    - Data validation
    - Performance optimization

    :param sql: SQL query to execute
    :param database_conn_id: Airflow connection ID
    :param parameters: Query parameters
    :param batch_size: Batch size for processing
    :param autocommit: Auto-commit transactions
    :param return_last: Return last query result only
    :param split_statements: Split SQL into separate statements
    """

    template_fields = ('sql', 'parameters')
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        sql: Union[str, List[str]],
        database_conn_id: str = 'postgres_default',
        parameters: Optional[Union[Dict, List]] = None,
        batch_size: int = 1000,
        autocommit: bool = False,
        return_last: bool = True,
        split_statements: bool = False,
        validate_query: bool = True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.database_conn_id = database_conn_id
        self.parameters = parameters
        self.batch_size = batch_size
        self.autocommit = autocommit
        self.return_last = return_last
        self.split_statements = split_statements
        self.validate_query = validate_query

    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Execute database operation.

        :param context: Airflow context
        :return: Query results or row count
        """
        self.log.info(f"Executing DatabaseOperator with conn_id: {self.database_conn_id}")

        # Get database hook
        hook = self._get_hook()

        # Validate query if enabled
        if self.validate_query:
            self._validate_sql(self.sql)

        # Execute query
        start_time = datetime.now()

        try:
            result = self._execute_query(hook, context)

            execution_time = (datetime.now() - start_time).total_seconds()
            self.log.info(f"Query executed successfully in {execution_time:.2f}s")

            return result

        except Exception as e:
            self.log.error(f"Query execution failed: {str(e)}")
            raise

    def _get_hook(self) -> PostgresHook:
        """
        Get database hook.

        :return: Database hook instance
        """
        return PostgresHook(postgres_conn_id=self.database_conn_id)

    def _validate_sql(self, sql: Union[str, List[str]]) -> None:
        """
        Validate SQL query.

        :param sql: SQL query to validate
        :raises ValueError: If query is invalid
        """
        if not sql:
            raise ValueError("SQL query cannot be empty")

        if isinstance(sql, list):
            for query in sql:
                if not query.strip():
                    raise ValueError("SQL query cannot be empty")

        # Add more validation as needed
        self.log.info("SQL validation passed")

    def _execute_query(
        self,
        hook: PostgresHook,
        context: Dict[str, Any]
    ) -> Any:
        """
        Execute SQL query.

        :param hook: Database hook
        :param context: Airflow context
        :return: Query results
        """
        if isinstance(self.sql, str):
            return self._execute_single_query(hook, self.sql, self.parameters)
        else:
            return self._execute_multiple_queries(hook, self.sql, self.parameters)

    def _execute_single_query(
        self,
        hook: PostgresHook,
        sql: str,
        parameters: Optional[Union[Dict, List]] = None
    ) -> Any:
        """
        Execute single SQL query.

        :param hook: Database hook
        :param sql: SQL query
        :param parameters: Query parameters
        :return: Query result
        """
        self.log.info(f"Executing query: {sql[:100]}...")

        # Check if SELECT query
        is_select = sql.strip().upper().startswith('SELECT')

        if is_select:
            # Fetch results
            records = hook.get_records(sql, parameters=parameters)
            self.log.info(f"Query returned {len(records)} rows")
            return records
        else:
            # Execute non-SELECT query
            hook.run(sql, autocommit=self.autocommit, parameters=parameters)
            self.log.info("Query executed successfully")
            return None

    def _execute_multiple_queries(
        self,
        hook: PostgresHook,
        sql_list: List[str],
        parameters: Optional[List] = None
    ) -> Any:
        """
        Execute multiple SQL queries.

        :param hook: Database hook
        :param sql_list: List of SQL queries
        :param parameters: List of query parameters
        :return: Last query result or list of results
        """
        results = []

        for idx, sql in enumerate(sql_list):
            params = parameters[idx] if parameters and idx < len(parameters) else None
            result = self._execute_single_query(hook, sql, params)
            results.append(result)

        if self.return_last:
            return results[-1] if results else None
        else:
            return results


class BulkInsertOperator(BaseOperator):
    """
    Bulk insert operator for efficient data loading.

    Uses PostgreSQL COPY or bulk insert for optimal performance.

    :param table: Target table name
    :param data: Data to insert (DataFrame or list of dicts)
    :param database_conn_id: Airflow connection ID
    :param if_exists: What to do if table exists ('fail', 'replace', 'append')
    :param batch_size: Batch size for bulk insert
    """

    template_fields = ('table',)
    ui_color = '#e8f7e8'

    @apply_defaults
    def __init__(
        self,
        table: str,
        data: Union[pd.DataFrame, List[Dict]],
        database_conn_id: str = 'postgres_default',
        if_exists: str = 'append',
        batch_size: int = 10000,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table = table
        self.data = data
        self.database_conn_id = database_conn_id
        self.if_exists = if_exists
        self.batch_size = batch_size

    def execute(self, context: Dict[str, Any]) -> int:
        """
        Execute bulk insert.

        :param context: Airflow context
        :return: Number of rows inserted
        """
        self.log.info(f"Bulk inserting data into table: {self.table}")

        # Convert to DataFrame if needed
        if isinstance(self.data, list):
            df = pd.DataFrame(self.data)
        else:
            df = self.data

        # Get database connection
        hook = PostgresHook(postgres_conn_id=self.database_conn_id)
        engine = hook.get_sqlalchemy_engine()

        # Bulk insert
        start_time = datetime.now()

        df.to_sql(
            self.table,
            engine,
            if_exists=self.if_exists,
            index=False,
            method='multi',
            chunksize=self.batch_size
        )

        execution_time = (datetime.now() - start_time).total_seconds()
        row_count = len(df)

        self.log.info(
            f"Bulk insert completed: {row_count} rows in {execution_time:.2f}s "
            f"({row_count/execution_time:.0f} rows/sec)"
        )

        return row_count


class DatabaseToXComOperator(BaseOperator):
    """
    Fetch data from database and push to XCom.

    Useful for small datasets that need to be passed to downstream tasks.

    :param sql: SQL query to execute
    :param database_conn_id: Airflow connection ID
    :param xcom_key: XCom key to use
    :param max_rows: Maximum rows to fetch (safety limit)
    """

    template_fields = ('sql',)
    ui_color = '#fff7e6'

    @apply_defaults
    def __init__(
        self,
        sql: str,
        database_conn_id: str = 'postgres_default',
        xcom_key: str = 'query_result',
        max_rows: int = 10000,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.database_conn_id = database_conn_id
        self.xcom_key = xcom_key
        self.max_rows = max_rows

    def execute(self, context: Dict[str, Any]) -> List[Dict]:
        """
        Execute query and push to XCom.

        :param context: Airflow context
        :return: Query results
        """
        self.log.info(f"Fetching data: {self.sql[:100]}...")

        hook = PostgresHook(postgres_conn_id=self.database_conn_id)

        # Fetch as DataFrame for easier handling
        df = hook.get_pandas_df(self.sql)

        if len(df) > self.max_rows:
            self.log.warning(
                f"Query returned {len(df)} rows, "
                f"limiting to {self.max_rows} rows"
            )
            df = df.head(self.max_rows)

        # Convert to dict for XCom
        result = df.to_dict('records')

        # Push to XCom
        context['task_instance'].xcom_push(key=self.xcom_key, value=result)

        self.log.info(f"Pushed {len(result)} rows to XCom with key: {self.xcom_key}")

        return result


class DatabaseUpsertOperator(BaseOperator):
    """
    Upsert operator for INSERT ... ON CONFLICT updates.

    Supports idempotent data loading.

    :param table: Target table name
    :param data: Data to upsert
    :param conflict_columns: Columns to check for conflicts
    :param update_columns: Columns to update on conflict
    :param database_conn_id: Airflow connection ID
    """

    template_fields = ('table',)
    ui_color = '#e6f7ff'

    @apply_defaults
    def __init__(
        self,
        table: str,
        data: Union[pd.DataFrame, List[Dict]],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        database_conn_id: str = 'postgres_default',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table = table
        self.data = data
        self.conflict_columns = conflict_columns
        self.update_columns = update_columns
        self.database_conn_id = database_conn_id

    def execute(self, context: Dict[str, Any]) -> int:
        """
        Execute upsert operation.

        :param context: Airflow context
        :return: Number of rows affected
        """
        self.log.info(f"Upserting data into table: {self.table}")

        # Convert to DataFrame if needed
        if isinstance(self.data, list):
            df = pd.DataFrame(self.data)
        else:
            df = self.data

        hook = PostgresHook(postgres_conn_id=self.database_conn_id)

        # Build upsert SQL
        columns = df.columns.tolist()

        if self.update_columns is None:
            # Update all columns except conflict columns
            update_columns = [col for col in columns if col not in self.conflict_columns]
        else:
            update_columns = self.update_columns

        # Generate SQL
        placeholders = ', '.join(['%s'] * len(columns))
        conflict_cols = ', '.join(self.conflict_columns)
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])

        sql = f"""
            INSERT INTO {self.table} ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols})
            DO UPDATE SET {update_set}
        """

        # Execute upserts
        row_count = 0
        for _, row in df.iterrows():
            values = tuple(row[col] for col in columns)
            hook.run(sql, parameters=values, autocommit=True)
            row_count += 1

        self.log.info(f"Upserted {row_count} rows")

        return row_count
