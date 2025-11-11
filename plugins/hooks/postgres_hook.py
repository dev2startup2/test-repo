"""
Custom PostgreSQL Hook
======================

Enhanced PostgreSQL hook with additional features:
- Connection pooling
- Query optimization
- Batch operations
- Transaction management
- Error handling
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from airflow.providers.postgres.hooks.postgres import PostgresHook as BasePostgresHook
import pandas as pd
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine, pool


class EnhancedPostgresHook(BasePostgresHook):
    """
    Enhanced PostgreSQL hook with additional features.

    Features:
    - Connection pooling optimization
    - Batch insert with COPY
    - Query result streaming
    - Transaction management
    - Query performance monitoring
    """

    def __init__(
        self,
        postgres_conn_id: str = 'postgres_default',
        schema: Optional[str] = None,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_pre_ping: bool = True,
    ):
        super().__init__(postgres_conn_id=postgres_conn_id, schema=schema)
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_pre_ping = pool_pre_ping
        self.log = logging.getLogger(__name__)

    def get_sqlalchemy_engine_with_pool(self):
        """
        Get SQLAlchemy engine with connection pooling.

        :return: SQLAlchemy engine
        """
        conn = self.get_connection(self.postgres_conn_id)

        connection_string = (
            f"postgresql://{conn.login}:{conn.password}@"
            f"{conn.host}:{conn.port}/{conn.schema}"
        )

        engine = create_engine(
            connection_string,
            poolclass=pool.QueuePool,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_pre_ping=self.pool_pre_ping,
            echo=False
        )

        return engine

    def bulk_insert_dataframe(
        self,
        df: pd.DataFrame,
        table: str,
        if_exists: str = 'append',
        method: str = 'multi',
        chunksize: int = 1000
    ) -> int:
        """
        Bulk insert DataFrame with optimization.

        :param df: DataFrame to insert
        :param table: Target table name
        :param if_exists: What to do if table exists
        :param method: Insert method ('multi' or None)
        :param chunksize: Chunk size for bulk insert
        :return: Number of rows inserted
        """
        self.log.info(f"Bulk inserting {len(df)} rows into {table}")

        engine = self.get_sqlalchemy_engine_with_pool()

        try:
            df.to_sql(
                table,
                engine,
                if_exists=if_exists,
                index=False,
                method=method,
                chunksize=chunksize
            )

            self.log.info(f"Successfully inserted {len(df)} rows")
            return len(df)

        except Exception as e:
            self.log.error(f"Bulk insert failed: {str(e)}")
            raise

    def copy_from_dataframe(
        self,
        df: pd.DataFrame,
        table: str,
        columns: Optional[List[str]] = None
    ) -> int:
        """
        Use PostgreSQL COPY for fast bulk insert.

        Much faster than regular INSERT for large datasets.

        :param df: DataFrame to insert
        :param table: Target table name
        :param columns: List of columns (default: all)
        :return: Number of rows inserted
        """
        from io import StringIO

        self.log.info(f"Using COPY to insert {len(df)} rows into {table}")

        # Prepare CSV buffer
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # Get connection
        conn = self.get_conn()
        cursor = conn.cursor()

        try:
            # Use COPY command
            if columns is None:
                columns = df.columns.tolist()

            copy_sql = f"COPY {table} ({','.join(columns)}) FROM STDIN WITH CSV"

            cursor.copy_expert(copy_sql, buffer)
            conn.commit()

            self.log.info(f"COPY completed: {len(df)} rows")
            return len(df)

        except Exception as e:
            conn.rollback()
            self.log.error(f"COPY failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    def execute_with_return(
        self,
        sql: str,
        parameters: Optional[Union[Dict, Tuple]] = None
    ) -> List[Tuple]:
        """
        Execute query and return results.

        :param sql: SQL query
        :param parameters: Query parameters
        :return: Query results
        """
        conn = self.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(sql, parameters)
            results = cursor.fetchall()
            conn.commit()
            return results
        except Exception as e:
            conn.rollback()
            self.log.error(f"Query execution failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    @contextmanager
    def transaction(self):
        """
        Transaction context manager.

        Usage:
            with hook.transaction():
                hook.run(sql1)
                hook.run(sql2)
        """
        conn = self.get_conn()

        try:
            yield conn
            conn.commit()
            self.log.info("Transaction committed")
        except Exception as e:
            conn.rollback()
            self.log.error(f"Transaction rolled back: {str(e)}")
            raise
        finally:
            conn.close()

    def upsert_dataframe(
        self,
        df: pd.DataFrame,
        table: str,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None
    ) -> int:
        """
        Upsert DataFrame using INSERT ... ON CONFLICT.

        :param df: DataFrame to upsert
        :param table: Target table
        :param conflict_columns: Columns for conflict detection
        :param update_columns: Columns to update (default: all except conflict)
        :return: Number of rows affected
        """
        self.log.info(f"Upserting {len(df)} rows into {table}")

        if update_columns is None:
            update_columns = [col for col in df.columns if col not in conflict_columns]

        conn = self.get_conn()
        cursor = conn.cursor()

        try:
            # Build SQL
            all_columns = df.columns.tolist()
            placeholders = ', '.join(['%s'] * len(all_columns))
            conflict_cols = ', '.join(conflict_columns)
            update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])

            sql = f"""
                INSERT INTO {table} ({', '.join(all_columns)})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_cols})
                DO UPDATE SET {update_set}
            """

            # Execute for each row
            for _, row in df.iterrows():
                values = tuple(row[col] for col in all_columns)
                cursor.execute(sql, values)

            conn.commit()
            self.log.info(f"Upserted {len(df)} rows")
            return len(df)

        except Exception as e:
            conn.rollback()
            self.log.error(f"Upsert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    def get_table_stats(self, table: str) -> Dict[str, Any]:
        """
        Get table statistics.

        :param table: Table name
        :return: Dictionary with stats
        """
        stats_sql = f"""
            SELECT
                COUNT(*) as row_count,
                pg_size_pretty(pg_total_relation_size('{table}')) as total_size,
                pg_size_pretty(pg_table_size('{table}')) as table_size,
                pg_size_pretty(pg_indexes_size('{table}')) as index_size
        """

        result = self.get_first(stats_sql)

        return {
            'row_count': result[0],
            'total_size': result[1],
            'table_size': result[2],
            'index_size': result[3]
        }

    def vacuum_table(self, table: str, analyze: bool = True) -> None:
        """
        Vacuum table to reclaim space and update statistics.

        :param table: Table name
        :param analyze: Run ANALYZE after VACUUM
        """
        self.log.info(f"Vacuuming table: {table}")

        conn = self.get_conn()
        old_isolation_level = conn.isolation_level
        conn.set_isolation_level(0)  # AUTOCOMMIT mode for VACUUM

        cursor = conn.cursor()

        try:
            if analyze:
                cursor.execute(f"VACUUM ANALYZE {table}")
            else:
                cursor.execute(f"VACUUM {table}")

            self.log.info(f"Vacuum completed for {table}")

        finally:
            conn.set_isolation_level(old_isolation_level)
            cursor.close()
            conn.close()

    def create_index(
        self,
        table: str,
        columns: List[str],
        index_name: Optional[str] = None,
        unique: bool = False,
        if_not_exists: bool = True
    ) -> None:
        """
        Create index on table.

        :param table: Table name
        :param columns: Columns to index
        :param index_name: Index name (auto-generated if None)
        :param unique: Create unique index
        :param if_not_exists: Add IF NOT EXISTS clause
        """
        if index_name is None:
            index_name = f"idx_{table}_{'_'.join(columns)}"

        unique_clause = "UNIQUE" if unique else ""
        if_not_exists_clause = "IF NOT EXISTS" if if_not_exists else ""

        sql = f"""
            CREATE {unique_clause} INDEX {if_not_exists_clause} {index_name}
            ON {table} ({', '.join(columns)})
        """

        self.log.info(f"Creating index: {index_name}")
        self.run(sql)
        self.log.info(f"Index created: {index_name}")

    def get_slow_queries(self, limit: int = 10) -> List[Dict]:
        """
        Get slow queries from pg_stat_statements.

        Requires pg_stat_statements extension.

        :param limit: Number of queries to return
        :return: List of slow queries
        """
        sql = f"""
            SELECT
                query,
                calls,
                total_exec_time,
                mean_exec_time,
                max_exec_time
            FROM pg_stat_statements
            ORDER BY mean_exec_time DESC
            LIMIT {limit}
        """

        try:
            df = self.get_pandas_df(sql)
            return df.to_dict('records')
        except Exception as e:
            self.log.warning(f"Could not get slow queries: {str(e)}")
            return []

    def check_connection_health(self) -> bool:
        """
        Check if database connection is healthy.

        :return: True if healthy
        """
        try:
            result = self.get_first("SELECT 1")
            return result[0] == 1
        except Exception as e:
            self.log.error(f"Connection health check failed: {str(e)}")
            return False
