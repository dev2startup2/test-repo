"""
Database ETL Pipeline
=====================

Bu DAG database'dan ma'lumotlarni olish va boshqa database'ga yuklashni ko'rsatadi:
1. PostgreSQL'dan ma'lumot o'qish
2. Ma'lumotlarni transform qilish
3. Target database'ga yuklash

Schedule: Har 6 soatda
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import pandas as pd

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'database_etl_pipeline',
    default_args=default_args,
    description='PostgreSQL dan ma\'lumot olish va qayta ishlash',
    schedule_interval='0 */6 * * *',  # Har 6 soatda
    catchup=False,
    tags=['etl', 'database', 'postgresql'],
)


def extract_from_postgres(**context):
    """
    PostgreSQL database'dan ma'lumot olish
    """
    logging.info("PostgreSQL'dan ma'lumot olish boshlandi...")

    # PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_source')

    # SQL query
    sql = """
        SELECT
            user_id,
            username,
            email,
            created_at,
            last_login,
            is_active
        FROM users
        WHERE created_at >= NOW() - INTERVAL '1 day'
        ORDER BY created_at DESC
    """

    # Ma'lumotlarni olish
    df = pg_hook.get_pandas_df(sql)

    logging.info(f"Jami {len(df)} ta record olindi")

    # DataFrame ni dict ga aylantirish
    data = df.to_dict('records')

    # XCom ga yuborish
    context['task_instance'].xcom_push(key='extracted_data', value=data)

    return f"Extracted {len(data)} records"


def transform_user_data(**context):
    """
    User ma'lumotlarini qayta ishlash
    """
    logging.info("Ma'lumotlarni transform qilish...")

    ti = context['task_instance']
    data = ti.xcom_pull(key='extracted_data', task_ids='extract_from_postgres')

    # Pandas DataFrame ga aylantirish
    df = pd.DataFrame(data)

    # Transform operations
    # 1. Email domain'ni ajratish
    df['email_domain'] = df['email'].str.split('@').str[1]

    # 2. Active users filter
    active_users = df[df['is_active'] == True].copy()

    # 3. Last login bo'yicha kategoriya
    now = pd.Timestamp.now()
    active_users['last_login'] = pd.to_datetime(active_users['last_login'])
    active_users['days_since_login'] = (now - active_users['last_login']).dt.days

    def user_engagement(days):
        if days <= 7:
            return 'high'
        elif days <= 30:
            return 'medium'
        else:
            return 'low'

    active_users['engagement'] = active_users['days_since_login'].apply(user_engagement)

    # 4. Aggregation - email domain bo'yicha
    domain_stats = df.groupby('email_domain').agg({
        'user_id': 'count',
        'is_active': 'sum'
    }).reset_index()
    domain_stats.columns = ['domain', 'total_users', 'active_users']

    # Natijalarni XCom ga yuborish
    ti.xcom_push(key='transformed_users', value=active_users.to_dict('records'))
    ti.xcom_push(key='domain_stats', value=domain_stats.to_dict('records'))

    logging.info(f"Transform qilindi: {len(active_users)} active users")

    return "Transform completed"


def load_to_warehouse(**context):
    """
    Ma'lumotlarni data warehouse'ga yuklash
    """
    logging.info("Data warehouse'ga yuklash...")

    ti = context['task_instance']
    users_data = ti.xcom_pull(key='transformed_users', task_ids='transform_user_data')
    stats_data = ti.xcom_pull(key='domain_stats', task_ids='transform_user_data')

    # Target database connection
    target_hook = PostgresHook(postgres_conn_id='postgres_warehouse')

    # Users data'ni yuklash
    users_df = pd.DataFrame(users_data)

    # Bulk insert using pandas
    # Real projectda COPY yoki bulk insert ishlatiladi
    insert_sql = """
        INSERT INTO dwh.user_activity
        (user_id, username, email, email_domain, engagement, last_login, loaded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id)
        DO UPDATE SET
            engagement = EXCLUDED.engagement,
            last_login = EXCLUDED.last_login,
            loaded_at = EXCLUDED.loaded_at
    """

    loaded_count = 0
    for _, row in users_df.iterrows():
        target_hook.run(
            insert_sql,
            parameters=(
                row['user_id'],
                row['username'],
                row['email'],
                row['email_domain'],
                row['engagement'],
                row['last_login'],
                datetime.now()
            )
        )
        loaded_count += 1

    logging.info(f"Users loaded: {loaded_count}")

    # Stats data'ni yuklash
    stats_df = pd.DataFrame(stats_data)
    stats_loaded = 0

    for _, row in stats_df.iterrows():
        target_hook.run(
            """
            INSERT INTO dwh.domain_statistics
            (domain, total_users, active_users, report_date)
            VALUES (%s, %s, %s, %s)
            """,
            parameters=(
                row['domain'],
                row['total_users'],
                row['active_users'],
                datetime.now().date()
            )
        )
        stats_loaded += 1

    logging.info(f"Stats loaded: {stats_loaded}")

    return f"Loaded {loaded_count} users, {stats_loaded} domain stats"


def data_quality_check(**context):
    """
    Ma'lumotlar sifatini tekshirish
    """
    logging.info("Data quality check...")

    ti = context['task_instance']
    users_data = ti.xcom_pull(key='transformed_users', task_ids='transform_user_data')

    df = pd.DataFrame(users_data)

    # Quality checks
    checks = []

    # 1. Null check
    null_emails = df['email'].isnull().sum()
    if null_emails > 0:
        checks.append(f"WARNING: {null_emails} users with null email")

    # 2. Email format check
    invalid_emails = df[~df['email'].str.contains('@', na=False)]
    if len(invalid_emails) > 0:
        checks.append(f"WARNING: {len(invalid_emails)} invalid email formats")

    # 3. Duplicate check
    duplicates = df.duplicated(subset=['user_id']).sum()
    if duplicates > 0:
        checks.append(f"ERROR: {duplicates} duplicate user_ids found")
        raise ValueError(f"Data quality failed: duplicate user_ids")

    # 4. Data freshness
    max_login = pd.to_datetime(df['last_login']).max()
    days_old = (datetime.now() - max_login).days
    if days_old > 30:
        checks.append(f"WARNING: Most recent login is {days_old} days old")

    if checks:
        for check in checks:
            logging.warning(check)
    else:
        logging.info("✓ All quality checks passed")

    return "Quality check completed"


# DAG tasks
with dag:

    # Create target tables (idempotent)
    create_tables = PostgresOperator(
        task_id='create_target_tables',
        postgres_conn_id='postgres_warehouse',
        sql="""
        CREATE TABLE IF NOT EXISTS dwh.user_activity (
            user_id INTEGER PRIMARY KEY,
            username VARCHAR(255),
            email VARCHAR(255),
            email_domain VARCHAR(255),
            engagement VARCHAR(50),
            last_login TIMESTAMP,
            loaded_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS dwh.domain_statistics (
            id SERIAL PRIMARY KEY,
            domain VARCHAR(255),
            total_users INTEGER,
            active_users INTEGER,
            report_date DATE,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_domain_report
        ON dwh.domain_statistics(domain, report_date);
        """
    )

    # Extract task
    extract = PythonOperator(
        task_id='extract_from_postgres',
        python_callable=extract_from_postgres,
    )

    # Transform task
    transform = PythonOperator(
        task_id='transform_user_data',
        python_callable=transform_user_data,
    )

    # Quality check
    quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
    )

    # Load task
    load = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_to_warehouse,
    )

    # Archive old data
    archive = PostgresOperator(
        task_id='archive_old_data',
        postgres_conn_id='postgres_warehouse',
        sql="""
        DELETE FROM dwh.domain_statistics
        WHERE report_date < NOW() - INTERVAL '90 days';
        """
    )

    # Task dependencies
    create_tables >> extract >> transform >> quality_check >> load >> archive
