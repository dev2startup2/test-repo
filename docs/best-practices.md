# Best Practices

## DAG Design Principles

### 1. Idempotency

**Har bir task bir xil natija berishi kerak:**

```python
# ❌ Bad: Non-idempotent
def load_data():
    db.execute("INSERT INTO table VALUES (...)")  # Har safar yangi qo'shadi

# ✅ Good: Idempotent
def load_data():
    db.execute("""
        INSERT INTO table VALUES (...)
        ON CONFLICT (id) DO UPDATE SET ...
    """)  # Har safar bir xil natija
```

### 2. Atomicity

**Task'lar kichik va fokusli bo'lishi kerak:**

```python
# ❌ Bad: One task does everything
def process_everything():
    data = extract()
    transformed = transform(data)
    load(transformed)
    validate(transformed)

# ✅ Good: Separate concerns
extract_task >> transform_task >> load_task >> validate_task
```

### 3. Deterministic Execution

**Task natijasi input'ga bog'liq bo'lishi kerak:**

```python
# ❌ Bad: Non-deterministic
def process_data():
    data = fetch_latest()  # Har safar boshqa natija

# ✅ Good: Deterministic
def process_data(**context):
    execution_date = context['execution_date']
    data = fetch_for_date(execution_date)  # Bir xil input = bir xil output
```

## Scheduling Best Practices

### 1. Start Date

```python
# ✅ Good: Fixed start date
default_args = {
    'start_date': datetime(2024, 1, 1),  # O'zgarmas sana
}

# ❌ Bad: Dynamic start date
default_args = {
    'start_date': datetime.now(),  # Har safar o'zgaradi
}
```

### 2. Catchup

```python
# Default: catchup=True (barcha o'tgan run'larni bajaradi)
# MVP uchun: catchup=False (faqat hozirgi run)

dag = DAG(
    'my_dag',
    catchup=False,  # O'tgan run'larni skip qilish
    max_active_runs=1,  # Bir vaqtda bitta run
)
```

### 3. Schedule Intervals

```python
# ✅ Cron notation
schedule_interval='0 6 * * *'  # Har kuni 6:00

# ✅ Airflow presets
schedule_interval='@daily'
schedule_interval='@hourly'
schedule_interval='@weekly'

# ✅ Timedelta
schedule_interval=timedelta(hours=6)

# Manual only
schedule_interval=None
```

## Error Handling

### 1. Retry Strategy

```python
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1),
}
```

### 2. Task Callbacks

```python
def on_failure_callback(context):
    """Task fail bo'lganda"""
    logging.error(f"Task {context['task_instance'].task_id} failed")
    send_alert(context)

def on_retry_callback(context):
    """Retry qilganda"""
    logging.warning(f"Retrying task {context['task_instance'].task_id}")

def on_success_callback(context):
    """Success bo'lganda"""
    logging.info(f"Task {context['task_instance'].task_id} succeeded")

task = PythonOperator(
    task_id='my_task',
    on_failure_callback=on_failure_callback,
    on_retry_callback=on_retry_callback,
    on_success_callback=on_success_callback,
)
```

### 3. Exception Handling

```python
def safe_extract(**context):
    try:
        data = extract_data()
        return data
    except ConnectionError as e:
        logging.error(f"Connection failed: {str(e)}")
        raise  # Retry uchun re-raise
    except DataError as e:
        logging.error(f"Data error: {str(e)}")
        # Skip this task
        return None
    finally:
        cleanup_connections()
```

## Data Management

### 1. XCom Limitations

```python
# ❌ Bad: Large data in XCom
def extract():
    large_df = pd.read_csv('huge_file.csv')
    return large_df  # XCom limited to ~48KB

# ✅ Good: Use external storage
def extract():
    df = pd.read_csv('huge_file.csv')
    filepath = '/tmp/data.parquet'
    df.to_parquet(filepath)
    return filepath  # Return path only
```

### 2. Data Passing

```python
# Small data: XCom
ti.xcom_push(key='count', value=100)

# Medium data: Temporary files
filepath = '/tmp/intermediate_data.json'
with open(filepath, 'w') as f:
    json.dump(data, f)

# Large data: External storage (S3, GCS, etc.)
s3_key = f's3://bucket/data/{execution_date}.parquet'
df.to_parquet(s3_key)
```

### 3. Data Validation

```python
def validate_data(df):
    """Data quality checks"""

    # Null check
    null_count = df.isnull().sum().sum()
    if null_count > 0:
        logging.warning(f"Found {null_count} null values")

    # Duplicate check
    dup_count = df.duplicated().sum()
    if dup_count > 0:
        raise ValueError(f"Found {dup_count} duplicates")

    # Schema validation
    expected_cols = ['id', 'name', 'value']
    missing_cols = set(expected_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")

    return True
```

## Performance Optimization

### 1. Parallel Execution

```python
# Task parallelism
task1 = PythonOperator(task_id='task1', ...)
task2 = PythonOperator(task_id='task2', ...)
task3 = PythonOperator(task_id='task3', ...)

# Parallel
start >> [task1, task2, task3] >> end

# Sequential
start >> task1 >> task2 >> task3 >> end
```

### 2. Pools

```python
# Create pool
# airflow pools set db_pool 5 "Database connection pool"

# Use pool
task = PythonOperator(
    task_id='db_task',
    pool='db_pool',  # Limit concurrent connections
    priority_weight=10,  # Higher = higher priority
)
```

### 3. Database Optimization

```python
# ✅ Batch processing
def batch_insert(data, batch_size=1000):
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        db.bulk_insert(batch)

# ✅ Connection pooling
from sqlalchemy.pool import QueuePool

engine = create_engine(
    connection_string,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
)

# ✅ Use COPY for PostgreSQL
df.to_sql('table', engine, method='multi', chunksize=1000)
```

## Security Best Practices

### 1. Secrets Management

```python
# ❌ Bad: Hardcoded credentials
password = "my_password"

# ✅ Good: Use Airflow Connections
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection('my_db')
password = conn.password

# ✅ Better: Use Secrets Backend
from airflow.models import Variable

api_key = Variable.get("api_key")
```

### 2. Environment Variables

```python
import os

# Sensitive data
db_password = os.getenv('DB_PASSWORD')
api_key = os.getenv('API_KEY')

# Non-sensitive config
environment = os.getenv('ENVIRONMENT', 'development')
```

### 3. Connection Encryption

```python
# PostgreSQL with SSL
conn_string = (
    "postgresql://user:pass@host:5432/db"
    "?sslmode=require"
    "&sslrootcert=/path/to/ca.pem"
)
```

## Logging Best Practices

### 1. Structured Logging

```python
import logging

def process_data(**context):
    execution_date = context['execution_date']

    logging.info(f"Processing data for {execution_date}")
    logging.info(f"Record count: {len(data)}")

    try:
        result = process(data)
        logging.info(f"✓ Processed {result['count']} records")
        return result
    except Exception as e:
        logging.error(f"✗ Processing failed: {str(e)}", exc_info=True)
        raise
```

### 2. Log Levels

```python
logging.debug("Detailed debug information")
logging.info("General information")
logging.warning("Warning - something unexpected")
logging.error("Error - task might fail")
logging.critical("Critical - system-level problem")
```

## Testing Best Practices

### 1. Unit Tests

```python
import pytest
from dags.my_dag import extract_data

def test_extract_data():
    """Test extract function"""
    result = extract_data()
    assert result is not None
    assert len(result) > 0
```

### 2. DAG Validation

```python
def test_dag_structure():
    """Validate DAG structure"""
    from airflow.models import DagBag

    dagbag = DagBag(dag_folder='dags/')
    dag = dagbag.get_dag('my_dag')

    # Check DAG exists
    assert dag is not None

    # Check task count
    assert len(dag.tasks) == 5

    # Check dependencies
    assert dag.has_task('extract')
    assert dag.has_task('transform')
```

### 3. Integration Tests

```python
def test_full_pipeline():
    """Test complete pipeline"""
    # Setup test data
    test_data = create_test_data()

    # Run pipeline
    result = run_pipeline(test_data)

    # Validate results
    assert result['status'] == 'success'
    assert result['records_processed'] == len(test_data)
```

## Monitoring Best Practices

### 1. Health Checks

```python
def health_check(**context):
    """Check system health before running pipeline"""

    # Database connectivity
    try:
        db.execute("SELECT 1")
    except Exception as e:
        raise ValueError(f"Database unavailable: {str(e)}")

    # Disk space
    import shutil
    stat = shutil.disk_usage('/')
    free_gb = stat.free / (1024**3)
    if free_gb < 10:
        raise ValueError(f"Low disk space: {free_gb:.2f}GB")

    # API availability
    response = requests.get(api_health_url)
    if response.status_code != 200:
        raise ValueError("API unavailable")

    return "Health check passed"
```

### 2. SLA Monitoring

```python
default_args = {
    'sla': timedelta(hours=2),  # Task should complete within 2 hours
}

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Called when SLA is missed"""
    logging.error(f"SLA missed for tasks: {task_list}")
    send_alert("SLA Violation", task_list)

dag = DAG(
    'my_dag',
    default_args=default_args,
    sla_miss_callback=sla_miss_callback,
)
```

## Documentation Best Practices

### 1. DAG Documentation

```python
"""
Customer Data ETL Pipeline
=========================

**Purpose**: Extract customer data from source DB, transform, and load to warehouse

**Schedule**: Daily at 6:00 AM

**Owner**: Data Team

**Dependencies**:
- Source database must be available
- API credentials must be configured

**Notifications**:
- Success: None
- Failure: team@example.com

**SLA**: 2 hours
"""

dag = DAG(
    'customer_etl',
    description='Daily customer data ETL',
    doc_md=__doc__,
)
```

### 2. Task Documentation

```python
task = PythonOperator(
    task_id='extract_customers',
    python_callable=extract_customers,
    doc_md="""
    ### Extract Customers

    Extracts customer records from PostgreSQL source database.

    **Query**: Customers modified in last 24 hours
    **Output**: Customer records with full profile
    """,
)
```

## Code Organization

### 1. Folder Structure

```
dags/
├── common/           # Shared utilities
│   ├── __init__.py
│   ├── database.py
│   ├── validation.py
│   └── notifications.py
├── customer/         # Customer domain
│   ├── extract.py
│   ├── transform.py
│   └── load.py
└── customer_etl.py   # Main DAG
```

### 2. Reusable Functions

```python
# common/database.py
def get_db_connection(conn_id):
    """Reusable DB connection"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=conn_id)

# Use in DAG
from common.database import get_db_connection

def extract(**context):
    db = get_db_connection('source_db')
    return db.get_records(...)
```

## Deployment Best Practices

### 1. Environment Management

```python
import os

ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')

if ENVIRONMENT == 'production':
    DB_CONN = 'prod_db'
    SCHEDULE = '@daily'
elif ENVIRONMENT == 'staging':
    DB_CONN = 'staging_db'
    SCHEDULE = '@weekly'
else:
    DB_CONN = 'dev_db'
    SCHEDULE = None  # Manual only
```

### 2. Version Control

```bash
# DAG versioning
dags/
├── v1/
│   └── customer_etl.py
└── v2/
    └── customer_etl.py
```

### 3. CI/CD Integration

```yaml
# .github/workflows/test.yml
name: Test DAGs
on: [push]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run tests
        run: pytest tests/
```

## Common Pitfalls

### ❌ Anti-patterns

1. **Top-level code execution**
```python
# ❌ Bad: Executes during DAG parsing
data = fetch_data()  # Called every time Airflow parses DAG

# ✅ Good: Execute only during task run
def task_function():
    data = fetch_data()  # Called only when task runs
```

2. **Dynamic DAG generation abuse**
```python
# ❌ Bad: Too many DAGs
for customer in get_all_customers():  # 10000 customers = 10000 DAGs
    create_dag(customer)

# ✅ Good: Single DAG with dynamic tasks
with DAG('process_customers') as dag:
    customers = Variable.get('customers_list', deserialize_json=True)
    for customer in customers:
        process_task(customer)
```

3. **Ignoring timezones**
```python
# ❌ Bad: Naive datetime
start_date = datetime(2024, 1, 1)

# ✅ Good: Timezone aware
from airflow.utils import timezone
start_date = timezone.datetime(2024, 1, 1)
```
