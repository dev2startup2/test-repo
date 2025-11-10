# Development Guide

## Development Environment Setup

### 1. Prerequisites

```bash
# Python 3.8+
python --version

# Virtual environment
python -m venv venv
source venv/bin/activate

# Dependencies
pip install -r requirements.txt
```

### 2. Airflow Setup

```bash
# Set Airflow home
export AIRFLOW_HOME=$(pwd)/airflow

# Initialize database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Start services
airflow webserver -D
airflow scheduler -D
```

### 3. Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit configuration
nano .env

# Load environment variables
source .env
```

## DAG Development

### DAG Structure

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'your_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'my_dag',
    default_args=default_args,
    description='Description',
    schedule_interval='@daily',
    catchup=False,
    tags=['tag1', 'tag2'],
) as dag:

    task = PythonOperator(
        task_id='my_task',
        python_callable=my_function,
    )
```

### Best Practices

#### 1. Idempotent Tasks
```python
def idempotent_load(**context):
    """
    Task should produce same result when run multiple times
    """
    # Use UPSERT instead of INSERT
    # Use CREATE TABLE IF NOT EXISTS
    # Use WHERE conditions for updates
    pass
```

#### 2. XCom for Data Passing
```python
def extract(**context):
    data = fetch_data()
    # Small data only (< 48KB)
    context['task_instance'].xcom_push(key='data', value=data)

def transform(**context):
    ti = context['task_instance']
    data = ti.xcom_pull(key='data', task_ids='extract')
    # Process data
```

#### 3. Error Handling
```python
def safe_task(**context):
    try:
        risky_operation()
    except SpecificError as e:
        logging.error(f"Error occurred: {str(e)}")
        # Handle gracefully or re-raise
        raise
    finally:
        cleanup()
```

#### 4. Resource Management
```python
task = PythonOperator(
    task_id='heavy_task',
    pool='heavy_pool',  # Limit concurrent runs
    priority_weight=10,  # Higher priority
    queue='high_memory',  # Specific queue
)
```

## Custom Operators

### Creating Custom Operator

```python
# plugins/operators/custom_operator.py

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class MyCustomOperator(BaseOperator):
    """
    Custom operator for specific task
    """

    @apply_defaults
    def __init__(
        self,
        my_param,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.my_param = my_param

    def execute(self, context):
        """
        Main execution logic
        """
        logging.info(f"Executing with param: {self.my_param}")

        # Your logic here
        result = self.do_something()

        return result

    def do_something(self):
        # Implementation
        pass
```

### Using Custom Operator

```python
from operators.custom_operator import MyCustomOperator

task = MyCustomOperator(
    task_id='custom_task',
    my_param='value',
)
```

## Custom Hooks

### Creating Custom Hook

```python
# plugins/hooks/custom_hook.py

from airflow.hooks.base import BaseHook

class MyCustomHook(BaseHook):
    """
    Hook for connecting to custom system
    """

    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.conn = self.get_connection(conn_id)

    def get_conn(self):
        """
        Return connection object
        """
        return create_connection(
            host=self.conn.host,
            port=self.conn.port,
            user=self.conn.login,
            password=self.conn.password
        )

    def fetch_data(self, query):
        """
        Fetch data from system
        """
        conn = self.get_conn()
        result = conn.execute(query)
        return result
```

## Testing

### Unit Tests

```python
# tests/test_dags.py

import pytest
from airflow.models import DagBag

def test_dag_loaded():
    """Test that DAG is loaded without errors"""
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    assert len(dagbag.import_errors) == 0

def test_dag_structure():
    """Test DAG has correct structure"""
    dagbag = DagBag(dag_folder='dags/')
    dag = dagbag.get_dag('simple_etl_pipeline')

    assert dag is not None
    assert len(dag.tasks) == 5
    assert dag.has_task('extract')
```

### Integration Tests

```python
# tests/test_integration.py

from airflow.models import DagBag
from datetime import datetime

def test_dag_run():
    """Test full DAG execution"""
    dagbag = DagBag()
    dag = dagbag.get_dag('simple_etl_pipeline')

    # Test run
    execution_date = datetime.now()
    dag.test(execution_date=execution_date)
```

### Running Tests

```bash
# All tests
pytest tests/

# Specific test
pytest tests/test_dags.py::test_dag_loaded

# With coverage
pytest --cov=dags --cov-report=html tests/
```

## DAG Validation

```bash
# List all DAGs
airflow dags list

# Check for DAG errors
python dags/my_dag.py

# Test specific DAG
airflow dags test simple_etl_pipeline 2024-01-01

# Test specific task
airflow tasks test simple_etl_pipeline extract 2024-01-01
```

## Debugging

### Local Testing

```python
# Add at end of DAG file
if __name__ == "__main__":
    from airflow.utils.state import State
    dag.clear(dag_run_state=State.NONE)
    dag.run()
```

### Logging

```python
import logging

def my_task(**context):
    logging.info("Info message")
    logging.warning("Warning message")
    logging.error("Error message")
    logging.debug("Debug message")
```

### Interactive Testing

```bash
# Python shell with Airflow context
airflow shell

# IPython with DAG context
ipython
>>> from airflow.models import DagBag
>>> dagbag = DagBag()
>>> dag = dagbag.get_dag('my_dag')
>>> dag.tasks
```

## Code Quality

### Linting

```bash
# Pylint
pylint dags/

# Flake8
flake8 dags/ --max-line-length=100

# Black (formatting)
black dags/

# MyPy (type checking)
mypy dags/
```

### Pre-commit Hooks

```bash
# Install pre-commit
pip install pre-commit

# Setup hooks
pre-commit install

# Run manually
pre-commit run --all-files
```

## Performance Optimization

### 1. DAG Performance

```python
# Optimize DAG parsing
default_args = {
    'start_date': datetime(2024, 1, 1),  # Fixed date
}

# Reduce DAG complexity
# - Break large DAGs into smaller ones
# - Use SubDAGs sparingly
# - Minimize dynamic task generation
```

### 2. Task Performance

```python
# Use pools for resource management
airflow pools set heavy_pool 5 "Pool for heavy tasks"

# Batch processing
BATCH_SIZE = 1000
for batch in chunks(data, BATCH_SIZE):
    process_batch(batch)
```

### 3. Database Optimization

```python
# Connection pooling
from sqlalchemy.pool import NullPool

engine = create_engine(
    connection_string,
    poolclass=NullPool,
    pool_pre_ping=True
)
```

## Monitoring Development

### Metrics Collection

```python
from airflow.metrics.statsd_logger import SafeStatsdLogger

stats = SafeStatsdLogger()
stats.incr('my_metric')
stats.gauge('queue_size', len(queue))
stats.timing('execution_time', duration)
```

### Health Checks

```bash
# Check scheduler health
airflow jobs check --job-type SchedulerJob

# Check database
airflow db check

# Check connectivity
airflow connections test my_connection
```

## Troubleshooting

### Common Issues

1. **Import Errors**
   ```bash
   # Check Python path
   echo $PYTHONPATH

   # Add to path
   export PYTHONPATH="${PYTHONPATH}:${AIRFLOW_HOME}"
   ```

2. **Database Lock**
   ```bash
   # Reset database
   airflow db reset

   # Or migrate
   airflow db upgrade
   ```

3. **Stale DAGs**
   ```bash
   # Refresh DAG
   airflow dags trigger my_dag --conf '{}'
   ```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Test DAGs

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.9
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
      - name: Run tests
        run: |
          pytest tests/
```

## Documentation

### Docstring Standards

```python
def my_function(**context):
    """
    Brief description of function.

    Longer description explaining what this function does,
    when it should be used, and any important considerations.

    Args:
        context (dict): Airflow context dictionary

    Returns:
        str: Description of return value

    Raises:
        ValueError: When data is invalid

    Example:
        >>> result = my_function(context={'execution_date': datetime.now()})
        >>> print(result)
        'Success'
    """
    pass
```

## Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [API Reference](https://airflow.apache.org/docs/apache-airflow/stable/python-api-ref.html)
