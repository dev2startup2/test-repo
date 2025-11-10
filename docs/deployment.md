# Deployment Guide

## Production Deployment

### Docker Deployment

#### 1. Prerequisites

```bash
# Docker va Docker Compose
docker --version
docker-compose --version

# Git
git --version
```

#### 2. Environment Setup

```bash
# Clone repository
git clone https://github.com/dev2startup2/test-repo.git
cd test-repo

# Create .env file
cp .env.example .env
nano .env  # Edit configuration
```

#### 3. Docker Compose Deployment

```bash
# Build and start services
docker-compose up -d

# Check logs
docker-compose logs -f

# Check status
docker-compose ps

# Create admin user (first time only)
docker-compose run airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

#### 4. Access Airflow

```
URL: http://localhost:8080
Username: admin
Password: admin (or your configured password)
```

### Kubernetes Deployment

#### 1. Helm Chart

```bash
# Add Airflow Helm repository
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Create namespace
kubectl create namespace airflow

# Install Airflow
helm install airflow apache-airflow/airflow \
    --namespace airflow \
    --values k8s-values.yaml
```

#### 2. Kubernetes Values (k8s-values.yaml)

```yaml
# Executor
executor: "KubernetesExecutor"

# Airflow configuration
config:
  AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
  AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'true'

# Git sync for DAGs
dags:
  gitSync:
    enabled: true
    repo: https://github.com/dev2startup2/test-repo.git
    branch: main
    subPath: "dags"

# PostgreSQL
postgresql:
  enabled: true
  postgresqlPassword: airflow

# Redis (for CeleryExecutor)
redis:
  enabled: false

# Ingress
ingress:
  enabled: true
  hosts:
    - name: airflow.example.com
      path: /
```

#### 3. Deploy

```bash
# Install
helm install airflow apache-airflow/airflow \
    --namespace airflow \
    --values k8s-values.yaml

# Check pods
kubectl get pods -n airflow

# Get web UI URL
kubectl get ingress -n airflow
```

### Cloud Deployments

#### AWS (Amazon MWAA)

```hcl
# Terraform configuration
resource "aws_mwaa_environment" "airflow" {
  name = "etl-airflow"

  airflow_version = "2.8.1"
  environment_class = "mw1.small"

  dag_s3_path = "dags/"
  source_bucket_arn = aws_s3_bucket.airflow.arn

  execution_role_arn = aws_iam_role.airflow.arn

  network_configuration {
    security_group_ids = [aws_security_group.airflow.id]
    subnet_ids = aws_subnet.private[*].id
  }

  logging_configuration {
    dag_processing_logs {
      enabled = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled = true
      log_level = "INFO"
    }
    task_logs {
      enabled = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled = true
      log_level = "INFO"
    }
    worker_logs {
      enabled = true
      log_level = "INFO"
    }
  }
}
```

#### GCP (Cloud Composer)

```bash
# Create Composer environment
gcloud composer environments create etl-airflow \
    --location us-central1 \
    --python-version 3.9 \
    --machine-type n1-standard-1 \
    --node-count 3

# Upload DAGs
gcloud composer environments storage dags import \
    --environment etl-airflow \
    --location us-central1 \
    --source dags/
```

#### Azure (Data Factory + Custom)

```bash
# Create resource group
az group create --name airflow-rg --location eastus

# Create container instances
az container create \
    --resource-group airflow-rg \
    --name airflow-webserver \
    --image apache/airflow:2.8.1 \
    --cpu 2 \
    --memory 4 \
    --port 8080
```

## Configuration Management

### 1. Connections

```bash
# Via CLI
airflow connections add 'postgres_prod' \
    --conn-type 'postgres' \
    --conn-host 'prod-db.example.com' \
    --conn-login 'airflow' \
    --conn-password 'secure_password' \
    --conn-port '5432' \
    --conn-schema 'warehouse'

# Via UI
# Navigate to Admin > Connections > Add Connection

# Via environment variables
export AIRFLOW_CONN_POSTGRES_PROD='postgresql://user:pass@host:5432/db'

# Via secrets backend (recommended)
# Configure in airflow.cfg or environment
AIRFLOW__SECRETS__BACKEND=airflow.providers.hashicorp.secrets.vault.VaultBackend
```

### 2. Variables

```bash
# Via CLI
airflow variables set environment "production"
airflow variables set email_alerts "team@example.com"

# Bulk import
airflow variables import config/variables.yaml

# Via UI
# Navigate to Admin > Variables > Add Variable
```

### 3. Secrets Backend

#### HashiCorp Vault

```python
# airflow.cfg
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {
    "url": "https://vault.example.com:8200",
    "token": "your-vault-token",
    "mount_point": "airflow",
    "connections_path": "connections",
    "variables_path": "variables"
}
```

#### AWS Secrets Manager

```python
# airflow.cfg
[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
backend_kwargs = {
    "connections_prefix": "airflow/connections",
    "variables_prefix": "airflow/variables"
}
```

## Monitoring Setup

### 1. Prometheus Integration

```python
# airflow.cfg
[metrics]
statsd_on = True
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow
```

### 2. Grafana Dashboards

```yaml
# docker-compose.yml additions
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana-storage:/var/lib/grafana
```

### 3. ELK Stack

```yaml
  elasticsearch:
    image: elasticsearch:8.11.0
    environment:
      - discovery.type=single-node
    ports:
      - "9200:9200"

  logstash:
    image: logstash:8.11.0
    volumes:
      - ./monitoring/logstash.conf:/usr/share/logstash/pipeline/logstash.conf

  kibana:
    image: kibana:8.11.0
    ports:
      - "5601:5601"
```

## Scaling Strategies

### 1. CeleryExecutor

```python
# airflow.cfg
[core]
executor = CeleryExecutor

[celery]
broker_url = redis://redis:6379/0
result_backend = db+postgresql://airflow:airflow@postgres/airflow
worker_concurrency = 16
```

```bash
# Start workers
docker-compose up -d airflow-worker

# Scale workers
docker-compose up -d --scale airflow-worker=5
```

### 2. KubernetesExecutor

```python
# airflow.cfg
[core]
executor = KubernetesExecutor

[kubernetes]
namespace = airflow
worker_container_repository = apache/airflow
worker_container_tag = 2.8.1
delete_worker_pods = True
```

### 3. Auto-scaling

```yaml
# Kubernetes HPA
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: airflow-worker
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: airflow-worker
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

## Backup and Recovery

### 1. Database Backup

```bash
# PostgreSQL backup
pg_dump -h localhost -U airflow airflow > backup.sql

# Automated backup script
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR=/backups
pg_dump -h $DB_HOST -U $DB_USER $DB_NAME | gzip > $BACKUP_DIR/airflow_$DATE.sql.gz

# Keep only last 30 days
find $BACKUP_DIR -name "airflow_*.sql.gz" -mtime +30 -delete
```

### 2. DAG Backup

```bash
# Git-based versioning (recommended)
git add dags/
git commit -m "Update DAGs"
git push

# S3 sync
aws s3 sync dags/ s3://airflow-backup/dags/ --delete
```

### 3. Recovery

```bash
# Restore database
gunzip < backup.sql.gz | psql -h localhost -U airflow airflow

# Restore DAGs
git pull
# or
aws s3 sync s3://airflow-backup/dags/ dags/
```

## Health Checks

### 1. Application Health

```bash
# Webserver health
curl http://localhost:8080/health

# Scheduler health
airflow jobs check --job-type SchedulerJob

# Database health
airflow db check
```

### 2. Automated Monitoring

```python
# monitoring/health_check.py
import requests
import sys

def check_airflow_health():
    try:
        response = requests.get('http://localhost:8080/health', timeout=10)
        if response.status_code == 200:
            health = response.json()
            if health.get('metadatabase', {}).get('status') == 'healthy':
                return True
    except Exception as e:
        print(f"Health check failed: {str(e)}")
    return False

if __name__ == "__main__":
    if not check_airflow_health():
        sys.exit(1)
```

## Security Hardening

### 1. SSL/TLS Configuration

```python
# airflow.cfg
[webserver]
web_server_ssl_cert = /path/to/cert.pem
web_server_ssl_key = /path/to/key.pem
```

### 2. Authentication

```python
# LDAP
[ldap]
uri = ldap://ldap.example.com
user_filter = objectClass=*
user_name_attr = uid
group_member_attr = memberOf
superuser_filter = memberOf=cn=airflow-admins,ou=groups,dc=example,dc=com
bind_user = cn=admin,dc=example,dc=com
bind_password = secure_password
```

### 3. Network Security

```yaml
# docker-compose.yml
services:
  airflow-webserver:
    environment:
      - AIRFLOW__WEBSERVER__SECRET_KEY=${SECRET_KEY}
      - AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}
    networks:
      - airflow-network

networks:
  airflow-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.28.0.0/16
```

## CI/CD Pipeline

### GitHub Actions

```yaml
# .github/workflows/deploy.yml
name: Deploy to Production

on:
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run tests
        run: |
          pip install -r requirements.txt
          pytest tests/

  deploy:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Deploy to server
        run: |
          ssh user@server 'cd /opt/airflow && git pull && docker-compose restart'
```

## Troubleshooting

### Common Issues

1. **DAG not appearing**
```bash
# Check parsing errors
airflow dags list-import-errors

# Refresh DAGs
airflow dags list
```

2. **Task stuck in queued**
```bash
# Check scheduler
airflow scheduler

# Check worker capacity
airflow celery workers
```

3. **Database connection issues**
```bash
# Test connection
airflow db check

# Reset database
airflow db reset
```

## Maintenance

### Regular Tasks

```bash
# Clean old logs
find /opt/airflow/logs -name "*.log" -mtime +30 -delete

# Cleanup metadata
airflow db clean --clean-before-timestamp $(date -d '30 days ago' +%Y-%m-%d)

# Vacuum database
psql -U airflow -c "VACUUM ANALYZE;"
```

## Rollback Strategy

```bash
# Quick rollback
git revert HEAD
git push

# Full rollback
docker-compose down
git checkout <previous-commit>
docker-compose up -d
```

## Performance Tuning

### Database Optimization

```sql
-- Index creation
CREATE INDEX idx_task_instance_dag_run ON task_instance(dag_id, run_id);
CREATE INDEX idx_dag_run_state ON dag_run(state);

-- Connection pooling
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET shared_buffers = '2GB';
```

### Airflow Configuration

```ini
[core]
parallelism = 32
max_active_runs_per_dag = 3
max_active_tasks_per_dag = 16

[scheduler]
max_threads = 2
min_file_process_interval = 30
```

## Cost Optimization

### Resource Management

```yaml
# Kubernetes resource limits
resources:
  limits:
    cpu: "2"
    memory: "4Gi"
  requests:
    cpu: "500m"
    memory: "1Gi"
```

### Scheduling Optimization

```python
# Off-peak scheduling
schedule_interval = '0 2 * * *'  # 2 AM when resources cheaper
```
