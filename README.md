# ETL Tools - Apache Airflow Based Data Pipeline

Modern va kengaytiriladigan ETL (Extract, Transform, Load) platforma Apache Airflow asosida qurilgan.

## 📋 Loyiha haqida

Bu loyiha turli manbalardan ma'lumotlarni olish, qayta ishlash va saqlash uchun mo'ljallangan zamonaviy ETL platformasidir. Apache Airflow orqali ma'lumotlar oqimini boshqarish, monitoring qilish va avtomatlashtirish imkoniyatini beradi.

## ✨ Asosiy imkoniyatlar

- **Workflow Management**: Apache Airflow DAG (Directed Acyclic Graph) orqali murakkab pipeline'larni boshqarish
- **Scheduling**: Vaqt jadvaliga ko'ra avtomatik ishga tushirish
- **Monitoring**: Real-time monitoring va alerting
- **Scalability**: Hajmni kengaytirish imkoniyati
- **Extensibility**: Custom operator va hook'lar yaratish
- **Error Handling**: Xatolarni boshqarish va retry mexanizmi
- **Data Quality**: Ma'lumotlar sifatini nazorat qilish

## 🏗️ Arxitektura

```
┌─────────────────┐
│   Data Sources  │
│  (DB, API, CSV) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apache Airflow │
│   DAG Pipeline  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Transform      │
│  (Python/SQL)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Data Warehouse │
│  (Target DB)    │
└─────────────────┘
```

## 🚀 Tezkor boshlash

### Talablar

- Python 3.8+
- Docker va Docker Compose (tavsiya etiladi)
- PostgreSQL yoki boshqa database (metadata uchun)

### O'rnatish

1. Repository'ni clone qiling:
```bash
git clone https://github.com/dev2startup2/test-repo.git
cd test-repo
```

2. Virtual environment yarating:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# yoki
venv\Scripts\activate  # Windows
```

3. Dependencies'larni o'rnating:
```bash
pip install -r requirements.txt
```

4. Airflow'ni sozlang:
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

5. Airflow'ni ishga tushiring:
```bash
# Webserver
airflow webserver --port 8080

# Scheduler (yangi terminal oynasida)
airflow scheduler
```

6. Brauzerda oching: http://localhost:8080

### Docker bilan ishga tushirish

```bash
docker-compose up -d
```

## 📁 Loyiha strukturasi

```
test-repo/
├── dags/                    # Airflow DAG fayllari
│   ├── example_etl.py
│   ├── data_quality_check.py
│   └── ...
├── plugins/                 # Custom operator va hook'lar
│   ├── operators/
│   ├── hooks/
│   └── sensors/
├── data/                    # Test ma'lumotlar
│   ├── raw/
│   ├── processed/
│   └── output/
├── config/                  # Konfiguratsiya fayllari
│   ├── connections.yaml
│   └── variables.yaml
├── sql/                     # SQL scriptlar
│   ├── transforms/
│   └── schemas/
├── tests/                   # Unit va integration testlar
├── docs/                    # Qo'shimcha dokumentatsiya
├── docker-compose.yml       # Docker konfiguratsiyasi
├── requirements.txt         # Python dependencies
├── .env.example            # Environment variables template
└── README.md
```

## 📝 DAG yaratish

Oddiy ETL DAG namunasi:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract():
    # Ma'lumotlarni olish
    pass

def transform():
    # Ma'lumotlarni qayta ishlash
    pass

def load():
    # Ma'lumotlarni yuklash
    pass

with DAG(
    'simple_etl',
    default_args=default_args,
    description='Oddiy ETL pipeline',
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load
    )

    extract_task >> transform_task >> load_task
```

## 🔧 Konfiguratsiya

### Environment Variables

`.env` faylini yarating:

```bash
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:pass@localhost/airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__SECRET_KEY=your-secret-key
```

### Connections

Airflow UI orqali yoki CLI orqali connection'lar qo'shing:

```bash
airflow connections add 'my_db' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-login 'user' \
    --conn-password 'password' \
    --conn-port '5432' \
    --conn-schema 'database'
```

## 📊 Monitoring va Alerting

- **Airflow Web UI**: http://localhost:8080
- **Task logs**: `logs/` katalogida
- **Metrics**: Prometheus va Grafana integratsiyasi (keyingi versiya)

## 🧪 Testing

Testlarni ishga tushirish:

```bash
pytest tests/
```

DAG'ni validate qilish:

```bash
airflow dags test simple_etl 2024-01-01
```

## 🛠️ Development

### Custom Operator yaratish

```python
from airflow.models import BaseOperator

class CustomOperator(BaseOperator):
    def __init__(self, my_param, **kwargs):
        super().__init__(**kwargs)
        self.my_param = my_param

    def execute(self, context):
        # Custom logic
        pass
```

## 📚 Dokumentatsiya

Qo'shimcha dokumentatsiya `docs/` katalogida:

- [Architecture Guide](docs/architecture.md) - Arxitektura tafsilotlari
- [Development Guide](docs/development.md) - Development yo'riqnomasi
- [Deployment Guide](docs/deployment.md) - Production'ga deploy qilish
- [Best Practices](docs/best-practices.md) - Eng yaxshi amaliyotlar

## 🤝 Contributing

1. Fork qiling
2. Feature branch yarating (`git checkout -b feature/AmazingFeature`)
3. Commit qiling (`git commit -m 'Add some AmazingFeature'`)
4. Push qiling (`git push origin feature/AmazingFeature`)
5. Pull Request oching

## 📄 License

Bu loyiha MIT litsenziyasi ostida.

## 🔗 Foydali linklar

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [DAG Writing Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#writing-a-dag)

## 📧 Aloqa

Savollar yoki takliflar bo'lsa, issue oching yoki email yuboring.

---

**Status**: MVP Development Phase 🚧

**Version**: 0.1.0
