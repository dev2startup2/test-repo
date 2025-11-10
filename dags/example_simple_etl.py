"""
Oddiy ETL Pipeline
==================

Bu DAG oddiy ETL jarayonini ko'rsatadi:
1. Ma'lumotlarni olish (Extract)
2. Ma'lumotlarni qayta ishlash (Transform)
3. Ma'lumotlarni yuklash (Load)

Schedule: Har kuni ertalab 6:00 da
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

# Default argumentlar
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# DAG yaratish
dag = DAG(
    'simple_etl_pipeline',
    default_args=default_args,
    description='Oddiy ETL pipeline misoli',
    schedule_interval='0 6 * * *',  # Har kuni 6:00 da
    catchup=False,
    tags=['etl', 'example', 'daily'],
)


def extract_data(**context):
    """
    Ma'lumotlarni manbadan olish

    Bu misolda CSV fayldan ma'lumot o'qiymiz
    Real projectda bu database, API yoki boshqa manba bo'lishi mumkin
    """
    logging.info("Ma'lumotlarni olish boshlandi...")

    # Demo data
    data = {
        'users': [
            {'id': 1, 'name': 'Ali', 'age': 25, 'city': 'Toshkent'},
            {'id': 2, 'name': 'Vali', 'age': 30, 'city': 'Samarqand'},
            {'id': 3, 'name': 'Sami', 'age': 28, 'city': 'Buxoro'},
            {'id': 4, 'name': 'Laylo', 'age': 26, 'city': 'Xiva'},
        ],
        'total_records': 4
    }

    logging.info(f"Jami {data['total_records']} ta record olindi")

    # XCom orqali keyingi task'ga yuborish
    context['task_instance'].xcom_push(key='raw_data', value=data)

    return "Extract muvaffaqiyatli bajarildi"


def transform_data(**context):
    """
    Ma'lumotlarni qayta ishlash

    - Ma'lumotlarni tozalash
    - Format o'zgartirish
    - Validatsiya qilish
    - Business logic qo'llash
    """
    logging.info("Ma'lumotlarni qayta ishlash boshlandi...")

    # Oldingi task'dan ma'lumot olish
    ti = context['task_instance']
    raw_data = ti.xcom_pull(key='raw_data', task_ids='extract')

    # Transform logic
    transformed_users = []
    for user in raw_data['users']:
        # Yosh guruhini aniqlash
        if user['age'] < 26:
            age_group = 'yosh'
        elif user['age'] < 30:
            age_group = 'o\'rta'
        else:
            age_group = 'katta'

        transformed_user = {
            'id': user['id'],
            'full_name': user['name'].upper(),  # Uppercase
            'age': user['age'],
            'age_group': age_group,
            'location': user['city'],
            'processed_at': datetime.now().isoformat()
        }
        transformed_users.append(transformed_user)

    processed_data = {
        'users': transformed_users,
        'total_records': len(transformed_users)
    }

    logging.info(f"Jami {processed_data['total_records']} ta record qayta ishlandi")

    # Keyingi task'ga yuborish
    ti.xcom_push(key='processed_data', value=processed_data)

    return "Transform muvaffaqiyatli bajarildi"


def load_data(**context):
    """
    Ma'lumotlarni target system'ga yuklash

    Bu misolda log'ga yozamiz
    Real projectda database, data warehouse yoki file'ga yozish mumkin
    """
    logging.info("Ma'lumotlarni yuklash boshlandi...")

    # Oldingi task'dan ma'lumot olish
    ti = context['task_instance']
    processed_data = ti.xcom_pull(key='processed_data', task_ids='transform')

    # Load logic (demo)
    logging.info("Target system'ga yuklash:")
    for user in processed_data['users']:
        logging.info(f"  - {user['full_name']} ({user['age_group']}) - {user['location']}")

    # Statistika
    stats = {
        'total_loaded': processed_data['total_records'],
        'load_time': datetime.now().isoformat()
    }

    logging.info(f"Jami {stats['total_loaded']} ta record yuklandi")

    return f"Load muvaffaqiyatli bajarildi: {stats['total_loaded']} records"


def validate_results(**context):
    """
    Natijalarni tekshirish

    - Record count'ni solishtirish
    - Data quality check
    """
    logging.info("Natijalarni validatsiya qilish...")

    ti = context['task_instance']
    raw_data = ti.xcom_pull(key='raw_data', task_ids='extract')
    processed_data = ti.xcom_pull(key='processed_data', task_ids='transform')

    # Sanity check
    if raw_data['total_records'] != processed_data['total_records']:
        raise ValueError(
            f"Record count mos kelmaydi! "
            f"Extracted: {raw_data['total_records']}, "
            f"Processed: {processed_data['total_records']}"
        )

    logging.info("✓ Validatsiya muvaffaqiyatli o'tdi")
    return "Validation successful"


# Task'larni yaratish
with dag:
    # Task 1: Extract
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        provide_context=True,
    )

    # Task 2: Transform
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True,
    )

    # Task 3: Load
    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True,
    )

    # Task 4: Validate
    validate_task = PythonOperator(
        task_id='validate',
        python_callable=validate_results,
        provide_context=True,
    )

    # Task 5: Cleanup (optional)
    cleanup_task = BashOperator(
        task_id='cleanup',
        bash_command='echo "Cleanup completed"',
    )

    # Task dependencies (ketma-ketlik)
    extract_task >> transform_task >> load_task >> validate_task >> cleanup_task

# DAG haqida ma'lumot
if __name__ == "__main__":
    dag.cli()
