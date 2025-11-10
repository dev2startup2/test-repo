"""
API ETL Pipeline
=================

Bu DAG REST API'dan ma'lumot olish va saqlashni ko'rsatadi:
1. REST API'ga request yuborish
2. JSON response'ni parse qilish
3. Ma'lumotlarni transform qilish
4. Database'ga yoki file'ga saqlash

Schedule: Har soat
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
import logging
import json

default_args = {
    'owner': 'api_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
}

dag = DAG(
    'api_etl_pipeline',
    default_args=default_args,
    description='REST API dan ma\'lumot olish va saqlash',
    schedule_interval='@hourly',
    catchup=False,
    tags=['etl', 'api', 'rest'],
)


def check_api_availability(**context):
    """
    API mavjudligini tekshirish
    """
    logging.info("API availability check...")
    # Bu real projectda HttpSensor ishlatiladi
    return True


def extract_from_api(**context):
    """
    REST API'dan ma'lumot olish

    Demo: JSONPlaceholder API ishlatamiz
    Real projectda o'z API endpoint'ingiz bo'ladi
    """
    logging.info("API'dan ma'lumot olish...")

    import requests

    # API endpoint
    api_url = "https://jsonplaceholder.typicode.com/posts"

    try:
        # Request yuborish
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()

        # JSON parse
        data = response.json()

        logging.info(f"API'dan {len(data)} ta post olindi")

        # XCom ga yuborish
        context['task_instance'].xcom_push(key='api_data', value=data)

        return f"Successfully fetched {len(data)} records"

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {str(e)}")
        raise


def enrich_data(**context):
    """
    Ma'lumotlarni boyitish

    Qo'shimcha API call'lar bilan ma'lumot to'ldirish
    """
    logging.info("Ma'lumotlarni boyitish...")

    ti = context['task_instance']
    posts = ti.xcom_pull(key='api_data', task_ids='extract_from_api')

    import requests

    enriched_posts = []

    # Har bir post uchun user ma'lumotlarini olish (demo)
    # Real projectda bu batching bilan optimizatsiya qilinadi
    for post in posts[:5]:  # Faqat birinchi 5 ta (demo)
        user_url = f"https://jsonplaceholder.typicode.com/users/{post['userId']}"

        try:
            user_response = requests.get(user_url, timeout=10)
            user_data = user_response.json()

            # Enriched post
            enriched_post = {
                **post,
                'author_name': user_data.get('name'),
                'author_email': user_data.get('email'),
                'author_company': user_data.get('company', {}).get('name'),
                'fetched_at': datetime.now().isoformat()
            }

            enriched_posts.append(enriched_post)

        except Exception as e:
            logging.warning(f"Failed to enrich post {post['id']}: {str(e)}")
            # Enrichment bajarilmasa ham asosiy ma'lumotni saqlaymiz
            enriched_posts.append({
                **post,
                'fetched_at': datetime.now().isoformat()
            })

    logging.info(f"{len(enriched_posts)} ta post boyitildi")

    ti.xcom_push(key='enriched_data', value=enriched_posts)

    return "Data enrichment completed"


def transform_api_data(**context):
    """
    API ma'lumotlarini transform qilish
    """
    logging.info("API ma'lumotlarini transform qilish...")

    ti = context['task_instance']
    data = ti.xcom_pull(key='enriched_data', task_ids='enrich_data')

    transformed_data = []

    for item in data:
        # Text analysis (oddiy versiya)
        title_words = len(item['title'].split())
        body_words = len(item['body'].split())

        transformed_item = {
            'post_id': item['id'],
            'user_id': item['userId'],
            'author_name': item.get('author_name', 'Unknown'),
            'author_email': item.get('author_email', ''),
            'company': item.get('author_company', ''),
            'title': item['title'],
            'title_length': len(item['title']),
            'title_words': title_words,
            'body_preview': item['body'][:100] + '...',
            'body_length': len(item['body']),
            'body_words': body_words,
            'processed_at': datetime.now().isoformat()
        }

        transformed_data.append(transformed_item)

    ti.xcom_push(key='transformed_data', value=transformed_data)

    logging.info(f"{len(transformed_data)} ta item transform qilindi")

    return "Transform completed"


def load_to_storage(**context):
    """
    Ma'lumotlarni saqlash

    Bu misolda JSON file'ga saqlaymiz
    Real projectda database yoki cloud storage ishlatiladi
    """
    logging.info("Ma'lumotlarni saqlash...")

    ti = context['task_instance']
    data = ti.xcom_pull(key='transformed_data', task_ids='transform_api_data')

    # Execution date ni olish
    execution_date = context['execution_date'].strftime('%Y%m%d_%H%M%S')

    # File path
    output_file = f"/home/user/test-repo/data/output/api_posts_{execution_date}.json"

    # JSON ga saqlash
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logging.info(f"Ma'lumotlar saqlandi: {output_file}")
    logging.info(f"Jami {len(data)} ta record")

    # Metadata saqlash
    metadata = {
        'total_records': len(data),
        'execution_date': execution_date,
        'output_file': output_file,
        'created_at': datetime.now().isoformat()
    }

    ti.xcom_push(key='load_metadata', value=metadata)

    return f"Loaded {len(data)} records to {output_file}"


def generate_summary(**context):
    """
    Pipeline summary yaratish
    """
    logging.info("Summary generatsiya qilish...")

    ti = context['task_instance']

    # Barcha metadata'ni yig'ish
    api_data = ti.xcom_pull(key='api_data', task_ids='extract_from_api')
    enriched_data = ti.xcom_pull(key='enriched_data', task_ids='enrich_data')
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_api_data')
    load_metadata = ti.xcom_pull(key='load_metadata', task_ids='load_to_storage')

    summary = {
        'pipeline': 'API ETL Pipeline',
        'execution_date': context['execution_date'].isoformat(),
        'stats': {
            'extracted_records': len(api_data) if api_data else 0,
            'enriched_records': len(enriched_data) if enriched_data else 0,
            'transformed_records': len(transformed_data) if transformed_data else 0,
            'loaded_records': load_metadata.get('total_records', 0) if load_metadata else 0,
        },
        'output_file': load_metadata.get('output_file', '') if load_metadata else '',
        'status': 'SUCCESS',
        'completed_at': datetime.now().isoformat()
    }

    # Summary'ni log'ga yozish
    logging.info("=" * 50)
    logging.info("PIPELINE SUMMARY")
    logging.info("=" * 50)
    for key, value in summary.items():
        logging.info(f"{key}: {value}")
    logging.info("=" * 50)

    # Summary'ni file ga saqlash
    summary_file = f"/home/user/test-repo/data/output/summary_{context['execution_date'].strftime('%Y%m%d_%H%M%S')}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)

    return summary


# Tasks
with dag:

    # Task 1: API availability check
    check_api = PythonOperator(
        task_id='check_api_availability',
        python_callable=check_api_availability,
    )

    # Task 2: Extract from API
    extract = PythonOperator(
        task_id='extract_from_api',
        python_callable=extract_from_api,
    )

    # Task 3: Enrich data
    enrich = PythonOperator(
        task_id='enrich_data',
        python_callable=enrich_data,
    )

    # Task 4: Transform
    transform = PythonOperator(
        task_id='transform_api_data',
        python_callable=transform_api_data,
    )

    # Task 5: Load to storage
    load = PythonOperator(
        task_id='load_to_storage',
        python_callable=load_to_storage,
    )

    # Task 6: Generate summary
    summary = PythonOperator(
        task_id='generate_summary',
        python_callable=generate_summary,
    )

    # Dependencies
    check_api >> extract >> enrich >> transform >> load >> summary
