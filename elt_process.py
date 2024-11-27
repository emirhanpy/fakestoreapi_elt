from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from main import extracted_data, load_data_to_file

default_args = {
    'owner': 'emirhan',
    'start_date': days_ago(0),
    'end_date': datetime(2024,27,11),
    'email': ['mremirhan131@gmail.com'],
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=60)
}

dag = DAG(
    'etl_data_pipeline',
    default_args=default_args,
    description='Apache Airflow DAG for ELT process',
    schedule_interval='@hourly',
)

extract_task = PythonOperator(
    task_id='extract_phase',
    python_callable=extracted_data,
    depends_on_past=True,
    dag=dag,
)


load_task = PythonOperator(
    task_id='load_phase',
    python_callable=load_data_to_file,
    depends_on_past=True,
    dag=dag,
)

extract_task >> load_task