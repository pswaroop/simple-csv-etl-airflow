from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 27),
}

with DAG(
    dag_id = 'simple_csv_etl',
    default_args = default_args,
    catchup=False,
    tags = ['ETL','CSV'],
) as dag:
    extract = BashOperator(
        task_id = 'download_csv',
        bash_command = 'python3 /opt/airflow/scripts/download_csv.py',
    )

    transform = BashOperator(
        task_id='transform_csv',
        bash_command='python3 /opt/airflow/scripts/transform_csv.py'
    )

    extract >> transform