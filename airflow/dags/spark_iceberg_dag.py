from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'spark_iceberg_job',
    default_args=default_args,
    description='Simple Spark Iceberg DAG',
    schedule_interval='@daily',
    catchup=False
) as dag:

    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command='docker exec spark-iceberg spark-submit --verbose /opt/spark/spark.py'
    )

    run_spark_job
