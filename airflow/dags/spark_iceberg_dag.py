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
    'retry_delay': timedelta(minutes=1)  # Reduced from 5m to 1m for minute-level runs
}

with DAG(
    'spark_iceberg_job',
    default_args=default_args,
    description='Simple Spark Iceberg DAG - Runs Every Minute',
    # ⬇️⬇️⬇️ CHANGED HERE: Cron for "Every Minute" ⬇️⬇️⬇️
    schedule_interval='* * * * *',
    # ⬇️⬇️⬇️ CRITICAL SAFETY SETTINGS ⬇️⬇️⬇️
    catchup=False,              # PREVENTS 1,440 backfill runs on startup
    max_active_runs=1,          # PREVENTS overlapping runs if job > 60s
    dagrun_timeout=timedelta(minutes=55), # Optional: Auto-fail if stuck near next run
) as dag:

    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command='docker exec spark-iceberg spark-submit --verbose /opt/spark/spark_batch.py'
    )

    run_spark_job
