from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import json
import os

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='taxi_clean_pipeline',
    default_args=default_args,
    description='Incremental NYC Taxi cleaning with quality checks',
    schedule_interval=None, 
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['spark', 'taxi', 'incremental'],
) as dag:

    run_spark_clean = BashOperator(
        task_id="run_spark_clean",
        bash_command="/opt/spark/bin/spark-submit --master local[*] /opt/airflow/scripts/clean_taxi.py",
    )
    
    def check_data_quality(**context):
        stats_file = "/opt/airflow/data/stats.json"
        if not os.path.exists(stats_file):
            raise ValueError("Stats file not found  spark job likely failed")

        with open(stats_file) as f:
            stats = json.load(f)

        processed_months = stats.get("processed_months", [])
        records_after = stats.get("records_after", 0)

        if records_after == 0:
            raise ValueError(f"Data quality failed: 0 valid records after cleaning for months {processed_months}")


        context['ti'].xcom_push(key='cleaning_stats', value=stats)
        return 'push_stats_to_xcom'

    quality_check = BranchPythonOperator(
        task_id='quality_check',
        python_callable=check_data_quality,
        provide_context=True,
    )

    push_stats_to_xcom = PythonOperator(
        task_id='push_stats_to_xcom',
        python_callable=lambda **ctx: ctx['ti'].xcom_push(key='last_run_stats',
                                                          value=json.load(open("/opt/airflow/data/stats.json"))),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    run_spark_clean >> quality_check
    quality_check >> push_stats_to_xcom
