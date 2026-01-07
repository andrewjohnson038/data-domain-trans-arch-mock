from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'loan_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for loan transaction data',
    schedule_interval='0 2 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['lending', 'etl'],
)

def log_pipeline_start(**context):
    with open('/opt/airflow/logs/success.log', 'a') as f:
        f.write(f"{datetime.now()} - Pipeline started for batch_date: {context['ds']}\n")

def log_pipeline_success(**context):
    with open('/opt/airflow/logs/success.log', 'a') as f:
        f.write(f"{datetime.now()} - Pipeline completed successfully for batch_date: {context['ds']}\n")

def log_pipeline_failure(**context):
    with open('/opt/airflow/logs/failure.log', 'a') as f:
        f.write(f"{datetime.now()} - Pipeline failed for batch_date: {context['ds']}\n")

log_start = PythonOperator(
    task_id='log_pipeline_start',
    python_callable=log_pipeline_start,
    provide_context=True,
    dag=dag,
)

validate_s3 = SnowflakeOperator(
    task_id='validate_s3_files',
    snowflake_conn_id='snowflake_default',
    sql="LIST @raw_data_stage;",
    dag=dag,
)

dbt_structured = BashOperator(
    task_id='dbt_run_structured',
    bash_command='cd /opt/airflow/dbt && dbt run --select structured --vars \'{"batch_date": "{{ ds }}"}\' --profiles-dir .',
    dag=dag,
)

dbt_test_structured = BashOperator(
    task_id='dbt_test_structured',
    bash_command='cd /opt/airflow/dbt && dbt test --select structured --profiles-dir .',
    dag=dag,
)

dbt_curated = BashOperator(
    task_id='dbt_run_curated',
    bash_command='cd /opt/airflow/dbt && dbt run --select curated --vars \'{"batch_date": "{{ ds }}"}\' --profiles-dir .',
    dag=dag,
)

dbt_test_curated = BashOperator(
    task_id='dbt_test_curated',
    bash_command='cd /opt/airflow/dbt && dbt test --select curated --profiles-dir .',
    dag=dag,
)

dbt_views = BashOperator(
    task_id='dbt_run_views',
    bash_command='cd /opt/airflow/dbt && dbt run --select views --vars \'{"batch_date": "{{ ds }}"}\' --profiles-dir .',
    dag=dag,
)

log_success = PythonOperator(
    task_id='log_pipeline_success',
    python_callable=log_pipeline_success,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)

log_failure = PythonOperator(
    task_id='log_pipeline_failure',
    python_callable=log_pipeline_failure,
    provide_context=True,
    trigger_rule='one_failed',
    dag=dag,
)

log_start >> validate_s3 >> dbt_structured >> dbt_test_structured
dbt_test_structured >> dbt_curated >> dbt_test_curated >> dbt_views
dbt_views >> log_success
[validate_s3, dbt_structured, dbt_test_structured, dbt_curated, dbt_test_curated, dbt_views] >> log_failure

