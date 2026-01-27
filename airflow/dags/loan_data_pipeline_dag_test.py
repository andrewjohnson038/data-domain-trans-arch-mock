#############################################################
# ------ RUN TEST DAGS (run test csv's in repo) ---------- #
#############################################################

import os
from datetime import datetime, timedelta  # datetime for logging timestamps, timedelta for retry delays
from airflow import DAG  # Core Airflow DAG object
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator  # Run SQL in Snowflake
from airflow.operators.bash import BashOperator  # Execute shell commands (dbt)
from airflow.operators.python import PythonOperator  # Execute Python functions
from airflow.utils.dates import days_ago  # Helper for relative start dates

# Use absolute path to the logging_test folder inside your repo
LOG_DIR = os.path.join(os.getcwd(), "logging_test")  # Folder for test logs
FAILURE_LOG = os.path.join(LOG_DIR, "failure.log")  # Failure log file
SUCCESS_LOG = os.path.join(LOG_DIR, "success.log")  # Success log file

default_args = {  # Default arguments applied to all tasks
    'owner': 'data_engineering',  # Logical owner shown in Airflow UI
    'depends_on_past': False,  # Do not wait for previous DAG run state
    'email_on_failure': False,  # Send email alerts on task failure
    'retries': 2,  # Number of retry attempts per task
    'retry_delay': timedelta(minutes=5),  # Wait time between retries
}

dag = DAG(  # Define the Airflow DAG
    'loan_data_pipeline_test',  # Unique DAG ID for test DAG
    default_args=default_args,  # Apply default task arguments
    description='Test DAG for loan transaction data',  # DAG description
    schedule_interval=None,  # Run on-demand
    start_date=days_ago(1),  # Start DAG from yesterday
    catchup=False,  # Do not backfill missed runs
    tags=['lending', 'test'],  # Tags for UI filtering
)


# Log Start of DAG execution to repo file success.log file
def log_pipeline_start(**context):
    os.makedirs(LOG_DIR, exist_ok=True)  # Ensure logging folder exists
    with open(SUCCESS_LOG, 'a') as f:  # Open success log file
        f.write(f"{datetime.now()} - Pipeline TEST started for batch_date: {context['ds']}\n")  # Write start entry


# Log Success of DAG execution to repo success.log file
def log_pipeline_success(**context):
    os.makedirs(LOG_DIR, exist_ok=True)  # Ensure logging folder exists
    with open(SUCCESS_LOG, 'a') as f:  # Open success log file
        f.write(f"{datetime.now()} - Pipeline TEST completed successfully for batch_date: {context['ds']}\n")  # Write success entry


# Log Failure of DAG execution to repo file failure.log file
def log_pipeline_failure(**context):
    os.makedirs(LOG_DIR, exist_ok=True)  # Ensure logging folder exists
    with open(FAILURE_LOG, 'a') as f:  # Open failure log file
        f.write(f"{datetime.now()} - Pipeline TEST failed for batch_date: {context['ds']}\n")  # Write failure entry


# Task to log pipeline start
log_start = PythonOperator(
    task_id='log_pipeline_start',  # Task identifier
    python_callable=log_pipeline_start,  # Function to execute
    provide_context=True,  # Pass Airflow execution context
    dag=dag,  # Assign task to DAG
)

# Task to log pipeline start
validate_s3 = SnowflakeOperator(
    task_id='validate_s3_files',  # Task identifier
    snowflake_conn_id='snowflake_default',  # Snowflake connection ID
    sql="LIST @raw_data_stage;",  # List files in Snowflake stage
    dag=dag,  # Assign task to DAG
)

# Task to log pipeline start
dbt_structured = BashOperator(
    task_id='dbt_run_structured',  # Task identifier
    bash_command='cd /opt/airflow/dbt && dbt run --select structured --vars \'{"batch_date": "{{ ds }}"}\' --profiles-dir .',  # Execute dbt run
    dag=dag,  # Assign task to DAG
)

# Task to log pipeline start
dbt_test_structured = BashOperator(
    task_id='dbt_test_structured',  # Task identifier
    bash_command='cd /opt/airflow/dbt && dbt test --select structured --profiles-dir .',  # Execute dbt tests
    dag=dag,  # Assign task to DAG
)

# Task to log pipeline start
dbt_curated = BashOperator(
    task_id='dbt_run_curated',  # Task identifier
    bash_command='cd /opt/airflow/dbt && dbt run --select curated --vars \'{"batch_date": "{{ ds }}"}\' --profiles-dir .',  # Execute dbt run
    dag=dag,  # Assign task to DAG
)

# Task to log pipeline start
dbt_test_curated = BashOperator(
    task_id='dbt_test_curated',  # Task identifier
    bash_command='cd /opt/airflow/dbt && dbt test --select curated --profiles-dir .',  # Execute dbt tests
    dag=dag,  # Assign task to DAG
)

# Task to log pipeline start
dbt_views = BashOperator(
    task_id='dbt_run_views',  # Task identifier
    bash_command='cd /opt/airflow/dbt && dbt run --select views --vars \'{"batch_date": "{{ ds }}"}\' --profiles-dir .',  # Execute dbt run
    dag=dag,  # Assign task to DAG
)

# Task to log pipeline success
log_success = PythonOperator(
    task_id='log_pipeline_success',  # Task identifier
    python_callable=log_pipeline_success,  # Function to execute
    provide_context=True,  # Pass Airflow execution context
    trigger_rule='all_success',  # Run only if all upstream tasks succeed
    dag=dag,  # Assign task to DAG
)

# Task to log pipeline failure
log_failure = PythonOperator(
    task_id='log_pipeline_failure',  # Task identifier
    python_callable=log_pipeline_failure,  # Function to execute
    provide_context=True,  # Pass Airflow execution context
    trigger_rule='one_failed',  # Run if any upstream task fails
    dag=dag,  # Assign task to DAG
)

# ---------- Task dependencies for TEST DAG ----------
log_start >> dbt_test_structured >> dbt_test_curated >> log_success  # Main test execution chain
[dbt_test_structured, dbt_test_curated] >> log_failure  # Log failure on any test failure
