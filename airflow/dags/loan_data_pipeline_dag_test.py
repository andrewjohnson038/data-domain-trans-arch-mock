#############################################################
# ------ RUN TEST DAGS (run test csv's in repo) ---------- #
#############################################################

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dotenv import find_dotenv, load_dotenv

# Paths
LOG_DIR = os.path.join(os.getcwd(), "logging_test")
FAILURE_LOG = os.path.join(LOG_DIR, "failure.log")
SUCCESS_LOG = os.path.join(LOG_DIR, "success.log")

# get env directory path for repo dag location
load_dotenv(find_dotenv())
BASE_DIR = os.environ.get('DBT_PROJECT_DIR')

DBT_DIR = os.path.join(BASE_DIR, 'dbt')  # dbt project folder
BASE_OUTPUT_DIR = os.path.join(BASE_DIR, '../../example_data')  # output folder for CSV exports
DUCKDB_PATH = os.path.join(BASE_OUTPUT_DIR, 'test.duckdb')  # temporary DuckDB file

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'loan_data_pipeline_test',
    default_args=default_args,
    description='Test DAG for loan transaction data using CSV seeds',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['lending', 'test'],
)


def log_pipeline_start(**context):
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(SUCCESS_LOG, 'a') as f:
        f.write(f"{datetime.now()} - Pipeline TEST started for batch_date: {context['ds']}\n")


def log_pipeline_success(**context):
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(SUCCESS_LOG, 'a') as f:
        f.write(f"{datetime.now()} - Pipeline TEST completed successfully for batch_date: {context['ds']}\n")


def log_pipeline_failure(**context):
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(FAILURE_LOG, 'a') as f:
        f.write(f"{datetime.now()} - Pipeline TEST failed for batch_date: {context['ds']}\n")


def export_results_to_csv(**context):
    """Export test results from DuckDB to CSV files in layer-specific folders"""
    import duckdb

    os.makedirs(f"{BASE_OUTPUT_DIR}/structured_layer", exist_ok=True)
    os.makedirs(f"{BASE_OUTPUT_DIR}/curated_layer", exist_ok=True)
    os.makedirs(f"{BASE_OUTPUT_DIR}/view_layer", exist_ok=True)

    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)

        layer_mapping = {
            'structured_layer': [
                'customer_demographics',
                'sys_a_loan_trans',
                'sys_b_loan_trans',
                'country_ref',
                'country_state_ref'
            ],
            'curated_layer': [
                'tbl_account',
                'tbl_account_loan',
                'tbl_customer',
                'tbl_loan'
            ],
            'view_layer': [
                'monthly_loan_sales'
            ]
        }

        exported_count = 0

        for layer_folder, table_list in layer_mapping.items():
            layer_path = f"{BASE_OUTPUT_DIR}/{layer_folder}"

            for table_name in table_list:
                try:
                    table_exists = conn.execute(
                        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
                    ).fetchone()[0]

                    if table_exists > 0:
                        csv_path = f"{layer_path}/{table_name}.csv"
                        conn.execute(f"COPY {table_name} TO '{csv_path}' (HEADER, DELIMITER ',')")

                        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                        print(f"✅ Exported {table_name} ({row_count} rows) to {csv_path}")
                        exported_count += 1
                    else:
                        print(f"⚠️  Table {table_name} not found in DuckDB, skipping...")

                except Exception as e:
                    print(f"⚠️  Warning: Could not export {table_name}: {e}")

        conn.close()
        print(f"\n📊 Successfully exported {exported_count} tables to {BASE_OUTPUT_DIR}")

    except Exception as e:
        print(f"❌ Error during export: {e}")
        raise


def cleanup_duckdb(**context):
    """Remove temporary DuckDB file"""
    try:
        if os.path.exists(DUCKDB_PATH):
            os.remove(DUCKDB_PATH)
            print(f"🗑️  Cleaned up {DUCKDB_PATH}")
    except Exception as e:
        print(f"⚠️  Warning: Could not remove DuckDB file: {e}")


# Task: Log pipeline start
log_start = PythonOperator(
    task_id='log_pipeline_start',
    python_callable=log_pipeline_start,
    dag=dag,
)

# Task: Load seed data (test CSVs) into DuckDB
dbt_seed = BashOperator(
    task_id='dbt_seed',
    bash_command=f'dbt seed --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target test',
    dag=dag,
)
# Task: Run structured layer models
dbt_run_structured = BashOperator(
    task_id='dbt_run_structured',
    bash_command=f'dbt run --select structured --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target test',
    dag=dag,
)

# Task: Test structured layer
dbt_test_structured = BashOperator(
    task_id='dbt_test_structured',
    bash_command=f'dbt test --select structured --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target test',
    dag=dag,
)

# Task: Run curated layer models
dbt_run_curated = BashOperator(
    task_id='dbt_run_curated',
    bash_command=f'dbt run --select curated --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target test',
    dag=dag,
)

# Task: Test curated layer
dbt_test_curated = BashOperator(
    task_id='dbt_test_curated',
    bash_command=f'dbt test --select curated --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target test',
    dag=dag,
)

# Task: Run views layer
dbt_run_views = BashOperator(
    task_id='dbt_run_views',
    bash_command=f'dbt run --select views --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target test',
    dag=dag,
)

# Task: Export DuckDB tables to CSV
export_csv = PythonOperator(
    task_id='export_results_to_csv',
    python_callable=export_results_to_csv,
    dag=dag,
)

# Task: Cleanup DuckDB file
cleanup = PythonOperator(
    task_id='cleanup_duckdb',
    python_callable=cleanup_duckdb,
    trigger_rule='all_done',
    dag=dag,
)

# Task: Log pipeline success
log_success = PythonOperator(
    task_id='log_pipeline_success',
    python_callable=log_pipeline_success,
    trigger_rule='all_success',
    dag=dag,
)

# Task: Log pipeline failure
log_failure = PythonOperator(
    task_id='log_pipeline_failure',
    python_callable=log_pipeline_failure,
    trigger_rule='one_failed',
    dag=dag,
)

# ---------- Task dependencies for TEST DAG ----------
log_start >> dbt_seed >> dbt_run_structured >> dbt_test_structured >> dbt_run_curated >> dbt_test_curated >> dbt_run_views >> export_csv >> cleanup >> log_success

[dbt_seed, dbt_run_structured, dbt_test_structured, dbt_run_curated, dbt_test_curated, dbt_run_views, export_csv] >> log_failure
cleanup >> log_failure
