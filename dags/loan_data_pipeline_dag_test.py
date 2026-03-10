#############################################################
# ------ RUN TEST DAGS (run test csv's in repo) ---------- #
# loan_data_pipeline_dag_test.py
#############################################################

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dotenv import find_dotenv, load_dotenv
import traceback

# Paths
LOG_DIR = os.path.join(os.getcwd(), "logging_test")
FAILURE_LOG = os.path.join(LOG_DIR, "failure.log")
SUCCESS_LOG = os.path.join(LOG_DIR, "success.log")

# get env directory path for repo dag location
load_dotenv(find_dotenv())
BASE_DIR = os.environ.get('DBT_PROJECT_DIR')

DBT_DIR = os.path.join(BASE_DIR, 'dbt')  # dbt project folder
BASE_OUTPUT_DIR = os.path.join(BASE_DIR, '../example_data')  # output folder for CSV exports
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


# log start of pipeline
def log_pipeline_start(**context):
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(SUCCESS_LOG, 'a') as f:
        f.write(f"{datetime.now()} - Pipeline TEST started for batch_date: {context['ds']}\n")


# add dependencies
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command=f'dbt deps --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}',
    dag=dag,
)


# lists all successful tasks and the task that caused to fail if so to the logging_test folder. Will log as all success if all successful
def log_pipeline_success(**context):
    os.makedirs(LOG_DIR, exist_ok=True)

    with open(SUCCESS_LOG, "a") as f:
        f.write(
            f"{datetime.now()} - Pipeline TEST completed for batch_date: {context['ds']}\n"
        )
        f.write("All tasks completed successfully.\n\n")


# logs failed task and what caused it to the logging_test folder in failure.log
def log_pipeline_failure(**context):
    os.makedirs(LOG_DIR, exist_ok=True)

    ti = context.get("ti")  # Airflow 3 uses "ti" not "task_instance"
    exception = context.get("exception")

    with open(FAILURE_LOG, "a") as f:
        f.write(
            f"{datetime.now()} - Pipeline TEST failed for batch_date: {context.get('ds', 'unknown')}\n"
        )

        if ti:
            f.write(f"DAG: {ti.dag_id} | Task: {ti.task_id}\n")

        if exception:
            f.write(f"Error: {str(exception)}\n")
            f.write("Traceback:\n")
            f.write("".join(traceback.format_exception(type(exception), exception, exception.__traceback__)))
        else:
            f.write("No exception details available.\n")

        f.write("\n")


def export_results_to_csv(**context):
    """Export test results from DuckDB to CSV files in layer-specific folders"""
    import duckdb
    from dotenv import find_dotenv, load_dotenv

    # Reload env vars at task execution time
    load_dotenv(find_dotenv())

    # Derive path relative to DAG file location
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.environ.get('DBT_PROJECT_DIR') or os.path.dirname(dag_dir)
    base_output_dir = os.path.join(base_dir, 'example_data')
    duckdb_path = os.path.join(base_output_dir, 'test.duckdb')

    os.makedirs(f"{base_output_dir}/structured_layer", exist_ok=True)
    os.makedirs(f"{base_output_dir}/curated_layer", exist_ok=True)
    os.makedirs(f"{base_output_dir}/view_layer", exist_ok=True)

    try:
        conn = duckdb.connect(duckdb_path, read_only=True)

        layer_mapping = {
            'structured_layer': [
                'main_structured_zone.customer_demographics',
                'main_structured_zone.sys_a_loan_trans',
                'main_structured_zone.sys_b_loan_trans',
                'main_structured_zone.country_ref',
                'main_structured_zone.country_state_ref'
            ],
            'curated_layer': [
                'main_curated_zone.tbl_account',
                'main_curated_zone.tbl_account_loan',
                'main_curated_zone.tbl_customer',
                'main_curated_zone.tbl_loan'
            ],
            'view_layer': [
                'main_curated_zone.monthly_loan_sales'
            ]
        }

        exported_count = 0

        for layer_folder, table_list in layer_mapping.items():
            layer_path = f"{base_output_dir}/{layer_folder}"

            for table_name in table_list:
                try:
                    short_name = table_name.split('.')[-1]
                    csv_path = f"{layer_path}/{short_name}.csv"
                    conn.execute(f"COPY {table_name} TO '{csv_path}' (HEADER, DELIMITER ',')")

                    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    print(f"✅ Exported {short_name} ({row_count} rows) to {csv_path}")
                    exported_count += 1

                except Exception as e:
                    print(f"⚠️  Warning: Could not export {table_name}: {e}")

        conn.close()
        print(f"\n✅ Successfully exported {exported_count} tables to {base_output_dir}")

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
    bash_command=f'dbt seed --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target test --no-partial-parse',
    dag=dag,
)
# Task: Run structured layer models
dbt_run_structured = BashOperator(
    task_id='dbt_run_structured',
    bash_command=f'dbt run --select structured --no-partial-parse --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target test',
    dag=dag,
)

# Task: Test structured layer
dbt_test_structured = BashOperator(
    task_id='dbt_test_structured',
    bash_command=f'dbt test --select structured --no-partial-parse --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target test',
    dag=dag,
)

# Task: Run curated layer models
dbt_run_curated = BashOperator(
    task_id='dbt_run_curated',
    bash_command=f'dbt run --select curated --no-partial-parse --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target test',
    dag=dag,
)

# Task: Test curated layer
dbt_test_curated = BashOperator(
    task_id='dbt_test_curated',
    bash_command=f'dbt test --select curated --no-partial-parse --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target test',
    dag=dag,
)

# Task: Run views layer
dbt_run_views = BashOperator(
    task_id='dbt_run_views',
    bash_command=f'dbt run --select views --no-partial-parse --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target test',
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
log_start >> cleanup >> dbt_seed >> dbt_run_structured >> dbt_test_structured >> dbt_run_curated >> dbt_test_curated >> dbt_run_views >> export_csv >> log_success

[dbt_seed, dbt_run_structured, dbt_test_structured, dbt_run_curated, dbt_test_curated, dbt_run_views, export_csv] >> log_failure
