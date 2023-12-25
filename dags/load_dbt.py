from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/postgres_dpt"
default_args = {
    'owner':'beza',
    'retries':1,
    'retry_delay':timedelta(minutes = 5)
}
with DAG(
    dag_id = "create_view_with_dag",
    start_date=datetime(2023, 12, 24),
    description="DAG that invokes dbt runs",
    schedule_interval=None,
    catchup=False,
    default_args=default_args, 
) as dag:
    
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir ../",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir ..",
    )

    dbt_doc_generate = BashOperator(
        task_id="dbt_doc_gen", 
        bash_command=f"dbt docs generate --project-dir {DBT_PROJECT_DIR} --profiles-dir .. ",

    )

dbt_run >> dbt_test >> dbt_doc_generate