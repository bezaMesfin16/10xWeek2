from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner':'beza',
    'retries':'1',
    'retry_delay':timedelta(minutes = 3)
}

connection_params = {
  "host": "host.docker.internal",
  "user": "airflow",
  "password": "airflow",
  "port": "5432",
  "schema":"DWH",
  "database":"airflow"
}

def import_data_from_csv():
    track_info_df = pd.read_csv('/opt/airflow/data/track_info.csv')
    trajectory_df = pd.read_csv('/opt/airflow/data/trajectory_info.csv')

    engine = create_engine(f"postgresql+psycopg2://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}")

    load_data_to_db(track_info_df,'track_info',engine)
    load_data_to_db(trajectory_df,'trajectory_info',engine)

def load_data_to_db(df,table_name,engine):
    df.to_sql(table_name, con=engine, schema= connection_params['schema'],if_exists='replace', index=False)
    

with DAG(
    dag_id='dag_with_python_operator',
    default_args = default_args,
    start_date=datetime(2023,12,24),
    schedule_interval = '0 0 * * *'
)as dag:

    task_load_data = PythonOperator(
        task_id = 'extract_and_load_data',
        python_callable=import_data_from_csv
    )
    task_load_data