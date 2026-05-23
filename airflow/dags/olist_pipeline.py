from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


# Define variables for paths to avoid repetition and make it easier to update in the future
PROJECT = "/mnt/d/Practise\\ Projects/E-commerce"
VENV = f"{PROJECT}/venv-linux/bin/activate"
DBT = f"{PROJECT}/olist_dbt"



# default args apply to every task in the DAG
default_args = {
    'owner': 'olist',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['abaqasif@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}


with DAG("olist_pipeline", schedule="@daily", start_date=datetime(2024, 1, 1),catchup=False) as dag:

    bronze = BashOperator(
        task_id="spark_bronze", 
        bash_command=f"source {VENV} && python {PROJECT}/jobs/bronze_ingest.py"
        )
    
    silver = BashOperator(
        task_id="spark_silver", 
        bash_command=f"source {VENV} && python {PROJECT}/jobs/silver_transform.py"
        )
    
    dbt_run = BashOperator(
        task_id="dbt_run", 
        bash_command=f"source {VENV} && cd {DBT} && dbt run"
        )
    dbt_test = BashOperator(
        task_id="dbt_test", 
        bash_command=f"source {VENV} && cd {DBT}  && dbt test"
        )
    

    bronze >> silver >> dbt_run >> dbt_test