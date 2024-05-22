# Databricks notebook source
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_pentaho.operators.CarteJobOperator import CarteJobOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
import pendulum


local_tz = schedule_interval='America/Sao_Paulo'

DAG_NAME = "open_brewery_db_api"

DEFAULT_ARGS = {
    'owner': 'tiago.vieira',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['tiagozvieira@gmail.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False
}

with DAG(dag_id=DAG_NAME,
         default_args=DEFAULT_ARGS,
         tags=["open_brewery_db_api"],
         schedule_interval='0 7 * * *',
         catchup=False,
         dagrun_timeout=timedelta(hours=2)
         ) as dag:
    
    notebook_run = DatabricksRunNowOperator(
        task_id="notebook_run",
        job_id="261770633869419",
        dag=dag
        )
 
notebook_run
