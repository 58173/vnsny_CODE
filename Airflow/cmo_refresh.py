import pandas as pd
import datetime as dt
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.oracle_hook import OracleHook
from airflow.operators.email_operator import EmailOperator
from airflow.operators.oracle_operator import OracleOperator
from airflow.sensors.external_task_sensor import ExternalTaskMarker, ExternalTaskSensor
from datetime import timedelta

default_args = {
'owner': 'Lin.Wang',
'depends_on_past':False,
'start_date': dt.datetime(2020,2,24),
'email': ['lin.wang@vnsny.org', 'Ripul.Patel@vnsny.org','Neha.Teli@vnsny.org'],
'email_on_failure': True,
'email_on_retry': True,
'retries': 1}

dag = DAG(dag_id = 'CMO_Quality',
          default_args= default_args, 
          catchup=False,
          schedule_interval='0 12 * * *',
         description = 'CMO Quality Dashbaord data refresh') 

opr_sql = OracleOperator(
    task_id='refresh_data',
    oracle_conn_id='POP_HEALTH_BI',
    sql= 'begin POP_HEALTH_BI.P_MEASURES;END;',
    autocommit ='True',
    dag=dag)             