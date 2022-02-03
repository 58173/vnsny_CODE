
import pandas as pd
import datetime as dt
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.oracle_operator import OracleOperator

default_args = {
    'owner': 'Lin.Wang',
    'start_date': dt.datetime(2020,2,1),
    'email': ['lin.wang@vnsny.org'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG(dag_id = 'OUT_JDCD_CHO_REFRESH_FACT_PAID_CLAIMS1',
          default_args= default_args, 
          catchup=False
         ) 

opr_sql = OracleOperator(task_id='OUT_JDCD_CHO_REFRESH_FACT_PAID_CLAIMS1',
                        oracle_conn_id='CHOICEBI',
                        sql= 'begin CHOICEBI.P_REFRESH_MV;END;',
                        autocommit ='True',
                        dag=dag)   