import pandas as pd
import datetime as dt
import logging
import sqlalchemy as sqla
import string
import platform
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
#from airflow.hooks.mssql_hook import MsSqlHook
#from airflow.hooks.oracle_hook import OracleHook
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
#from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email_operator import EmailOperator
#from airflow.operators.oracle_operator import OracleOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow import AirflowException
from airflow.models import DagRun
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow.sensors.sql import SqlSensor
from airflow.models import Variable
from sqlalchemy import create_engine

default_args = {
    'owner': 'Neha.Teli',
    'depends_on_past': False,
    'start_date': dt.datetime(2021,12,20),
    'email': ['neha.teli@vnsny.org', 'Ripul.Patel@vnsny.org', 'Lin.Wang@vnsny.org'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=15),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}        

AIRFLOW_ENVIRONMENT = Variable.get("AIRFLOW_ENVIRONMENT")
ENV=AIRFLOW_ENVIRONMENT.lower()

def LU_table_refresh_process(**kwargs):
    print(kwargs['tablename'])
    
  
    query="""
        call bia.choicebi.BI_TBL_REFRESH_LOAD('""" + kwargs['tablename'] + """')
    """
    print(query)
    
    # connect to snowflake database 
    try: 
        SF_hook = SnowflakeHook(snowflake_conn_id='SF_BIABI')
        conn=SF_hook.get_conn()
        cursor = conn.cursor()

        # need to work on error handleing 
		
    finally: 
        if cursor: 
            cursor.close() 

dag = DAG(
    'LU_Tables_Refresh_snowflake',
    default_args=default_args,
    catchup=False,
    schedule_interval='30 21 * * *', #9.30 PM EST
    description='LU_Tables_Refresh_snowflake'
)

# Dummy operator is set for upstream dependency to work
DUMMY_OPERATOR=DummyOperator(
    task_id='DUMMY_OPERATOR',
    dag=dag,
    )

#Task1 - refresh LU_DAY STAGE table data for all the years
T1_REFRESH_TEMP_LU_DAY_STAGE=PythonOperator(
    task_id='T1_REFRESH_TEMP_LU_DAY_STAGE',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'TEMP_LU_DAY_STAGE_DETAILS'},
    dag=dag
    )    
T1_REFRESH_TEMP_LU_DAY_STAGE.set_upstream(DUMMY_OPERATOR)

#Task2 - refresh LU_DAY table data until next year from current_date
T2_REFRESH_LU_DAY=PythonOperator(
    task_id='T2_REFRESH_LU_DAY',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'LU_DAY'},
    dag=dag
    )    
T2_REFRESH_LU_DAY.set_upstream(T1_REFRESH_TEMP_LU_DAY_STAGE)

#Task3 - refresh LU_MONTH_STAGE table data for all the years
T3_REFRESH_TEMP_LU_MONTH_STAGE=PythonOperator(
    task_id='T3_REFRESH_TEMP_LU_MONTH_STAGE',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'TEMP_LU_MONTH_STAGE_DETAILS'},
    dag=dag
    )    
T3_REFRESH_TEMP_LU_MONTH_STAGE.set_upstream(T2_REFRESH_LU_DAY)

#Task4 - refresh LU_MONTH table data until next year from current_date
T4_REFRESH_LU_MONTH=PythonOperator(
    task_id='T4_REFRESH_LU_MONTH',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'LU_MONTH'},
    dag=dag
    )    
T4_REFRESH_LU_MONTH.set_upstream(T3_REFRESH_TEMP_LU_MONTH_STAGE)

#Task5 - refresh LU_QUARTER_STAGE table data for all the years
T5_REFRESH_TEMP_LU_QUARTER_STAGE=PythonOperator(
    task_id='T5_REFRESH_TEMP_LU_QUARTER_STAGE',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'TEMP_LU_QUARTER_STAGE_DETAILS'},
    dag=dag
    )    
T5_REFRESH_TEMP_LU_QUARTER_STAGE.set_upstream(T4_REFRESH_LU_MONTH)

#Task6 - refresh LU_QUARTER table data until next year from current_date
T6_REFRESH_LU_QUARTER=PythonOperator(
    task_id='T6_REFRESH_LU_QUARTER',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'LU_QUARTER'},
    dag=dag
    )    
T6_REFRESH_LU_QUARTER.set_upstream(T5_REFRESH_TEMP_LU_QUARTER_STAGE)

#Task7 - refresh LU_SEMI_ANN_STAGE table data for all the years
T7_REFRESH_TEMP_LU_SEMI_ANN_STAGE=PythonOperator(
    task_id='T7_REFRESH_TEMP_LU_SEMI_ANN_STAGE',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'TEMP_LU_SEMI_ANN_STAGE_DETAILS'},
    dag=dag
    )    
T7_REFRESH_TEMP_LU_SEMI_ANN_STAGE.set_upstream(T6_REFRESH_LU_QUARTER)

#Task8- refresh LU_SEMI_ANNUAL table data until next year from current_date
T8_REFRESH_LU_SEMI_ANN_STAGE=PythonOperator(
    task_id='T8_REFRESH_LU_SEMI_ANN_STAGE',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'LU_SEMI_ANNUAL'},
    dag=dag
    )    
T8_REFRESH_LU_SEMI_ANN_STAGE.set_upstream(T7_REFRESH_TEMP_LU_SEMI_ANN_STAGE)

#Task9- refresh LU_YEAR_STAGE table data for all the years
T9_REFRESH_TEMP_LU_YEAR_STAGE=PythonOperator(
    task_id='T9_REFRESH_TEMP_LU_YEAR_STAGE',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'TEMP_LU_YEAR_STAGE_DETAILS'},
    dag=dag
    )    
T9_REFRESH_TEMP_LU_YEAR_STAGE.set_upstream(T8_REFRESH_LU_SEMI_ANN_STAGE)

#Task10- refresh LU_YEAR table data until next year from current_date
T10_REFRESH_LU_YEAR=PythonOperator(
    task_id='T10_REFRESH_LU_YEAR',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'LU_YEAR'},
    dag=dag
    )    
T10_REFRESH_LU_YEAR.set_upstream(T9_REFRESH_TEMP_LU_YEAR_STAGE)

#Task11- refresh PREV_SEMI_SIX_MONTHS_BIANNUAL table data 
T11_REFRESH_PREV_SEMI_SIX_MONTHS_BIANNUAL=PythonOperator(
    task_id='T11_REFRESH_PREV_SEMI_SIX_MONTHS_BIANNUAL',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'PREV_SEMI_SIX_MONTHS_BIANNUAL'},
    dag=dag
    )    
T11_REFRESH_PREV_SEMI_SIX_MONTHS_BIANNUAL.set_upstream(T10_REFRESH_LU_YEAR)

#Task12- refresh PREV_13_CHHA_WEEKS stage table data 
T12_REFRESH_TEMP_PREV_13_CHHA_WEEKS=PythonOperator(
    task_id='T12_REFRESH_TEMP_PREV_13_CHHA_WEEKS',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'TEMP_PREV_13_CHHA_WEEKS_RANK_DETAIL'},
    dag=dag
    )    
T12_REFRESH_TEMP_PREV_13_CHHA_WEEKS.set_upstream(T11_REFRESH_PREV_SEMI_SIX_MONTHS_BIANNUAL)

#Task13- refresh PREV_THIRTEEN_CHHA_WEEKS table data 
T13_REFRESH_PREV_THIRTEEN_CHHA_WEEKS=PythonOperator(
    task_id='T13_REFRESH_PREV_THIRTEEN_CHHA_WEEKS',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'PREV_THIRTEEN_CHHA_WEEKS'},
    dag=dag
    )    
T13_REFRESH_PREV_THIRTEEN_CHHA_WEEKS.set_upstream(T12_REFRESH_TEMP_PREV_13_CHHA_WEEKS)

#Task14- refresh TEMP tables data for previous months
T14_REFRESH_TEMP_RANK_MONTHS_1_DETAILS=PythonOperator(
    task_id='T14_REFRESH_TEMP_RANK_MONTHS_1_DETAILS',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'TEMP_RANK_MONTHS_1_DETAILS'},
    dag=dag
    )    
T14_REFRESH_TEMP_RANK_MONTHS_1_DETAILS.set_upstream(T13_REFRESH_PREV_THIRTEEN_CHHA_WEEKS)

#Task15- refresh TEMP tables data for previous months
T15_REFRESH_TEMP_RANK_MONTHS_2_DETAILS=PythonOperator(
    task_id='T15_REFRESH_TEMP_RANK_MONTHS_2_DETAILS',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'TEMP_RANK_MONTHS_2_DETAILS'},
    dag=dag
    )    
T15_REFRESH_TEMP_RANK_MONTHS_2_DETAILS.set_upstream(T14_REFRESH_TEMP_RANK_MONTHS_1_DETAILS)

#Task16- refresh PREV_THREE_MONTHS table data 
T16_REFRESH_PREV_THREE_MONTHS=PythonOperator(
    task_id='T16_REFRESH_PREV_THREE_MONTHS',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'PREV_THREE_MONTHS'},
    dag=dag
    )    
T16_REFRESH_PREV_THREE_MONTHS.set_upstream(T15_REFRESH_TEMP_RANK_MONTHS_2_DETAILS)

#Task17- refresh PREV_TRAIL_SIX_MONTHS table data 
T17_REFRESH_PREV_TRAIL_SIX_MONTHS=PythonOperator(
    task_id='T17_REFRESH_PREV_TRAIL_SIX_MONTHS',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'PREV_TRAIL_SIX_MONTHS'},
    dag=dag
    )    
T17_REFRESH_PREV_TRAIL_SIX_MONTHS.set_upstream(T16_REFRESH_PREV_THREE_MONTHS)

#Task18- refresh PREV_TRAIL_SIX_MONTHS_YTD table data 
T18_REFRESH_PREV_TRAIL_SIX_MONTHS_YTD=PythonOperator(
    task_id='T18_REFRESH_PREV_TRAIL_SIX_MONTHS_YTD',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'PREV_TRAIL_SIX_MONTHS_YTD'},
    dag=dag
    )    
T18_REFRESH_PREV_TRAIL_SIX_MONTHS_YTD.set_upstream(T17_REFRESH_PREV_TRAIL_SIX_MONTHS)

#Task19- refresh PREV_TRAIL_THREE_MONTHS table data 
T19_REFRESH_PREV_TRAIL_THREE_MONTHS=PythonOperator(
    task_id='T19_REFRESH_PREV_TRAIL_THREE_MONTHS',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'PREV_TRAIL_THREE_MONTHS'},
    dag=dag
    )    
T19_REFRESH_PREV_TRAIL_THREE_MONTHS.set_upstream(T18_REFRESH_PREV_TRAIL_SIX_MONTHS_YTD)

#Task20- refresh PREV_TWELVE_MONTHS table data 
T20_REFRESH_PREV_TWELVE_MONTHS=PythonOperator(
    task_id='T20_REFRESH_PREV_TWELVE_MONTHS',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'PREV_TWELVE_MONTHS'},
    dag=dag
    )    
T20_REFRESH_PREV_TWELVE_MONTHS.set_upstream(T19_REFRESH_PREV_TRAIL_THREE_MONTHS)

#Task21- refresh PRIOR_YEAR_MONTH table data 
T21_REFRESH_PRIOR_YEAR_MONTH=PythonOperator(
    task_id='T21_REFRESH_PRIOR_YEAR_MONTH',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'PRIOR_YEAR_MONTH'},
    dag=dag
    )    
T21_REFRESH_PRIOR_YEAR_MONTH.set_upstream(T20_REFRESH_PREV_TWELVE_MONTHS)

#Task22- refresh PY_YTD_DAY table data 
T22_REFRESH_PY_YTD_DAY=PythonOperator(
    task_id='T22_REFRESH_PY_YTD_DAY',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'PY_YTD_DAY'},
    dag=dag
    )    
T22_REFRESH_PY_YTD_DAY.set_upstream(T21_REFRESH_PRIOR_YEAR_MONTH)

#Task23- refresh QTD_DAY table data 
T23_REFRESH_QTD_DAY=PythonOperator(
    task_id='T23_REFRESH_QTD_DAY',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'QTD_DAY'},
    dag=dag
    )    
T23_REFRESH_QTD_DAY.set_upstream(T22_REFRESH_PY_YTD_DAY)

#Task24- refresh QTD_MONTH table data 
T24_REFRESH_QTD_MONTH=PythonOperator(
    task_id='T24_REFRESH_QTD_MONTH',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'QTD_MONTH'},
    dag=dag
    )    
T24_REFRESH_QTD_MONTH.set_upstream(T23_REFRESH_QTD_DAY)

#Task25- refresh YTD_DAY table data 
T25_REFRESH_YTD_DAY=PythonOperator(
    task_id='T25_REFRESH_YTD_DAY',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'YTD_DAY'},
    dag=dag
    )    
T25_REFRESH_YTD_DAY.set_upstream(T24_REFRESH_QTD_MONTH)

#Task26- refresh YTD_MONTH table data 
T26_REFRESH_YTD_MONTH=PythonOperator(
    task_id='T26_REFRESH_YTD_MONTH',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'YTD_MONTH'},
    dag=dag
    )    
T26_REFRESH_YTD_MONTH.set_upstream(T25_REFRESH_YTD_DAY)

#Task27- refresh YTD_MONTH_DAY table data 
T27_REFRESH_YTD_MONTH_DAY=PythonOperator(
    task_id='T27_REFRESH_YTD_MONTH_DAY',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'YTD_MONTH_DAY'},
    dag=dag
    )    
T27_REFRESH_YTD_MONTH_DAY.set_upstream(T26_REFRESH_YTD_MONTH)

#Task28- refresh YTD_MONTH_DAYS table data 
T28_REFRESH_YTD_MONTH_DAYS=PythonOperator(
    task_id='T28_REFRESH_YTD_MONTH_DAYS',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'YTD_MONTH_DAYS'},
    dag=dag
    )    
T28_REFRESH_YTD_MONTH_DAYS.set_upstream(T27_REFRESH_YTD_MONTH_DAY)

#Task29- refresh YTD_QUARTER table data 
T29_REFRESH_YTD_QUARTER=PythonOperator(
    task_id='T29_REFRESH_YTD_QUARTER',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'YTD_QUARTER'},
    dag=dag
    )    
T29_REFRESH_YTD_QUARTER.set_upstream(T28_REFRESH_YTD_MONTH_DAYS)

#Task30- refresh LU_CHOICE_WEEK table data 
T30_REFRESH_LU_CHOICE_WEEK=PythonOperator(
    task_id='T30_REFRESH_LU_CHOICE_WEEK',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'LU_CHOICE_WEEK'},
    dag=dag
    )    
T30_REFRESH_LU_CHOICE_WEEK.set_upstream(T29_REFRESH_YTD_QUARTER)


#Task31- refresh LU_CHHA_WEEK table data 
T31_REFRESH_LU_CHHA_WEEK=PythonOperator(
    task_id='T31_REFRESH_LU_CHHA_WEEK',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'LU_CHHA_WEEK'},
    dag=dag
    )    
T31_REFRESH_LU_CHHA_WEEK.set_upstream(T30_REFRESH_LU_CHOICE_WEEK)

#Task32- refresh LU_PARTNERS_IN_CARE_WEEK table data 
T32_REFRESH_LU_PARTNERS_IN_CARE_WEEK=PythonOperator(
    task_id='T32_REFRESH_LU_PARTNERS_IN_CARE_WEEK',
    python_callable=LU_table_refresh_process,
    op_kwargs={'tablename': 'LU_PARTNERS_IN_CARE_WEEK'},
    dag=dag
    )    
T32_REFRESH_LU_PARTNERS_IN_CARE_WEEK.set_upstream(T31_REFRESH_LU_CHHA_WEEK)