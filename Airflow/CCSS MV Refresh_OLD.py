

import cx_Oracle
import pandas as pd
import pyodbc
import sqlalchemy
import datetime as dt
import string
import platform
import os
import logging


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
#from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.oracle_hook import OracleHook
#from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.oracle_operator import OracleOperator
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.dagrun_operator import TriggerDagRunOperator
# from airflow.operators.sensors import ExternalTaskSensor
# from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder
from airflow.models import DagRun
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow.sensors.sql import SqlSensor


default_args = {
    'owner': 'Lin Wang',
    'depends_on_past': False,
    'start_date': dt.datetime(2021,9,1),
    'email': ['Lin.Wang@vnsny.org','Neha.Teli@vnsny.org','Ripul.Patel@vnsny.org'],
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

def process_fmm(**kwargs):
    print(kwargs['mvname'])
    
    #today=dt.datetime.today()
    
    query="""
        begin
            etl.p_refresh_mv('""" + kwargs['mvname'] + """');
        end;
    """
    print(query)
    # Create a table in Oracle database 
    try: 
        sql_hook = OracleHook(oracle_conn_id='CHOICEBI')
        conn=sql_hook.get_conn()
        cursor = conn.cursor()

        # Creating a table srollno heading which is number 
        #conn.run(query) 
        cursor.execute(query)            
        print("Command executed successful") 
           
    except cx_Oracle.DatabaseError as e: 
        print("There is a problem with Oracle", e) 
        raise;
	#
        # by writing finally if any error occurs 
        # then also we can close the all database operation 
    finally: 
        if cursor: 
            cursor.close() 
        if conn: 
           conn.close() 


dag = DAG(
    'CCSS',
    default_args=default_args,
    catchup=False,
    schedule_interval='15 2 * * *', #2.15 AM EST
    description='CCSS MATERLIZED VIEWS REFRESH'
)


###############################################################
# Check DW_OWNER.F9_STATS data loading 

DUMMY_OPERATOR_All_NODE_In=DummyOperator(
    task_id='DUMMY_OPERATOR_All_NODE_In',
    dag=dag,
    )

# connection
def Insert_script():
    hook=OracleHook(oracle_conn_id='CHOICEBI')
    conn=hook.get_connection(conn_id='CHOICEBI')
    HOST=conn.host
    USER=conn.login
    PASSWORD=conn.password
    SCHEMA =conn.schema
    PORT=conn.port
    SID="NEXUS2"
    engine =sqla.create_engine("oracle://{user}:{password}@{host}:{port}/{sid}".format(user=USER, password=PASSWORD, host=HOST, database=SCHEMA, port=PORT, sid=SID))
    oracle_c = engine.connect()

    cnt_chk_DAS="""
select count(table_n) 
from DW_OWNER.F9_STATS
where trunc(updated) = (trunc(sysdate)-1)
and table_n in('F9_AGENT', 'F9_CALL_LOG', 'F9_CALL_SEGMENT','F9_ACD_QUEUE', 'F9_CONTACT', 'F9_DNIS')
group by trunc(updated)
"""

df_read_oracle_cont=pd.read_sql(cnt_chk_DAS,con=oracle_c)

cnt_chk_q=df_read_oracle_cont.iloc[0,0]

if  cnt_chk_q == 6:
    DUMMY_OPERATOR_All_NODE_In=DummyOperator(
    task_id='DUMMY_OPERATOR_All_NODE_In',
    dag=dag,
    )
else:
    print('source load is not complete')
    oracle_c.close() 
###############################################################





#Node1 - Task1: Refresh MV_DIM_F9_AGENT
N1T1_MV_DIM_F9_AGENT=PythonOperator(
    task_id='N1T1_MV_DIM_F9_AGENT',
    python_callable=process_fmm,
    op_kwargs={'mvname':'CHOICEBI.MV_DIM_F9_AGENT'},
    dag=dag
    )
N1T1_MV_DIM_F9_AGENT.set_upstream(DUMMY_OPERATOR_All_NODE_In)


#Node1 - Task2: #REFRESH SKILLS FROM ALL TABLES
N1T2_MERGE_INTO_MV_DIM_F9_SKILL=OracleOperator(
    task_id='N1T2_MERGE_INTO_MV_DIM_F9_SKILL',
    oracle_conn_id='CHOICEBI',
    sql="""
begin MERGE INTO MV_DIM_F9_SKILL A USING
(
select distinct * from
(
SELECT DISTINCT SKILL FROM DW_OWNER.F9_CALL_LOG WHERE SKILL IS NOT NULL
UNION ALL
SELECT DISTINCT SKILL FROM DW_OWNER.F9_ACD_QUEUE WHERE SKILL IS NOT NULL
UNION ALL
SELECT DISTINCT SKILL FROM DW_OWNER.F9_CALL_SEGMENT WHERE SKILL IS NOT NULL
UNION ALL
SELECT DISTINCT SKILL FROM DW_OWNER.F9_AGENT WHERE SKILL IS NOT NULL
union all
SELECT DISTINCT SKILL FROM CHOICEBI.V_DIM_F9_AGENT_SKILL_MAP WHERE SKILL IS NOT NULL
)
) B
ON (A.SKILL = B.SKILL)
WHEN NOT MATCHED THEN
INSERT (
dl_skill_sk,
SKILL)
VALUES
(
SEQ_F9_SKILL.NEXTVAL,
B.SKILL
);END;""",
    autocommit ='True',
    dag=dag
)
N1T2_MERGE_INTO_MV_DIM_F9_SKILL.set_upstream(DUMMY_OPERATOR_All_NODE_In)



#Node1 - Task3: #REFRESH CAMPAIGN FROM ALL TABLES
N1T3_MERGE_INTO_MV_DIM_F9_CAMPAIGN=OracleOperator(
    task_id='N1T3_MERGE_INTO_MV_DIM_F9_CAMPAIGN',
    oracle_conn_id='CHOICEBI',
    sql="""
begin MERGE INTO MV_DIM_F9_CAMPAIGN A USING
(
select distinct * from
(
SELECT DISTINCT CAMPAIGN FROM DW_OWNER.F9_CALL_LOG WHERE SKILL IS NOT NULL
UNION ALL
SELECT DISTINCT CAMPAIGN FROM DW_OWNER.F9_ACD_QUEUE WHERE SKILL IS NOT NULL
UNION ALL
SELECT DISTINCT CAMPAIGN FROM DW_OWNER.F9_CALL_SEGMENT WHERE SKILL IS NOT NULL
UNION ALL
SELECT DISTINCT CAMPAIGN FROM DW_OWNER.F9_AGENT WHERE SKILL IS NOT NULL
)
) B
ON (A.CAMPAIGN = B.CAMPAIGN)
WHEN NOT MATCHED THEN
INSERT (
dl_CAMPAIGN_sk,
CAMPAIGN)
VALUES
(
SEQ_F9_CAMPAIGN.NEXTVAL,
B.CAMPAIGN
);END;""",
    autocommit ='True',
    dag=dag
)
N1T3_MERGE_INTO_MV_DIM_F9_CAMPAIGN.set_upstream(DUMMY_OPERATOR_All_NODE_In)
    

#Node1 - Task4: Refresh MV_DIM_F9_CONTACT
N1T4_MV_DIM_F9_CONTACT=PythonOperator(
    task_id='N1T4_MV_DIM_F9_CONTACT',
    python_callable=process_fmm,
    op_kwargs={'mvname':'CHOICEBI.MV_DIM_F9_CONTACT'},
    dag=dag
    )
N1T4_MV_DIM_F9_CONTACT.set_upstream(DUMMY_OPERATOR_All_NODE_In)




#Node2 - Task1: Refresh MV_FACT_F9_MASTER
N2T1_MV_FACT_F9_MASTER=PythonOperator(
    task_id='N2T1_MV_FACT_F9_MASTER',
    python_callable=process_fmm,
    op_kwargs={'mvname':'CHOICEBI.MV_FACT_F9_MASTER'},
    dag=dag
    )
[N1T2_MERGE_INTO_MV_DIM_F9_SKILL, N1T3_MERGE_INTO_MV_DIM_F9_CAMPAIGN]>>N2T1_MV_FACT_F9_MASTER




#Node2 - Task2: Refresh MV_DIM_F9_CAMPAIGN_SKILL_MAP
N2T2_MV_DIM_F9_CAMPAIGN_SKILL_MAP=PythonOperator(
    task_id='N2T2_MV_DIM_F9_CAMPAIGN_SKILL_MAP',
    python_callable=process_fmm,
    op_kwargs={'mvname':'CHOICEBI.MV_DIM_F9_CAMPAIGN_SKILL_MAP'},
    dag=dag
    )
[N1T2_MERGE_INTO_MV_DIM_F9_SKILL, N1T3_MERGE_INTO_MV_DIM_F9_CAMPAIGN]>>N2T2_MV_DIM_F9_CAMPAIGN_SKILL_MAP





#Node2 - Task3: Refresh MV_FACT_F9_CALL_LOG
N2T3_MV_FACT_F9_CALL_LOG=PythonOperator(
    task_id='N2T3_MV_FACT_F9_CALL_LOG',
    python_callable=process_fmm,
    op_kwargs={'mvname':'CHOICEBI.MV_FACT_F9_CALL_LOG'},
    dag=dag
    )
[N1T2_MERGE_INTO_MV_DIM_F9_SKILL, N1T3_MERGE_INTO_MV_DIM_F9_CAMPAIGN]>>N2T3_MV_FACT_F9_CALL_LOG



#Node2 - Task4: Refresh MV_FACT_F9_AGENT_ACTIVITY_LOG
N2T4_MV_FACT_F9_AGENT_ACTIVITY_LOG=PythonOperator(
    task_id='N2T4_MV_FACT_F9_AGENT_ACTIVITY_LOG',
    python_callable=process_fmm,
    op_kwargs={'mvname':'CHOICEBI.MV_FACT_F9_AGENT_ACTIVITY_LOG'},
    dag=dag
    )
[N1T2_MERGE_INTO_MV_DIM_F9_SKILL, N1T3_MERGE_INTO_MV_DIM_F9_CAMPAIGN]>>N2T4_MV_FACT_F9_AGENT_ACTIVITY_LOG





#Node2 - Task5: Refresh MV_FACT_F9_ACD_QUEUE
N2T5_MV_FACT_F9_ACD_QUEUE=PythonOperator(
    task_id='N2T5_MV_FACT_F9_ACD_QUEUE',
    python_callable=process_fmm,
    op_kwargs={'mvname':'CHOICEBI.MV_FACT_F9_ACD_QUEUE'},
    dag=dag
    )
[N1T2_MERGE_INTO_MV_DIM_F9_SKILL, N1T3_MERGE_INTO_MV_DIM_F9_CAMPAIGN]>>N2T5_MV_FACT_F9_ACD_QUEUE





#Node2 - Task6: Refresh MV_FACT_F9_CALL_SEGMENT
N2T6_MV_FACT_F9_CALL_SEGMENT=PythonOperator(
    task_id='N2T6_MV_FACT_F9_CALL_SEGMENT',
    python_callable=process_fmm,
    op_kwargs={'mvname':'CHOICEBI.MV_FACT_F9_CALL_SEGMENT'},
    dag=dag
    )
[N1T2_MERGE_INTO_MV_DIM_F9_SKILL, N1T3_MERGE_INTO_MV_DIM_F9_CAMPAIGN]>>N2T6_MV_FACT_F9_CALL_SEGMENT




#Node2 - Task8: Refresh MV_DIM_F9_AGENT_SKILL_MAP
N2T8_MV_DIM_F9_AGENT_SKILL_MAP=PythonOperator(
    task_id='N2T8_MV_DIM_F9_AGENT_SKILL_MAP',
    python_callable=process_fmm,
    op_kwargs={'mvname':'CHOICEBI.MV_DIM_F9_AGENT_SKILL_MAP'},
    dag=dag
    )
N2T8_MV_DIM_F9_AGENT_SKILL_MAP.set_upstream(N1T2_MERGE_INTO_MV_DIM_F9_SKILL)



#Node 3 - Task 1: Trigger bat file: CALL_CENTER_DAILY_LOAD
N3T1_CALL_CENTER_DAILY_LOAD = SSHOperator (
    ssh_conn_id='ssh_MSTR',
    task_id='N3T1_CALL_CENTER_DAILY_LOAD',
    command="""E:\Support\MicroStrategy\Command_Manager_Scripts\Enterprise\CALL_CENTER_DAILY_LOAD.bat""",
dag =dag
    )
N3T1_CALL_CENTER_DAILY_LOAD.set_upstream(N2T1_MV_FACT_F9_MASTER)