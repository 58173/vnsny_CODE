
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

dag = DAG(dag_id = 'OUT_JDCD_CHO_DUMMY_NXS_FD_CADM_LOAD_MEMBERSHIP',
          default_args= default_args, 
          catchup=False
         ) 


task0 = DummyOperator(task_id='task0', dag=dag)

task1 = OracleOperator(task_id='OUT_JDCD_CHO_FACT_MEMBER_MONTH1',
                        oracle_conn_id='CHOICEBI',
                        sql= 'begin CHOICEBI.P_REFRESH_MV;END;',
                        autocommit ='True',
                        dag=dag)   

task2 = OracleOperator(task_id='OUT_JDCD_CHO_VALIDATE_FACT_MEMBER_MONTH',
                        oracle_conn_id='CHOICEBI',
                        sql= 'begin CHOICEBI.f_validate_ltp_ind;END;',
                        autocommit ='True',
                        dag=dag) 

task31 = task_id ='OUT_JDCD_CHO_FIDA_HHA_UTILIZATION1'  

task32 =  OracleOperator(task_id='OUT_JDCD_CHO_MA_HOSP_ELIG1',
                        oracle_conn_id='CHOICEBI',
                        sql= 'begin CHOICEBI.P_REFRESH_MV;END;',
                        autocommit ='True',
                        dag=dag) 

task33 =  OracleOperator(task_id='OUT_JDCD_CHO_MLTC_HHA_UTILIZATION1',
                        oracle_conn_id='CHOICEBI',
                        sql= 'begin CHOICEBI.p_mltc_,e,ber_paid_hrs_by_day;END;',
                        autocommit ='True',
                        dag=dag)           

task34 =  OracleOperator(task_id='OUT_JDCD_CHO_REFRESH_FACT_HHA_CLAIMS',
                        oracle_conn_id='CHOICEBI',
                        sql= 'begin CHOICEBI.P_REFRESH_MV;END;',
                        autocommit ='True',
                        dag=dag) 

task35 = OracleOperator(task_id='OUT_JDCD_CHO_REFRESH_FACT_MEMBER_ENROLL_DISENROLL1',
                        oracle_conn_id='CHOICEBI',
                        sql= 'begin CHOICEBI.P_REFRESH_MV;END;',
                        autocommit ='True',
                        dag=dag) 

 task36 = OracleOperator(task_id='OUT_JDCD_CHO_REFRESH_FACT_MEMBER_MONTH_CCW',
                        oracle_conn_id='CHOICEBI',
                        sql= 'begin CHOICEBI.P_REFRESH_MV;END;',
                        autocommit ='True',
                        dag=dag)   

task37 = OracleOperator(task_id='OUT_JDCD_CHO_REFRESH_FACT_PCSP_MATRIX_DATA1',
                        oracle_conn_id='CHOICEBI',
                        sql= 'begin CHOICEBI.P_REFRESH_MV;END;',
                        autocommit ='True',
                        dag=dag)   

task38 = OracleOperator(task_id='OUT_JDCD_CHO_REFRESH_FACT_QUALITY_TIMELINESS_MEASURES',
                        oracle_conn_id='CHOICEBI',
                        sql= 'begin CHOICEBI.P_REFRESH_MV;END;',
                        autocommit ='True',
                        dag=dag)   



 