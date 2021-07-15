SELECT DISTINCT   
                PATIENT_ID, 
                1053 MEASURE_ID,               
                 CASE 
                      WHEN QUESTION_OPTION_ID IN (10253, 10254,10255, 10256,10257)   --- pain_score: 0-4
                      THEN 10531
                      WHEN QUESTION_OPTION_ID IN (10258,10259,10260)                 --- pain_score: 5-7
                      THEN 10533
                      WHEN  QUESTION_OPTION_ID IN (10261,10262,10263)                  --- pain_score: 8-10
                      THEN 10532
                     ELSE NULL
                 END          SUB_MEASURE_ID
                 ,1 AS FLAG,
                 CASE
                      WHEN QUESTION_OPTION_ID IN (10253, 10254,10255, 10256,10257)
                      THEN 10
                      WHEN QUESTION_OPTION_ID IN (10258,10259,10260)
                      THEN 30
                      WHEN QUESTION_OPTION_ID IN (10261,10262,10263)
                      THEN 90
                      ELSE NULL
                 END              SUB_MEASURE_ID_1,
                 start_date   THE_DAY,   'CMSBAR' DATE_TYPE,  PAYOR 
  FROM               
(SELECT distinct * 
FROM
    ( 
SELECT DISTINCT                 
                                 a.patient_id,
                                 a.FIRST_NAME,
                                 a.LAST_NAME,
                                 a.epi_start_date  admit_date,
                                 a.epi_end_date,
                                 UPPER(a.PLAN_DESC) PAYOR,
                                 B.SCRIPT_ID,
                                 TRUNC(B.SCRIPT_START_DATE)  start_date,
                                 B.SCRIPT_RUN_LOG_ID,
                                 B.QUESTION_ID,
                                 B.question_option_id
                                 ,DENSE_RANK()
                                  OVER (PARTITION BY A.PATIENT_ID,A.epi_start_date ORDER BY B.SCRIPT_START_DATE) RK
                   FROM POP_HEALTH_BI.VW_FACT_PHGC_TIME_BASED_EPI a
                        JOIN
                        POP_HEALTH_BI.VW_FACT_PHGC B
                           ON     A.PATIENT_ID = B.patient_id
                              AND B.SCRIPT_START_DATE BETWEEN a.epi_start_date
                                                     AND NVL (a.epi_end_date,
                                                            '31-dec-9999')
                  WHERE    epi_start_date between ADD_MONTHS (sysdate,-6)
                                         and  to_date(sysdate) - 30
                        AND script_id IN (115, 223)
                        AND QUESTION_ID = 3856
                        AND B.script_end_date IS NOT NULL 
                                                            ) D
  WHERE RK =1 )                                                         