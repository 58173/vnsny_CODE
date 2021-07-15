 SELECT DISTINCT
               patient_id,
               1052 measure_id,
               CASE
                  WHEN   patient_id IS NOT NULL
                       AND  QUESTION_OPTION_ID IN (7945,7946,7947) 
                       AND (PHQ9_DATE - trunc(CMSBAR_START_DATE))  BETWEEN 0 AND 30 
                  THEN
                     10521
                  WHEN   patient_id IS NOT NULL
                         AND  QUESTION_OPTION_ID IN (7943,7944)
                         AND (PHQ9_DATE - trunc(CMSBAR_START_DATE))  BETWEEN 0 AND 30  
                  THEN 10523
                  WHEN  patient_id IS NOT NULL
                        AND  QUESTION_OPTION_ID is null 
                        OR (PHQ9_DATE - trunc(CMSBAR_START_DATE)) > 30 
                  THEN  10522
                  ELSE 
                        10522
               END
                  AS sub_measure_id,
               1 flag,
               CASE
                  WHEN     patient_id IS NOT NULL
                       AND QUESTION_OPTION_ID IN (7945,7946,7947) 
                       AND (PHQ9_DATE - trunc(CMSBAR_START_DATE))  BETWEEN 0 AND 30  
                  THEN
                     10
                  WHEN   patient_id IS NOT NULL
                         AND  QUESTION_OPTION_ID IN (7943,7944)
                         AND (PHQ9_DATE - trunc(CMSBAR_START_DATE))  BETWEEN 0 AND 30   
                  THEN 
                     30
                  WHEN  patient_id IS NOT NULL
                        AND  QUESTION_OPTION_ID is null 
                        OR (PHQ9_DATE - trunc(CMSBAR_START_DATE)) > 30 
                  THEN
                     90
                  ELSE           
                     90
               END
                  AS sub_measure_id_1,
               trunc(CMSBAR_START_DATE) AS the_date,
               'CMSBAR' date_type,
               PAYOR
 FROM              
(SELECT distinct * 
FROM
    ( 
    SELECT DISTINCT
       D.PATIENT_ID,
       D.FIRST_NAME,
       D.LAST_NAME,
       D.ADMIT_DATE, 
       D.EPI_END_DATE, 
       D.PAYOR, 
       D.SCRIPT_ID  SCRIPT_223, 
       D.START_DATE  CMSBAR_START_DATE,
       N.PHQ9_date,
       N.OPTION_VALUE,
       N.QUESTION_OPTION_ID
       ,RANK()
        OVER (PARTITION BY D.PATIENT_ID,D.admit_date ORDER BY N.PHQ9_date) RK1
       ,RANK()
        OVER (PARTITION BY D.PATIENT_ID,D.admit_date ORDER BY D.START_DATE) RK2
FROM 
   ( SELECT DISTINCT                 
                                 a.patient_id,
                                 a.FIRST_NAME,
                                 a.LAST_NAME,
                                 a.epi_start_date  admit_date,
                                 a.epi_end_date,
                                 UPPER(a.PLAN_DESC) PAYOR,
                                 B.SCRIPT_ID,
                                 B.SCRIPT_START_DATE  start_date,
                                 b.SCRIPT_RUN_LOG_ID,
                                 b.question_option_id
                   FROM POP_HEALTH_BI.VW_FACT_PHGC_TIME_BASED_EPI a
                        JOIN
                        POP_HEALTH_BI.VW_FACT_PHGC b
                           ON     A.PATIENT_ID = b.patient_id
                              AND b.SCRIPT_START_DATE BETWEEN a.epi_start_date
                                                     AND NVL (a.epi_end_date,
                                                            '31-dec-9999')
                  WHERE    epi_start_date between ADD_MONTHS (sysdate,-6)
                                         and  to_date(sysdate) - 30
                        AND script_id IN (115, 223)
                        AND b.script_end_date IS NOT NULL 
                                                            ) D
       LEFT JOIN                                      
                (SELECT DISTINCT PATIENT_ID, 
                                 PATIENT_FIRST_NAME, 
                                 PATIENT_LAST_NAME, 
                                 SCRIPT_ID      PHQ9, 
                                trunc(SCRIPT_START_DATE)    PHQ9_date,
                                OPTION_VALUE,
                                QUESTION_OPTION_ID
                            FROM POP_HEALTH_BI.VW_FACT_PHGC
                            WHERE SCRIPT_ID = 163  --- CMO PHQ9
                                 AND QUESTION_ID = 2896
                                                               ) N
                            ON D.patient_id = N.PATIENT_ID 
                            AND N.PHQ9_date BETWEEN D.admit_date
                                            AND NVL (D.epi_end_date,'31-dec-9999')   
                                                            ) 
                                   WHERE RK1 =1 and rk2 = 1
                                              )