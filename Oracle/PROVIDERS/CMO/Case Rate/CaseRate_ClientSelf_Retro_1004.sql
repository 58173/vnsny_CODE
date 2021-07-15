   SELECT DISTINCT
               patient_id,
               1004 measure_id,
               CASE
                  WHEN   patient_id IS NOT NULL
                       AND total_score >= 13
                       AND (SCRIPT_214_DATE - CMSBAR_START_DATE) BETWEEN 0 AND 30 
                  THEN
                     10041
                  WHEN   patient_id IS NOT NULL
                         AND  total_score BETWEEN 7 AND 12 
                         AND (SCRIPT_214_DATE - CMSBAR_START_DATE) BETWEEN 0 AND 30 
                  THEN 10043
                  WHEN  patient_id IS NOT NULL
                        AND  total_score < 7
                        AND (SCRIPT_214_DATE - CMSBAR_START_DATE) BETWEEN 0 AND 30 
                  THEN  10044
                  ELSE 
                        10042
               END
                  AS sub_measure_id,
               1 flag,
               CASE
                  WHEN     patient_id IS NOT NULL
                       AND total_score >= 13
                       AND (SCRIPT_214_DATE - CMSBAR_START_DATE) BETWEEN 0 AND 30 
                  THEN
                     10
                  WHEN   patient_id IS NOT NULL
                         AND  total_score BETWEEN 7 AND 12 
                         AND (SCRIPT_214_DATE - CMSBAR_START_DATE) BETWEEN 0 AND 30    
                  THEN 
                     30
                  WHEN  patient_id IS NOT NULL
                        AND total_score < 7
                        AND (SCRIPT_214_DATE - CMSBAR_START_DATE) BETWEEN 0 AND 30 
                  THEN
                     40
                  ELSE           
                     90
               END
                  AS sub_measure_id_1,
               TRUNC (ADMIT_DATE) AS the_date,
               'Admit_Date' date_type,
               PAYOR
FROM               
(SELECT distinct * FROM
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
       N.TOTAL_SCORE,
       N.SCRIPT_214_DATE  
       ,RANK()
        OVER (PARTITION BY D.PATIENT_ID,D.admit_date ORDER BY N.SCRIPT_214_DATE) RK 
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
               (SELECT  patient_id,
                       a.script_run_log_id,
                       script_id   SCRIPT_214,
                     -- script_score,
                      TRUNC (a.start_date)    SCRIPT_214_DATE,
                      SUM(script_score)  total_score               
                 FROM PHGC.SCPT_PATIENT_SCRIPT_RUN_LOG A
                 JOIN PHGC.SCPT_PATIENT_SCPT_RUN_LOG_DET B
                   ON A.SCRIPT_RUN_LOG_ID = B.SCRIPT_RUN_LOG_ID
                 JOIN PHGC.scpt_question_response C
                   ON B.SCRIPT_RUN_LOG_DETAIL_ID = C.SCRIPT_RUN_LOG_DETAIL_ID
                WHERE a.status_id = 1 AND B.SCRIPT_ID = 214
                GROUP BY PATIENT_ID,
                         a.script_run_log_id,
                         SCRIPT_ID,
                         start_date
                                                                  )   N
            ON D.PATIENT_ID = N.PATIENT_ID
            AND N.SCRIPT_214_DATE BETWEEN D.admit_date
                                    AND NVL (D.epi_end_date,'31-dec-9999') 
                           ) 
            WHERE RK =1              
                                  )    
       