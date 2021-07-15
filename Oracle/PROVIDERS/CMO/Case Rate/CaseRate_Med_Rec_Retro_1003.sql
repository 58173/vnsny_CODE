SELECT DISTINCT
               patient_id,
               1003 measure_id,
               CASE WHEN PATIENT_ID IS NOT NULL AND (TRUNC(REC_START_DATE)  BETWEEN ADMIT_DATE AND ADMIT_DATE +30) THEN 10031 
               ELSE 10032 
               END
                  AS SUB_MEASURE_ID,
               1 AS flag,
               CASE WHEN PATIENT_ID IS NOT NULL AND (TRUNC(REC_START_DATE)  BETWEEN ADMIT_DATE AND ADMIT_DATE +30) THEN 10 
               ELSE 90 
               END
                  AS SUB_MEASURE_ID_1,
               TRUNC (ADMIT_DATE) the_date,
               'Admit_Date' date_type,
               PAYOR
FROM               
(SELECT distinct * FROM
(
SELECT DISTINCT
       D.PATIENT_ID,
       D.FIRST_NAME,
       D.LAST_NAME,
       D.CLIENT_PATIENT_ID,
       D.ADMIT_DATE, 
       D.EPI_END_DATE, 
       D.PAYOR, 
       D.SCRIPT_ID  SCRIPT_223, 
       D.START_DATE  CMSBAR_START_DATE,
       A.SCRIPT_ID  SCRIPT_220,
       A.SCRIPT_START_DATE  REC_START_DATE
       ,RANK()
        OVER (PARTITION BY D.PATIENT_ID,D.admit_date ORDER BY A.SCRIPT_START_DATE) RK FROM 
(
SELECT DISTINCT                  a.patient_id,
                                 a.first_name,
                                 a.last_name,
                                 a.CLIENT_PATIENT_ID,
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
                                                            )D
          LEFT JOIN 
                     POP_HEALTH_BI.VW_FACT_PHGC A
                     ON   D.patient_id = A.patient_id 
                     AND A.script_id = 220 -- CMO Med Rec
                     AND A.SCRIPT_START_DATE BETWEEN D.admit_date
                                                   AND NVL (D.epi_end_date,
                                                            '31-dec-9999')                
                  )
                  WHERE RK =1              
                                  )       


                  
                  
