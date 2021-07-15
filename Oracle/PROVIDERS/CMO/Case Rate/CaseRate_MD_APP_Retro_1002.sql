SELECT DISTINCT
               patient_id,
               1002 measure_id,
               CASE
                  WHEN TRUNC (record_date) - TRUNC (admit_date) BETWEEN 0 AND 10 
                  THEN
                     10021
                  WHEN     TRUNC (record_date) - TRUNC (admit_date) BETWEEN 11 AND  30
                  THEN
                     10023
                  ELSE
                     10022
               END
                  AS sub_measure_id,
               1 flag,
               CASE
                  WHEN TRUNC (record_date) - TRUNC (admit_date) BETWEEN 0 AND 10 
                  THEN
                     10
                  WHEN     TRUNC (record_date) - TRUNC (admit_date) BETWEEN 11 AND  30
                  THEN
                     30
                 ELSE  90
               END
                  AS sub_measure_id_1,
               TRUNC (admit_date) the_date,
               'Admit_Date' date_type,
               PAYOR
FROM               
(SELECT distinct * FROM
(
SELECT DISTINCT
       D.PATIENT_ID,
       D.ADMIT_DATE, 
       D.EPI_END_DATE, 
       D.PAYOR, 
       D.SCRIPT_ID  SCRIPT_223, 
       D.START_DATE  CMSBAR_START_DATE,
       A.PARAMETER_ID,
       A.RECORD_DATE  
       ,RANK()
        OVER (PARTITION BY D.PATIENT_ID,D.admit_date ORDER BY A.RECORD_DATE) RK 
FROM 
    (
SELECT DISTINCT                  a.patient_id,
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
    LEFT JOIN    PHGC.HEALTH_INDICATOR_RECORD  A
                 ON  D.patient_id = A.patient_id 
                 AND A.RECORD_DATE BETWEEN D.admit_date
                                   AND NVL (D.epi_end_date,'31-dec-9999')
                 AND A.PARAMETER_ID IN (84, 85, 86)  --- 84.MD Appt - Post Acute Discharge  85.MD Appt - PCP referral  86.MD Appt - Clinic Referral
                 AND A.DELETED_BY IS NULL
             ---    AND A.value_enterEd = 'Yes' 
                 )
                  WHERE RK =1              
                                  )  
        
                                      