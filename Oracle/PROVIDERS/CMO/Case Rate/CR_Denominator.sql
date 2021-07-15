

select distinct A.patient_id, A.first_name, A.last_name,A.epi_start_date,A.plan_name
                ,I.CLIENT_PATIENT_ID, I.FIRST_NAME, I.LAST_NAME 
FROM  
  (SELECT DISTINCT               a.patient_id,
                                 a.first_name,
                                 a.last_name,
                                 a.plan_name,
                                 a.epi_start_date,
                                 a.epi_end_date,
                        --         B.SCRIPT_ID,
                                 B.SCRIPT_START_DATE start_date
                        --         ,b.SCRIPT_RUN_LOG_ID
                            --     ,b.question_option_id
                   FROM POP_HEALTH_BI.VW_FACT_PHGC_TIME_BASED_EPI a
                        JOIN
                        POP_HEALTH_BI.VW_FACT_PHGC b
                           ON     A.PATIENT_ID = b.patient_id
                             AND b.SCRIPT_START_DATE BETWEEN a.epi_start_date
                                                  AND NVL (a.epi_end_date,'31-dec-9999')
                        JOIN PHGC.member_carestaff MC
                           ON a.patient_id = MC.PATIENT_ID
                        JOIN PHGC.CARE_STAFF_DEPARTMENT csd2
                           ON csd2.care_staff_id = mc.member_id
                  WHERE     epi_start_date between ADD_MONTHS (sysdate,-6)
                                         and  to_date(sysdate)-7
                       --- script_id IN (115, 223)
                        AND b.script_end_date IS NOT NULL
                        AND csd2.care_staff_id IS NOT NULL) A
JOIN PHGC.PATIENT_DETAILS I
ON I.PATIENT_ID = A. PATIENT_ID