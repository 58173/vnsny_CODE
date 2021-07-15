SELECT patient_id,
       measure_id,
       sub_measure_id,
       flag,
       sub_measure_id_1,
       the_date,
       date_type
  FROM (WITH CR_D
             AS (SELECT DISTINCT a.patient_id,
                                 a.epi_start_date  admit_date,
                                 a.epi_end_date,
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
              --          JOIN PHGC.member_carestaff MC
             --              ON a.patient_id = MC.PATIENT_ID
             --           JOIN PHGC.CARE_STAFF_DEPARTMENT csd2
              --             ON csd2.care_staff_id = mc.member_id
                  WHERE    epi_start_date between ADD_MONTHS (sysdate,-6)
                                         and  to_date(sysdate) --- -7
                        AND script_id IN (115, 223)
                        AND b.script_end_date IS NOT NULL
             --           AND csd2.care_staff_id IS NOT NULL
                                                                 )
SELECT DISTINCT
               D.patient_id,
               1003 measure_id,
               CASE WHEN A.patient_id IS NULL THEN 10032 ELSE 10031 END
                  AS SUB_MEASURE_ID,
               1 AS flag,
               CASE WHEN A.patient_id IS NULL THEN 90 ELSE 10 END
                  AS SUB_MEASURE_ID_1,
               TRUNC (d.admit_date) the_date,
               'cr_start_date' date_type
          FROM (select *                             
                from POP_HEALTH_BI.VW_FACT_PHGC where script_id = 220 -- CMO Med Rec 
                                                                              ) A
               RIGHT JOIN
               CR_D D
                  ON   D.patient_id = A.patient_id                  
                  )

