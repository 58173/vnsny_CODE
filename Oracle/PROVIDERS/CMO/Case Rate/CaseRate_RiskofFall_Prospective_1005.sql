SELECT patient_id,
       measure_id,
       sub_measure_id,
       flag,
       sub_measure_id_1,
       the_date,
       date_type
  FROM (WITH CR_D
             AS (SELECT DISTINCT a.patient_id,
                                 a.epi_start_date,
                                 a.epi_end_date,
                                 B.SCRIPT_ID,
                                 B.SCRIPT_START_DATE start_date,
                                 b.SCRIPT_RUN_LOG_ID,
                                 b.question_option_id
                   FROM POP_HEALTH_BI.VW_FACT_PHGC_TIME_BASED_EPI a
                        JOIN
                        POP_HEALTH_BI.VW_FACT_PHGC b
                           ON     A.PATIENT_ID = b.patient_id
                              AND b.SCRIPT_START_DATE BETWEEN a.epi_start_date
                                                   AND NVL (a.epi_end_date,
                                                            '31-dec-9999')
                        JOIN PHGC.member_carestaff MC
                           ON a.patient_id = MC.PATIENT_ID
                        JOIN PHGC.CARE_STAFF_DEPARTMENT csd2
                           ON csd2.care_staff_id = mc.member_id
                  WHERE     epi_start_date between ADD_MONTHS ( ('19-JAN-2020'),-6)
                                         and  to_date('19-JAN-2020')-7
                        AND script_id IN (115, 223)
                        AND b.script_end_date IS NOT NULL
                        AND csd2.care_staff_id IS NOT NULL)
SELECT         d.patient_id,
               1005 measure_id,
               CASE
                  WHEN TRUNC (n.start_date) - TRUNC (d.START_DATE) <= 7
                  THEN
                     10051
                  ELSE
                     10052
               END
                  AS sub_measure_id,
               1 AS flag,
               CASE
                  WHEN TRUNC (n.start_date) - TRUNC (d.START_DATE) <= 7
                  THEN
                     10
                  ELSE
                     90
               END
                  AS sub_measure_id_1,
               TRUNC (d.epi_start_date) AS the_date,
               'cr_start_date' date_type
          FROM (SELECT DISTINCT
                       B.SCRIPT_RUN_LOG_ID,
                       B.SCRIPT_ID,
                       patient_id,
                       TRUNC (a.start_date) start_date,
                       DENSE_RANK ()
                       OVER (PARTITION BY patient_id ORDER BY a.start_date)
                          rk
                  FROM PHGC.SCPT_PATIENT_SCRIPT_RUN_LOG A
                       JOIN PHGC.SCPT_PATIENT_SCPT_RUN_LOG_DET B
                          ON A.SCRIPT_RUN_LOG_ID = B.SCRIPT_RUN_LOG_ID
                       JOIN
                       PHGC.scpt_question_response C
                          ON B.SCRIPT_RUN_LOG_DETAIL_ID =
                                C.SCRIPT_RUN_LOG_DETAIL_ID
                 WHERE a.status_id = 1 AND B.SCRIPT_ID IN (208, 206)) n
               JOIN (SELECT *
                       FROM CR_D
                      WHERE question_option_id = 10264) d
                  ON n.patient_id = d.patient_id AND rk = 1)