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
                                 B.SCRIPT_START_DATE    start_date,
                                 b.SCRIPT_RUN_LOG_ID,
                                 b.question_option_id
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
                                       and  sysdate  ---7
                        AND script_id IN (115, 223)
                        AND b.script_end_date IS NOT NULL
                        AND csd2.care_staff_id IS NOT NULL )
SELECT DISTINCT
               d.patient_id,
               1002 measure_id,
               CASE
                  WHEN TRUNC(d.start_date) - TRUNC (record_date)  >= 14
                  THEN 10021
                  WHEN TRUNC (record_date) - TRUNC (d.start_date) <= 10
                  THEN
                     10022
                  WHEN     TRUNC (record_date) - TRUNC (d.start_date) <= 30
                       AND TRUNC (record_date) - TRUNC (d.start_date) > 10
                  THEN
                     10024
                  ELSE
                     10023
               END
                  AS sub_measure_id,
               1 flag,
               CASE
                  WHEN TRUNC (record_date) - TRUNC(d.start_date) >= 14
                  THEN 10
                  WHEN TRUNC (record_date) - TRUNC (d.start_date) <= 10
                  THEN
                     30
                  WHEN     TRUNC (record_date) - TRUNC (d.start_date) <= 30
                       AND TRUNC (record_date) - TRUNC (d.start_date) > 10
                  THEN
                     40
                  ELSE
                     90
               END
                  AS sub_measure_id_1,
               TRUNC (d.epi_start_date) the_date,
               'cr_start_date' date_type
FROM ((SELECT patient_id,
                        RANK ()
                        OVER (PARTITION BY patient_id ORDER BY record_date)
                           rk,
                        record_date
                   FROM PHGC.HEALTH_INDICATOR_RECORD
                  WHERE     PARAMETER_ID IN (84, 85, 86)  --- 84.MD Appt - Post Acute Discharge  85.MD Appt - PCP referral  86.MD Appt - Clinic Referral

                        AND DELETED_BY IS NULL
                        AND value_enterEd = 'Yes') md_n
                RIGHT JOIN CR_D d ON md_n.patient_id = d.patient_id)
        WHERE    rk = 1
        AND record_date >= d.start_date
                                )    