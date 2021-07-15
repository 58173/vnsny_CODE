SELECT PATIENT_ID,
       1017            measure_id,
       CASE
           WHEN PAIN_SCORE <= 4 THEN 10171
           WHEN PAIN_SCORE BETWEEN 5 AND 7 THEN 10174
           WHEN PAIN_SCORE >= 8 THEN 10173
           ELSE 10172
       END             AS sub_measure_id,
       1               AS flag,
       CASE
           WHEN PAIN_SCORE <= 4 THEN 10
           WHEN PAIN_SCORE BETWEEN 5 AND 7 THEN 40
           WHEN PAIN_SCORE >= 8 THEN 30
           ELSE 90
       END             AS sub_measure_id_1,
       admit_date      the_date,
       'admit_date'    date_type
  FROM (WITH
            denom_overall
            AS
                (SELECT pd.PATIENT_ID,
                        pd.FIRST_NAME,
                        pd.LAST_NAME,
                        TRUNC (a.START_DATE)
                            AS ADMIT_DATE,
                        TRUNC (srl.START_DATE)
                            AS SCRIPT_DATE,
                        rld.SCRIPT_ID,
                        lbp.LOB_ID,
                        SYSDATE - TRUNC (srl.START_DATE)
                            AS time_elapsed,
                        rld.SCRIPT_RUN_LOG_DETAIL_ID,
                        rld.QUESTION_ID,
                        MONTHS_BETWEEN (SYSDATE, TRUNC (srl.START_DATE))
                            AS MONTHS_SINCE_ADMIT,
                        (TRUNC (srl.START_DATE) - TRUNC (a.START_DATE))
                            AS ADMIT_TO_SCRIPT
                   FROM PHGC.PATIENT_DETAILS  pd
                        LEFT JOIN PHGC.SCPT_PATIENT_SCRIPT_RUN_LOG srl
                            ON pd.PATIENT_ID = srl.PATIENT_ID
                        LEFT JOIN PHGC.SCPT_PATIENT_SCPT_RUN_LOG_DET rld
                            ON srl.SCRIPT_RUN_LOG_ID = rld.SCRIPT_RUN_LOG_ID
                        LEFT JOIN
                        (  SELECT DISTINCT (pd.PATIENT_ID), srl.START_DATE
                             FROM PHGC.PATIENT_DETAILS pd
                                  LEFT JOIN
                                  PHGC.SCPT_PATIENT_SCRIPT_RUN_LOG srl
                                      ON pd.PATIENT_ID = srl.PATIENT_ID
                                  LEFT JOIN
                                  PHGC.SCPT_PATIENT_SCPT_RUN_LOG_DET rld
                                      ON srl.SCRIPT_RUN_LOG_ID =
                                         rld.SCRIPT_RUN_LOG_ID
                            WHERE rld.SCRIPT_ID = 143
                         ORDER BY pd.PATIENT_ID, srl.START_DATE) a
                            ON srl.PATIENT_ID = a.PATIENT_ID
                        LEFT JOIN PHGC.MEM_BENF_PLAN m
                            ON srl.PATIENT_ID = m.MEMBER_ID
                        LEFT JOIN PHGC.LOB_BENF_PLAN lbp
                            ON m.LOB_BEN_ID = lbp.LOB_BEN_ID
                        LEFT JOIN PHGC.LOB l ON lbp.LOB_ID = l.LOB_ID
                  WHERE     l.LOB_ID = 5
                        AND UPPER (pd.FIRST_NAME) NOT IN
                                ('AHQATEST', 'VNSQATEST', 'TEST'))
        SELECT PATIENT_ID,
               FIRST_NAME,
               LAST_NAME,
               ADMIT_DATE,
               TIME_ELAPSED,
               TO_NUMBER (SUBSTR (sqr.OPTION_VALUE, 1, 1))    AS PAIN_SCORE,
               SCRIPT_DATE,
               ADMIT_TO_SCRIPT,
               SCRIPT_ID,
               QUESTION_ID,
               ROW_NUMBER ()
                   OVER (PARTITION BY PATIENT_ID
                         ORDER BY ADMIT_TO_SCRIPT DESC)       AS SCRIPT_ORDER --for patients with both scripts, most recent pain score will have SCRIPT_ORDER = 1
          FROM denom_overall  do
               LEFT JOIN PHGC.SCPT_QUESTION_RESPONSE sqr
                   ON do.SCRIPT_RUN_LOG_DETAIL_ID =
                      sqr.SCRIPT_RUN_LOG_DETAIL_ID
         WHERE   --  TIME_ELAPSED >= 30
              -- AND MONTHS_SINCE_ADMIT <= 6 AND
                ADMIT_TO_SCRIPT BETWEEN 0 AND 30 -- pain score within 30 days of admission
               AND (   (do.SCRIPT_ID = 197 AND do.QUESTION_ID = 3388) -- FHG also includes a script called "CMO NP Assessment" which has a pain scale question (QUESTION_ID = 3520) that is reported in FHG
                    OR (do.SCRIPT_ID = 143 AND do.QUESTION_ID = 2570))
               AND ADMIT_DATE IS NOT NULL
               AND IS_ACTIVE = 1) a
 WHERE SCRIPT_ORDER = 1 OR SCRIPT_ORDER IS NULL