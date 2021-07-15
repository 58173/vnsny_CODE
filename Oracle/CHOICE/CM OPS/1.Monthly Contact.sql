  SELECT MONTH_ID,
           1
               DL_CM_MEASURE_SK,
           MEMBER_ID,
           DL_LOB_ID,
           SUBSCRIBER_ID,
           DL_PLAN_SK,
           DECODE (SRC_KEY_DESC1,'SCRIPT_ID', SRC_KEY1,
                   DECODE (SRC_KEY_DESC2, 'SCRIPT_ID', SRC_KEY2))
               SCRIPT_ID,
           DECODE (SRC_KEY_DESC1,
                   'HEALT H_NOTE_TYPE_ID', SRC_KEY1,
                   DECODE (SRC_KEY_DESC2, 'HEALTH_NOTE_TYPE_ID', SRC_KEY2))
               HEALTH_NOTE_TYPE_ID,
           DECODE (SRC_KEY_DESC1,
                   'PATIENT_FOLLOWUP_ID', SRC_KEY1,
                   DECODE (SRC_KEY_DESC2, 'PATIENT_FOLLOWUP_ID', SRC_KEY2))
               PATIENT_FOLLOWUP_ID,
           DECODE (SRC_KEY_DESC1,
                   'RECORD_ID', SRC_KEY1,
                   DECODE (SRC_KEY_DESC2, 'RECORD_ID', SRC_KEY2))
               DL_ASSESS_SK,
           CM_MEMBER_ID,
           CCM_MEMBER_ID,
           NULL
               PATIENT_FORM_ID,
           SRC_KEY_DESC1,
           SRC_KEY1,
           SRC_KEY_DESC2,
           SRC_KEY2,
           CREATED_DATE
               ACTIVITY_DATE,
           NUM,
           DENUM,
           CASE
               WHEN a.num = 0
               THEN
                   CASE
                       WHEN DECODE (
                                LAG (num)
                                    OVER (PARTITION BY subscriber_id
                                          ORDER BY month_id),
                                0, 0,
                                NULL) =
                            0
                       THEN
                           CASE
                               WHEN DECODE (
                                        LAG (num, 2, '')
                                            OVER (PARTITION BY subscriber_id
                                                  ORDER BY month_id),
                                        0, 0,
                                        NULL) =
                                    0
                               THEN
                                   3
                               ELSE
                                   2
                           END
                       ELSE
                           1
                   END
               ELSE
                   0
           END
               NOT_COMPLETED_AGING,
           NULL
               LAST_UAS_DATE,
           NULL
               LAST_PSP_DATE
      FROM V_CM_MONTLY_CARE_CM_DATA a
     WHERE month_id > 201910
  