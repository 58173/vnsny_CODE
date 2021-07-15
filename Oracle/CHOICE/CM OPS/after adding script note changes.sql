

 select month_id, MEMBER_ID,DL_LOB_ID,SUBSCRIBER_ID,PROGRAM,NUM,DENUM  from 
  (WITH
        v_cm_ope_data
        AS
            (SELECT 1                                               SEQ,
                    TO_NUMBER (TO_CHAR (CREATED_DATE, 'YYYYMM'))    MONTH_ID,
                    a.PATIENT_ID,
                    CREATED_DATE,
                    CREATED_BY,
                    'PATIENT_FOLLOWUP_ID'                           SRC1,
                    a.PATIENT_FOLLOWUP_ID                           SRC_ID1,
                    'SCRIPT_ID'                                     SRC2,
                    DL_REF_CM_SCRIPT_SK                             SRC_ID2,
                    a.MEMBER_ID,
                    a.SUBSCRIBER_ID
               --*
               FROM CHOICE.FCT_CM_MEMBER_FOLLOWUP@dlake  a
                    JOIN choice.FCT_CM_SCRIPT_MEM_RUN_LOG@dlake b
                        ON     a.DL_FCT_CM_MEMBER_FOLLOWUP_SK =
                               b.DL_FCT_CM_MEMBER_FOLLOWUP_SK
                           AND status_id = 1
              WHERE DL_REF_CM_SCRIPT_SK IN (231,                      --MASTER
                                            107,                         --HRA
                                            243,         --CDPAS_REFERRAL_FORM
                                            250,           --MASTER FINAL GCV8
                                            244, --TRANSITION OF CARE FINAL GCV8
                                            245,     --REASSESSMENT FINAL GCV8
                                            246,     --WELCOME CALL FINAL GCV8
                                            237,    --COVID - 19 CHOICE SCRIPT
                                            252 --ADVANCED ILLNESS ASSESSMENT PPS
                                               )
             UNION ALL
             SELECT 2                                               SEQ,
                    TO_NUMBER (TO_CHAR (CREATED_DATE, 'YYYYMM'))    MONTH_ID,
                    PATIENT_ID,
                    CREATED_DATE,
                    CREATED_BY,
                    'PATIENT_FOLLOWUP_ID'                           SRC1,
                    PATIENT_FOLLOWUP_ID                             SRC_ID1,
                    'DL_REF_CM_ACTIVITY_TYPE_SK'                    SRC2,
                    DL_REF_CM_ACTIVITY_TYPE_SK                      SRC_ID2,
                    MEMBER_ID,
                    SUBSCRIBER_ID
               --*
               FROM CHOICE.FCT_CM_MEMBER_FOLLOWUP@dlake A
              WHERE A.DL_REF_CM_ACTIVITY_TYPE_SK IN
                        (SELECT DL_REF_CM_ACTIVITY_TYPE_SK
                           FROM choice.REF_CM_ACTIVITY_TYPE@dlake
                          WHERE cm_activity_type_name IN
                                    ('CARE COORDINATION',
                                     'CARE TRANSITIONS',
                                     'TCM: CARE PLANNING',
                                     'TCM: CARE COORDINATION',
                                     'TCM: CARE TRANSITIONS',
                                     'TCM: DISENROLLMENT PLANNING',
                                     'WELLNESS INITIATIVES',
                                     'CASE CONFERENCE',
                                     'ICT-IDT CONFERENCE',
                                     'MEC - MEDICAID ISSUES' --'CASE CONFERENCE','CARE MANAGEMENT ENCOUNTER' ,'TCM: POST HOSP D/C 14 DAY MD FOLLOW-UP',
                                                            --'MEC - CONCRETE ENTITLEMENTS' ,
                                                            ))
             UNION ALL
             SELECT 3
                        SEQ,
                    TO_NUMBER (TO_CHAR (a.CREATED_ON, 'YYYYMM'))
                        MONTH_ID,
                    a.PATIENT_ID,
                    a.CREATED_ON,
                    a.CREATED_BY,
                    'HEALTH_NOTES_ID'
                        SRC1,
                    HEALTH_NOTES_ID
                        SRC_ID1,
                    'HEALTH_NOTE_TYPE_ID'
                        SRC2,
                    HEALTH_NOTE_TYPE_ID
                        SRC_ID2,
                    NULL
                        MEMBER_ID,
                    UNIQUE_ID
                        SUBSCRIBER_ID
               FROM CMGC.HEALTH_NOTES  a
                    --join choice.dim_member_detail@dlake b on (a.patient_id = b.meme_ck and src_sys= 'TMG')
                    JOIN cmgc.patient_details b
                        ON (a.patient_id = b.patient_id)
              WHERE HEALTH_NOTE_TYPE_ID IN
                        (SELECT note_type_id
                           FROM cmgc.HEALTH_NOTE_TYPE
                          WHERE NOTE_TYPE IN
                                    ('TCM: Member Call',
                                     'TCM: PCSP/CARE PLAN',
                                     'End of Life Care Planning Consult',
                                     'TCM: Care Giver/Family Support',
                                     'IDT/ICT Note',
                                     'TCM: General' --,'MEC Concrete Entitlements'
                                                   ,
                                     'MEC Medicaid Issues' --,'TCM: Provider Call'
                                                          --,'TCM: Other Services Discussed'
                                                          ))),
        v_cm_ope_data1
        AS
            (SELECT *
               FROM (SELECT a.*,
                            ROW_NUMBER ()
                                OVER (PARTITION BY patient_id, month_id
                                      ORDER BY seq, month_id ASC)    opre_seq
                       FROM v_cm_ope_data A)
              WHERE opre_seq = 1)
    SELECT SEQ,
           B.MONTH_ID,
           B.MEMBER_ID,
           B.DL_LOB_ID,
           B.PROGRAM,
           B.SUBSCRIBER_ID,
           B.dl_plan_sk,
           A.PATIENT_ID,
           A.CREATED_DATE,
           A.CREATED_BY,
           A.SRC1                             SRC_KEY_DESC1,
           A.SRC_ID1                          SRC_KEY1,
           A.SRC2                             SRC_KEY_DESC2,
           A.SRC_ID2                          SRC_KEY2,
           OPRE_SEQ,
           DECODE (A.SRC_ID1, NULL, 0, 1)     NUM,
           1                                  DENUM,
           c.cm_staff_id                      AS CM_MEMBER_ID,
           b.first_name                       AS CM_First_Name,
           b.last_name                        AS CM_Last_Name, --Co-ordinated Care Manager Info (CCM)
           d.cm_staff_id                      AS CCM_MEMBER_ID,
           d.first_name                       AS CCM_First_Name,
           d.last_name                        AS CCM_Last_name
      FROM FACT_MEMBER_MONTH  B
           LEFT JOIN v_cm_ope_data1 A
               ON (    A.SUBSCRIBER_ID = B.SUBSCRIBER_ID
                   AND A.MONTH_ID = B.MONTH_ID)
           LEFT JOIN choice.dim_member_care_manager@DLAKE b1
               ON (B.cm_sk_id = b1.cm_sk_id AND b1.DL_ACTIVE_REC_IND = 'Y')
           LEFT JOIN choice.DIM_CM_STAFF_DETAILS@dlake c
               ON (    c.dl_active_rec_ind = 'Y'
                   AND c.cm_staff_id = b1.care_manager_id)
           LEFT JOIN choice.DIM_CM_STAFF_DETAILS@dlake d
               ON (    c.dl_active_rec_ind = 'Y'
                   AND d.cm_staff_id = c.assigned_to
                   AND d.dl_active_rec_ind = 'Y')
     WHERE B.PROGRAM = 'MLTC') 
   minus
    select month_id, MEMBER_ID,DL_LOB_ID,SUBSCRIBER_ID,PROGRAM,NUM,DENUM  from 
     (WITH
        v_cm_ope_data
        AS
            (SELECT 1                                               SEQ,
                    TO_NUMBER (TO_CHAR (CREATED_DATE, 'YYYYMM'))    MONTH_ID,
                    a.PATIENT_ID,
                    CREATED_DATE,
                    CREATED_BY,
                    'PATIENT_FOLLOWUP_ID'                           SRC1,
                    a.PATIENT_FOLLOWUP_ID                           SRC_ID1,
                    'SCRIPT_ID'                                     SRC2,
                    DL_REF_CM_SCRIPT_SK                             SRC_ID2,
                    a.MEMBER_ID,
                    a.SUBSCRIBER_ID
               --*
               FROM CHOICE.FCT_CM_MEMBER_FOLLOWUP@dlake  a
                    JOIN choice.FCT_CM_SCRIPT_MEM_RUN_LOG@dlake b
                        ON     a.DL_FCT_CM_MEMBER_FOLLOWUP_SK =
                               b.DL_FCT_CM_MEMBER_FOLLOWUP_SK
                           AND status_id = 1
              WHERE DL_REF_CM_SCRIPT_SK IN (231,
                                            107,
                                            243,
                                            250            --MASTER FINAL GCV8
                                               ,
                                            244 --TRANSITION OF CARE FINAL GCV8
                                               ,
                                            245      --REASSESSMENT FINAL GCV8
                                               ,
                                            246      --WELCOME CALL FINAL GCV8
                                               )
             UNION ALL
             SELECT 2                                               SEQ,
                    TO_NUMBER (TO_CHAR (CREATED_DATE, 'YYYYMM'))    MONTH_ID,
                    PATIENT_ID,
                    CREATED_DATE,
                    CREATED_BY,
                    'PATIENT_FOLLOWUP_ID'                           SRC1,
                    PATIENT_FOLLOWUP_ID                             SRC_ID1,
                    'DL_REF_CM_ACTIVITY_TYPE_SK'                    SRC2,
                    DL_REF_CM_ACTIVITY_TYPE_SK                      SRC_ID2,
                    MEMBER_ID,
                    SUBSCRIBER_ID
               --*
               FROM CHOICE.FCT_CM_MEMBER_FOLLOWUP@dlake A
              WHERE A.DL_REF_CM_ACTIVITY_TYPE_SK IN
                        (SELECT DL_REF_CM_ACTIVITY_TYPE_SK
                           FROM choice.REF_CM_ACTIVITY_TYPE@dlake
                          WHERE cm_activity_type_name IN
                                    ('CARE COORDINATION',
                                     'CARE TRANSITIONS',
                                     'TCM: CARE PLANNING',
                                     'TCM: CARE COORDINATION',
                                     'TCM: CARE TRANSITIONS',
                                     'TCM: DISENROLLMENT PLANNING',
                                     'WELLNESS INITIATIVES',
                                     'MEC - MEDICAID ISSUES' --'CASE CONFERENCE','CARE MANAGEMENT ENCOUNTER' ,'TCM: POST HOSP D/C 14 DAY MD FOLLOW-UP',
                                                            --'MEC - CONCRETE ENTITLEMENTS' ,
                                                            ))
             UNION ALL
             SELECT 3
                        SEQ,
                    TO_NUMBER (TO_CHAR (a.CREATED_ON, 'YYYYMM'))
                        MONTH_ID,
                    a.PATIENT_ID,
                    a.CREATED_ON,
                    a.CREATED_BY,
                    'HEALTH_NOTES_ID'
                        SRC1,
                    HEALTH_NOTES_ID
                        SRC_ID1,
                    'HEALTH_NOTE_TYPE_ID'
                        SRC2,
                    HEALTH_NOTE_TYPE_ID
                        SRC_ID2,
                    NULL
                        MEMBER_ID,
                    UNIQUE_ID
                        SUBSCRIBER_ID
               FROM CMGC.HEALTH_NOTES  a
                    --join choice.dim_member_detail@dlake b on (a.patient_id = b.meme_ck and src_sys= 'TMG')
                    JOIN cmgc.patient_details b
                        ON (a.patient_id = b.patient_id)
              WHERE HEALTH_NOTE_TYPE_ID IN
                        (SELECT note_type_id
                           FROM cmgc.HEALTH_NOTE_TYPE
                          WHERE NOTE_TYPE IN
                                    ('TCM: Member Call',
                                     'TCM: PCSP/CARE PLAN',
                                     'End of Life Care Planning Consult',
                                     'TCM: Care Giver/Family Support',
                                     'IDT/ICT Note',
                                     'TCM: General' --,'MEC Concrete Entitlements'
                                                   ,
                                     'MEC Medicaid Issues' --,'TCM: Provider Call'
                                                          --,'TCM: Other Services Discussed'
                                                          ))),
        v_cm_ope_data1
        AS
            (SELECT *
               FROM (SELECT a.*,
                            ROW_NUMBER ()
                                OVER (PARTITION BY patient_id, month_id
                                      ORDER BY seq, month_id ASC)    opre_seq
                       FROM v_cm_ope_data A)
              WHERE opre_seq = 1)
    SELECT SEQ,
           B.MONTH_ID,
           B.MEMBER_ID,
           B.DL_LOB_ID,
           B.PROGRAM,
           B.SUBSCRIBER_ID,
           B.dl_plan_sk,
           A.PATIENT_ID,
           A.CREATED_DATE,
           A.CREATED_BY,
           A.SRC1                             SRC_KEY_DESC1,
           A.SRC_ID1                          SRC_KEY1,
           A.SRC2                             SRC_KEY_DESC2,
           A.SRC_ID2                          SRC_KEY2,
           OPRE_SEQ,
           DECODE (A.SRC_ID1, NULL, 0, 1)     NUM,
           1                                  DENUM,
           c.cm_staff_id                      AS CM_MEMBER_ID,
           b.first_name                       AS CM_First_Name,
           b.last_name                        AS CM_Last_Name, --Co-ordinated Care Manager Info (CCM)
           d.cm_staff_id                      AS CCM_MEMBER_ID,
           d.first_name                       AS CCM_First_Name,
           d.last_name                        AS CCM_Last_name
      FROM FACT_MEMBER_MONTH  B
           LEFT JOIN v_cm_ope_data1 A
               ON (    A.SUBSCRIBER_ID = B.SUBSCRIBER_ID
                   AND A.MONTH_ID = B.MONTH_ID)
           LEFT JOIN choice.dim_member_care_manager@DLAKE b1
               ON (B.cm_sk_id = b1.cm_sk_id AND b1.DL_ACTIVE_REC_IND = 'Y')
           LEFT JOIN choice.DIM_CM_STAFF_DETAILS@dlake c
               ON (    c.dl_active_rec_ind = 'Y'
                   AND c.cm_staff_id = b1.care_manager_id)
           LEFT JOIN choice.DIM_CM_STAFF_DETAILS@dlake d
               ON (    c.dl_active_rec_ind = 'Y'
                   AND d.cm_staff_id = c.assigned_to
                   AND d.dl_active_rec_ind = 'Y')
     WHERE B.PROGRAM = 'MLTC') 