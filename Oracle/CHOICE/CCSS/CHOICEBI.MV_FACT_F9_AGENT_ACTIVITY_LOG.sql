



DROP MATERIALIZED VIEW CHOICEBI.MV_FACT_F9_AGENT_ACTIVITY_LOG;

CREATE MATERIALIZED VIEW CHOICEBI.MV_FACT_F9_AGENT_ACTIVITY_LOG 
AS 
SELECT DISTINCT
       TO_TIMESTAMP (SUBSTR (TIMESTAMP1, 6), 'DD MON YYYY HH24:MI:SS')   TIMESTAMP,
       AGENT_ID,
       AGENT_ID   AGENT_SK,
       DATE1,
       CALL_ID,
       A.SKILL,
       S.DL_SKILL_SK,
       A.CAMPAIGN,
       C.DL_CAMPAIGN_SK,
       CAMPAIGN_TYPE,
       LOGIN_TIME,
       TO_TIMESTAMP (SUBSTR (LOGIN_TIMESTAMP, 6), 'DD MON YYYY HH24:MI:SS')
           LOGIN_TIMESTAMP,
        (TO_NUMBER ( SUBSTR( LOGIN_TIME, 1, INSTR(LOGIN_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR( LOGIN_TIME, INSTR(LOGIN_TIME, ':', 1, 1 ) + 1, INSTR(LOGIN_TIME, ':', 1, 2 ) - INSTR(LOGIN_TIME, ':', 1, 1 ) - 1 )) * 60)
      + (TO_NUMBER (SUBSTR( LOGIN_TIME, INSTR(LOGIN_TIME, ':', 1, 2 ) + 1, INSTR(LOGIN_TIME, ':', 1, 2 ) - INSTR( LOGIN_TIME, ':', 1, 1 ) - 1 )))
           AS LOGIN_TIME_IN_SECONDS,
       LOGOUT_TIME,
       TO_TIMESTAMP (SUBSTR (LOGOUT_TIMESTAMP, 6), 'DD MON YYYY HH24:MI:SS')
           LOGOUT_TIMESTAMP,
        (TO_NUMBER ( SUBSTR( LOGOUT_TIME, 1, INSTR(LOGOUT_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR( LOGOUT_TIME, INSTR(LOGOUT_TIME, ':', 1, 1 ) + 1, INSTR(LOGOUT_TIME, ':', 1, 2 ) - INSTR(LOGOUT_TIME, ':', 1, 1 ) - 1 )) * 60)
      + (TO_NUMBER (SUBSTR( LOGOUT_TIME, INSTR( LOGOUT_TIME, ':', 1, 2 ) + 1, INSTR(LOGOUT_TIME, ':', 1, 2 ) - INSTR(LOGOUT_TIME, ':', 1, 1 ) - 1 )))
           AS LOGOUT_TIME_IN_SECONDS,
       STATE,
       AGENT_STATE_TIME,
         (TO_NUMBER (SUBSTR(AGENT_STATE_TIME, 1, INSTR(AGENT_STATE_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR(AGENT_STATE_TIME, INSTR(AGENT_STATE_TIME, ':', 1, 1 ) + 1, INSTR(AGENT_STATE_TIME, ':', 1, 2 ) - INSTR(AGENT_STATE_TIME, ':', 1, 1 ) - 1 )) * 60)
      + (TO_NUMBER (SUBSTR( AGENT_STATE_TIME, INSTR( AGENT_STATE_TIME, ':', 1, 2 ) + 1, INSTR(AGENT_STATE_TIME, ':', 1, 2 ) - INSTR(AGENT_STATE_TIME, ':', 1, 1 ) - 1 )))
           AS AGENT_STATE_TIME_IN_SECONDS,
       AGENT_STATES,
       PAID_TIME,
         (TO_NUMBER (SUBSTR(PAID_TIME, 1, INSTR(PAID_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR(PAID_TIME, INSTR(PAID_TIME, ':', 1, 1 ) + 1, INSTR(PAID_TIME, ':', 1, 2 ) - INSTR(PAID_TIME, ':', 1, 1 ) - 1 )) * 60)
       + (TO_NUMBER (SUBSTR( PAID_TIME, INSTR( PAID_TIME, ':', 1, 2 ) + 1, INSTR(PAID_TIME, ':', 1, 2 ) - INSTR(PAID_TIME, ':', 1, 1 ) - 1 )))
           AS PAID_TIME_IN_SECONDS,
       UNPAID_TIME,
         (TO_NUMBER (SUBSTR(UNPAID_TIME, 1, INSTR(UNPAID_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR(UNPAID_TIME, INSTR(UNPAID_TIME, ':', 1, 1 ) + 1, INSTR(UNPAID_TIME, ':', 1, 2 ) - INSTR(UNPAID_TIME, ':', 1, 1 ) - 1 )) * 60)
       + (TO_NUMBER (SUBSTR(UNPAID_TIME, INSTR(UNPAID_TIME, ':', 1, 2 ) + 1, INSTR(UNPAID_TIME, ':', 1, 2 ) - INSTR(UNPAID_TIME, ':', 1, 1 ) - 1 )))
           AS UNPAID_TIME_IN_SECONDS,
       READY_TIME,
         (TO_NUMBER (SUBSTR(READY_TIME, 1, INSTR(READY_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR(READY_TIME, INSTR(READY_TIME, ':', 1, 1 ) + 1, INSTR(READY_TIME, ':', 1, 2 ) - INSTR(READY_TIME, ':', 1, 1 ) - 1 )) * 60)
       + (TO_NUMBER (SUBSTR(READY_TIME, INSTR(READY_TIME, ':', 1, 2 ) + 1, INSTR(READY_TIME, ':', 1, 2 ) - INSTR(READY_TIME, ':', 1, 1 ) - 1 )))
           AS READY_TIME_IN_SECONDS,
       NOT_READY_TIME,
         (TO_NUMBER (SUBSTR(NOT_READY_TIME, 1, INSTR(NOT_READY_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR(NOT_READY_TIME, INSTR(NOT_READY_TIME, ':', 1, 1 ) + 1, INSTR(NOT_READY_TIME, ':', 1, 2 ) - INSTR(NOT_READY_TIME, ':', 1, 1 ) - 1 )) * 60)
       + (TO_NUMBER (SUBSTR(NOT_READY_TIME, INSTR(NOT_READY_TIME, ':', 1, 2 ) + 1, INSTR(NOT_READY_TIME, ':', 1, 2 ) - INSTR(NOT_READY_TIME, ':', 1, 1 ) - 1 )))
           AS NOT_READY_TIME_IN_SECONDS,
       REASON_CODE,        --- Reason code for the NOT READY and LOGOUT states
       ON_ACW_TIME,
         (TO_NUMBER (SUBSTR(ON_ACW_TIME, 1, INSTR(ON_ACW_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR(ON_ACW_TIME, INSTR(ON_ACW_TIME, ':', 1, 1 ) + 1, INSTR(ON_ACW_TIME, ':', 1, 2 ) - INSTR(ON_ACW_TIME, ':', 1, 1 ) - 1 )) * 60)
       + (TO_NUMBER (SUBSTR(ON_ACW_TIME, INSTR(ON_ACW_TIME, ':', 1, 2 ) + 1, INSTR(ON_ACW_TIME, ':', 1, 2 ) - INSTR(ON_ACW_TIME, ':', 1, 1 ) - 1 )))
           AS ON_ACW_TIME_IN_SECONDS,
       ON_CALL_TIME,
         (TO_NUMBER (SUBSTR(ON_CALL_TIME, 1, INSTR(ON_CALL_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR(ON_CALL_TIME, INSTR(ON_CALL_TIME, ':', 1, 1 ) + 1, INSTR(ON_CALL_TIME, ':', 1, 2 ) - INSTR(ON_CALL_TIME, ':', 1, 1 ) - 1 )) * 60)
       + (TO_NUMBER (SUBSTR(ON_CALL_TIME, INSTR(ON_CALL_TIME, ':', 1, 2 ) + 1, INSTR(ON_CALL_TIME, ':', 1, 2 ) - INSTR(ON_CALL_TIME, ':', 1, 1 ) - 1 )))
           AS ON_CALL_TIME_IN_SECONDS,
       ON_VOICEMAIL_TIME,
          (TO_NUMBER (SUBSTR(ON_VOICEMAIL_TIME, 1, INSTR(ON_VOICEMAIL_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR(ON_VOICEMAIL_TIME, INSTR(ON_VOICEMAIL_TIME, ':', 1, 1 ) + 1, INSTR(ON_VOICEMAIL_TIME, ':', 1, 2 ) - INSTR(ON_VOICEMAIL_TIME, ':', 1, 1 ) - 1 )) * 60)
       + (TO_NUMBER (SUBSTR(ON_VOICEMAIL_TIME, INSTR(ON_VOICEMAIL_TIME, ':', 1, 2 ) + 1, INSTR(ON_VOICEMAIL_TIME, ':', 1, 2 ) - INSTR(ON_VOICEMAIL_TIME, ':', 1, 1 ) - 1 )))
           AS ON_VOICEMAIL_TIME_IN_SECONDS,
       MANUAL_TIME,
          (TO_NUMBER (SUBSTR(MANUAL_TIME, 1, INSTR(MANUAL_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR(MANUAL_TIME, INSTR(MANUAL_TIME, ':', 1, 1 ) + 1, INSTR(MANUAL_TIME, ':', 1, 2 ) - INSTR(MANUAL_TIME, ':', 1, 1 ) - 1 )) * 60)
       + (TO_NUMBER (SUBSTR(MANUAL_TIME, INSTR(MANUAL_TIME, ':', 1, 2 ) + 1, INSTR(MANUAL_TIME, ':', 1, 2 ) - INSTR(MANUAL_TIME, ':', 1, 1 ) - 1 )))
           AS MANUAL_TIME_IN_SECONDS,
       VM_IN_PROGRESS_TIME,
         (TO_NUMBER (SUBSTR(VM_IN_PROGRESS_TIME, 1, INSTR(VM_IN_PROGRESS_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR(VM_IN_PROGRESS_TIME, INSTR(VM_IN_PROGRESS_TIME, ':', 1, 1 ) + 1, INSTR(VM_IN_PROGRESS_TIME, ':', 1, 2 ) - INSTR(VM_IN_PROGRESS_TIME, ':', 1, 1 ) - 1 )) * 60)
       + (TO_NUMBER (SUBSTR(VM_IN_PROGRESS_TIME, INSTR(VM_IN_PROGRESS_TIME, ':', 1, 2 ) + 1, INSTR(VM_IN_PROGRESS_TIME, ':', 1, 2 ) - INSTR(VM_IN_PROGRESS_TIME, ':', 1, 1 ) - 1 )))
           AS VM_IN_PROGRESS_TIME_IN_SECONDS,
       WAIT_TIME,
         (TO_NUMBER (SUBSTR(WAIT_TIME, 1, INSTR(WAIT_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR(WAIT_TIME, INSTR(WAIT_TIME, ':', 1, 1 ) + 1, INSTR(WAIT_TIME, ':', 1, 2 ) - INSTR(WAIT_TIME, ':', 1, 1 ) - 1 )) * 60)
       + (TO_NUMBER (SUBSTR(WAIT_TIME, INSTR(WAIT_TIME, ':', 1, 2 ) + 1, INSTR(WAIT_TIME, ':', 1, 2 ) - INSTR(WAIT_TIME, ':', 1, 1 ) - 1 )))
           AS WAIT_TIME_IN_SECONDS,
       VIDEO_TIME,
          (TO_NUMBER (SUBSTR(VIDEO_TIME, 1, INSTR(VIDEO_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR(VIDEO_TIME, INSTR(VIDEO_TIME, ':', 1, 1 ) + 1, INSTR(VIDEO_TIME, ':', 1, 2 ) - INSTR(VIDEO_TIME, ':', 1, 1 ) - 1 )) * 60)
       + (TO_NUMBER (SUBSTR(VIDEO_TIME, INSTR(VIDEO_TIME, ':', 1, 2 ) + 1, INSTR(VIDEO_TIME, ':', 1, 2 ) - INSTR(VIDEO_TIME, ':', 1, 1 ) - 1 )))
           AS VIDEO_TIME_IN_SECONDS,
       RINGING_TIME,
          (TO_NUMBER (SUBSTR(RINGING_TIME, 1, INSTR(RINGING_TIME, ':', 1, 1 ) - 1  )) * 60 * 60)
       + (TO_NUMBER (SUBSTR(RINGING_TIME, INSTR(RINGING_TIME, ':', 1, 1 ) + 1, INSTR(RINGING_TIME, ':', 1, 2 ) - INSTR(RINGING_TIME, ':', 1, 1 ) - 1 )) * 60)
       + (TO_NUMBER (SUBSTR(RINGING_TIME, INSTR(RINGING_TIME, ':', 1, 2 ) + 1, INSTR(RINGING_TIME, ':', 1, 2 ) - INSTR(RINGING_TIME, ':', 1, 1 ) - 1 )))
           AS RINGING_TIME_IN_SECONDS,
       MEDIA_AVAILABILITY,
       AVAILABLE_FOR_ALL,
       AVAILABLE_FOR_CALLS,
       AVAILABLE_FOR_VM,
       CALL_TYPE,
       LONG_AFTER_CALL_WORK,
       SHORT_CALLS,
       AGENT_DISCONNECTS_FIRST
  FROM DW_OWNER.F9_AGENT   A
  JOIN MV_DIM_F9_SKILL S ON (A.SKILL = S.SKILL)
  JOIN MV_DIM_F9_CAMPAIGN C  ON (A.CAMPAIGN = C.CAMPAIGN);



GRANT SELECT ON CHOICEBI.MV_FACT_F9_AGENT_ACTIVITY_LOG TO CHOICEBI_RO,CHOICEBI_RO_NEW, LINKADM,LINKADM2, MSTRSTG,ROC_RO;
