------------------------------------------------------------------------------------------------
-----------------------------------F9_AGENT-----------------------------------------------------

select * from DW_OWNER.F9_AGENT

;



----AGENT_A
SELECT DISTINCT 
                AGENT_ID,
                AGENT_NAME,
                AGENT_FIRST_NAME,
                AGENT_LAST_NAME,
                AGENT,
                AGENT_EMAIL,
                AGENT_GROUP,
                EXTENSION,
                AGENT_START_DATE,
                ENABLED_FOR_VIDEO           
FROM DW_OWNER.F9_AGENT
     ;

            
--- AGENT_D
SELECT DISTINCT    
                 AGENT_ID, CALL_ID,DATE1,
                 HALF_HOUR,HOUR1,TIME1,TIMESTAMP1,END_TIME_MILLISECOND,TIMESTAMP_MILLISECOND,
                 AGENT_STATES, AVAILABLE_FOR_ALL, AVAILABLE_FOR_CALLS, AVAILABLE_FOR_VM, MEDIA_AVAILABILITY,                
                 REASON_CODE,SKILL_AVAILABILITY,STATE, UNAVAILABLE_FOR_CALLS, UNAVAILABLE_FOR_VM,  AGENT_STATE_TIME,
                 LOGIN_TIME, LOGIN_TIMESTAMP,LOGOUT_TIME, LOGOUT_TIMESTAMP,MANUAL_TIME,
                 NOT_READY_TIME, ON_ACW_TIME, ON_CALL_TIME, ON_VOICEMAIL_TIME,
                 PAID_TIME, READY_TIME, RINGING_TIME, UNPAID_TIME,VIDEO_TIME,VM_IN_PROGRESS_TIME
                 WAIT_TIME,AGENT_DISCONNECTS_FIRST,
                --- CAMPAIGN,
                 DISPOSITION,
                 AFTER_CALL_WORK_TIME,CONFERENCE_TIME,CONFERENCES,CONSULT_TIME,CONSULTS,DIAL_TIME,
                 HANDLE_TIME, HOLD_TIME, HOLDS, LONG_AFTER_CALL_WORK, LONG_CALLS, LONG_HOLDS, LONG_PARKS, 
                 MISSED_CALLS, MISSED_CALLS_RETURNED, PARK_TIME, PARKS, PREVIEW_INTERRUPTED, PREVIEW_INTERRUPTED_BY_CALL, 
                 PREVIEW_INTERRUPTED_BY_SKILL_VM, PREVIEW_TIME, QUEUE_CALLBACK_PROCESSING, QUEUE_CALLBACK_REGISTERED, RING_TIME, 
                 SHORT_AFTER_CALL_WORK, SHORT_CALLS, TALK_TIME, TALK_TIME_LESS_HOLD_AND_PARK, TIME_TO_RETURN_MISSED_CALL, TRANSFERS,
                  WORKSHEET, VOICEMAIL_HANDLE_TIME, VOICEMAILS, VOICEMAILS_DECLINED, VOICEMAILS_DELETED, VOICEMAILS_HANDLED, VOICEMAILS_RETURNED_CALL, VOICEMAILS_TRANSFERRED
FROM DW_OWNER.F9_AGENT
order by CALL_ID;                 



------------------------------------------------------------------------------------------------
-----------------------------------F9_CALL_LOG---------------------------------------------------

SELECT * FROM DW_OWNER.F9_CALL_LOG;


-------CALL_A
SELECT CALL_ID,
       CALL_TYPE,
       DISPOSITION,
       DNIS,          ---- dialed number Identification service
       SESSION_ID,
       AGENT_ID,
       CONTACT_ID
FROM  DW_OWNER.F9_CALL_LOG
;      




----CALL_D
SELECT CALL_ID,
       TIMESTAMP1,
       ABANDON_RATE,
       CALLS,
       CALL_TYPE,
       CONTACTED,
       CUSTOMER_NAME,
       DISCONNECTED_FROM_HOLD,
       DISPOSITION,DISPOSITION_GROUP_A, DISPOSITION_GROUP_B, DISPOSITION_GROUP_C, DISPOSITION_PATH,
       SKILL,
       DNIS,  ---- dialed number Identification service
       ANI,    ---Automatic Number Identification 
       ANI_AREA_CODE,
       ANI_COUNTRY,
       ANI_COUNTRY_CODE,
       ANI_STATE,
    --   DNIS_AREA_CODE, DNIS_COUNTRY, DNIS_COUNTRY_CODE, DNIS_STATE,
       CALL_SURVEY_RESULT
       CALLS_COMPLETED_IN_IVR,
       CAMPAIGN_TYPE,
       IVR_PATH,
       LIST_NAME, LIVE_CONNECT, NO_PARTY_CONTACT, NOTES, PARENT_SESSION_ID, RECORDINGS, SERVICE_LEVEL,
       SKILL, SPEED_OF_ANSWER, THIRD_PARTY_TALK_TIME, AFTER_CALL_WORK_TIME, BILL_TIME_ROUNDED, 
       CALL_TIME, CONFERENCE_TIME, CONFERENCES, CONSULT_TIME, COST, DIAL_TIME, HANDLE_TIME, HOLD_TIME, 
       HOLDS, IVR_COST, IVR_RATE, IVR_TIME, MANUAL_TIME, PARK_TIME, PARKS, PREVIEW_INTERRUPTED, PREVIEW_INTERRUPTED_BY_CALL, 
       PREVIEW_INTERRUPTED_BY_SKILL_VM, PREVIEW_TIME, QUEUE_CALLBACK_PROCESSING, QUEUE_CALLBACK_REGISTERED, QUEUE_CALLBACK_WAIT_TIME, 
       QUEUE_WAIT_TIME, RATE, RING_TIME, TALK_TIME, TALK_TIME_LESS_HOLD_AND_PARK, TIME_TO_ABANDON, TOTAL_QUEUE_TIME, TRANSFERS, VIDEO_TIME, VOICEMAILS, 
       VOICEMAILS_DECLINED, VOICEMAILS_DELETED, VOICEMAILS_HANDLE_TIME, VOICEMAILS_HANDLED, VOICEMAILS_RETURNED_CALL, VOICEMAILS_TRANSFERRED
FROM  DW_OWNER.F9_CALL_LOG;      
       
       


------------------------------------------------------------------------------------------------
-----------------------------------F9_CALL_IVR--------------------------------------------------

SELECT * FROM DW_OWNER.F9_IVR; ----Interactive Voice Response


---IVR_A
SELECT DISTINCT 
       MODULE,
       MODULE_TYPE,
       PATH_TO_MODULE
FROM DW_OWNER.F9_IVR
ORDER BY MODULE
;

SELECT DISTINCT   
        MODULE,
        PATH_TO_MODULE
FROM DW_OWNER.F9_IVR
;


---IVR_D
SELECT 
       CALL_ID,
       MODULE_TIME,
       IVR_TIME_TO_MODULE,
       PATH_TO_MODULE,
       IVR_SESSION_ID,
       CALLS_ABANDONED_IN_QUEUE,
       CALLS_COMPLETED_IN_IVR, 
       CALLS_DISCONNECTED_IN_IVR, 
       CALLS_TRANSFERRED_TO_AGENT,
       IVR_COST,IVR_RATE,
       IVR_PATH,IVR_SCRIPT,
       IVR_TIME, IVR_TIME_TO_ABANDON, IVR_TIME_TO_FIRST_PROMPT, IVR_TIME_TO_FIRST_QUEUE,
       MEDIA_TYPE, PATH_TO_ABANDON, PATH_TO_AGENT, PATH_TO_AGENT_TRANSFER, PATH_TO_NO_MATCH, PATH_TO_SKILL, PATH_WITH_MAX_AGENT_TRANSFER, VISUAL_IVR, VOICE_IVR,
       DTMF_INPUTS, INPUT_ATTEMPTS, INPUT_TIMEOUTS,
       QUERY_MODULE_ERROR, QUERY_MODULE_LATENCY, QUERY_MODULE_TIMEOUT, RECORDING, SILENCE_TIMEOUTS, SPEECH_INPUTS, TERMINATIONS, USER_INPUT
FROM DW_OWNER.F9_IVR
--where terminations = 1
;       
       



------------------------------------------------------------------------------------------------
-----------------------------------F9_CALL_SEGMENT-------------------------------------------------

SELECT * FROM DW_OWNER.F9_CALL_SEGMENT;


---SEGMENT
SELECT 
       CALL_ID,
       CALL_SEGMENT_ID, 
       CALLED_PARTY,
       CALLING_PARTY, 
       RESULT, SEGMENT_TIME, SEGMENT_TYPE
FROM DW_OWNER.F9_CALL_SEGMENT
;


------------------------------------------------------------------------------------------------
-----------------------------------F9_DNIS-------------------------------------------------
    SELECT * FROM    DW_OWNER.F9_DNIS;
    
    
    
       ---DNIS
       SELECT DISTINCT
        CAMPAIGN, DNIS
       FROM DW_OWNER.F9_DNIS 
       ORDER BY CAMPAIGN;
       
       select count(distinct campaign)
      FROM DW_OWNER.F9_DNIS ;
       


------------------------------------------------------------------------------------------------
-----------------------------------F9_CONACT-------------------------------------------------

SELECT * FROM DW_OWNER.F9_CONTACT
where call_id = 976;

----CONTACT
SELECT DISTINCT
       call_id, CONTACT_ID,CONTACT_IN_DNC,CONTACT_RECORDS,EMAIL,FIRST_NAME,LAST_NAME,NUMBER1,
       NUMBER2, NUMBER3, STATE, STREET, ZIP
FROM DW_OWNER.F9_CONTACT;

----DNC
SELECT DNC_NUMBER, 
       RECORD_COUNTER, 
       CITY, COMPANY, 
       CONTACT_ID, 
       CUSTOMER_NAME,
        EMAIL, 
        FIRST_NAME, 
        LAST_NAME, 
        NUMBER1, 
        NUMBER2, 
        NUMBER3, 
        STATE, 
        STREET, ZIP 
FROM DW_OWNER.F9_DNC;