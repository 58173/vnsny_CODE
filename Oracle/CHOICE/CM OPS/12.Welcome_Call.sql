WITH 
    NEW_ENROLL
 AS
   (
    SELECT MONTH_ID,
           SUBSCRIBER_ID,
           MEMBER_ID,
           DL_LOB_ID,
           DL_PLAN_SK,
           ENROLLMENT_DATE,
           VPIN
    FROM CHOICEBI.FACT_MEMBER_MONTH 
    WHERE ENROLLED_FLAG = 1
     AND dl_lob_id IN (2, 5)
     AND PROGRAM IN ('MLTC') 
     AND ENROLLMENT_DATE >= ADD_MONTHS (TRUNC (SYSDATE, 'month'), -12)
  )
  SELECT  
  DEN.*, scr.*                
  FROM  NEW_ENROLL DEN
  LEFT JOIN V_CM_OPS_SCRIPT_DATA  SCR   
         ON SCR.SUBSCRIBER_ID = DEN.SUBSCRIBER_ID
        AND DEN.MONTH_ID = SCR.MONTH_ID
        AND SCR.SCRIPT_ID = 259
       -- and SCR.DENUM = 1
;





select * from fact_cm_measures; 
select * from dim_cm_measures; --12

SELECT * FROM CHOICEBI.V_CM_MD_COLLABRATOR;


SELECT * from V_CM_BACKUPCARE_DATA;

V_CM_WELCOMECALL_DATA;