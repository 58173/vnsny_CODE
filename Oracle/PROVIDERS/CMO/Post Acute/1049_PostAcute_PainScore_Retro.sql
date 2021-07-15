with post_d as (SELECT *
               FROM (SELECT DISTINCT
                            M.VPIN,
                            S.ALT_ID,
                            S.PATIENT_ID,
                            M.SUBSCRIBER_ID,
                            M.MEMBER_ID,
                            M.FIRST_NAME,
                            M.LAST_NAME,
                            M.MEMBER_STATUS,
                            M.PAYOR,
                            M.EPISODE_START_DATE,
                            M.EPISODE_END_DATE,
                            M.DISCHARGE_DATE_MANUAL            discharge_date,
                            M.PROGRAM,
                            S.script_id,
                            trunc(S.PERFORMED_DATE)      PERFORMED_DATE     ---SCRIPT PERFORMED DATE
                            ,S.QUESTION_ID 
                            ,S.QUESTION_OPTION_ID
                           ,DENSE_RANK()
                                 OVER (PARTITION BY S.ALT_ID,
                                                    S.PATIENT_ID,
                                                    M.EPISODE_START_DATE, 
                                                    M.EPISODE_END_DATE,
                                                    M.PAYOR
                                 ORDER BY  S.SCRIPT_ID DESC, S.PERFORMED_DATE)  RK
                       FROM CMODM.CMO_ACTIVE_MEMBER@dlake M
                       LEFT JOIN CMODM.CMO_SCRIPT_DETAILS@DLAKE S
                              ON M.VPIN = S.VPIN
                             AND  S.PERFORMED_DATE BETWEEN M.episode_start_date
                                                     AND NVL(M.episode_end_Date,SYSDATE)                                                
             WHERE   S.script_id = 223
                            AND M.program ='Post Acute'
                            AND M.episode_start_date IS NOT NULL  
                            AND S.QUESTION_ID = 3856
                                           )                   
              WHERE   RK= 1 )                                                                                                                                                                                          
        SELECT D.PATIENT_ID, 1049 MEASURE_ID,               
                 CASE 
                      WHEN QUESTION_OPTION_ID IN (10253, 10254,10255, 10256,10257)   --- pain_score: 0-4
                      THEN 10491
                      WHEN QUESTION_OPTION_ID IN (10258,10259,10260)                 --- pain_score: 5-7
                      THEN 10493
                      WHEN QUESTION_OPTION_ID IN (10261,10262,10263)                  --- pain_score: 8-10
                      THEN 10492
                     ELSE NULL
                 END          SUB_MEASURE_ID
                 ,1 AS FLAG,
                 CASE
                      WHEN QUESTION_OPTION_ID IN (10253, 10254,10255, 10256,10257)
                      THEN 10
                      WHEN QUESTION_OPTION_ID IN (10258,10259,10260)
                      THEN 30
                      WHEN QUESTION_OPTION_ID IN (10261,10262,10263)
                      THEN 90
                      ELSE NULL
                 END              SUB_MEASURE_ID_1,
                D.PERFORMED_DATE   THE_DAY,   'CMSBAR' DATE_TYPE,  D.PAYOR, D.discharge_date                                      
          FROM    post_d   D     