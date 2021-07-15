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
                            M.DISCHARGE_DATE_MANUAL      DISCHARGE_DATE,
                            M.PROGRAM,
                            S.script_id,
                            trunc(S.PERFORMED_DATE)      PERFORMED_DATE     ---SCRIPT PERFORMED DATE
                           ,DENSE_RANK()
                                 OVER (PARTITION BY S.ALT_ID,
                                                    M.EPISODE_START_DATE, 
                                                    M.EPISODE_END_DATE,
                                                    M.PAYOR
                                 ORDER BY  S.SCRIPT_ID DESC, S.PERFORMED_DATE)  RK
                       FROM CMODM.CMO_ACTIVE_MEMBER@dlake M
                       LEFT JOIN CMODM.CMO_SCRIPT_DETAILS@DLAKE S
                              ON M.VPIN = S.VPIN
                             AND  S.PERFORMED_DATE BETWEEN M.episode_start_date
                                                     AND NVL(M.episode_end_Date,SYSDATE)
                      WHERE  S.script_id IN (223)
                            AND M.program ='Post Acute'
                            AND S.QUESTION_ID = 3857 
                            AND S.QUESTION_OPTION_ID = 10264  --- who were "at risk of falls" per the CM SBAR 
                            AND M.episode_start_date IS NOT NULL
                         --   AND TRUNC(M.episode_end_date )<= TRUNC(sysdate)  ---  episode has ended  
                         --   AND M.DISCHARGE_DATE_MANUAL IS NOT NULL                        
                            )
              WHERE RK = 1) 
                  ,sub_fall AS(   SELECT PATIENT_ID
                                         ,VPIN
                                         ,ALT_ID
                                         ,FIRST_NAME
                                         ,LAST_NAME
                                         ,SCRIPT_ID    SUB_ID
                                         ,SCRIPT_STATUS
                                         ,trunc(PERFORMED_DATE)      PERFORMED_DATE_SUB
                                         ,DENSE_RANK()OVER (PARTITION BY PATIENT_ID,SCRIPT_ID
                                                       ORDER BY PERFORMED_DATE DESC) RK
                                   FROM CMODM.CMO_SCRIPT_DETAILS@DLAKE
                                   WHERE SCRIPT_ID = 208
                                                        )                 
              SELECT DISTINCT D.PATIENT_ID, 1051 MEASURE_ID,
                     CASE
                         WHEN SUB_ID = 208 AND S.SCRIPT_STATUS = 'Completed' 
                                              AND TRUNC(PERFORMED_DATE_SUB) BETWEEN TRUNC(D.EPISODE_START_DATE) AND TRUNC(D.EPISODE_END_DATE) THEN 10511
                         ELSE 10512
                 END          SUB_MEASURE_ID
                 ,1 AS FLAG,
                     CASE
                         WHEN SUB_ID = 208 AND SCRIPT_STATUS = 'Completed' 
                                              AND TRUNC(PERFORMED_DATE_SUB) BETWEEN TRUNC(D.EPISODE_START_DATE) AND TRUNC(D.EPISODE_END_DATE) THEN 10                         
                         ELSE 90
                         END               SUB_MEASURE_ID_1,
                         D.PERFORMED_DATE   THE_DAY,   'CMSBAR' DATE_TYPE,  D.PAYOR, D.discharge_date                                      
          FROM    post_d   D     
           LEFT JOIN sub_fall S on D.PATIENT_ID = S.PATIENT_ID
                                                                  
              
              
              
              
        