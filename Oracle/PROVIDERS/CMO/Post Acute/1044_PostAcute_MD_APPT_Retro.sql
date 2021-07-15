WITH
        post_d
        AS
            (SELECT *
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
                            M.DISCHARGE_DATE_MANUAL  discharge_date,
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
                      WHERE  S.script_id = 223
                            and M.program ='Post Acute'
                            AND M.episode_start_date IS NOT NULL                           
                            )
                 WHERE RK = 1)
      , MD_APPT AS (   SELECT * FROM (SELECT patient_id,
                        RANK ()
                        OVER (PARTITION BY patient_id ORDER BY record_date)
                           rk,
                        record_date
                   FROM PHGC.HEALTH_INDICATOR_RECORD
                  WHERE     PARAMETER_ID IN (84,85,86)     
                        AND DELETED_BY IS NULL ))
select PATIENT_ID,1044 measure_id ,   
       case when DATE_DIFF <=9 then 10441
            when DATE_DIFF >9 and DATE_DIFF<=29 then 10443
            else 10442
       end sub_measure_id
       ,1 as flag,
       case when DATE_DIFF <=9 then 10
            when DATE_DIFF >9 and DATE_DIFF<=29 then 30
            else 90
       end sub_measure_id_1
       ,trunc(RECORD_DATE) the_day
       ,'RECORD_DATE' date_type
       ,payor,discharge_date
from(
      SELECT D.PATIENT_ID, D.ALT_ID,PAYOR,EPISODE_START_DATE,PERFORMED_DATE
            ,DISCHARGE_DATE,RECORD_DATE,trunc(RECORD_DATE)-trunc(DISCHARGE_DATE) DATE_DIFF
            ,ROW_NUMBER()OVER(PARTITION BY D.PATIENT_ID ,PAYOR,EPISODE_START_DATE 
                                ORDER BY RECORD_DATE-DISCHARGE_DATE) RK
      FROM POST_D D LEFT JOIN  MD_APPT N ON D.PATIENT_ID = N.PATIENT_ID 
                             AND record_date>=DISCHARGE_DATE
      WHERE DISCHARGE_DATE <= SYSDATE-30)
WHERE RK = 1