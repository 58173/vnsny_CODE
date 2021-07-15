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
                            M.DISCHARGE_DATE_MANUAL              discharge_date,
                            M.PROGRAM,
                            S.script_id,
                            trunc(S.PERFORMED_DATE)      PERFORMED_DATE     ---SCRIPT PERFORMED DATE
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
                      WHERE  S.script_id = 223
                            and M.program ='Post Acute'
                            AND M.episode_start_date IS NOT NULL
                        --    AND M.DISCHARGE_DATE_MANUAL IS NOT NULL
                                                                          )
              WHERE rk = 1)
  select  d.patient_id,1047 measure_id
          ,case when f.question_option_id <7945 then 10471
               when f.question_option_id >=7945 then 10473
               else 10472
           end sub_measure_id
          ,1 as flag
          ,case when f.question_option_id <7945 then 10
               when f.question_option_id >=7945 then 30
               else 90
           end sub_measure_id_1
          ,trunc(d.PERFORMED_DATE) the_day, 'script_start_day' date_type
          ,payor,discharge_date
  from post_d d 
    left join
      (select * from (
        select patient_id,PERFORMED_DATE, question_option_id,option_value
             ,rank() over(partition by patient_id order by PERFORMED_DATE) option_rk
             -- ,rank() over(partition by patient_id order by script_start_date desc) script_date_rk
        from CMODM.CMO_SCRIPT_DETAILS@DLAKE
        where script_id = 163 
            and question_id = 2896
        ) where option_rk = 1 )f
   on d.PATIENT_ID = f.patient_id 
          and f.PERFORMED_DATE >= d.PERFORMED_DATE        
  where d.PERFORMED_DATE <= sysdate-30  
