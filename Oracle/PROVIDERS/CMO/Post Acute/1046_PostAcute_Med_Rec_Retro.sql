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
                            M.DISCHARGE_DATE_MANUAL discharge_date,
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
                            and M.program ='Post Acute'
                            AND M.episode_start_date IS NOT NULL                           
                            )
              WHERE RK = 1)
    SELECT PATIENT_ID,
           1046           ---- retrospective
               measure_id,
           CASE WHEN day_diff <= 30 THEN 10461 ELSE 10462 END
               sub_measure_id,
           1
               AS flag,
          CASE WHEN day_diff <= 30 THEN 10 ELSE 90 END
               sub_measure_id_1,
           TRUNC(med_rec_day)
               the_date,
           'med_rec_day'
               date_type,
           payor,discharge_date
      FROM (SELECT d.ALT_ID,
                   d.PATIENT_ID,
                   DENSE_RANK ()
                       OVER (
                           PARTITION BY d.ALT_ID, d.PERFORMED_DATE
                           ORDER BY
                               (n.PERFORMED_DATE - d.PERFORMED_DATE))
                       d_rk,
                   n.PERFORMED_DATE
                       med_rec_day,
                   d.PERFORMED_DATE
                       cm_sbar_day,
                   trunc(n.PERFORMED_DATE) - d.discharge_date
                       day_diff,
                   d.discharge_date,
                   payor
              FROM (SELECT DISTINCT
                           ALT_ID,PATIENT_ID,PERFORMED_DATE, start_date, end_date,script_id
                      FROM CMODM.CMO_SCRIPT_DETAILS@DLAKE 
                     WHERE script_id IN (220, 196))  n
                   RIGHT JOIN post_d   d
                       ON     d.ALT_ID = n.ALT_ID
                          AND n.PERFORMED_DATE > d.PERFORMED_DATE
             WHERE SYSDATE - d.discharge_date >= 30)
     WHERE d_rk = 1