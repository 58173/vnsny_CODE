SELECT DISTINCT PATIENT_ID, FIRST_NAME, LAST_NAME, ADMIT_DATE, CLIENT_PATIENT_ID,DOB
FROM
                (SELECT pd.PATIENT_ID,
                        pd.FIRST_NAME,
                        pd.LAST_NAME,
                        PD.CLIENT_PATIENT_ID,
                        TRUNC(PD.BIRTH_YEAR) DOB,
                        TRUNC (srl.START_DATE)
                            AS ADMIT_DATE,
                        rld.SCRIPT_ID,
                        lbp.LOB_ID,
                        SYSDATE - TRUNC (srl.START_DATE)
                            AS DAYS_SINCE_ADMIT,
                        (MONTHS_BETWEEN (SYSDATE, TRUNC (srl.START_DATE)))
                            AS MONTHS_SINCE_ADMIT,
                        ROW_NUMBER ()
                            OVER (PARTITION BY pd.PATIENT_ID, rld.SCRIPT_ID
                                  ORDER BY pd.PATIENT_ID, rld.SCRIPT_ID)
                            AS ROW_NUM
                   FROM PHGC.PATIENT_DETAILS  pd
                        LEFT JOIN PHGC.SCPT_PATIENT_SCRIPT_RUN_LOG srl
                            ON pd.PATIENT_ID = srl.PATIENT_ID
                        LEFT JOIN PHGC.SCPT_PATIENT_SCPT_RUN_LOG_DET rld
                            ON srl.SCRIPT_RUN_LOG_ID = rld.SCRIPT_RUN_LOG_ID
                        LEFT JOIN PHGC.MEM_BENF_PLAN m
                            ON srl.PATIENT_ID = m.MEMBER_ID
                        LEFT JOIN PHGC.LOB_BENF_PLAN lbp
                            ON m.LOB_BEN_ID = lbp.LOB_BEN_ID
                 WHERE     lbp.LOB_ID = 5
                       ---AND rld.SCRIPT_ID = 143  --- Longitudinal Initial Assessment
                        AND UPPER (pd.FIRST_NAME) NOT IN
                               ('AHQATEST', 'VNSQATEST', 'TEST')
                        AND srl.STATUS_ID = 1    --- script status = completed
                        )   
      --  WHERE PATIENT_ID = 51255                       
               
                                
                                
                               