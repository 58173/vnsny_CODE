
select distinct PATIENT_ID, CLIENT_PATIENT_ID, FIRST_NAME, LAST_NAME,BIRTH_YEAR,ADMIT_DATE
from 
  (
SELECT a.*, hi.COMMENTS, hi.HI_APPT_DATE, hi.INDICATOR_ID, hi.INDICATOR_NAME, hi.RECORD_ID
FROM (
    SELECT pd.PATIENT_ID,pd.CLIENT_PATIENT_ID,pd.first_name,pd.last_name,pd.BIRTH_YEAR, TRUNC(aa.START_DATE) AS ADMIT_DATE, 
        MONTHS_BETWEEN(sysdate, TRUNC(aa.START_DATE)) AS MONTHS_SINCE_ADMIT,
        row_number() over(partition by pd.PATIENT_ID, aa.SCRIPT_ID order by pd.PATIENT_ID, aa.START_DATE DESC) AS row_num
    FROM PHGC.PATIENT_DETAILS pd
    LEFT JOIN PHGC.MEM_BENF_PLAN m
        ON pd.PATIENT_ID = m.MEMBER_ID
    LEFT JOIN PHGC.LOB_BENF_PLAN lbp
        ON m.LOB_BEN_ID = lbp.LOB_BEN_ID
    LEFT JOIN (SELECT *
                FROM PHGC.SCPT_PATIENT_SCRIPT_RUN_LOG srl
                LEFT JOIN PHGC.SCPT_PATIENT_SCPT_RUN_LOG_DET rld
                    ON srl.SCRIPT_RUN_LOG_ID = rld.SCRIPT_RUN_LOG_ID
                WHERE rld.SCRIPT_ID = 143
                    AND srl.STATUS_ID = 1 -- ACTIVE 
                    ) aa
        ON pd.PATIENT_ID = aa.PATIENT_ID
    WHERE lbp.LOB_ID = 5
        AND aa.STATUS_ID = 1) a
LEFT JOIN ( SELECT hir.PATIENT_ID,  hi.INDICATOR_ID, hi.INDICATOR_NAME, hir.RECORD_ID, TRUNC(hir.RECORD_DATE) AS HI_APPT_DATE, hir.COMMENTS
                FROM PHGC.HEALTH_INDICATOR_RECORD hir
                INNER JOIN PHGC.HEALTH_INDICATOR_PARAMETER  hip
                    ON hir.PARAMETER_ID = hip.PARAMETER_ID
                INNER JOIN PHGC.HEALTH_INDICATOR hi
                    ON hip.INDICATOR_ID = hi.INDICATOR_ID
                WHERE hi.INDICATOR_ID IN (80, 81, 82)       -- 80-82 = MD appointment (80 = post acute discharge, 81 = PCP referral, 82 = clinic referral)
                ) hi                                          -- post-acute should be included (CF email from 10/4 Andrea Spencer for explanation)
    ON a.PATIENT_ID  = hi.PATIENT_ID
WHERE a.ROW_NUM = 1
ORDER BY a.PATIENT_ID, hi.RECORD_ID  
)