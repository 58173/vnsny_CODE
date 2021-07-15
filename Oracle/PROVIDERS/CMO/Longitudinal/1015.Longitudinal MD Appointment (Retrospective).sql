
SELECT    DISTINCT   
               PATIENT_ID,
               1015 MEASURE_ID,
               CASE
                 WHEN TRUNC (RECORD_DATE) BETWEEN TRUNC(ADMIT_DATE)-30 AND TRUNC(ADMIT_DATE)+30
                  THEN 10151
                  ELSE 10152
                  END AS SUB_MEASURE_ID,
                  1 AS FLAG,
               CASE 
                 WHEN TRUNC (RECORD_DATE) BETWEEN TRUNC(ADMIT_DATE)-30 AND TRUNC(ADMIT_DATE)+30 
                  THEN 10
                  ELSE 90
                END AS SUB_MEASURE_ID_1,
                TRUNC (ADMIT_DATE) AS the_date,
               'Admit_Date'  DATE_TYPE,
               'Choice' PAYOR
FROM
( 
SELECT DISTINCT * 
FROM (
SELECT DISTINCT
       PATIENT_ID,
       FIRST_NAME,
       LAST_NAME,
       ADMIT_DATE,
       PAYOR,
       SCRIPT_143,
       RK,
       PARAMETER_ID,
       RECORD_DATE,
       DIFF,
       RANK ()
       OVER (PARTITION BY PATIENT_ID, admit_date ORDER BY DIFF)  DIFF_RK
 FROM (
SELECT DISTINCT
       D.PATIENT_ID,
       D.FIRST_NAME,
       D.LAST_NAME,
       D.ADMIT_DATE,  
       D.PAYOR, 
       D.SCRIPT_ID    SCRIPT_143, 
       D.RK ,
       A.PARAMETER_ID,
       TRUNC(A.RECORD_DATE) RECORD_DATE  
       ,RANK()
        OVER (PARTITION BY D.PATIENT_ID,D.admit_date ORDER BY TRUNC(A.RECORD_DATE)) RECORD_RK 
       , CASE WHEN A.RECORD_DATE IS NULL 
              THEN NULL
              WHEN A.RECORD_DATE IS NOT NULL 
              THEN ABS(TRUNC(A.RECORD_DATE) - TRUNC(ADMIT_DATE))
              END AS DIFF
              
FROM 
    (
SELECT DISTINCT
                    L.*,
                    GC.PATIENT_DETAILS_ID,
                    GC.SCRIPT_ID,
                    TRUNC(GC.SCRIPT_START_DATE)   admit_date
                    ,RANK()
                        OVER (PARTITION BY L.PATIENT_ID,GC.SCRIPT_START_DATE ORDER BY GC.SCRIPT_ID) RK
                   ,BP.PLAN_NAME,
                   BP.PLAN_DESC
                   ,CASE 
                    WHEN BP.PLAN_DESC IS NULL
                      THEN 'NOT CHOICE'
                      ELSE BP.PLAN_DESC
                      END 
                       AS PAYOR
            FROM v_longi_mbr L
                  JOIN POP_HEALTH_BI.VW_FACT_PHGC GC
                    ON L.PATIENT_ID = GC.PATIENT_ID 
                   AND GC.SCRIPT_ID = 143
                   AND GC.SCRIPT_ID IS NOT NULL
                LEFT JOIN phgc.MEM_BENF_PROG mbp
                    ON L.PATIENT_ID = MBP.MEMBER_ID
                LEFT  JOIN PHGC.BENF_PLAN_PROG  bpp
                    ON mbp.ben_plan_prog_id = bpp.ben_plan_prog_id
                LEFT  JOIN PHGC.LOB_BENF_PLAN LBP
                    ON MBP.LOB_BEN_ID = LBP.LOB_BEN_ID
                LEFT  JOIN PHGC.BENEFIT_PLAN BP
                    ON BP.BENEFIT_PLAN_ID = LBP.BENEFIT_PLAN_ID                  
                                                                       ) D
                                                                       
   LEFT JOIN                                         
                 PHGC.HEALTH_INDICATOR_RECORD  A
                 ON  D.patient_id = A.patient_id 
                 AND A.PARAMETER_ID IN (84, 85, 86)  --- 84.MD Appt - Post Acute Discharge  85.MD Appt - PCP referral  86.MD Appt - Clinic Referral
                 AND A.DELETED_BY IS NULL
             ---    AND A.value_enterEd = 'Yes'
                                           )
   WHERE  (sysdate - TRUNC(ADMIT_DATE))>= 30              )
WHERE DIFF_RK = 1)