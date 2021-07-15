            
     SELECT DISTINCT
               PATIENT_ID,
               1025 MEASURE_ID,
               CASE
                   WHEN TRUNC (PHQ9_DATE) - TRUNC (ADMIT_DATE) BETWEEN -7 AND 30
                        AND QUESTION_OPTION_ID IN (7945,7946,7947) 

                   THEN
                       10251
                   WHEN TRUNC (PHQ9_DATE) - TRUNC (ADMIT_DATE) BETWEEN -7 AND 30
                        AND QUESTION_OPTION_ID IN (7943,7944)
                   THEN
                       10253
                   WHEN    TRUNC (PHQ9_DATE) - TRUNC (ADMIT_DATE) > 30
                           OR TRUNC (PHQ9_DATE) - TRUNC (ADMIT_DATE) < -8
                           OR  PHQ9_DATE IS NULL
                   THEN
                       10252
                   ELSE
                       10252
               END
                  AS sub_measure_id,
                  1 AS FLAG,
               CASE
                   WHEN TRUNC (PHQ9_DATE) - TRUNC (ADMIT_DATE) BETWEEN -7 AND 30
                        AND QUESTION_OPTION_ID IN (7945,7946,7947) 
                   THEN   
                       10
                   WHEN TRUNC (PHQ9_DATE) - TRUNC (ADMIT_DATE) BETWEEN -7 AND 30
                        AND QUESTION_OPTION_ID IN (7943,7944)
                   THEN
                       30
                   WHEN     TRUNC (PHQ9_DATE) - TRUNC (ADMIT_DATE) > 30
                           OR TRUNC (PHQ9_DATE) - TRUNC (ADMIT_DATE) < -8
                           OR  PHQ9_DATE IS NULL
                   THEN
                       90
                   ELSE
                       90
               END
                  AS sub_measure_id_1,
               TRUNC (ADMIT_DATE) AS the_date,
               'Admit_Date'  DATE_TYPE,
               'Choice'  PAYOR             
FROM
( 
SELECT DISTINCT * FROM (
SELECT DISTINCT 
       PATIENT_ID,
       FIRST_NAME,
       LAST_NAME,
       ADMIT_DATE,  
       PAYOR, 
       SCRIPT_143, 
       RK ,
       PHQ9,
       PHQ9_DATE,
       OPTION_VALUE,
       QUESTION_OPTION_ID,
       DIFF,
       COUNTS
      ,RANK()
          OVER(PARTITION BY PATIENT_ID, ADMIT_DATE ORDER BY PHQ9_DATE)  RECORD_RK
FROM (
select PATIENT_ID,
       FIRST_NAME,
       LAST_NAME,
       ADMIT_DATE,  
       PAYOR, 
       SCRIPT_143, 
       RK ,
       PHQ9,
       PHQ9_DATE,
       OPTION_VALUE,
       QUESTION_OPTION_ID,
       DIFF,
       COUNTS
      ,CASE WHEN COUNTS =1 
            THEN 1
            WHEN COUNTS > 1 AND DIFF < 0 
            THEN 0
            ELSE 1
            END AS SELEC
FROM
(
SELECT DISTINCT
       D.PATIENT_ID,
       D.FIRST_NAME,
       D.LAST_NAME,
       D.ADMIT_DATE,  
       D.PAYOR, 
       D.SCRIPT_ID    SCRIPT_143, 
       D.RK ,
       N.PHQ9,
       N.PHQ9_DATE 
     ,N.PHQ9_DATE - D.ADMIT_DATE   DIFF,
     N.OPTION_VALUE,
     N.QUESTION_OPTION_ID
     ,COUNT (*)
        OVER(PARTITION BY D.PATIENT_ID,D.ADMIT_DATE ) COUNTS
FROM 
     (
SELECT DISTINCT
                    L.*,
                    GC.PATIENT_DETAILS_ID,
                    GC.SCRIPT_ID,
                    trunc(GC.SCRIPT_START_DATE)  admit_date
                       ,RANK()
                        OVER (PARTITION BY L.PATIENT_ID,GC.SCRIPT_START_DATE ORDER BY GC.SCRIPT_ID) RK
                    ,BP.PLAN_NAME,
                    CASE 
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
                (SELECT DISTINCT PATIENT_ID, 
                                 PATIENT_FIRST_NAME, 
                                 PATIENT_LAST_NAME, 
                                 SCRIPT_ID      PHQ9, 
                                trunc(SCRIPT_START_DATE)    PHQ9_date,
                                OPTION_VALUE,
                                QUESTION_OPTION_ID
                            FROM POP_HEALTH_BI.VW_FACT_PHGC
                             WHERE SCRIPT_ID = 163  --- CMO PHQ9
                                 AND QUESTION_ID = 2896
                                                               ) N
        ON D.patient_id = N.PATIENT_ID   )  )
      WHERE SELEC != 0
          and (sysdate - ADMIT_DATE)>= 30 
            )
  WHERE RECORD_RK = 1 )                                                            
                                                                       
                                                                       
                                                                       
                                                                       
                                                                       
                                                             
      
      
      