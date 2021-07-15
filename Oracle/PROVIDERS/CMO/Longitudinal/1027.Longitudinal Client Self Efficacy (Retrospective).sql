            
     SELECT  DISTINCT
               PATIENT_ID,
               1027 MEASURE_ID,
               CASE
                   WHEN TRUNC (RECORD_DATE) - TRUNC (ADMIT_DATE) BETWEEN -7 AND 30
                        AND OPTION_VALUE >= 13

                   THEN
                       10271
                   WHEN TRUNC (RECORD_DATE) - TRUNC (ADMIT_DATE) BETWEEN -7 AND 30
                        AND OPTION_VALUE BETWEEN 7 AND 12
                   THEN
                       10273
                   WHEN    TRUNC (RECORD_DATE) - TRUNC (ADMIT_DATE) BETWEEN -7 AND 30
                           AND OPTION_VALUE BETWEEN 0 AND 6
                   THEN
                       10274
                   ELSE
                       10272
               END
                  AS sub_measure_id,
                  1 AS FLAG,
               CASE
                   WHEN TRUNC (RECORD_DATE) - TRUNC (ADMIT_DATE) BETWEEN -7 AND 30
                        AND OPTION_VALUE >= 13
                   THEN   
                       10
                   WHEN TRUNC (RECORD_DATE) - TRUNC (ADMIT_DATE) BETWEEN -7 AND 30
                        AND OPTION_VALUE BETWEEN 7 AND 12
                   THEN
                       30
                  WHEN    TRUNC (RECORD_DATE) - TRUNC (ADMIT_DATE) BETWEEN -7 AND 30
                           AND OPTION_VALUE BETWEEN 0 AND 6
                   THEN
                       40
                   ELSE
                       90
               END
                  AS sub_measure_id_1,
               TRUNC (ADMIT_DATE) AS the_date,
               'Admit_Date'  DATE_TYPE,
                'Choice'  PAYOR             
FROM
( 
SELECT * FROM (
SELECT PATIENT_ID,
       FIRST_NAME,
       LAST_NAME,
       ADMIT_DATE,  
       PAYOR, 
       SCRIPT_143, 
       RK ,
       SELF_EFFICACY,
       RECORD_DATE,
       OPTION_VALUE,
       DIFF,
       COUNTS
      ,RANK()
          OVER(PARTITION BY PATIENT_ID, ADMIT_DATE ORDER BY RECORD_DATE)  RECORD_RK
FROM (
select PATIENT_ID,
       FIRST_NAME,
       LAST_NAME,
       ADMIT_DATE,  
       PAYOR, 
       SCRIPT_143, 
       RK ,
       SELF_EFFICACY,
       RECORD_DATE,
       OPTION_VALUE,
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
       N.self_efficacy,
       N.record_date
     ,N.record_date - D.ADMIT_DATE   DIFF,
      N.OPTION_VALUE
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
                                 SCRIPT_ID                 self_efficacy, 
                                trunc(SCRIPT_START_DATE)    record_date,
                                OPTION_VALUE
                            FROM POP_HEALTH_BI.VW_FACT_PHGC
                             WHERE SCRIPT_ID = 214         --- self-efficay screening
                              AND QUESTION_ID = 3728       --- question 9 "Please indicate overall score
                                                               ) N
        ON D.patient_id = N.PATIENT_ID  )  ) 
        WHERE SELEC != 0
             and (sysdate - ADMIT_DATE)>= 30 
        )  
          WHERE RECORD_RK = 1 )                                                             