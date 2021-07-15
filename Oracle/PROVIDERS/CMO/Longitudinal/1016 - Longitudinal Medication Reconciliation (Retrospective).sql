
     SELECT  DISTINCT
                PATIENT_ID,
               1016 MEASURE_ID,
               CASE
                   WHEN TRUNC (med_rec_date) - TRUNC (ADMIT_DATE) BETWEEN -7 AND 10
                   THEN
                       10161
                   WHEN TRUNC (med_rec_date) - TRUNC (ADMIT_DATE) BETWEEN 11 AND 30
                   THEN
                       10163
                   WHEN    TRUNC (med_rec_date) - TRUNC (ADMIT_DATE) > 30
                           OR med_rec_date IS NULL
                   THEN
                       10162
                   ELSE
                       10162
               END
                  AS sub_measure_id,
                  1 AS FLAG,
               CASE
                   WHEN TRUNC (med_rec_date) - TRUNC (ADMIT_DATE) BETWEEN -7  AND 10
                   THEN
                       10
                   WHEN TRUNC (med_rec_date) - TRUNC (ADMIT_DATE) BETWEEN 11 AND 30
                   THEN
                       30
                   WHEN    TRUNC (med_rec_date) - TRUNC (ADMIT_DATE) > 30
                            OR med_rec_date IS NULL
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
SELECT distinct * FROM (
SELECT PATIENT_ID,
       FIRST_NAME,
       LAST_NAME,
       ADMIT_DATE,  
       PAYOR, 
       SCRIPT_143, 
       RK ,
       MED_REC,
       MED_REC_DATE 
      ,DIFF
      ,COUNTS
      ,RANK()
          OVER(PARTITION BY PATIENT_ID, ADMIT_DATE ORDER BY MED_REC_DATE) RECORD_RK
FROM (
select PATIENT_ID,
       FIRST_NAME,
       LAST_NAME,
       ADMIT_DATE,  
       PAYOR, 
       SCRIPT_143, 
       RK ,
       MED_REC,
       MED_REC_DATE 
      ,DIFF
      ,COUNTS
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
       N.MED_REC,
       N.MED_REC_DATE 
      ,TRUNC(N.MED_REC_DATE) - TRUNC(D.ADMIT_DATE) DIFF
      ,COUNT (*)
        OVER(PARTITION BY D.PATIENT_ID,D.ADMIT_DATE ) COUNTS
FROM 
     (
SELECT DISTINCT
                    L.*,
                    GC.PATIENT_DETAILS_ID,
                    GC.SCRIPT_ID,
                    GC.SCRIPT_START_DATE  admit_date
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
                                 SCRIPT_ID      med_rec, 
                                 SCRIPT_START_DATE    med_rec_date
                            FROM POP_HEALTH_BI.VW_FACT_PHGC
                            WHERE SCRIPT_ID IN (196, 220) 
                                                               ) N
        ON D.patient_id = N.PATIENT_ID  ))
        WHERE SELEC != 0 
            and (sysdate - ADMIT_DATE)>= 30 
            )
  WHERE RECORD_RK = 1 )
  
  