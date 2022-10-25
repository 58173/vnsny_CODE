create or replace view BIA.CHOICEBI.V_MEDI_STAR_MA_PRODUCT_ID as 
SELECT CASE WHEN plan_package = '011' THEN 1 
			WHEN plan_package = '012' THEN 2
			WHEN lob='MAP' THEN 3
			ELSE -1
			END MA_PRODUCT_ID,
a.* 
FROM dlake.choice.ref_plan a
WHERE program = 'MA'
AND (plan_package IN ('011','012')  OR lob= 'MAP');
create or replace view BIA.CHOICEBI.DIM_MEDI_STAR_MSR_YEAR_DET as
    SELECT msr_year_id,
           report_year_id,
           PLAN_NAME,
           D.ma_product_id,
           b.msr_name,
           b.stars_measure_name,
           mapd_perf_weight,
           correlations,
           stars_ind,
           improvement_measure,
           measure_type,
           source,
           sort_order,
           a.dl_msr_id,
           a.DL_MEDICARE_PLAN_ID,
           DL_SE_DEF_ID,
           DL_TREND_ID,
           MEASURE_UPDATE_FREQ
      FROM BIA.CHOICEBI.DIM_MEDI_STAR_PLAN_MSR_MAP  A
           JOIN BIA.CHOICEBI.DIM_MEDI_STAR_MEASURES B ON (A.dl_msr_id = b.dl_msr_id)
           JOIN BIA.CHOICEBI.DIM_MEDI_STAR_PLAN c ON (a.DL_MEDICARE_PLAN_ID = c.DL_MEDICARE_PLAN_ID)
           JOIN (select distinct ma_product_id from BIA.CHOICEBI.v_medi_star_ma_product_id) D on  (1=1)  ---- ADDING MA_PRODUCT_ID FOR MA_STAR ENHANCEMENT 7/19/2022  
           ;
create or replace view BIA.CHOICEBI.V_FACT_MEDI_STAR_MEASURES_YEARLY as
    SELECT msr.MONTH_ID,
           msr.DL_MSR_ID,
           b.ma_product_id,  ---- enhancement adding product_id 7/19/2022
           DENUM    DENOMINATOR,
           NUM      NUMERATOR,
           CASE
               WHEN  A.inverted_measure = 1
               THEN
                   100 - RATE
               ELSE
                   RATE
           END      RATE,
           HIST_IND,
           DAYSCNT
      FROM (SELECT msr.*,
                   ROW_NUMBER ()
                       OVER (PARTITION BY month_id, dl_msr_id ORDER BY seq)    HistOrder
              FROM (SELECT -- THis is needed for Annual Measures to populate from previous year value for all year current year
                           1
                               seq,
                           TO_NUMBER (TO_CHAR (CURRENT_DATE, 'YYYYMM'))
                               MONTH_ID,
                           DL_MSR_ID,
                           'VML03007'  PRODUCT_ID,
                           151 DL_PLAN_SK, --- enhancement request: adding plans --- Lin Feb2022
                           DENUM,
                           NUM,
                           RATE * 100
                               RATE,
                           'Y'
                               HIST_IND,
                           NULL
                               DAYSCNT
                      FROM BIA.CHOICEBI.fact_medi_star_hist_load  A
                           JOIN BIA.CHOICEBI.dim_medi_star_measures b
                               ON (a.measures_name = b.msr_name)
                     WHERE     SUBSTR (a.month_id, 1, 4) =
                               TO_CHAR (ADD_MONTHS (CURRENT_DATE, -12), 'YYYY')
                           AND UPPER (measure_update_freq) LIKE 'ANN%'
                    UNION ALL
                    SELECT 1              seq,
                           MONTH_ID,
                           DL_MSR_ID,
                           'VML03007'     PRODUCT_ID,
                           151            DL_PLAN_SK, --- enhancement request: adding plans --- Lin Feb2022
                           --DL_MEDICARE_PLAN_ID,
                           DENUM,
                           NUM,
                           RATE * 100,
                           'Y'            HIST_IND,
                           NULL           DAYSCNT
                      FROM BIA.CHOICEBI.FACT_MEDI_STAR_HIST_LOAD  a
                           JOIN BIA.CHOICEBI.DIM_MEDI_STAR_MEASURES b
                               ON (UPPER (a.MEASURES_NAME) =
                                   UPPER (b.msr_name))
                    UNION ALL
                    SELECT 2                               seq,
                           MONTH_ID,
                           155                             DL_MSR_ID,
                           PRODUCT_ID, DL_PLAN_SK, --- enhancement request: adding plans --- Lin Feb022
                           SUM (DENOMINATOR)
                               OVER (PARTITION BY SUBSTR (MONTH_ID, 1, 4)
                                     ORDER BY MONTH_ID)    DENOMINATOR,
                           SUM (NUMERATOR)
                               OVER (PARTITION BY SUBSTR (MONTH_ID, 1, 4)
                                     ORDER BY MONTH_ID)    NUMERATOR,
                             ROUND (
                                   SUM (NUMERATOR)
                                       OVER (
                                           PARTITION BY SUBSTR (MONTH_ID,
                                                                1,
                                                                4)
                                           ORDER BY MONTH_ID)
                                 / SUM (DENOMINATOR)
                                       OVER (
                                           PARTITION BY SUBSTR (MONTH_ID,
                                                                1,
                                                                4)
                                           ORDER BY MONTH_ID),
                                 4)
                           * 100                           rate,
                           'N'                             HIST_IND,
                           NULL                            DAYSCNT
                      FROM (SELECT MONTH_ID,
                                   PRODUCT_ID,DL_PLAN_SK, --- enhancement request: adding plans --- Lin Feb022
                                   SUM (DENOMINATOR)
                                       OVER (
                                           PARTITION BY SUBSTR (MONTH_ID,
                                                                1,
                                                                4)
                                           ORDER BY month_id)    DENOMINATOR,
                                   SUM (NUMERATOR)
                                       OVER (
                                           PARTITION BY SUBSTR (MONTH_ID,
                                                                1,
                                                                4)
                                           ORDER BY month_id)    NUMERATOR
                              FROM (  SELECT MONTH_ID,
                                             PRODUCT_ID, DL_PLAN_SK, --- enhancement request: adding plans --- Lin Feb022
                                             SUM (DENOMINATOR)     DENOMINATOR,
                                             SUM (NUMERATOR)       NUMERATOR
                                        FROM BIA.CHOICEBI.V_FACT_MEDI_STAR_HRA_MEASURE
                                    GROUP BY month_id , PRODUCT_ID, DL_PLAN_SK--- enhancement request: adding plans --- Lin Feb022
                                   )) 
                           JOIN
                           ( SELECT MAX (MONTH_ID)     MX_MONTH_ID
                              FROM  BIA.CHOICEBI.V_FACT_MEDI_STAR_HRA_MEASURE
                              WHERE month_id <= TO_CHAR (CURRENT_DATE, 'YYYYMM')
                            GROUP BY SUBSTR (MONTH_ID, 1, 4)
                           ) MAXYRMTH
                               ON MONTH_ID = MX_MONTH_ID
                    UNION ALL
                    SELECT 2              seq,
                           a.MONTH_ID,
                           a.DL_MSR_ID,
                           a.PRODUCT_ID,a.dl_plan_sk, --- enhancement request: adding plans --- Lin Feb022
                           --DL_MEDICARE_PLAN_ID,
                           --DL_MEDICARE_PLAN_ID,
                           DENUM,
                           VOL_NUM,
                           RATE * 100     rate,
                           'N'            HIST_IND,
                           NULL           DAYSCNT
                      FROM BIA.CHOICEBI.FACT_MEDI_STAR_MBDSS_MSR_RATE  A
                           --JOIN CHOICEBI.DIM_MEDI_STAR_MSR_YEAR_DET b ON (a.dl_msr_id= b.dl_msr_id and DECODE(PART_C_D,'C',1,2) = DL_MEDICARE_PLAN_ID and msr_year_id = substr(month_id,1,4))
                           JOIN
                           (  SELECT DL_MSR_ID          MX_DL_MSR_ID,
                                     MAX (MONTH_ID)     MX_MONTH_ID
                                FROM BIA.CHOICEBI.FACT_MEDI_STAR_MBDSS_MSR_RATE
                            GROUP BY DL_MSR_ID, SUBSTR (MONTH_ID, 1, 4)
                           )
                           MAXYRMTH
                               ON     MONTH_ID = MX_MONTH_ID
                                  AND MX_MONTH_ID = MONTH_ID
                    UNION ALL
                    SELECT 2                        seq,
                           MONTH_ID,
                           DL_MSR_ID,
                           PRODUCT_ID,DL_PLAN_SK, --- enhancement request: adding plans --- Lin Feb022
                           DENOMINATOR,
                           NUMERATOR,
                           ADHERENCE_RATE * 100     RATE,
                           'N'                      HIST_IND,
                           NULL                     DAYSCNT
                      FROM BIA.CHOICEBI.FACT_MEDI_STAR_ACUMEN_MSR_RATE
                           JOIN
                           (  SELECT /*+ no_merge materialize */
                                     DL_MSR_ID          MX_DL_MSR_ID,
                                     MAX (MONTH_ID)     MX_MONTH_ID
                                FROM BIA.CHOICEBI.FACT_MEDI_STAR_ACUMEN_MSR
                            GROUP BY DL_MSR_ID, SUBSTR (MONTH_ID, 1, 4))
                           MAXYRMTH
                               ON     MONTH_ID = MX_MONTH_ID
                                  AND MX_MONTH_ID = MONTH_ID
                    UNION ALL
                    SELECT 2              seq,
                           MONTH_ID,
                           DL_MSR_ID,
                           PRODUCT_ID,DL_PLAN_SK, --- enhancement request: adding plans --- Lin Feb022
                           DENOMINATOR,
                           NUMERATOR,
                           RATE * 100     rate,
                           'N'            HIST_IND,
                           NULL           DAYSCNT
                      FROM BIA.CHOICEBI.FACT_MEDI_STAR_MTM_MSR_RATE
                           JOIN
                           (  SELECT /*+ no_merge materialize */
                                     DL_MSR_ID          MX_DL_MSR_ID,
                                     MAX (MONTH_ID)     MX_MONTH_ID
                                FROM BIA.CHOICEBI.FACT_MEDI_STAR_MTM_MSR_RATE
                            GROUP BY DL_MSR_ID, SUBSTR (MONTH_ID, 1, 4)
                           )  MAXYRMTH
                               ON MONTH_ID = MX_MONTH_ID
                              AND MX_MONTH_ID = MONTH_ID
                    UNION ALL
                    SELECT 2              seq,
                           TO_NUMBER (MONTH_ID),
                           DL_MSR_ID,
                           PRODUCT_ID,DL_PLAN_SK, --- enhancement request: adding plans --- Lin Feb022
                           DENOMINATOR,
                           NUMERATOR,
                           RATE * 100     rate,
                           'N'            HIST_IND,
                           NULL           DAYSCNT
                      FROM BIA.CHOICEBI.FACT_MEDI_STAR_HEDIS_MSR_RATE a
                           JOIN
                           (  SELECT 
                                     MAX (MONTH_ID)             MX_MONTH_ID,
                                     MAX (MEASURE_RUN_MONTH)    MX_MEASURE_RUN_MONTH
                                FROM BIA.CHOICEBI.FACT_MEDI_STAR_HEDIS_MSR_RATE
                            GROUP BY SUBSTR (MONTH_ID, 1, 4)) MAXYRMTH
                               ON     a.MONTH_ID = MX_MONTH_ID
                                  AND MEASURE_RUN_MONTH =
                                      MX_MEASURE_RUN_MONTH
                    UNION ALL
                    SELECT 2        seq,
                           MONTH_ID,
                           DL_MSR_ID,
                           PRODUCT_ID,DL_PLAN_SK, --- enhancement request: adding plans --- Lin Feb022
                           DENUM,
                           NUM,
                           RATE     rate,
                           'N'      HIST_IND,
                           DAYSCNT
                      FROM BIA.CHOICEBI.FACT_MEDI_STAR_CTM_MSR_RATE  A,
                           (  SELECT 
                                     MAX (MONTH_ID)     MX_MONTH_ID
                                FROM BIA.CHOICEBI.FACT_MEDI_STAR_CTM_MSR_RATE
                               WHERE month_id > 201912 -- This is required as data before 2020 is available from HIST LOAD table.
                            GROUP BY SUBSTR (MONTH_ID, 1, 4)) MAXYRMTH
                     WHERE MONTH_ID = MX_MONTH_ID) msr
           ) msr
             LEFT JOIN BIA.CHOICEBI.dim_medi_star_measures a
                  ON a.dl_msr_id = msr.dl_msr_id
             join BIA.CHOICEBI.V_MEDI_STAR_MA_PRODUCT_ID  b  on  b.product_id = msr.product_id
     WHERE  msr.HistOrder = 1
     ;
	create or replace view BIA.CHOICEBI.V_LU_FACT_MEDI_STAR_IMPRV_MEASURES(
	MSR_YEAR_ID,
	REPORT_YEAR_ID,
	DL_MSR_ID,
	DL_MEDICARE_PLAN_ID,
	PLAN_NAME,
	IMPROVEMENT_MEASURE,
  ma_product_id,
	MAPD_PERF_WEIGHT,
	CORRELATIONS,
	MONTH_ID_T1,
	DENOMINATOR_T1,
	NUMERATOR_T1,
	RATE_T1,
	SEM_T1,
	MONTH_ID_T2,
	DENOMINATOR_T2,
	NUMERATOR_T2,
	RATE_T2,
	SEM_T2,
	RATE_CHANGE,
	IMPROVE_CHANGE_SCORE,
	TREND_DESC,
	RATE_CHANGE_SE,
	TTEST_VAL,
	SIGNIFICANT_CHANGE_CAT,
	IMPROVE_IND,
	DECLINE_IND
) as
    WITH
        yr
        AS
            (SELECT *
               FROM bia.mstrstg.lu_year
              WHERE year_id >= 2019),
        DIM_PLAN_MSR_YEAR
        AS
            (SELECT a.*
               FROM BIA.CHOICEBI.DIM_MEDI_STAR_MSR_YEAR_DET  a,
                    (SELECT *
                       FROM BIA.mstrstg.lu_year
                      WHERE year_id >= 2019) b
              WHERE     1 = 1
                    AND b.year_id = msr_year_id
                    AND correlations IS NOT NULL
                    AND improvement_measure = 1),
        temp1
        AS
            (SELECT a.*,
                    SQRT (
                          POWER (SEM_t2, 2)
                        + POWER (SEM_t1, 2)
                        - 2 * correlations * SEM_t2 * SEM_t1)
                        AS rate_change_SE,
                    CASE
                        WHEN NVL (
                                 SQRT (
                                       POWER (SEM_t2, 2)
                                     + POWER (SEM_t1, 2)
                                     - 2 * correlations * SEM_t2 * SEM_t1),
                                 0) !=
                             0
                        THEN
                              improve_change_score
                            / SQRT (
                                    POWER (SEM_t2, 2)
                                  + POWER (SEM_t1, 2)
                                  - 2 * correlations * SEM_t2 * SEM_t1)
                    END
                        AS ttest_val
               FROM (SELECT dim1.*,                              --timepoint 1
                            t1.month_id
                                AS month_id_t1,
                            t1.denominator
                                AS denominator_t1,
                            t1.numerator
                                AS numerator_t1,
                            t1.rate
                                AS rate_t1,
                            CASE
                                WHEN dim1.dl_se_def_id IN (1, 2)
                                THEN
                                    CASE
                                        WHEN   (t1.rate * (100 - t1.rate))
                                             / t1.denominator <
                                             0
                                        THEN
                                            NULL
                                        ELSE
                                            SQRT (
                                                  (t1.rate * (100 - t1.rate))
                                                / t1.denominator)
                                    END
                                WHEN dim1.dl_se_def_id = 5
                                THEN
                                    CASE
                                        WHEN   t1.numerator
                                             / POWER (t1.denominator, 2)
                                             * (  1000
                                                * 30
                                                / NVL (t1.dayscnt, 365)) <
                                             0
                                        THEN
                                            NULL
                                        ELSE
                                              SQRT (
                                                    t1.numerator
                                                  / POWER (t1.denominator, 2))
                                            * (  1000
                                               * 30
                                               / NVL (t1.dayscnt, 365))
                                    END
                            END
                                AS SEM_t1,       --SEF for Measure timepoint 2
                            t2.month_id
                                AS month_id_t2,
                            t2.denominator
                                AS denominator_t2,
                            t2.numerator
                                AS numerator_t2,
                            t2.rate
                                AS rate_t2,
                            CASE
                                WHEN dim1.dl_se_def_id IN (1, 2)
                                THEN
                                    CASE
                                        WHEN   (t2.rate * (100 - t2.rate))
                                             / t2.denominator <
                                             0
                                        THEN
                                            NULL
                                        ELSE
                                            SQRT (
                                                  (t2.rate * (100 - t2.rate))
                                                / t2.denominator)
                                    END
                                WHEN dim1.dl_se_def_id = 5
                                THEN
                                    CASE
                                        WHEN   t2.numerator
                                             / POWER (t2.denominator, 2)
                                             * (  1000
                                                * 30
                                                / NVL (t2.dayscnt, 365)) <
                                             0
                                        THEN
                                            NULL
                                        ELSE
                                              SQRT (
                                                    t2.numerator
                                                  / POWER (t2.denominator, 2))
                                            * (  1000
                                               * 30
                                               / NVL (t2.dayscnt, 365))
                                    END
                            END
                                AS SEM_t2, --SEF for Measure improvement change score
                            t1.rate - t2.rate
                                AS rate_change,
                            (t1.rate - t2.rate) * d.trend_val
                                AS improve_change_score,
                            d.trend_desc
                       FROM (SELECT a.*
                               FROM BIA.CHOICEBI.DIM_MEDI_STAR_MSR_YEAR_DET  a,
                                    (SELECT *
                                       FROM BIA.mstrstg.lu_year
                                      WHERE year_id >= 2019) b
                              WHERE     1 = 1
                                    AND b.year_id = msr_year_id
                                    AND correlations IS NOT NULL
                                    AND improvement_measure = 1) dim1
                            LEFT JOIN
                            BIA.CHOICEBI.V_FACT_MEDI_STAR_MEASURES_YEARLY t1
                                ON (    dim1.msr_year_id =
                                        SUBSTR (t1.month_id, 1, 4)
                                    AND dim1.dl_msr_id = t1.dl_msr_id)
                            LEFT JOIN
                            BIA.choicebi.V_FACT_MEDI_STAR_MEASURES_YEARLY t2
                                ON (    dim1.msr_year_id - 1 =
                                        SUBSTR (t2.month_id, 1, 4)
                                    AND dim1.dl_msr_id = t2.dl_msr_id)
                            JOIN BIA.choicebi.DIM_MEDI_STAR_TREND d
                                ON (dim1.dl_trend_id = d.dl_trend_id)) A)
    SELECT MSR_YEAR_ID,
           REPORT_YEAR_ID,
           DL_MSR_ID,
           DL_MEDICARE_PLAN_ID,
           PLAN_NAME,
           IMPROVEMENT_MEASURE,
           ma_product_id,
           --MSR_NAME,STARS_MEASURE_NAME,STARS_IND,MEASURE_TYPE, SOURCE,SORT_ORDER,
           MAPD_PERF_WEIGHT,
           CORRELATIONS,
           ---DL_SE_DEF_ID,
           --DL_TREND_ID,
           MONTH_ID_T1,
           DENOMINATOR_T1,
           NUMERATOR_T1,
           RATE_T1,
           SEM_T1,
           MONTH_ID_T2,
           DENOMINATOR_T2,
           NUMERATOR_T2,
           RATE_T2,
           SEM_T2,
           RATE_CHANGE,
           IMPROVE_CHANGE_SCORE,
           TREND_DESC,
           rate_change_SE,
           ttest_val,
           CASE
               WHEN ttest_val > 1.96
               THEN
                   'Improvement'
               WHEN ttest_val < -1.96
               THEN
                   'Decline'
               WHEN ttest_val <= 1.96 AND ttest_val >= -1.96
               THEN
                   'Not Significant'
           END                                              AS significant_change_cat,
           CASE WHEN ttest_val > 1.96 THEN 1 ELSE 0 END     AS improve_ind,
           CASE WHEN ttest_val < -1.96 THEN 1 ELSE 0 END    AS decline_ind          
      FROM temp1 a;
 CREATE OR REPLACE TABLE BIA.CHOICEBI.FACT_MEDI_STAR_IMPRV_MEASURES AS
 SELECT * FROM BIA.CHOICEBI.V_LU_FACT_MEDI_STAR_IMPRV_MEASURES
;
update BIA.CHOICEBI.BI_TBL_REFRESH
set SQL_INSERT_COLUMN ='	
    MSR_YEAR_ID,
	REPORT_YEAR_ID,
	DL_MSR_ID,
	DL_MEDICARE_PLAN_ID,
	PLAN_NAME,
	IMPROVEMENT_MEASURE,
	MA_PRODUCT_ID,
	MAPD_PERF_WEIGHT,
	CORRELATIONS,
	MONTH_ID_T1,
	DENOMINATOR_T1,
	NUMERATOR_T1,
	RATE_T1,
	SEM_T1,
	MONTH_ID_T2,
	DENOMINATOR_T2,
	NUMERATOR_T2,
	RATE_T2,
	SEM_T2,
	RATE_CHANGE,
	IMPROVE_CHANGE_SCORE,
	TREND_DESC,
	RATE_CHANGE_SE,
	TTEST_VAL,
	SIGNIFICANT_CHANGE_CAT,
	IMPROVE_IND,
	DECLINE_IND'
where TABLE_NAME ='FACT_MEDI_STAR_IMPRV_MEASURES';     
create or replace view BIA.CHOICEBI.V_FACT_MEDI_STAR_IMPRV_SCORE(
	MSR_YEAR_ID,
	REPORT_YEAR_ID,
	DL_MSR_ID,
    MA_PRODUCT_ID,
	DL_MEDICARE_PLAN_ID,
	PLAN_NAME,
	IMPROVE_MSR_SCORE
) as
    WITH
        impr_scr2
        AS
            (  SELECT msr_year_id,
                      report_year_id,
                      DECODE (a.DL_MEDICARE_PLAN_ID, 1, 157, 156)
                          dl_msr_id,
              MA_PRODUCT_ID,
                      a.DL_MEDICARE_PLAN_ID,
                      plan_name,
                      --measure_type,
                      mapd_perf_weight,
                      SUM (improve_ind)
                          AS improve_n,
                      SUM (decline_ind)
                          AS decline_n,
                      SUM (improve_ind) - SUM (decline_ind)
                          AS net_improve,
                      SUM (improvement_measure)
                          AS elig_n
                 FROM BIA.CHOICEBI.FACT_MEDI_STAR_IMPRV_MEASURES a
             GROUP BY msr_year_id,
                      report_year_id,
                    MA_PRODUCT_ID,
                      a.DL_MEDICARE_PLAN_ID,
                      plan_name,
                      --measure_type,
                      mapd_perf_weight)
      SELECT msr_year_id,
             report_year_id,
             dl_msr_id,
              MA_PRODUCT_ID,
             DL_MEDICARE_PLAN_ID,
             plan_name,
             DECODE (
                 SUM (elig_n * mapd_perf_weight),
                 0, NULL,
                   SUM (net_improve * mapd_perf_weight)
                 / SUM (elig_n * mapd_perf_weight))    AS improve_msr_score
        FROM impr_scr2
    GROUP BY msr_year_id,
             dl_msr_id,
              MA_PRODUCT_ID,
             report_year_id,
             DL_MEDICARE_PLAN_ID,
             plan_name
    ORDER BY msr_year_id,
             dl_msr_id,
              MA_PRODUCT_ID,
             report_year_id,
             DL_MEDICARE_PLAN_ID,
             plan_name;
create or replace view BIA.CHOICEBI.V_LU_V_FACT_MEDI_STAR_MEASURES as
     SELECT  msr1.MEASURE_RUN_MONTH,
            msr1.MONTH_ID,
            msr1.DL_MSR_ID,
            msr1.MA_PRODUCT_ID,
            msr1.DENOMINATOR,
            msr1.NUMERATOR,
           CASE
               WHEN  a.inverted_measure = 1
               THEN
                   100 - RATE
               ELSE
                   RATE
           END    RATE,
            msr1.HIST_IND,
            msr1.HYBRID_FLAG
      FROM ((SELECT msr.SEQ,
                   msr.MEASURE_RUN_MONTH,
                   msr.MONTH_ID,
                   msr.DL_MSR_ID,
                   msr.DENOMINATOR,
                   msr.NUMERATOR,
                   msr.RATE,
                   msr.HIST_IND,
                   msr.HYBRID_FLAG,
                   b.MA_PRODUCT_ID,
                   ROW_NUMBER ()
                       OVER (
                           PARTITION BY month_id,
                                        MEASURE_RUN_MONTH,
                                        dl_msr_id
                           ORDER BY seq)    HistOrder
              FROM (SELECT 2                               seq,
                           MONTH_ID                        MEASURE_RUN_MONTH,
                           MONTH_ID,
                           155                             DL_MSR_ID,
                       PRODUCT_ID,
                           DL_PLAN_SK,
                           SUM (DENOMINATOR)
                               OVER (PARTITION BY SUBSTR (MONTH_ID, 1, 4)
                                     ORDER BY MONTH_ID)    DENOMINATOR,
                           SUM (NUMERATOR)
                               OVER (PARTITION BY SUBSTR (MONTH_ID, 1, 4)
                                     ORDER BY MONTH_ID)    NUMERATOR,
                           ROUND (
                                 SUM (NUMERATOR)
                                     OVER (
                                         PARTITION BY SUBSTR (MONTH_ID, 1, 4)
                                         ORDER BY MONTH_ID)
                               / SUM (DENOMINATOR)
                                     OVER (
                                         PARTITION BY SUBSTR (MONTH_ID, 1, 4)
                                         ORDER BY MONTH_ID),
                               4)                          RATE,
                           'N'                             HIST_IND,
                           NULL                            HYBRID_FLAG
                      FROM (  SELECT MONTH_ID,PRODUCT_ID,DL_PLAN_SK,
                                     SUM (DENOMINATOR)     DENOMINATOR,
                                     SUM (NUMERATOR)       NUMERATOR                                   
                                FROM BIA.CHOICEBI.V_FACT_MEDI_STAR_HRA_MEASURE
                            GROUP BY MONTH_ID,PRODUCT_ID,DL_PLAN_SK)
                    UNION ALL
                    SELECT 2            seq,
                           MONTH_ID     MEASURE_RUN_MONTH,
                           MONTH_ID,
                           DL_MSR_ID,
                    PRODUCT_ID,
                    DL_PLAN_SK,
                           DENOMINATOR,
                           NUMERATOR,
                           RATE,
                           'N'          HIST_IND,
                           NULL         HYBRID_FLAG
                      FROM BIA.CHOICEBI.FACT_MEDI_STAR_MTM_MSR_RATE
                    UNION ALL
                    SELECT 2
                               seq,
                           TO_NUMBER (MEASURE_RUN_MONTH)
                               MEASURE_RUN_MONTH,
                           TO_NUMBER (MONTH_ID),
                           DL_MSR_ID,
                     PRODUCT_ID,
                    DL_PLAN_SK,
                           DENOMINATOR,
                           NUMERATOR,
                           RATE,
                           'N'
                               HIST_IND,
                           HYBRID_FLAG
                      FROM BIA.CHOICEBI.FACT_MEDI_STAR_HEDIS_MSR_RATE
                    UNION ALL
                    -- DS 10/9/2020 for CTM using calculated value from Detail table as Numerator 1 as Denominator to get correct summary level data
                    SELECT 2            seq,
                           MONTH_ID     MEASURE_RUN_MONTH,
                           MONTH_ID,
                           DL_MSR_ID,
                     PRODUCT_ID,
                    DL_PLAN_SK,
                           1            DENOMINATOR,
                           RATE         NUMERATOR,
                           RATE,
                           'N'          HIST_IND,
                           NULL         HYBRID_FLAG
                      FROM BIA.CHOICEBI.FACT_MEDI_STAR_CTM_MSR_RATE
                    UNION ALL
                    SELECT 2                  seq,
                           MONTH_ID           MEASURE_RUN_MONTH,
                           MONTH_ID,
                           DL_MSR_ID,
                     PRODUCT_ID,
                    DL_PLAN_SK,
                           DENOMINATOR,
                           NUMERATOR,
                           ADHERENCE_RATE     RATE,
                           'N'                HIST_IND,
                           NULL               HYBRID_FLAG
                      FROM BIA.CHOICEBI.FACT_MEDI_STAR_ACUMEN_MSR_RATE
                    UNION ALL
                    SELECT -- THis is needed for Annual Measures to populate from previous year value for all year current year
                           1              seq,
                           c.MONTH_ID     MEASURE_RUN_MONTH,
                           c.MONTH_ID,
                           DL_MSR_ID,
                     'VML03007'     PRODUCT_ID,
                           151            DL_PLAN_SK,
                           DENUM,
                           NUM,
                           RATE,
                           'Y'            HIST_IND,
                           NULL           HYBRID_FLAG
                      FROM BIA.CHOICEBI.fact_medi_star_hist_load  A
                           JOIN BIA.CHOICEBI.dim_medi_star_measures b
                               ON (a.measures_name = b.msr_name)
                           JOIN BIA.mstrstg.lu_month c
                               ON (c.year_id = TO_CHAR (CURRENT_DATE, 'YYYY'))
                     WHERE     SUBSTR (a.month_id, 1, 4) =
                               TO_CHAR (ADD_MONTHS (CURRENT_DATE, -12), 'YYYY')
                           AND UPPER (measure_update_freq) LIKE 'ANN%'
                    UNION ALL
                    -- DS 11/9/2020 - add Overall Improve Msr C & D
                    -- DS 11/9/2020 - repeats current value for entire year
//                    SELECT 2                                  seq,
//                           b.MONTH_ID                         MEASURE_RUN_MONTH,
//                           b.MONTH_ID,
//                           a.DL_MSR_ID,
//                           a.ma_product_id, 
//                           1                                  DENOMINATOR,
//                           ROUND (a.IMPROVE_MSR_SCORE, 2)     NUMERATOR,
//                           ROUND (a.IMPROVE_MSR_SCORE, 2)     RATE,
//                           'N'                                HIST_IND,
//                           NULL                               HYBRID_FLAG
//                      FROM BIA.CHOICEBI.V_FACT_MEDI_STAR_IMPRV_SCORE  a,
//                           BIA.MSTRSTG.LU_MONTH                       b
//                     WHERE     a.MSR_YEAR_ID = b.YEAR_ID
//                           AND current_date - 1 > MONTH_DATE
//                    UNION ALL
                    SELECT 1            seq,
                           MONTH_ID     MEASURE_RUN_MONTH,
                           MONTH_ID,
                           DL_MSR_ID,
                           'VML03007'     PRODUCT_ID,
                           151            DL_PLAN_SK, 
                           DENUM,
                           NUM,
                           RATE,
                           'Y'          HIST_IND,
                           NULL         HYBRID_FLAG
                      FROM BIA.CHOICEBI.FACT_MEDI_STAR_HIST_LOAD  a
                           JOIN BIA.CHOICEBI.DIM_MEDI_STAR_MEASURES b
                               ON (UPPER (a.MEASURES_NAME) =
                                   UPPER (b.msr_name))
                    UNION ALL
                    SELECT 2            seq,
                           MONTH_ID     MEASURE_RUN_MONTH,
                           MONTH_ID,
                           DL_MSR_ID,
                           PRODUCT_ID,
                           DL_PLAN_SK,
                           DENUM,
                           VOL_NUM,
                           RATE,
                           'N'          HIST_IND,
                           NULL         HYBRID_FLAG
                      FROM BIA.CHOICEBI.FACT_MEDI_STAR_MBDSS_MSR_RATE a) msr
                      join BIA.CHOICEBI.V_MEDI_STAR_MA_PRODUCT_ID  b  on  b.product_id = msr.product_id ) 
          union all
             (SELECT       2                                  seq,
                           b.MONTH_ID                         MEASURE_RUN_MONTH,
                           b.MONTH_ID,
                           a.DL_MSR_ID, 
                           1                                  DENOMINATOR,
                           ROUND (a.IMPROVE_MSR_SCORE, 2)     NUMERATOR,
                           ROUND (a.IMPROVE_MSR_SCORE, 2)     RATE,
                           'N'                                HIST_IND,
                           NULL                               HYBRID_FLAG,
                          a.ma_product_id,
                          ROW_NUMBER ()
                                OVER (
                                     PARTITION BY month_id,
                                                  b.MONTH_ID ,
                                                  a.DL_MSR_ID
                                  ORDER BY seq)    HistOrder
                      FROM BIA.CHOICEBI.V_FACT_MEDI_STAR_IMPRV_SCORE  a,
                           BIA.MSTRSTG.LU_MONTH                       b
                     WHERE     a.MSR_YEAR_ID = b.YEAR_ID
                           AND current_date - 1 > MONTH_DATE)
           ) msr1
     left join BIA.CHOICEBI.dim_medi_star_measures a  on a.dl_msr_id = msr1.dl_msr_id
     WHERE HistOrder = 1
;
CREATE OR REPLACE TABLE BIA.CHOICEBI.V_FACT_MEDI_STAR_MEASURES AS
SELECT * FROM  bia.choicebi.V_LU_V_FACT_MEDI_STAR_MEASURES;
update BIA.CHOICEBI.BI_TBL_REFRESH
set SQL_INSERT_COLUMN ='
MEASURE_RUN_MONTH	,
MONTH_ID	,
DL_MSR_ID	,
MA_PRODUCT_ID ,
DENOMINATOR	,
NUMERATOR	,
RATE	,
HIST_IND,
HYBRID_FLAG'
WHERE TABLE_NAME ='V_FACT_MEDI_STAR_MEASURES';
create or replace view BIA.CHOICEBI.V_FACT_MEDI_STAR_MEASURES_YR_END as
    SELECT "MEASURE_RUN_MONTH",
           "MEASUREMENT_YEAR",
           "REPORT_YEAR_ID",
           "MONTH_ID",
           "DL_MSR_ID",
           ma_product_id,
           "DL_MEDICARE_PLAN_ID",
           "SOURCE",
           "HYBRID_FLAG",
           "BENCHMARK_2STARS",
           "BENCHMARK_3STARS",
           "BENCHMARK_4STARS",
           "BENCHMARK_5STARS",
           "BENCHMARK_ASC",
           "BENCHMARK_TYPE",
           "CAI_VALUE",
           "MAPD_PERF_WEIGHT",
           "RESPONSE_THRESHOLD",
           "STARS_IND",
           "DENOMINATOR",
           "NUMERATOR",
           "RATE",
           "RANK"
      FROM (  SELECT MEASURE_RUN_MONTH,
                     TO_NUMBER (SUBSTR (MONTH_ID, 1, 4))         MEASUREMENT_YEAR,
                     REPORT_YEAR_ID,
                     MONTH_ID,
                     a.DL_MSR_ID,
                     a.ma_product_id,
                     DL_MEDICARE_PLAN_ID,
                     SOURCE,
                     HYBRID_FLAG,
                     BENCHMARK_2STARS,
                     BENCHMARK_3STARS,
                     BENCHMARK_4STARS,
                     BENCHMARK_5STARS,
                     BENCHMARK_ASC,
                     BENCHMARK_TYPE,
                     CAI_VALUE,
                     MAPD_PERF_WEIGHT,
                     RESPONSE_THRESHOLD,
                     STARS_IND,
                     DENOMINATOR,
                     NUMERATOR,
                     NUMERATOR / DENOMINATOR                     RATE,
                     RANK ()
                         OVER (
                             PARTITION BY TO_NUMBER (SUBSTR (MONTH_ID, 1, 4)),
                                          a.DL_MSR_ID,
                                          b.DL_MEDICARE_PLAN_ID
                             ORDER BY MEASURE_RUN_MONTH DESC)    RANK
                FROM BIA.CHOICEBI.V_FACT_MEDI_STAR_MEASURES a,
                     BIA.CHOICEBI.DIM_MEDI_STAR_PLAN_MSR_MAP b,
                     BIA.CHOICEBI.DIM_MEDI_STAR_MEASURES    c
               WHERE     TO_NUMBER (SUBSTR (a.MONTH_ID, 1, 4)) = b.MSR_YEAR_ID
                     AND a.DL_MSR_ID = b.DL_MSR_ID
                     AND a.DL_MSR_ID = c.DL_MSR_ID
            GROUP BY MEASURE_RUN_MONTH,
                     REPORT_YEAR_ID,
                     SUBSTR (MONTH_ID, 1, 4),
                     a.DL_MSR_ID,
            ma_product_id,
                     DL_MEDICARE_PLAN_ID,
                     SOURCE,
                     HYBRID_FLAG,
                     BENCHMARK_2STARS,
                     BENCHMARK_3STARS,
                     BENCHMARK_4STARS,
                     BENCHMARK_5STARS,
                     BENCHMARK_ASC,
                     BENCHMARK_TYPE,
                     CAI_VALUE,
                     MAPD_PERF_WEIGHT,
                     RESPONSE_THRESHOLD,
                     STARS_IND,
                     DENOMINATOR,
                     NUMERATOR,
                     MONTH_ID)
--WHERE RANK = 1 DS removed to allow for all to do lagging for CTM, MBDSS, PLAN C on mstr report side
;

