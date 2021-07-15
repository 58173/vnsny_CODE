WITH
RISKSCORE AS 
--(SELECT /*+ materialize */ * FROM   V_UAS_RISK_SCORE WHERE  MERCER_DOC_YEAR = (SELECT MAX(MERCER_DOC_YEAR) FROM UAS_RISK_SCORE)) 
(
SELECT source_unique_id as record_id --UAS Record ID
    , model_pred_value as prem_risk_score -- MLTC Premium Risk Score
FROM RISKDM.RISK_MODEL_PRED_DETAIL@DLAKE A  
--LEFT JOIN RISKDM.RISK_MODEL@DLAKE D ON (a.model_version_id = d.model_version_id)
--WHERE d.model_version_id=30001
LEFT JOIN RISKDM.RISK_MODEL@DLAKE D ON (a.MODEL_VRSN_ID = d.MODEL_VRSN_ID)
WHERE d.MODEL_VRSN_ID=30001
)
,MEMBER_MRN AS
(
    SELECT   MEMBER_ID, MAX(COALESCE(MRN, MRN_TMG, MRN_MF)) AS MRN
    FROM     (SELECT A.*, B.MRN_TMG, C.MRN AS MRN_MF
            FROM   (SELECT   MEMBER_ID, MEME_CK, MEDICAID_NUMBER, MRN
                    FROM     CHOICE.DIM_MEMBER_DETAIL@DLAKE
                    GROUP BY MEMBER_ID, MEME_CK, MEDICAID_NUMBER, MRN
                   ) A LEFT JOIN
                   (
                        SELECT MEME_CK,MEME_MEDCD_NO,
                               CASE
                                   WHEN (   REGEXP_INSTR(
                                                TRIM(
                                                    MEME_RECORD_NO),
                                                '[^0-9]') > 0
                                         OR TRIM(MEME_RECORD_NO) IS NULL) THEN
                                       NULL
                                   ELSE
                                       TO_NUMBER(TRIM(MEME_RECORD_NO))
                               END
                                   AS MRN_TMG
                        FROM   TMG.CMC_MEME_MEMBER
                        WHERE  TRIM(MEME_RECORD_NO) NOT IN ('123456789', '999999999', '12345', '123')
                        UNION ALL
                                                SELECT MEME_CK,MEME_MEDCD_NO,
                               CASE
                                   WHEN (   REGEXP_INSTR(
                                                TRIM(
                                                    MEME_RECORD_NO),
                                                '[^0-9]') > 0
                                         OR TRIM(MEME_RECORD_NO) IS NULL) THEN
                                       NULL
                                   ELSE
                                       TO_NUMBER(TRIM(MEME_RECORD_NO))
                               END
                                   AS MRN_TMG
                        FROM   TMG_FIDA.CMC_MEME_MEMBER
                        WHERE  TRIM(MEME_RECORD_NO) NOT IN ('123456789', '999999999', '12345', '123')
                    ) B ON (A.MEME_CK = B.MEME_CK AND A.MEDICAID_NUMBER  = B.MEME_MEDCD_NO)
                   LEFT JOIN (SELECT   MEDICAID_NUM, MAX(MRN) MRN
                              FROM     DW_OWNER.TPCLN_PATIENT
                              WHERE    MEDICAID_NUM IS NOT NULL
                              GROUP BY MEDICAID_NUM) C
                       ON (C.MEDICAID_NUM = A.MEDICAID_NUMBER)
           )
    GROUP BY MEMBER_ID
),
REF_LOB_GROUP_MAPPING AS
(
    SELECT 1 DL_LOB_GRP_ID, 1 DL_LOB_ID FROM DUAL
    UNION ALL
    SELECT 1 DL_LOB_GRP_ID, 5 DL_LOB_ID FROM DUAL
    UNION ALL
    SELECT 2, 2 DL_LOB_ID FROM DUAL
    UNION ALL
    SELECT 3, 3 DL_LOB_ID FROM DUAL
    UNION ALL
    SELECT 4, 4 DL_LOB_ID FROM DUAL
),
V_REF_PLAN AS
(
    SELECT 
        --D.DL_LOB_GRP_ID, E.LOB_GRP_DESC, B.*
        B.DL_LOB_ID DL_LOB_GRP_ID, B.LOB LOB_GRP_DESC, B.*
    FROM   CHOICE.REF_PLAN@DLAKE B
        JOIN CHOICE.REF_LOB@DLAKE C ON (C.DL_LOB_ID = B.DL_LOB_ID)
        JOIN REF_LOB_GROUP_MAPPING D ON (C.DL_LOB_ID = D.DL_LOB_ID)
        JOIN MSTRSTG.D_LOB_GROUP E ON (E.DL_LOB_GRP_ID = D.DL_LOB_GRP_ID)
)
select 
    a.*,
    DECODE(DISENROLLED_FLAG, 1, LEAD(ENROLLMENT_DATE) OVER (PARTITION BY  A.MEMBER_ID, A.PROGRAM ORDER BY A.MONTH_ID)) NEXT_LOB_ENROLLMENT_DATE ,
    DECODE(ENROLLED_FLAG, 1, LAG(DISENROLLMENT_DATE) OVER (PARTITION BY  A.MEMBER_ID, A.PROGRAM ORDER BY A.MONTH_ID)) PREV_LOB_DISENROLLMENT_DATE
    ,k.LTP_IND LTP_IND    
from
(
SELECT /*+ driving_site(a)  cardinality(A 400000) cardinality(b 60000000) cardinality(c 60000000)   cardinality(d 60000000)  cardinality(e 60000000) cardinality(f 60000000) no_merge */
      'CHOICEDM' DATA_SOURCE,
       --NVL(C.MRN, MEMBER_MRN.MRN) MRN,
       C.MRN,
       A.REPORTING_MONTH MONTH_ID,
       PLAN_PACKAGE,
       B.PROGRAM,
       E.BOROUGH,
       E.COUNTY_CODE,
       --NULL TEAM,
       H.CARE_MANAGER_ID STAFF_ID,
       NVL(d.MEDICAID_NUMBER, d1.MEDICAID_NUMBER) MEDICAID_NUM,
       NVL(D.HICN,D1.HICN) MEDICARE_NUM,
       NVL(D.SBSB_ID,C.SUBSCRIBER_ID) SUBSCRIBER_ID,
       C.BENEFIT_REGION,
       F.ASSESSMENTDATE UAS_DATE,
       F.LEVELOFCARESCORE UAS_NFLOC_SCORE,
       C.DISENROLL_RSN_CODE DC_REASON,
       C.DISENROLL_RSN_DESC DISENROLL_DISP,
       1 FLAG,
       CASE_NBR,
       E.STATE STATE_CODE,
       TO_DATE(TO_CHAR(ORIG_ENROLLMENT_START_MONTH)|| '01', 'YYYYMMDD') ENROLLMENT_DATE,
       ADD_MONTHS(TO_DATE(TO_CHAR(LATEST_ENROLLMENT_END_MONTH), 'YYYYMM'), 1) - 1 DISENROLLMENT_DATE,
       A.NEW_ENR_IND ENROLLED_FLAG,
       A.NEW_DISENR_IND DISENROLLED_FLAG,
       LOB_GRP_DESC LINE_OF_BUSINESS,
       G.PROVIDER_ID,
       G.PCP_NAME PROVIDER_NAME,
       A.REPORTING_MONTH MONTH,
       NVL(D.SSN,D1.SSN) SSN,
       NVL(D.LAST_NAME,D1.LAST_NAME) LAST_NAME,
       NVL(D.FIRST_NAME,D1.FIRST_NAME) FIRST_NAME,
       NVL(D.DOB, D1.DOB) DATE_OF_BIRTH,
       ROUND(MONTHS_BETWEEN(TO_DATE(REPORTING_MONTH,'YYYYMM'), D.DOB) / 12, 1) AGE,
       NVL(D.SEX_CODE, D1.SEX_CODE) SEX,
       E.COUNTY,
       TO_CHAR(CASE_NBR) HIGHEST_CASE_NUMBER,
       A.DL_LOB_ID LOB_ID,
       A.DL_ASSESS_SK UAS_RECORD_ID,
       I.REFERRAL_DATE,
       NVL(MANDATORY_ENROLLMENT, 'N') MANDATORY_ENROLLMENT,
       C.DISENROLL_RSN_DESC,
       B.PRODUCT_ID,
       B.PRODUCT_NAME,
       B.PLAN_ID,
       B.PLAN_DESC,
       REGION_NAME, 
       DESCRIPTION_1 REGION_NAME2, 
       A.DL_MEMBER_SK,
       A.MEMBER_ID,
       A.DL_PMPM_ENR_SK,
       A.DL_ENROLL_ID,
       A.DL_ENRL_SK,
       A.DL_LOB_ID,
       A.DL_PLAN_SK,
       A.DL_PROV_SK,
       A.CM_SK_ID,
       A.DL_ASSESS_SK,
       DL_LOB_GRP_ID,
       DL_MEMBER_ADDRESS_SK, 
       CNTY.DL_COUNTY_SK,
       A.DL_JOB_RUN_ID,
       A.DL_CRT_TS,
       A.DL_UPD_TS,
       A.RATE_CODE,
       --decode(A.RATE_CODE, null, null, NVL(J.RATE_CODE_DESC, A.RATE_CODE || ' - RATE CODE DESC MISSING'), null) RATE_CODE_DESC,
       case when A.RATE_CODE is not null then J.RATE_CODE_DESC
            else  A.RATE_CODE || ' - RATE CODE DESC MISSING' end RATE_CODE_DESC,
       TO_DATE(REPORTING_MONTH,'YYYYMM') REPORTING_DATE 
       ,CASE WHEN A.DL_LOB_ID IN(2) AND TO_DATE(TO_CHAR(ORIG_ENROLLMENT_START_MONTH)|| '01', 'YYYYMMDD')>='01jul2017' AND SUBJ90RULE='Y' THEN 'Y'
           WHEN A.DL_LOB_ID IN(2) AND TO_DATE(TO_CHAR(ORIG_ENROLLMENT_START_MONTH)|| '01', 'YYYYMMDD')>='01jul2017' AND SUBJ90RULE IS NULL THEN 'N'
      END AS SUBJ90RULE_IND,
      (CASE WHEN G.LOB = 'FIDA' THEN 'FIDA' ELSE 'TMG' END) AS PRVDR_SRC_SYS,
       null RISK_GROUP,
       prem_risk_score RISK_SCORE,
       MAP_REGION_NAME,
       GRGR_ID GROUP_ID,
       GRGR_NAME GROUP_NAME,
       SGSG_ID SUBGROUP_ID,
       SGSG_NAME SUBGROUP_NAME,   
        A.ICS_IND ICS_IND,        
        DELEGATED_CM_SK  DL_DELEGATED_CM_SK,        
        MOBILITY_DEVICE_IND MOBILITY_DEVICE_IND,
        a.DL_DISABLED_SK,
        VULNERABILITY_INDEX,
        VPIN,
        upper(trim(H.first_name) || ' ' || trim(H.last_name)) as care_staff_name,
        upper(trim(ccm.first_name) || ' ' || trim(ccm.last_name)) as care_staff_manager,
        D1.RACE
        --melc_eff_dt,        
        --melc_term_dt,
    --Crft_mctr_lsty                   
FROM   CHOICE.FCT_PMPM_ENROLLMENT_CURR@DLAKE A
       LEFT JOIN V_REF_PLAN B ON (A.DL_PLAN_SK = B.DL_PLAN_SK)
       LEFT JOIN CHOICE.DIM_MEMBER_ENROLLMENT@DLAKE C ON (A.DL_ENRL_SK = C.DL_ENRL_SK)
       LEFT JOIN CHOICE.REF_MEMBER_GROUP@DLAKE C1 ON (C.DL_MEMBER_GROUP_SK = C1.DL_MEMBER_GROUP_SK)
       LEFT JOIN CHOICE.REF_MEMBER_SUBGROUP@DLAKE C2 ON (C.DL_MEMBER_SUBGROUP_SK = C2.DL_MEMBER_SUBGROUP_SK)
       LEFT JOIN CHOICE.DIM_MEMBER_SBSB@DLAKE D ON ( C.MEMBER_ID = D.MEMBER_ID  AND C.SUBSCRIBER_ID = D.SBSB_ID AND D.DL_ACTIVE_REC_IND = 'Y')
       LEFT JOIN CHOICE.DIM_MEMBER@dlake D1 on (A.DL_MEMBER_SK = D1.DL_MEMBER_SK)
       LEFT JOIN CHOICE.DIM_MEMBER_ADDRESS@DLAKE E ON (E.DL_MBR_ADDR_SK = A.DL_MEMBER_ADDRESS_SK)
       LEFT JOIN CHOICE.DIM_MEMBER_ASSESSMENTS@DLAKE F ON (F.DL_ASSESS_SK = A.DL_ASSESS_SK)
       LEFT JOIN CHOICE.DIM_MEMBER_PRIMARY_PROVIDER@DLAKE G ON (G.DL_PROV_SK = A.DL_PROV_SK)
       LEFT JOIN CHOICE.DIM_MEMBER_CARE_MANAGER@DLAKE H ON (H.CM_SK_ID = A.CM_SK_ID)
       left join choice.ref_care_staff_details@dlake cm2 on(H.care_manager_id=cm2.care_staff_id)
       left join choice.ref_care_staff_details@dlake ccm on(cm2.assigned_to=ccm.care_staff_id)
       LEFT JOIN (SELECT DISTINCT A.CASE_NUM, CASE WHEN UPPER(B.SPLIT_TEXT) LIKE '%*9*%' THEN 'Y' END AS SUBJ90RULE 
            FROM DW_OWNER.CHOICEPRE_TRACK_NOTES A
            LEFT JOIN DW_OWNER.CHOICEPRE_TRACK_NOTES_T B ON(A.NOTES_ID=B.NOTES_ID)
            WHERE UPPER(SPLIT_TEXT) LIKE '%*9*%') SUB90RULE ON(CASE_NBR = SUB90RULE.CASE_NUM)
       LEFT JOIN CHOICE.REF_COUNTY@DLAKE CNTY ON (COUNTY_CODE = FIPS_CODE)
       LEFT JOIN ( 
                   SELECT DL_ENRL_SK, DL_ENROLL_ID, NVL(MIN(REFERRAL_DATE) OVER (PARTITION BY DL_ENROLL_ID),ORIG_ENROLLMENT_START_DT) AS REFERRAL_DATE  --keep the earliest referral date for the
                    --                                                                                                                                consecutive enrollment, if null then populate
                    --                                                                                                                                with orig enrollment date
                    FROM   CHOICE.DIM_MEMBER_ENROLLMENT@DLAKE
                                WHERE  DL_LOB_ID IN (2, 4, 5) --For MLTC, FIDA, and Total only
                 ) I      ON (I.DL_ENRL_SK = A.DL_ENRL_SK)
        LEFT JOIN CHOICE.REF_RATE_CODE@DLAKE J ON (A.RATE_CODE = J.RATE_CODE AND J.DL_ACTIVE_REC_IND ='Y')
        --LEFT JOIN RISKSCORE RSK ON (A.DL_ASSESS_SK = RSK.RECORD_ID)                            
        LEFT JOIN RISKSCORE RSK ON (A.DL_ASSESS_SK = RSK.record_id)
        LEFT JOIN V_UAS_PAT_ASSESSMENTS UASPAT ON (A.DL_ASSESS_SK = UASPAT.DL_ASSESS_SK)
        LEFT JOIN CEDL.VPIN_MEMBERID_XREF@dlake VPN on (A.MEMBER_ID = VPN.MEMBER_ID)
WHERE AS_OF_MONTH_DT = (SELECT /*+ driving_site(a)  cardinality(A 400000) no_merge */ MAX(AS_OF_MONTH_DT) FROM   CHOICE.FCT_PMPM_ENROLLMENT_CURR@DLAKE)
) a
LEFT JOIN FACT_HHA_LTP_MONTHLY_DATA K on (k.month_id = a.month_id and K.SUBSCRIBER_ID = a.subscriber_id /*and a.program = k.program*/)
