WITH
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
        B.DL_LOB_ID         DL_LOB_GRP_ID, 
        B.LOB LOB_GRP_DESC, B.*
    FROM   CHOICE.REF_PLAN@DLAKE B
        JOIN CHOICE.REF_LOB@DLAKE C ON (C.DL_LOB_ID = B.DL_LOB_ID)
        JOIN REF_LOB_GROUP_MAPPING D ON (C.DL_LOB_ID = D.DL_LOB_ID)
        JOIN MSTRSTG.D_LOB_GROUP E ON (E.DL_LOB_GRP_ID = D.DL_LOB_GRP_ID)
) 
select
         F.SUBSCRIBER_ID,
         F.MEMBER_ID,
         VPIN,
         F.FIRST_NAME,
         F.LAST_NAME,
         F.SSN,
         F.DOB                              DATE_OF_BIRTH,
         RACE,
         F.GENDER                           SEX,
         AGE,
         F.DL_LOB_ID,
         F.DL_LOB_ID                        LOB_ID,  
         F.LOB                              LINE_OF_BUSINESS,
         F.PROGRAM,
         F.REPORTING_MONTH                  MONTH_ID,
         F.REPORTING_MONTH                  MONTH,
         F.ORIG_ENROLLMENT_START_DT         ENROLLMENT_DATE,
         DISENROLLMENT_DATE,
         F.NEW_ENR_IND                      ENROLLED_FLAG,
         F.NEW_DISENR_IND                   DISENROLLED_FLAG,
         F.BENEFIT_REGION,  --
         F.BOROUGH,
         MEDICAID                         MEDICAID_NUM,
         CURRENT_MEDICARE                 MEDICARE_NUM,
         F.MAP_REGION_NAME,
         F.LTP_IND  LTP_IND_DLAKE, K.LTP_IND  ,
         F.DL_ENROLL_ID,
         F.PRODUCT_ID,
         F.PRODUCT_NAME,
         F.PROVIDER_ID,
         F.PROVIDER_NAME,
         GROUP_ID,
         GROUP_NAME,
         F.PLAN_ID,
         F.PLAN_DESC,
         F.PLAN_PACKAGE,
         SUB_GROUP_ID                    SUBGROUP_ID,
         SUB_GROUP_NAME                  SUBGROUP_NAME,
         F.RATE_CODE,
         CASE WHEN RATE_CODE_DESC IS NULL  THEN ' - RATE CODE DESC MISSING'
         ELSE RATE_CODE_DESC 
         END RATE_CODE_DESC,
         F.REGION_NAME,
         REGION_NAME2,
         F.COUNTY,
         F.COUNTY_CODE,
         UAS_RECORD_ID,
         F.CARE_MANAGER_FIRST_NAME || F.CARE_MANAGER_LAST_NAME      CARE_STAFF_NAME,
         F.REPORTING_MONTH_DATE            REPORTING_DATE,
         F.DL_ENRL_SK,
         F.DL_ASSESS_SK,
         F.DL_MEMBER_ADDRESS_SK,
         F.DL_DISABLED_SK,
         F.DELEGATED_CM_SK                 DL_DELEGATED_CM_SK,
         F.MOBILITY_DEVICE_IND,
         F.ICS_IND,
         F.DL_CRT_TS                       CRTE_TS,
         F.DL_CRT_TS,
         F.CASE_NBR,
         F.CM_SK_ID,
         F.DISENROLL_RSN_CODE                 DC_REASON,
         F.DISENROLL_RSN_DESC                 DISENROLL_DISP,
         F.DISENROLL_RSN_DESC,                
         F.MLTC_UAS_PREMIUM_RISK_SCORE      RISK_SCORE,
         NVL(MIN(F.REFERRAL_DATE) OVER (PARTITION BY F.DL_ENROLL_ID),F.ORIG_ENROLLMENT_START_DT) AS REFERRAL_DATE,  --keep the earliest referral date for the
                    --                                                                                               consecutive enrollment, if null then populate
                    --                                                                                             with orig enrollment date
         F.MRN,
         F.DL_COUNTY_SK,
         F.DL_MEMBER_SK,
         F.DL_PLAN_SK,
         F.DL_PMPM_ENR_SK,
         F.DL_PROV_SK,
         F.CARE_MANAGER_ID                    STAFF_ID,
         F.STATE                              STATE_CODE,
         F.UAS_DATE,
         F.UAS_NFLOC_SCORE,
         NVL(F.MANDATORY_ENROLLMENT, 'N')     MANDATORY_ENROLLMENT,
         (CASE WHEN F.LOB = 'FIDA' THEN 'FIDA' ELSE 'TMG' END) AS   PRVDR_SRC_SYS,
         NULL  RISK_GROUP,
         1     FLAG,
-----BELOW ARE IN CHOICEBI FMM BUT NOT IN DLAKE FMM
upper(trim(H.first_name) || ' ' || trim(H.last_name)) as     care_staff_name,
upper(trim(ccm.first_name) || ' ' || trim(ccm.last_name)) as care_staff_manager,
TO_CHAR(CASE_NBR)                    HIGHEST_CASE_NUMBER, ---
B.DL_LOB_GRP_ID, --NBU
CASE WHEN A.DL_LOB_ID IN(2) AND TO_DATE(TO_CHAR(ORIG_ENROLLMENT_START_MONTH)|| '01', 'YYYYMMDD')>='01jul2017' AND SUBJ90RULE='Y' THEN 'Y'
     WHEN A.DL_LOB_ID IN(2) AND TO_DATE(TO_CHAR(ORIG_ENROLLMENT_START_MONTH)|| '01', 'YYYYMMDD')>='01jul2017' AND SUBJ90RULE IS NULL THEN 'N'
END AS SUBJ90RULE_IND, ---NBU
DECODE(F.NEW_DISENR_IND , 1, LEAD(F.ORIG_ENROLLMENT_START_DT ) OVER (PARTITION BY  F.MEMBER_ID, F.PROGRAM ORDER BY F.REPORTING_MONTH)) NEXT_LOB_ENROLLMENT_DATE ,
DECODE(F.NEW_ENR_IND, 1, LAG(DISENROLLMENT_DATE) OVER (PARTITION BY  F.MEMBER_ID, F.PROGRAM ORDER BY F.REPORTING_MONTH)) PREV_LOB_DISENROLLMENT_DATE,
UASPAT.VULNERABILITY_INDEX,
'CHOICEDM'   DATA_SOURCE,
NULL DL_UPD_TS,
NULL UPDT_TS,
NULL UPDT_USR_ID,
NULL CRTE_USR_ID,
NULL DL_JOB_RUN_ID,
-------BELOW ARE IN DLAKE BUT NOT IN CHOICEBI FMM
F.ADDRESS_LINE1,
F.ADDRESS_LINE2,
F.ADDRESS_TYPE,
F.AREA_CODE,
F.AS_OF_MONTH_DT,
F.CARE_MANAGER_TITLE,
F.CELL_PHONE,
F.CITY,
F.CMS_MEDICARE,
F.COHORT,
F.COUNTY_COST_REPORTING,
F.CURRENT_MEDICAID,
F.DELEGATED_CM,
F.DELEGATED_CM_DESC,
F.DL_JOB_RUN_ID,
F.ENROLLMENT_END_DT,
F.ENROLLMENT_START_DT,
F.ESRD_IND,
F.EXPENSE_AMT_ADMIN,
F.EXPENSE_AMT_CM,
F.FIPS_COUNTY_CODE,
F.FIPS_COUNTY_NAME,
F.GRGR_CK,
F.HOME_PHONE,
F.HOSPICE_IND,
F.IN_NETWORK,
F.LANGUAGE_SPOKEN,
F.LANGUAGE_SPOKEN_DESC,
F.LATITUDE,
F.LIVES_WITH,
F.LONGITUDE,
F.LOW_INCOME_SUB_COSTSHR_AMT,
F.MAP_REGION_APPROVAL_STATUS,
F.MARITAL_STATUS,
F.MEDICARE,
F.MEMBER_FAMILY_LINK_ID,
F.MEMBER_IND,
F.MEMBER_STATUS,
F.MIDDLE_INITIAL,
F.MLTC_REGION_APPROVAL_STATUS,
F.MLTC_REGION_NAME,
F.MLTC_UAS_PREMIUM_RISK_SCORE,
F.NPI,
F.PARTA_PREMIUM_AMOUNT,
F.PARTB_PREMIUM_AMOUNT,
F.PARTD_PREMIUM_AMOUNT,
F.PART_C_RISK_SCORE,
F.PART_D_RISK_SCORE,
F.PLAN_SUBSIDY_LEVEL,
F.PREMIUM_AMOUNT,
F.RATE_CODE_COST_REPORTING,
F.RATE_CODE_DESC_COST_REPORTING,
F.REF_MEMBER_DISABLED_DESC,
F.SGSG_CK,
F.SPECIALITY,
F.STATE,
F.TITLE,
F.WD_PRODUCT_ID,
F.ZIP_CD
FROM CHOICE.FACT_MEMBER_MONTH@dlake  F
JOIN CHOICE.FCT_PMPM_ENROLLMENT_CURR@DLAKE A 
  ON ( A.MEMBER_ID = F.MEMBER_ID 
       AND A.REPORTING_MONTH = F.REPORTING_MONTH 
       AND A.DL_ENROLL_ID = F.DL_ENROLL_ID
       AND A.DL_ENRL_SK = F.DL_ENRL_SK)
LEFT JOIN V_REF_PLAN B ON (F.DL_PLAN_SK = B.DL_PLAN_SK)
LEFT JOIN CHOICE.DIM_MEMBER_CARE_MANAGER@DLAKE H ON (H.CM_SK_ID = F.CM_SK_ID)       
LEFT JOIN CHOICE.REF_CARE_STAFF_DETAILS@DLAKE CM2 ON(H.CARE_MANAGER_ID=CM2.CARE_STAFF_ID)
LEFT JOIN CHOICE.REF_CARE_STAFF_DETAILS@DLAKE CCM ON(CM2.ASSIGNED_TO=CCM.CARE_STAFF_ID)       
LEFT JOIN (SELECT DISTINCT AA.CASE_NUM, CASE WHEN UPPER(B.SPLIT_TEXT) LIKE '%*9*%' THEN 'Y' END AS SUBJ90RULE 
             FROM DW_OWNER.CHOICEPRE_TRACK_NOTES AA
             LEFT JOIN DW_OWNER.CHOICEPRE_TRACK_NOTES_T B ON(AA.NOTES_ID=B.NOTES_ID)
             WHERE UPPER(SPLIT_TEXT) LIKE '%*9*%') SUB90RULE 
     ON(CASE_NBR = SUB90RULE.CASE_NUM)
LEFT JOIN V_UAS_PAT_ASSESSMENTS UASPAT ON (F.DL_ASSESS_SK = UASPAT.DL_ASSESS_SK)
LEFT JOIN FACT_HHA_LTP_MONTHLY_DATA K on (k.month_id = F.REPORTING_MONTH and K.SUBSCRIBER_ID = F.SUBSCRIBER_ID /*and a.program = k.program*/)
where F.SUBSCRIBER_ID = 'V70000096'
;   


