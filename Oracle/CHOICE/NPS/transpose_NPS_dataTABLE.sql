
CREATE VIEW CHOICEBI.NPS_QA AS
WITH CODE1 AS
(
   SELECT CFMC_CASE_ID, to_char(QUESTION_ID)  QUESTION_ID, to_char(OPTION_NO)  OPTION_NO
   FROM
      (select CFMC_CASE_ID,  Q101,Q102,Q2,Q3, Q9,Q10 from CHOICEBI.NPS_DATA_CODE) 
      unpivot 
      (OPTION_NO FOR QUESTION_ID IN (Q101,Q102,Q2,Q3,Q9,Q10))
)
,
   CODE2 AS
(
     SELECT CFMC_CASE_ID, to_char(QUESTION_ID)  QUESTION_ID, to_char(OPTION_NO) OPTION_NO
     FROM
      (select CFMC_CASE_ID,Q4,Q5,Q6,Q7,Q8 from CHOICEBI.NPS_DATA_CODE) 
       unpivot 
      (OPTION_NO FOR QUESTION_ID IN (Q4,Q5,Q6,Q7,Q8 ))
      ORDER BY CFMC_CASE_ID
)
,
   LABEL1 AS
(
   SELECT CFMC_CASE_ID, to_char(QUESTION_ID)   QUESTION_ID, to_char(OPTION_NO)   OPTIONS
   FROM
      (select CFMC_CASE_ID,Q101,Q102,Q2,Q3,Q9,Q10 from CHOICEBI.NPS_DATA_LABELS) 
      unpivot 
      (OPTION_NO FOR QUESTION_ID IN (Q101,Q102,Q2,Q3,Q9,Q10))
      ORDER BY CFMC_CASE_ID
)
,
   LABEL2 AS
(
     SELECT CFMC_CASE_ID, to_char(QUESTION_ID) QUESTION_ID, to_char(OPTION_NO)   OPTIONS
   FROM
      (select CFMC_CASE_ID,Q4,Q5,Q6,Q7,Q8 from CHOICEBI.NPS_DATA_LABELS) 
      unpivot 
      (OPTION_NO FOR QUESTION_ID IN (Q4,Q5,Q6,Q7,Q8 ))
      ORDER BY CFMC_CASE_ID
)
,
NPS_CODE AS
(
  SELECT * FROM CODE1
  UNION ALL
  selecT * FROM CODE2 
  )
,
NPS_LABEL AS
(
   SELECT * FROM LABEL1
   UNION ALL
   SELECT * FROM LABEL2
)
--,COMPLAIN AS
--(     SELECT CFMC_CASE_ID,COMPLAINT, complaint_id, complain_detail  
--      FROM
--      (select CFMC_CASE_ID,  COMPLAINT, COMPLAINT2 from CHOICEBI.NPS_DATA_CODE) 
 --      unpivot 
 --     (complain_detail FOR complaint_id IN (COMPLAINT2)))
,
NPS_QA AS
(
    SELECT C.CFMC_CASE_ID,
           C.QUESTION_ID,Q.QUESTION, C.OPTION_NO, L.OPTIONS
          -- ,COM.COMPLAINT, COM.COMPLAINT_ID, COM.COMPLAIN_DETAIL
    FROM NPS_CODE C 
    JOIN NPS_LABEL L  ON C.CFMC_CASE_ID=L.CFMC_CASE_ID AND C.QUESTION_ID=L.QUESTION_ID
    JOIN CHOICEBI.NPS_QUESTIONS Q ON C.QUESTION_ID = Q.QUESTION_ID   
  --  JOIN CHOICEBI.NPS_DATA_CODE CC ON CC.CFMC_CASE_ID=C.CFMC_CASE_ID
  --  JOIN CHOICEBI.NPS_DATA_LABELS LL ON LL.CFMC_CASE_ID=C.CFMC_CASE_ID
  --  LEFT JOIN COMPLAIN COM ON COM.CFMC_CASE_ID = C.CFMC_CASE_ID
)  
SELECT * FROM NPS_QA
ORDER BY CFMC_CASE_ID,QUESTION_ID;             

      




SELECT CFMC_CASE_ID, STATUS, STATCODE, PT_CG, PT_CG_CODE, LOB, LOB_CODE, WEIGHT,
       WORKFORVNSNY, WORKFORVNSNY_CODE, GENDER, GENDER_CODE, LANGUAGE, LANGUAGE_CODE, 
       SURVEY_DATE, COMPLAINT, NAME, ADDR1, ADDR2, CITY, STATE, 
       ZIP, S_VPIN, S_PROVIDER_NAME, S_MEMBER_SRVED_CMO_FLAG, S_LANGUAGE_CODE, S_LANGUAGE, 
       S_PATIENT_NAME, S_PATIENT_PHONE, S_PATIENT_AGE, S_PA_GENDER, S_PA_RACE, S_R_DESC, 
       S_CAREGIVER_NAME, S_CAREGIVER_PHONE, S_PA_ID, S_PA_FIRSTNAME, S_PA_LASTNAME, S_PA_WPHONE,
        S_PA_APHONE, S_CA_ADDRESS1, S_CA_ADDRESS2, S_CA_CITY, S_CA_STATE, S_CA_ZIP, S_EPI_REFERRALSOURCE, 
        S_PT_ID, S_PT_DESC, S_CEC_FIRSTNAME, S_CEC_LASTNAME, S_CEC_WPHONE, S_CEC_HPHONE, S_CEC_APHONE, 
        S_CEC_STREET, S_CEC_CITY, S_CEC_STATE, S_CEC_ZIP, S_ICD_CODE, S_ICD_DESCRIPTION, S_BRANCH_CODE, 
        S_BRANCH_NAME, S_PA_EMAIL, S_EPI_DISCHARGEDATE, S_EPI_DCCID, S_DCC_DESC, S_CEC_EMAIL, S_CESL_SLOCTID, 
        S_SLOCT_DESCRIPTION, S_MEMBER_SRVED_PIC_FLAG, S_PIC_COMPANY_NAME, S_MEMBER_SRVED_HOSPICE_FLAG, 
        S_MEMBER_SRVED_CHOICE_FLAG, S_PTOT_VISIT_FLAG, S_CCT_ID, S_PATIENT_RELATION, S_DATEOFDEATH, 
        S_MEMBER_SRVED_CHHA_FLAG, S_BOROUGH_DESC, S_IS_PRIMARY, S_LANGUAGE_NAME, S_ETHNICITY, S_PLACE_OF_SERVICE_NAME, 
        S_PROVIDER_NAME2, S_PROVIDER_DESC, S_ENROLLMENT_DATE_ACROSS_LOB, S_MIN_MLTC_ENROLL_DTE, 
        S_MIN_SH_ENROLL_DTE, S_GRIEVANCE_STATUS, S_GRIEVANCE_FLAG, S_APPEAL_STATUS, S_APPEAL_FLAG, S_ICS_FLAG, 
        S_DUPLICATES, S_ALSO_IN_QTA1, S_ALSO_IN_QTA2, S_ALSO_IN_QTA3, S_ALSO_IN_QTA4, S_ALSO_IN_QTA5, S_ALSO_IN_QTA6,
         S_ALSO_IN_QTA7, S_ALSO_IN_QTA8, S_ETNICQTA, S_BRANCHQTA, S_BOROQTA, S_PTRELATIONQTA, S_CA_CITYQTA, GENDERQTA, 
         GENDERQTA_CODE, AGEQTA, AGEQTA_CODE, CITYQTA, CITYQTA_CODE, LANGQTA, LANGQTA_CODE, CA_CITY, CA_CITY_CODE, ETHNICITY, 
         ETHNICITY_CODE, BRANCH_NAME, BRANCH_NAME_CODE, BOROUGH_DESC, BOROUGH_DESC_CODE, APPEAL_FLAG, GRIEVANCE_FLAG, 
         IS_PRIMARY, LANGUAGE_SPOKEN, LANGUAGE_SPOKEN_CODE, PATIENT_RELATION, PATIENT_RELATION_CODE,
         COMPLAINT_CODE, COMPLAINT2, COMPLAINT_CODED01, COMPLAINT_CODED02, 
       COMPLAINT_CODED03, COMPLAINT_CODED04, COMPLAINT_CODED05, COMPLAINT_CODED06, 
       COMPLAINT_CODED06_CODE, COMPLAINT_CODED07, COMPLAINT_CODED07_CODE, COMPLAINT_CODED08, 
       COMPLAINT_CODED08_CODE, COMPLAINT_CODED09, COMPLAINT_CODED09_CODE, COMPLAINT_CODED10, 
       COMPLAINT_CODED10_CODE, COMPLAINT_CODED11, COMPLAINT_CODED11_CODE, COMPLAINT_CODED12, 
       COMPLAINT_CODED12_CODE, COMPLAINT_CODED13, COMPLAINT_CODED13_CODE, COMPLAINT_CODED14, 
       COMPLAINT_CODED14_CODE, COMPLAINT_CODED15, COMPLAINT_CODED15_CODE, COMPLAINT_CODED16, 
       COMPLAINT_CODED16_CODE, COMPLAINT_CODED17, COMPLAINT_CODED17_CODE, COMPLAINT_CODED18, 
       COMPLAINT_CODED18_CODE, COMPLAINT_CODED19, COMPLAINT_CODED19_CODE, COMPLAINT_CODED20, 
       COMPLAINT_CODED20_CODE, SP_COMPLAINT, SP_COMPLAINT_CODE, NPS_DATA_LOAD
FROM   CHOICEBI.VW_NPS_DATA;       







---------
SELECT CFMC_CASE_ID,COMPLAINT, complaint_id, complain_detail  
   FROM
      (select CFMC_CASE_ID,  COMPLAINT, COMPLAINT2 from CHOICEBI.NPS_DATA_CODE) 
       unpivot 
      (complain_detail FOR complaint_id IN (COMPLAINT2))
      
      
      
      
      SELECT * FROM CHOICEBI.VW_NPS_DATA;