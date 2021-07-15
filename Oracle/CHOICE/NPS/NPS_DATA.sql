SELECT * FROM CHOICEBI.NPS_DATA_LABELS;

SELECT * FROM CHOICEBI.NPS_DATA_CODE;

create view CHOICEBI.VW_NPS_DATA AS
select L.CFMC_CASE_ID,
       L.STATCODE  STATUS, C.STATCODE,
       L.PT_CG, C.PT_CG  PT_CG_CODE,
       L.SAMPTYPE_QUOTA  LOB,C.SAMPTYPE_QUOTA  LOB_CODE,
       L.weight, 
       L.Q101, C.Q101    Q101_CODE,
       L.Q102, C.Q102    Q102_CODE,
       L.Q2,C.Q2         Q2_CODE,
       L.Q3,C.Q3         Q3_CODE,
       L.Q4,C.Q4         Q4_CODE,
       L.Q4OT, C.Q4OT    Q4OT_CODE,
       L.q4coded01,C.q4coded01       q4coded01_CODE,
L.q5,C.q5       q5_CODE,
L.q5coded01,C.q5coded01       q5coded01_CODE,
L.q5coded02,C.q5coded02       q5coded02_CODE,
L.q5coded03,C.q5coded03       q5coded03_CODE,
L.q5coded04,C.q5coded04       q5coded04_CODE,
L.q5coded05,C.q5coded05       q5coded05_CODE,
L.q5coded06,C.q5coded06       q5coded06_CODE,
L.q5coded07,C.q5coded07       q5coded07_CODE,
L.q5coded08,C.q5coded08       q5coded08_CODE,
L.q5coded09,C.q5coded09       q5coded09_CODE,
L.q5coded10,C.q5coded10       q5coded10_CODE,
L.q5coded11,C.q5coded11       q5coded11_CODE,
L.q5coded12,C.q5coded12       q5coded12_CODE,
L.q5coded13,C.q5coded13       q5coded13_CODE,
L.q5coded14,C.q5coded14       q5coded14_CODE,
L.q5coded15,C.q5coded15       q5coded15_CODE,
L.q5coded16,C.q5coded16       q5coded16_CODE,
L.q5coded17,C.q5coded17       q5coded17_CODE,
L.q5coded18,C.q5coded18       q5coded18_CODE,
L.q5coded19,C.q5coded19       q5coded19_CODE,
L.q5coded20,C.q5coded20       q5coded20_CODE,
L.q6,
L.q6coded01,C.q6coded01       q6coded01_CODE,
L.q6coded02,C.q6coded02       q6coded02_CODE,
L.q6coded03,C.q6coded03       q6coded03_CODE,
L.q6coded04,C.q6coded04       q6coded04_CODE,
L.q6coded05,C.q6coded05       q6coded05_CODE,
L.q6coded06,C.q6coded06       q6coded06_CODE,
L.q6coded07,C.q6coded07       q6coded07_CODE,
L.q6coded08,C.q6coded08       q6coded08_CODE,
L.q6coded09,C.q6coded09       q6coded09_CODE,
L.q6coded10,C.q6coded10       q6coded10_CODE,
L.q6coded11,C.q6coded11       q6coded11_CODE,
L.q6coded12,C.q6coded12       q6coded12_CODE,
L.q6coded13,C.q6coded13       q6coded13_CODE,
L.q6coded14,C.q6coded14       q6coded14_CODE,
L.q6coded15,C.q6coded15       q6coded15_CODE,
L.q6coded16,C.q6coded16       q6coded16_CODE,
L.q6coded17,C.q6coded17       q6coded17_CODE,
L.q6coded18,C.q6coded18       q6coded18_CODE,
L.q6coded19,C.q6coded19       q6coded19_CODE,
L.q6coded20,C.q6coded20       q6coded20_CODE,
L.q7,C.q7       q7_CODE,
L.q7coded01,C.q7coded01       q7coded01_CODE,
L.q7coded02,C.q7coded02       q7coded02_CODE,
L.q7coded03,C.q7coded03       q7coded03_CODE,
L.q7coded04,C.q7coded04       q7coded04_CODE,
L.q7coded05,C.q7coded05       q7coded05_CODE,
L.q7coded06,C.q7coded06       q7coded06_CODE,
L.q7coded07,C.q7coded07       q7coded07_CODE,
L.q7coded08,C.q7coded08       q7coded08_CODE,
L.q7coded09,C.q7coded09       q7coded09_CODE,
L.q7coded10,C.q7coded10       q7coded10_CODE,
L.q7coded11,C.q7coded11       q7coded11_CODE,
L.q7coded12,C.q7coded12       q7coded12_CODE,
L.q7coded13,C.q7coded13       q7coded13_CODE,
L.q7coded14,C.q7coded14       q7coded14_CODE,
L.q7coded15,C.q7coded15       q7coded15_CODE,
L.q7coded16,C.q7coded16       q7coded16_CODE,
L.q7coded17,C.q7coded17       q7coded17_CODE,
L.q7coded18,C.q7coded18       q7coded18_CODE,
L.q7coded19,C.q7coded19       q7coded19_CODE,
L.q7coded20,C.q7coded20       q7coded20_CODE,
L.q8,
L.q8coded01,C.q8coded01       q8coded01_CODE,
L.q8coded02,C.q8coded02       q8coded02_CODE,
L.q8coded03,C.q8coded03       q8coded03_CODE,
L.q8coded04,C.q8coded04       q8coded04_CODE,
L.q8coded05,C.q8coded05       q8coded05_CODE,
L.q8coded06,C.q8coded06       q8coded06_CODE,
L.q8coded07,C.q8coded07       q8coded07_CODE,
L.q8coded08,C.q8coded08       q8coded08_CODE,
L.q8coded09,C.q8coded09       q8coded09_CODE,
L.q8coded10,C.q8coded10       q8coded10_CODE,
L.q8coded11,C.q8coded11       q8coded11_CODE,
L.q8coded12,C.q8coded12       q8coded12_CODE,
L.q8coded13,C.q8coded13       q8coded13_CODE,
L.q8coded14,C.q8coded14       q8coded14_CODE,
L.q8coded15,C.q8coded15       q8coded15_CODE,
L.q8coded16,C.q8coded16       q8coded16_CODE,
L.q8coded17,C.q8coded17       q8coded17_CODE,
L.q8coded18,C.q8coded18       q8coded18_CODE,
L.q8coded19,C.q8coded19       q8coded19_CODE,
L.q8coded20,C.q8coded20       q8coded20_CODE,
L.q9,C.q9       q9_CODE,
L.q10,C.q10       q10_CODE,
L.workforvnsny,C.workforvnsny       workforvnsny_CODE,
L.gender,C.gender       gender_CODE,
L.language,C.language       language_CODE,
L.survey_date,
L.complaint,C.complaint       complaint_CODE,
L.complaint2,
L.complaint_coded01,
L.complaint_coded02,
L.complaint_coded03,
L.complaint_coded04,
L.complaint_coded05,
L.complaint_coded06,C.complaint_coded06       complaint_coded06_CODE,
L.complaint_coded07,C.complaint_coded07       complaint_coded07_CODE,
L.complaint_coded08,C.complaint_coded08       complaint_coded08_CODE,
L.complaint_coded09,C.complaint_coded09       complaint_coded09_CODE,
L.complaint_coded10,C.complaint_coded10       complaint_coded10_CODE,
L.complaint_coded11,C.complaint_coded11       complaint_coded11_CODE,
L.complaint_coded12,C.complaint_coded12       complaint_coded12_CODE,
L.complaint_coded13,C.complaint_coded13       complaint_coded13_CODE,
L.complaint_coded14,C.complaint_coded14       complaint_coded14_CODE,
L.complaint_coded15,C.complaint_coded15       complaint_coded15_CODE,
L.complaint_coded16,C.complaint_coded16       complaint_coded16_CODE,
L.complaint_coded17,C.complaint_coded17       complaint_coded17_CODE,
L.complaint_coded18,C.complaint_coded18       complaint_coded18_CODE,
L.complaint_coded19,C.complaint_coded19       complaint_coded19_CODE,
L.complaint_coded20,C.complaint_coded20       complaint_coded20_CODE,
L.sp_complaint,C.sp_complaint       sp_complaint_CODE,
L.name,
L.addr1,
L.addr2,
L.city,
L.state,
L.zip,
L.s_vpin,
L.s_provider_name,
L.s_member_srved_cmo_flag,
L.s_language_code,
L.s_language,
L.s_patient_name,
L.s_patient_phone,
L.s_patient_age,
L.s_pa_gender,
L.s_pa_race,
L.s_r_desc,
L.s_caregiver_name,
L.s_caregiver_phone,
L.s_pa_id,
L.s_pa_firstname,
L.s_pa_lastname,
L.s_pa_wphone,
L.s_pa_aphone,
L.s_ca_address1,
L.s_ca_address2,
L.s_ca_city,
L.s_ca_state,
L.s_ca_zip,
L.s_epi_referralsource,
L.s_pt_id,
L.s_pt_desc, 
L.s_cec_firstname, 
L.s_cec_lastname, 
L.s_cec_wphone, 
L.s_cec_hphone, 
L.s_cec_aphone, 
L.s_cec_street, 
L.s_cec_city, 
L.s_cec_state, 
L.s_cec_zip, 
L.s_icd_code, 
L.s_icd_description, 
L.s_branch_code, 
L.s_branch_name, 
L.s_pa_email, 
L.s_epi_dischargedate, 
L.s_epi_dccid, 
L.s_dcc_desc, 
L.s_cec_email, 
L.s_cesl_sloctid, 
L.s_sloct_description, 
L.s_member_srved_pic_flag, 
L.s_pic_company_name, 
L.s_member_srved_hospice_flag, 
L.s_member_srved_choice_flag, 
L.s_ptot_visit_flag, 
L.s_cct_id, 
L.s_patient_relation, 
L.s_dateofdeath, 
L.s_member_srved_chha_flag, 
L.s_borough_desc, 
L.s_is_primary, 
L.s_language_name, 
L.s_ethnicity, 
L.s_place_of_service_name, 
L.s_provider_name2, 
L.s_provider_desc, 
L.s_enrollment_date_across_lob, 
L.s_min_mltc_enroll_dte, 
L.s_min_sh_enroll_dte, 
L.s_grievance_status,
L.s_grievance_flag, 
L.s_appeal_status, 
L.s_appeal_flag, 
L.s_ics_flag, 
L.s_duplicates, 
L.s_also_in_qta1, 
L.s_also_in_qta2, 
L.s_also_in_qta3, 
L.s_also_in_qta4, 
L.s_also_in_qta5, 
L.s_also_in_qta6, 
L.s_also_in_qta7, 
L.s_also_in_qta8, 
L.s_etnicqta, 
L.s_branchqta, 
L.s_boroqta, 
L.s_ptrelationqta, 
L.s_ca_cityqta, 
L.genderqta,C.genderqta       genderqta_CODE,
L.ageqta,C.ageqta       ageqta_CODE,
L.cityqta,C.cityqta       cityqta_CODE,
L.langqta,C.langqta       langqta_CODE,
L.ca_city,C.ca_city       ca_city_CODE,
L.ethnicity,C.ethnicity       ethnicity_CODE,
L.branch_name,C.branch_name       branch_name_CODE,
L.borough_desc,C.borough_desc       borough_desc_CODE,
L.appeal_flag, 
L.grievance_flag, 
L.is_primary, 
L.language_spoken,C.language_spoken       language_spoken_CODE,
L.patient_relation,C.patient_relation       patient_relation_CODE,
L.NPS_DATA_LOAD 
FROM    CHOICEBI.NPS_DATA_LABELS L
JOIN    CHOICEBI.NPS_DATA_CODE C ON C.CFMC_CASE_ID = L.CFMC_CASE_ID;