  SELECT DISTINCT 
     '004' MEASURE_ID
     ,PATIENT_CODE  CLIENTID
     ,SOC
     ,CONCAT(TO_CHAR(VISIT_DATE,'yyyy'),TO_CHAR(VISIT_DATE,'mm')) AS MONTH_ID
     ,VISIT_DATE AS  THE_DATE
     ,'Supervisory_Visit'  DATE_TYPE
     ,VISIT_DATE       ASSESS_DATE
     ,CAREGIVER_CODE   STAFF_ID
     ,COMPANYID        COMPANY_ID
     ,CASE
           WHEN (FORM_NAME IN ('Adult Assessment Recert','Adult Assessment - Recert',
                               'Adult Assessment - Readmission','Adult Assessment',
                               'Adult Assessment Readmission','Adult Assessment SOC')
                  AND FORM_FIELD_VALUE = 1) THEN 1
           ELSE 0
           END AS NUM
          , 1  AS DEN   
FROM 
(
SELECT SP.CAREGIVER_CODE, SP.FORM_NAME
       ,TO_DATE(SP.VISIT_DATE,'YYYYMMDD') VISIT_DATE
       ,SP.PATIENT_CODE
       ,P.FIRSTNAME
       ,P.LASTNAME
       ,SP.DATE_CREATED
       ,SP.FORM_STATUS 
       ,A.COMPANYID
       ,A.CHARTID
       ,A.SOC
       ,A.EOC
       ,F.FORM_FIELD_NAME
       ,F.FORM_FIELD_VALUE
       ,U.PRIMARY_USER_TYPE     
FROM PIC.SPOC_FORM  SP
JOIN PIC.SPOC_FORM_NEWFIELDS_V F
    ON SP.FORM_ID = F.FORM_ID 
   AND (UPPER(F.FORM_FIELD_NAME) = UPPER('Supervision performed this visit'))
   AND REMOVED ='N'
JOIN PIC.SPOC_PATIENT_CHART  C
    ON C.PATIENT_CODE = SP.PATIENT_CODE
   AND C.CHART_NUMBER = SP.CHART_NUMBER   
JOIN PIC.PATIENT_ADMISSIONS A
    ON A.clientid = SP.PATIENT_CODE
   AND TO_DATE(SP.VISIT_DATE,'YYYYMMDD') BETWEEN A.SOC AND NVL(A.EOC,SYSDATE)
   AND C.MR_NUMBER = A.CHARTID
JOIN PIC.SPOC_USERS U
    ON U.CAREGIVER_CODE = SP.CAREGIVER_CODE 
JOIN PIC.PATIENTS P ON SP.PATIENT_CODE = P.CLIENTID            
WHERE  SP.FORM_STATUS != 'Pending'  
       AND SUBSTR(A.CHARTID,-3,2)!= 'KD'
    --   AND COMPANYID IN(8) 
       AND TO_DATE(SP.VISIT_DATE,'YYYYMMDD') BETWEEN TO_DATE('7/01/2020', 'mm/dd/yyyy') AND sysdate
       --TO_DATE('7/31/2020', 'mm/dd/yyyy')
 )
   
   
   
   
   
   
   
   
