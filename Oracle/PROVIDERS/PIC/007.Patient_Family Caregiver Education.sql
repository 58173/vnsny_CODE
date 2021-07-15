

SELECT DISTINCT 
     '007' MEASURE_ID
     ,CLIENTID
     ,VISIT_DATE
     ,CONCAT(TO_CHAR(VISIT_DATE,'yyyy'),TO_CHAR(VISIT_DATE,'mm')) AS MONTH_ID
     ,CAREGIVER_CODE   STAFF_ID
     ,COMPANYID       COMPANY_ID
     ,CASE
           WHEN FORM_FIELD_VALUE  IS NOT NULL THEN 1
           ELSE 0
           END AS NUM
          , 1  AS DEN  
FROM
  (     
  SELECT DISTINCT den.CLIENTID,den.CAREGIVER_CODE, den.COMPANYID, den.DOB, den.EOC,den.SOC, den.VISIT_DATE, 
                  den.FIRSTNAME,den.LASTNAME, den.MR_NUMBER, den.FORM_ID, den.FORM_NAME, num.FORM_FIELD_NAME, num.FORM_FIELD_VALUE
      FROM
       (SELECT  SP.PATIENT_CODE,C.MR_NUMBER,TO_DATE(SP.VISIT_DATE, 'YYYYMMDD') VISIT_DATE,FIRSTNAME, LASTNAME, DOB,
               A.SOC, A.EOC, A.CLIENTID,A.COMPANYID, SP.CAREGIVER_CODE,SP.FORM_NAME,SP.FORM_ID
          FROM PIC.SPOC_FORM SP
          JOIN PIC.SPOC_PATIENT_CHART C
            ON C.PATIENT_CODE = SP.PATIENT_CODE
           AND C.CHART_NUMBER = SP.CHART_NUMBER
          JOIN PIC.PATIENT_ADMISSIONS A
            ON A.clientid = SP.PATIENT_CODE
           AND C.MR_NUMBER = A.CHARTID
           AND TO_DATE(SP.VISIT_DATE, 'YYYYMMDD') BETWEEN A.SOC AND NVL(A.EOC,SYSDATE)
          JOIN PIC.PATIENTS P ON SP.PATIENT_CODE = P.CLIENTID
        WHERE  SP.FORM_NAME LIKE ('%Adult Assessment%')
               AND SP.REMOVED ='N'
               AND SP.FORM_STATUS != 'Pending'  
               AND TO_DATE(SP.VISIT_DATE, 'YYYYMMDD') BETWEEN TO_DATE('7/01/2020', 'mm/dd/yyyy') AND sysdate                        
                        ) den                      
  LEFT JOIN                     
          (
           SELECT * 
            FROM  PIC.SPOC_FORM_NEWFIELDS_V 
           WHERE FORM_FIELD_NAME IN ('InstructMaterial10', --- Medication Regimen/ Administation
                                     'InstructMaterial4',  --- Standard precautions
                                     'InstructMaterial6',  --- home safety    
                                     'ima_FallPrevent_Chk'  -- Provided fall prevention                              
                                      )
                OR (FORM_FIELD_NAME = 'InstructMaterialOtherText'  
                   ---  AND FORM_FIELD_VALUE LIKE '%Education%')
                     AND FORM_FIELD_VALUE = 'Education provided to pt/family/HHA')                                      
                                                                 ) num 
          ON  num.FORM_ID = den.FORM_ID   
                                         )
      
      
     