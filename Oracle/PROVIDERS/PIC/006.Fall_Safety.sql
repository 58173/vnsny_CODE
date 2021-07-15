
SELECT DISTINCT 
     '006' MEASURE_ID
     ,CLIENTID
     ,SOC
     ,CONCAT(TO_CHAR(VISIT_DATE,'yyyy'),TO_CHAR(VISIT_DATE,'mm')) AS MONTH_ID
     ,VISIT_DATE       THE_DATE
     ,'ASSESS_DATE'    DATE_TYPE
     ,VISIT_DATE       ASSESS_DATE
     ,CAREGIVER_CODE   STAFF_ID
     ,COMPANYID        COMPANY_ID
     ,CASE
           WHEN FORM_FIELD_NAME IS NOT NULL THEN 1
           ELSE 0
           END AS NUM
          , 1  AS DEN  
FROM
  (     
  SELECT /*+ materialize use_hash(den, num)*/
     DISTINCT den.CLIENTID,den.CAREGIVER_CODE, den.COMPANYID, den.DOB, den.EOC,den.SOC, den.VISIT_DATE, 
                  den.FIRSTNAME,den.LASTNAME, den.MR_NUMBER, den.FORM_ID, den.FORM_NAME, num.FORM_FIELD_NAME, num.FORM_FIELD_VALUE
      FROM
       (SELECT /*+ use_hash(sp c A P) */
               SP.PATIENT_CODE,C.MR_NUMBER,TO_DATE(SP.VISIT_DATE, 'YYYYMMDD') VISIT_DATE, FIRSTNAME, LASTNAME, DOB,
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
               AND  SP.REMOVED ='N'
               AND SP.FORM_STATUS != 'Pending'  
               AND TO_DATE(SP.VISIT_DATE, 'YYYYMMDD') BETWEEN TO_DATE('7/01/2020', 'mm/dd/yyyy') AND sysdate                        
                        ) den                      
  LEFT JOIN                     
          (
           SELECT * 
            FROM  PIC.SPOC_FORM_NEWFIELDS_V 
           WHERE  FORM_ID IN            
                (SELECT  /*+ no_merge */ DISTINCT FORM_ID  FROM 
                        (
                        SELECT FORM_ID
                          FROM  PIC.SPOC_FORM_NEWFIELDS_V
                         WHERE FORM_FIELD_NAME LIKE 'SafetyHazards%'
                 INTERSECT
                        SELECT FORM_ID
                          FROM PIC.SPOC_FORM_NEWFIELDS_V 
                         WHERE FORM_FIELD_NAME = 'cp_safety_measures'
                           AND REGEXP_LIKE(FORM_FIELD_VALUE, 'Clear Pathways|Fall Precautions|Equipment Safety|Keep Pathways Clear
                                                              |Safety in ADLs|Keep Siderails Up|Seizure Precautions|Siderails up|
                                                               Slow Position Change|Support During Transfer and Ambulation|Use of Assistive Devices'))
                                                                )
                                                                 ) num 
          ON  num.FORM_ID = den.FORM_ID   
           AND ( FORM_FIELD_NAME LIKE 'SafetyHazards%'
                 OR  FORM_FIELD_NAME = 'cp_safety_measures')
                 )                                        
      

                                               
                                       
                                                