SELECT  SP.PATIENT_CODE,C.MR_NUMBER,SP.VISIT_DATE,FIRSTNAME, LASTNAME, DOB,
        A.SOC, A.EOC, A.CLIENTID,A.COMPANYID, SP.CAREGIVER_CODE
              ,RANK() OVER (PARTITION BY A.clientid,A.SOC ORDER BY SP.VISIT_DATE) RK
         FROM PIC.SPOC_FORM SP
         JOIN PIC.SPOC_PATIENT_CHART C
           ON C.PATIENT_CODE = SP.PATIENT_CODE
          AND C.CHART_NUMBER = SP.CHART_NUMBER
         JOIN PIC.PATIENT_ADMISSIONS A
           ON A.clientid = SP.PATIENT_CODE
           AND C.MR_NUMBER = A.CHARTID
           AND TO_DATE(SP.VISIT_DATE, 'YYYYMMDD') BETWEEN A.SOC AND NVL(A.EOC,SYSDATE)
         JOIN PIC.PATIENTS P ON SP.PATIENT_CODE = P.CLIENTID
        WHERE SP.FORM_NAME IN ('Adult Assessment SOC')
               AND SP.REMOVED ='N'
               AND SUBSTR(C.MR_NUMBER,-3,2)!= 'KD'
               AND SP.FORM_STATUS != 'Pending' 
           --    and A.COMPANYID = 8
               AND TO_DATE(SP.VISIT_DATE, 'YYYYMMDD') BETWEEN TO_DATE('7/01/2020', 'mm/dd/yyyy') AND TO_DATE('7/31/2020', 'mm/dd/yyyy')   
               AND MR_NUMBER IN ('I3359915-MLT','M3174753-MLT','M3344483-MAP','M3352682-MLT','M3359170-MLT','W3359486-MLT','M3298565-MLT')                
          
           
               
               
               
               select CLIENTID, FIRSTNAME, LASTNAME, DOB,MR_NUMBER
               FROM PIC.PATIENTS P
               JOIN PIC.SPOC_PATIENT_CHART C
                 ON C.PATIENT_CODE = P.CLIENTID
              WHERE REMOVED ='N'
                   and MR_NUMBER IN ('I3359915-MLT','M3174753-MLT','M3344483-MAP','M3352682-MLT','M3359170-MLT','W3359486-MLT','M3298565-MLT') 
               
               
               
               SELECT * FROM PIC.SPOC_PATIENT_CHART 
               WHERE MR_NUMBER IN ('I3359915-MLT','M3174753-MLT','M3344483-MAP','M3352682-MLT','M3359170-MLT','W3359486-MLT','M3298565-MLT') 
               
               SELECT * FROM PIC.PATIENT_ADMISSIONS
               WHERE CHARTID IN ('M3298565-MAP','M3298565-MLT')
               
               SELECT * FROM PIC.SPOC_FORM 
               WHERE PATIENT_CODE = 3298565
               and FORM_NAME IN ('Adult Assessment SOC')
               
               
               
               
               'M3298565-MAP','M3298565-MLT'