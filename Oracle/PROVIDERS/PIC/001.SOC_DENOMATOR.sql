SELECT  SP.PATIENT_CODE, SP.FIRSTNAME,SP.LASTNAME
              ,SP.CHARTID,SP.VISIT_DATE,A.SOC, A.EOC, A.CLIENTID,A.COMPANYID, FF.CAREGIVER_CODE
              ,RANK() OVER (PARTITION BY A.clientid,A.SOC ORDER BY SP.VISIT_DATE) RK
         FROM PICBI.V_SPOC SP
         JOIN PIC.PATIENT_ADMISSIONS A
           ON A.clientid = SP.PATIENT_CODE
           AND SP.CHARTID = A.CHARTID
           AND SP.VISIT_DATE BETWEEN A.SOC AND NVL(A.EOC,SYSDATE)
         JOIN PIC.SPOC_FORM FF ON FF.FORM_ID = SP.FORM_ID
        WHERE SP.FORM_NAME IN ('Adult Assessment SOC')
               AND SUBSTR(SP.CHARTID,-3,2)!= 'KD'
               AND SP.FORM_STATUS != 'Pending' 
               and A.COMPANYID = 11
               AND SP.VISIT_DATE BETWEEN TO_DATE('7/01/2020', 'mm/dd/yyyy') AND TO_DATE('7/31/2020', 'mm/dd/yyyy')                      
               AND A.CHARTID ='M3298565-MAP'



               
               
               SELECT * FROM PIC.SPOC_PATIENT_CHART WHERE MR_NUMBER = 'M3298565-MAP'
               
               SELECT * FROM PIC.PATIENT_ADMISSIONS
               WHERE CHARTID ='M3298565-MAP'
               
               SELECT *  FROM PICBI.V_SPOC SP
                WHERE CHARTID ='M3298565-MAP'
                AND FORM_ID = 282451

                
                SELECT * FROM PIC.SPOC_FORM WHERE PATIENT_CODE =3298565
                AND FORM_NAME  ='Adult Assessment SOC'
                AND 
                
                282451
283052
