SELECT DISTINCT
     '001' MEASURE_ID
     ,PATIENT_CODE  CLIENTID
     ,SOC
     ,CONCAT(TO_CHAR(VISIT_DATE,'yyyy'),TO_CHAR(VISIT_DATE,'mm')) AS MONTH_ID
     ,SCHEDULEDATE AS  THE_DATE
     ,'1st_HHA_Visit'  DATE_TYPE
     ,VISIT_DATE       ASSESS_DATE
     ,CAREGIVER_CODE   STAFF_ID
     ,COMPANYID        COMPANY_ID
     ,CASE
           WHEN (VISIT_DATE BETWEEN SCHEDULEDATE AND SCHEDULEDATE + 2) THEN 1
           ELSE 0
           END AS NUM
          , 1  AS DEN   
FROM
  (     
  select /*+ materialize use_hash(den, num)*/
        den.*, num.SCHEDULEDATE
      FROM
       (
        SELECT * FROM
        (SELECT  /*+ materialize use_hash(sp c  a p) */
        SP.PATIENT_CODE,C.MR_NUMBER,TO_DATE(SP.VISIT_DATE, 'YYYYMMDD') VISIT_DATE,FIRSTNAME, LASTNAME, DOB,
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
               AND TO_DATE(SP.VISIT_DATE, 'YYYYMMDD') BETWEEN TO_DATE('7/01/2020', 'mm/dd/yyyy') AND sysdate  
                        )
                        WHERE RK =1 
                        ) den
   LEFT JOIN 
       ( SELECT * FROM 
           (
            SELECT  /*+ materialize USE_HASH(A S ts) */
                    A.clientid, A.SOC, A.EOC,S.SCHEDULEDATE,A.ADMISSIONSTATUS,A.COMPANYID
                   ,RANK() OVER (PARTITION BY A.clientid, A.SOC ORDER BY S.SCHEDULEDATE) RK2
             FROM  PIC.PATIENT_ADMISSIONS A
             JOIN PICBI.VW_SCHEDULES S
               ON S.CLIENTID = A.CLIENTID
              AND S.SCHEDULEDATE BETWEEN A.SOC AND NVL(A.EOC,SYSDATE) 
             JOIN PIC.SCHEDULES_TASKS TS
               ON TS.SCHEDULEID = S.SCHEDULEID
             WHERE TS.TASKNAME != '0085 Travel Time' 
              )
         WHERE RK2 =1 
                        ) num
  ON den.PATIENT_CODE = num.clientid AND num.SOC = den.SOC
                                     AND NVL(num.EOC,SYSDATE) = NVL(den.EOC,SYSDATE))
                                          
                                                                    
             
     
                                             
