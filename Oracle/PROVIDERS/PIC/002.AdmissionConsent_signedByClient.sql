

SELECT  A.CLIENTID, A.SOC, A.EOC, A.COMPANYID, 
       CONCAT(TO_CHAR(A.SOC,'yyyy'),TO_CHAR(A.SOC,'mm')) AS MONTH_ID
FROM PIC.PATIENT_ADMISSIONS A



SELECT *
FROM PICBI.V_SPOC SP
WHERE SP.FORM_NAME IN ('Adult Assessment', 'Adult Assessment Readmission',
                       'Adult Assessment - Readmission','Adult Assessment SOC')
                       


select distinct form_field_name 
from PIC.SPOC_FORM_NEWFIELDS_V 
where FORM_NAME IN ('Adult Assessment', 'Adult Assessment Readmission',
                       'Adult Assessment - Readmission','Adult Assessment SOC')                       
      and form_field_name like '%consent%'    
      
select * 
FROM PIC.SPOC_FORM
where FORM_NAME IN ('Adult Assessment', 'Adult Assessment Readmission',
                       'Adult Assessment - Readmission','Adult Assessment SOC')                    
                      

SELECT * 
FROM ALL_TAB_COLUMNS
WHERE COLUMN_NAME LIKE '%consect%'