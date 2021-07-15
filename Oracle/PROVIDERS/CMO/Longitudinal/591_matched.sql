
select DISTINCT
        A.VPIN, A.SUBSCRIBER_ID,A.FIRST_NAME,C.FIRST_NAME,A.LAST_NAME, C.LAST_NAME,A.DATE_OF_BIRTH,C.BIRTH_YEAR,C.PATIENT_ID,C.client_patient_id
FROM        
(select * from CMODM.CMO_CLIENT_TRACKING_DETAILS@DLAKE 
where
    PROGRAM =  'Longitudinal / chronic care management'
    and payor ='CHOICE'
    AND OUTREACH_STATUS IN ( 'Telephonic assessment completed', 
                               'In-home assessment completed pre-outreach',
                               'In-home assessment completed post-outreach')    
                                                                               ) A                          
JOIN                              
                               
   (                            
    SELECT distinct vpin, patient_id
    FROM
     (
      select vpin, SRC_UNIQUE_ID1 patient_id from cedl.vpin_master@dlake b where b.src_sys  = 'GCPH'
      union all
      select vpin, patient_id from cedl.VPIN_PATIENTID_XREF@dlake b  
                                                                           )
                                                                               ) B 
  ON (A.vpin = B.vpin)                          
JOIN PHGC.PATIENT_DETAILS C 
   on (C.client_patient_id = B.patient_id) 
                                                

  