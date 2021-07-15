SELECT  distinct 'Longitudinal' PROGRAM,
         L.PATIENT_ID,  L.FIRST_NAME, L.LAST_NAME, L.DATE_OF_BIRTH,D.ADDRESS,D.CITY,D.STATE,D.ZIP, LA.LANGUAGE_NAME,PLAN_DESC, 'Choice' PAYPR
FROM v_longi_mbr L
LEFT JOIN PHGC.PATIENT_DETAILS D ON L.PATIENT_ID = D.PATIENT_ID
LEFT JOIN  PHGC.LANGUAGE  LA    ON D.PRIMARY_LANGUAGE_ID = LA.LANGUAGE_ID
 LEFT JOIN phgc.MEM_BENF_PROG mbp
                    ON L.PATIENT_ID = MBP.MEMBER_ID
                LEFT  JOIN PHGC.BENF_PLAN_PROG  bpp
                    ON mbp.ben_plan_prog_id = bpp.ben_plan_prog_id
                LEFT  JOIN PHGC.LOB_BENF_PLAN LBP
                    ON MBP.LOB_BEN_ID = LBP.LOB_BEN_ID
                LEFT  JOIN PHGC.BENEFIT_PLAN BP
                    ON BP.BENEFIT_PLAN_ID = LBP.BENEFIT_PLAN_ID
                    
                    



