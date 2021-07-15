select *  FROM CMODM.CMO_CLIENT_TRACKING_DETAILS@DLAKE
where program = 'Longitudinal / chronic care management'
and payor ='CHOICE'
AND OUTREACH_STATUS IN ( 'Telephonic assessment completed', 'In-home assessment completed pre-outreach','In-home assessment completed post-outreach')
and VPIN NOT IN (select DISTINCT VPIN
FROM CMODM.CMO_CLIENT_TRACKING_DETAILS@DLAKE  T
                            JOIN PHGC.PATIENT_INDEX I
                                ON    TRIM (T.subscriber_id) =
                                      TRIM (I.INDEX_VALUE)
                                   OR TRIM (T.medicare_number) =
                                      TRIM (I.INDEX_VALUE)
                                   OR TRIM (T.vpin) =
                                      TRIM (I.INDEX_VALUE)
                      WHERE     INDEX_ID IN (3, 4,11) --- MEDICARE_NO,subscriber id ,vpin
                            AND I.DELETED_BY IS NULL
                           -- AND script_id IN (223, 196, 220)
                            AND T.program = 'Longitudinal / chronic care management'
                            AND T.payor ='CHOICE'
                            AND T.OUTREACH_STATUS IN ( 'Telephonic assessment completed', 'In-home assessment completed pre-outreach','In-home assessment completed post-outreach')
                            )
                         --   AND episode_start_date IS NOT NULL
                            --AND episode_end_date IS NOT NULL