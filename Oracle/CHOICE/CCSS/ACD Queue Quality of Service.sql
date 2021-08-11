--- ACD Queue Quality of Service 

SELECT distinct 
       CL.date1, AQ.SKILL,  CL.CAMPAIGN_TYPE,
       CL.SERVICE_LEVEL, CL.SPEED_OF_ANSWER, CL.SPEED_OF_ANSWER_IN_SECONDS,CL.CALLS, CL.ABANDONED,CL.TIME_TO_ABANDON,CL.TIME_TO_ABANDON_IN_SECONDS,
       CL.HANDLE_TIME, CL.HANDLE_TIME_IN_SECONDS,CL.AFTER_CALL_WORK_TIME,CL.AFTER_CALL_WORK_TIME_IN_SECONDS,CL.CALL_TYPE, 
       CL.TALK_TIME,  CL.TALK_TIME_IN_SECONDS,
       AQ.QUEUE_WAIT_TIME, AQ.QUEUE_WAIT_TIME_IN_SECONDS, 
       A.AGENT_GROUP, 
       CL.CALL_ID, CL.CALL_TYPE   
from CHOICEBI.MV_FACT_F9_CALL_LOG   CL
LEFT JOIN CHOICEBI.MV_FACT_F9_AGENT_ACTIVITY_LOG  AA  ON  CL.CALL_ID = AA.CALL_ID  
LEFT JOIN CHOICEBI.MV_DIM_F9_AGENT A  ON AA. AGENT_ID = A.AGENT_ID
LEFT JOIN CHOICEBI.MV_FACT_F9_ACD_QUEUE AQ  ON AQ.CALL_ID = CL.CALL_ID
WHERE    CL.CAMPAIGN_TYPE IN ('Auto Dial', 'Inbound')  
 --   AND CL.DATE1 =TO_DATE('8/1/2021', 'MM/DD/YYYY') 
    AND CL.CALL_TYPE IN ('Inbound', 'Inbound Voicemail','Manual','Autodial') 
    AND AQ.DL_SKILL_SK IN (789,	745,	672,	730,	724,	677,	674,	826,	742,	729,	733,	
                           681,	679,	776,	734,	757,	767,	740,	728,	682,	759,	712,	
                           827,	708,	821,	779,	684,	725,	762,	781,	704,	782,	678,	
                           763,	814,	715,	758,	713,	732,	778,	743,	687,	741,	777,	
                           675,	685,	825,	707,	709,	673,	686,	761,	790,	688,	831,	
                           749,	832,	750,	748,	689,	820,	710,	783,	813,	753,	747,	
                           683,	766,	736,	731,	714,	751,	680,	822,	824,	746,	819,	
                           735,	760,	711,	676,	780,	706,	823,	768,	752,	705,	818)
;















 AND AQ.SKILL IN ('CCC Active Patient Eng',
'CCC Active Patient Spa',
'CCC Blended Eng',
'CCC Blended Spa',
'CCC CERT',
'CCC CMO Eng',
'CCC CMO Spa',
'CCC Hospice Eng',
'CCC Hospice Spa',
'CCC Other Eng',
'CCC Other Spa',
'CCC Referral Eng',
'CCC Referral Spa',
'CCC Risk',
'CCC RN Eng',
'CCC RN Spa',
'CCC RN to Direct',
'CCC Voicemail',
'Chce 7184 Eng',
'Chce 7184 Eng VM',
'Chce 7184 Spa',
'Chce 7184 Spa VM',
'Chce Medical Management MLTC',
'Chce Medical Management MLTC VM',
'Chce Member Experience MLTC',
'Chce Member Experience MLTC VM',
'Chce Member Experience Total',
'Chce Member Experience Total VM',
'Chce MLTC Member Services Chi',
'Chce MLTC Member Services Chi VM',
'Chce MLTC Member Services Eng',
'Chce MLTC Member Services Eng VM',
'Chce MLTC Member Services Rus',
'Chce MLTC Member Services Rus VM',
'Chce MLTC Member Services Spa',
'Chce MLTC Member Services Spa VM',
'Chce MLTC Other Eng',
'Chce MLTC Other Eng VM',
'Chce MLTC Other Spa',
'Chce MLTC Other Spa VM',
'Chce Provider Benefits',
'Chce Provider Benefits VM',
'Chce Provider Claims',
'Chce Provider Claims VM',
'Chce Provider MLTC',
'Chce Provider MLTC VM',
'Chce Provider Other',
'Chce Provider Other VM',
'Chce Select Health Member Eng',
'Chce Select Health Member Eng VM',
'Chce Select Health Member Spa',
'Chce Select Health Member Spa VM',
'Chce Select Health New Eng',
'Chce Select Health New Eng VM',
'Chce Select Health New Spa',
'Chce Select Health New Spa VM',
'Chce Total Plan Eng',
'Chce Total Plan Eng VM',
'Chce Total Plan Spa',
'Chce Total Plan Spa VM',
'Choice 7184 Choice',
'Choice DME Letter_ Russian',
'Choice DME Letter_Chinese',
'Choice DME Letter_ENG',
'Choice DME Letter_SPA',
'Choice Greivance and Appeals',
'Choice Hopital Discharge Reinstate HHA',
'Choice Hopital Discharge Reinstate HHA VM',
'Choice Hopital Discharge Transportation',
'Choice Hopital Discharge Transportation VM',
'Choice Hospital Discharge',
'Choice Medical Management',
'Choice Member Experience',
'Choice MLTC',
'Choice Provider',
'Choice Select Health',
'Choice Total',
'Hospice Nurse',
'Hospice Nurse VM',
'Hospice Patient',
'Hospice Patient VM',
'Hospice Referral',
'Hospice Referral Escalation',
'Hospice Referral Escalation VM',
'Hospice Referral VM',
'PIC Cert Client Eng',
'PIC Cert Client Spa',
'PIC Private Intake',
'PIC Private Intake VM',
'RCD Charity Care',
'RCD Charity Care VM',
'RCD Choice VM',
'RCD Escalation',
'RCD Escalation VM',
'RCD Governmental',
'RCD Governmental VM',
'RCD Hospice',
'RCD Hospice VM',
'RCD Managed Care',
'RCD Managed Care VM',
'RCD Partners',
'RCD Partners in Care Billing',
'RCD Partners VM',
'RCD Self Pay',
'RCD Self Pay VM'
)