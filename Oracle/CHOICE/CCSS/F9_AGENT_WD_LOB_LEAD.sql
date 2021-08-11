
--CREATE VIEW CHOICEBI.VW_F9_AGENT_WD_LOB_LEAD AS
SELECT DISTINCT
       A.AGENT_ID, AGENT_SK ,WD_EMPLOYEE_ID, A.AGENT_EMAIL, AGENT_FIRST_NAME, AGENT_LAST_NAME, AGENT_GROUP,SL.LOB,SL.LEADER,
       MANAGER_ID, MANAGER_NAME, DIRECTOR_ID, DIRECTOR_NAME, COMPANY_ID, COMPANY_NAME, PROGRAM_NAME,COST_CENTER_NAME      
FROM CHOICEBI.MV_DIM_F9_AGENT  A
LEFT JOIN CHOICEBI.MV_DIM_F9_AGENT_SKILL_MAP M ON A.AGENT_SK = M.DL_AGENT_SK
LEFT JOIN CHOICEBI.DIM_F9_SKILL_LOB_LEAD_MAP SL  ON SL.DL_SKILL_SK = M.DL_SKILL_SK
;



