

CREATE MATERIALIZED VIEW CHOICEBI.DIM_F9_SKILL_LOB_LEAD_MAP 
AS
SELECT DISTINCT C.DL_SKILL_SK, A.* FROM
(
SELECT SKILL, LOB, LEADER, CHANGED_ON, CHANGED_FROM
FROM CHOICEBI.F9_SKILL_LOB_LEADER_MAP
WHERE SKILL IS NOT NULL
) A
JOIN MV_DIM_F9_SKILL C ON (A.SKILL = C.SKILL);

GRANT SELECT ON CHOICEBI.DIM_F9_SKILL_LOB_LEAD_MAP TO CHOICEBI_RO_NEW;
GRANT SELECT ON CHOICEBI.DIM_F9_SKILL_LOB_LEAD_MAP TO MSTRSTG;


