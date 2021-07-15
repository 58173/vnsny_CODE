

---DIM_AGENT
SELECT 
    distinct
    B.staff_id, B.first_name,B.last_name, 
    B.staff_status, B.staff_type,B.employee_type, 
    B.company, B.company_id, B.cost_center, B.COST_CENTER_ID,
    B.job_title, B.job_family, B.region, 
    B.manager_id, B.STAFFS_MANAGER,
    C.first_name  manager_first_name, C.last_name  manager_last_name,
    A.agent_first_name, A.agent_last_name, A.agent_id, A.agent_start_date --,A.skill
FROM DW_OWNER.F9_AGENT A 
  JOIN DW_OWNER.CVX_STAFF B ON (UPPER(B.STAFFS_WORK_EMAIL) = UPPER(A.AGENT_EMAIL)) 
  JOIN DW_OWNER.CVX_STAFF C ON (B.MANAGER_ID = C.STAFF_ID)
WHERE TRIM(UPPER(A.AGENT_EMAIL)) LIKE '%VNSNY.ORG' AND B.STAFFS_WORK_EMAIL IS NOT NULL    
      ;
    
