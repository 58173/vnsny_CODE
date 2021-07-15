






 SELECT  PARENT_PROJECT_ID, MIN(TASK_START_DATE) 
FROM (SELECT A.parent_project_id, TASK_ID, TA.ACTUAL_START_DATE   TASK_START_DATE
     FROM
    (
select parent_project_id, STATUS_ID
from mstrapp.prj_list
where ACTUAL_START_DATE IS NULL AND STATUS_ID IN (10,20)  ## 10:In Progress; 20:Completed
      ) A
JOIN prj_task TA ON TA.parent_project_id  = A.parent_project_id
AND TA.ACTUAL_START_DATE IS NOT NULL
) AS A1
group by PARENT_PROJECT_ID;





UPDATE mstrapp.prj_list_testing
INNER JOIN 
(SELECT  PARENT_PROJECT_ID, MIN(TASK_START_DATE) MIN_DATE
FROM (SELECT A.parent_project_id, TASK_ID, TA.ACTUAL_START_DATE   TASK_START_DATE
     FROM
    (
select parent_project_id, STATUS_ID
from mstrapp.prj_list_testing 
where ACTUAL_START_DATE IS NULL AND STATUS_ID IN (10,20)  ## 10:In Progress; 20:Completed
      ) A
JOIN prj_task TA ON TA.parent_project_id  = A.parent_project_id
AND TA.ACTUAL_START_DATE IS NOT NULL
) AS A1
group by PARENT_PROJECT_ID ) B 
ON B.parent_project_id = prj_list_testing.PARENT_PROJECT_ID
SET prj_list_testing.ACTUAL_START_DATE = B.MIN_DATE 
WHERE prj_list_testing.ACTUAL_START_DATE IS NULL AND prj_list_testing.STATUS_ID IN (10,20);




SELECT prj_list_testing.PARENT_PROJECT_ID,prj_list_testing.ACTUAL_START_DATE, B.parent_project_id,B.MIN_DATE
FROM prj_list_testing
INNER JOIN 
(SELECT  PARENT_PROJECT_ID, MIN(TASK_START_DATE) MIN_DATE
FROM (SELECT A.parent_project_id, TASK_ID, TA.ACTUAL_START_DATE   TASK_START_DATE
     FROM
    (
select parent_project_id, STATUS_ID
from mstrapp.prj_list_testing
where ACTUAL_START_DATE IS NULL AND STATUS_ID IN (10,20)  ## 10:In Progress; 20:Completed
      ) A
JOIN prj_task TA ON TA.parent_project_id  = A.parent_project_id
AND TA.ACTUAL_START_DATE IS NOT NULL
) AS A1
group by PARENT_PROJECT_ID ) B 
ON B.parent_project_id = prj_list_testing.PARENT_PROJECT_ID
WHERE prj_list_testing.ACTUAL_START_DATE IS NULL AND prj_list_testing.STATUS_ID IN (10,20);




select * from mstrapp.prj_list_testing