
    






CREATE PROCEDURE `mstrapp`.`proc_update_start_date`()
BEGIN
# 1.UPDATING PHASE      
UPDATE mstrapp.prj_phase P
 INNER JOIN        
 (
    SELECT PROJECT_PHASE_ID, MIN(TASK_START_DATE)  MIN_DATE 
     FROM (
         SELECT A.PROJECT_PHASE_ID, A.ACTUAL_START_DATE, TASK_ID, TA.ACTUAL_START_DATE  TASK_START_DATE
          FROM 
             (
             SELECT PROJECT_PHASE_ID, PHASE_STATUS_ID,ACTUAL_START_DATE 
             FROM mstrapp.prj_phase
             WHERE ACTUAL_START_DATE IS NULL AND PHASE_STATUS_ID IN (10,20)
             ) A
          JOIN  mstrapp.prj_task TA ON TA.PHASE_ID = A.PROJECT_PHASE_ID AND TA.ACTUAL_START_DATE IS NOT NULL
          ) B 
     GROUP BY PROJECT_PHASE_ID
 ) AA    
 ON AA.PROJECT_PHASE_ID = P.PROJECT_PHASE_ID
SET P.ACTUAL_START_DATE = AA.MIN_DATE 
WHERE P.ACTUAL_START_DATE IS NULL AND P.PHASE_STATUS_ID IN (10,20);    
    
# 2.UPDATING PROJECT
UPDATE mstrapp.prj_list PR
 INNER JOIN
 (
    SELECT PROJECT_ID, MIN(TASK_START_DATE)  MIN_DATE
    FROM (
          SELECT A.PROJECT_ID, A.ACTUAL_START_DATE, TASK_ID, TA.ACTUAL_START_DATE TASK_START_DATE
           FROM   
              (
              SELECT PROJECT_ID, STATUS_ID, ACTUAL_START_DATE 
              FROM  mstrapp.prj_list
              WHERE ACTUAL_START_DATE IS NULL  AND  STATUS_ID IN (10,20)
              ) A
           JOIN mstrapp.prj_task TA ON TA.PROJECT_ID = A.PROJECT_ID AND TA.ACTUAL_START_DATE IS NOT NULL
          ) B 
      GROUP BY  PROJECT_ID
  ) AA 
 ON AA.PROJECT_ID = PR.PROJECT_ID
SET PR.ACTUAL_START_DATE = AA. MIN_DATE
WHERE PR.ACTUAL_START_DATE IS NULL AND PR.STATUS_ID IN (10,20);      
                              
# 3. UPDATING PARENT PROJECT
UPDATE  mstrapp.prj_list_prt     PRT
 INNER JOIN
 (
     SELECT PARENT_PROJECT_ID, MIN(PROJECT_START_DATE)  MIN_DATE
     FROM ( 
          SELECT A.PARENT_PROJECT_ID, A.ACTUAL_START_DATE, PROJECT_ID, PR.ACTUAL_START_DATE   PROJECT_START_DATE
          FROM 
             (
              SELECT PARENT_PROJECT_ID, STATUS_ID, ACTUAL_START_DATE 
              FROM  mstrapp.prj_list_prt 
              WHERE ACTUAL_START_DATE IS NULL  AND  STATUS_ID IN (10,20)
              ) A
           JOIN mstrapp.prj_list PR ON PR.PARENT_PROJECT_ID = A.PARENT_PROJECT_ID AND PR.ACTUAL_START_DATE  IS NOT NULL
           ) B
      GROUP BY  PARENT_PROJECT_ID    
 ) AA 
 ON AA.PARENT_PROJECT_ID = PRT.PARENT_PROJECT_ID
 SET PRT.ACTUAL_START_DATE = AA.MIN_DATE
 WHERE PRT.ACTUAL_START_DATE IS NULL AND PRT.STATUS_ID IN (10,20);
end;


call mstrapp.proc_update_start_date ();