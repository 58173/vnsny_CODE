





### 1.UPDATING PHASE 
##UPDATE mstrapp.prj_phase P 
SELECT P.PROJECT_PHASE_ID,P.PHASE_STATUS_ID,AA.PROJECT_PHASE_ID, AA.PROJECT_PHASE_STATUS_ID
FROM mstrapp.prj_phase P 
  INNER JOIN
   (
        SELECT PROJECT_PHASE_ID, PHASE_STATUS_ID,
                                  CASE WHEN PHASE_STATUS_ID IS NULL AND TASK_STATUS_ID = 10 THEN 10 
                                  ELSE NULL
                                  END PROJECT_PHASE_STATUS_ID
         FROM (
              SELECT A.PROJECT_PHASE_ID, A.PHASE_STATUS_ID, TA.TASK_ID,  TA.TASK_STATUS_ID
                FROM 
                   (
                    SELECT PROJECT_PHASE_ID, PHASE_STATUS_ID
                      FROM mstrapp.prj_phase
                     WHERE PHASE_STATUS_ID IS NULL  
                    ) A
                JOIN  mstrapp.prj_task TA ON TA.PHASE_ID = A.PROJECT_PHASE_ID AND TA.TASK_STATUS_ID IS NOT NULL
                ) B                 
    ) AA 
ON AA.PROJECT_PHASE_ID = P.PROJECT_PHASE_ID
#SET P.PHASE_STATUS_ID = AA.PROJECT_PHASE_STATUS_ID
;
                
                
                
# 2.UPDATING PROJECT
#UPDATE mstrapp.prj_list  PR
SELECT PR.PROJECT_ID,PR.STATUS_ID, AA.PROJECT_ID, AA.PROJECT_STATUS_ID  FROM mstrapp.prj_list PR               
 INNER JOIN
 (
    SELECT PROJECT_ID, CASE WHEN PROJECT_STATUS IS NULL AND TASK_STATUS_ID = 10 THEN 10 
                                  ELSE NULL
                                  END PROJECT_STATUS_ID
    FROM (
          SELECT A.PROJECT_ID, A.STATUS_ID  PROJECT_STATUS, TA.TASK_ID,TA.TASK_STATUS_ID
           FROM   
              (
              SELECT PROJECT_ID, STATUS_ID
              FROM  mstrapp.prj_list
              WHERE  STATUS_ID IS NULL
              ) A
           JOIN mstrapp.prj_task TA ON TA.PROJECT_ID = A.PROJECT_ID AND TA.TASK_STATUS_ID IS NOT NULL
          ) B   
  ) AA 
 ON AA.PROJECT_ID = PR.PROJECT_ID
#SET PR.STATUS_ID = AA. MIN_DATE
;





SELECT * FROM  mstrapp.prj_phase;
SELECT * FROM mstrapp.prj_lu_status;
SELECT * FROM mstrapp.prj_list_prt;

## 3.UPDATING PARENT PROJECT
##UPDATE  mstrapp.prj_list_prt     PRT
SELECT PRT.PARENT_PROJECT_ID,PRT.STATUS_ID, AA.PARENT_PROJECT_ID, AA.PARENT_PROJECT_STATUS_ID  FROM mstrapp.prj_list_prt     PRT
 INNER JOIN
 (
   SELECT PARENT_PROJECT_ID, CASE WHEN PARENT_PROJECT_STATUS IS NULL AND PROJECT_STATUS = 10 THEN 10 
                                  ELSE NULL
                                  END PARENT_PROJECT_STATUS_ID
    FROM (
        SELECT A.PARENT_PROJECT_ID, A.STATUS_ID  PARENT_PROJECT_STATUS, PROJECT_ID, PR.STATUS_ID  PROJECT_STATUS
          FROM 
             (
             SELECT PARENT_PROJECT_ID, STATUS_ID
              FROM  mstrapp.prj_list_prt 
              WHERE STATUS_ID IS NULL 
             ) A
          JOIN mstrapp.prj_list PR ON PR.PARENT_PROJECT_ID = A.PARENT_PROJECT_ID AND PR.STATUS_ID  IS NOT NULL  
         ) B
  ) AA 
  ON AA.PARENT_PROJECT_ID = PRT.PARENT_PROJECT_ID
 SET PRT.STATUS_ID = AA.PARENT_PROJECT_STATUS_ID
END;        
          
          