



--- VPIN DIFFERE: 1
select M.SUBSCRIBER_ID,F.SUBSCRIBER_ID, M.MONTH_ID, F.REPORTING_MONTH,M.PROGRAM, F.PROGRAM, M.VPIN, F.VPIN     
from CHOICEBI.FACT_MEMBER_MONTH M
join CHOICE.FACT_MEMBER_MONTH@dlake F on (M.SUBSCRIBER_ID = F.SUBSCRIBER_ID AND M.MONTH_ID = F.REPORTING_MONTH and M.PROGRAM = F.PROGRAM )
where M.VPIN != F.VPIN; -- SUBSCRIBER_ID = V80193609


---RACE DIFFERE: 572
select M.SUBSCRIBER_ID,F.SUBSCRIBER_ID, M.MONTH_ID, F.REPORTING_MONTH, M.PROGRAM,F.PROGRAM, M.VPIN, F.VPIN,
        M.RACE, F.RACE, M.LTP_IND, F.LTP_IND, M.REFERRAL_DATE, F.REFERRAL_DATE
from CHOICEBI.FACT_MEMBER_MONTH M
join CHOICE.FACT_MEMBER_MONTH@dlake F on (M.SUBSCRIBER_ID = F.SUBSCRIBER_ID AND M.MONTH_ID = F.REPORTING_MONTH and M.PROGRAM = F.PROGRAM )
where M.RACE != F.RACE;


---BIRTH DATE DIFFERE:
select M.SUBSCRIBER_ID,F.SUBSCRIBER_ID, M.MONTH_ID, F.REPORTING_MONTH, M.PROGRAM,F.PROGRAM, M.VPIN, F.VPIN,
       M.LTP_IND, F.LTP_IND, M.REFERRAL_DATE, F.REFERRAL_DATE,M.DATE_OF_BIRTH, F.DOB
from CHOICEBI.FACT_MEMBER_MONTH M
join CHOICE.FACT_MEMBER_MONTH@dlake F on (M.SUBSCRIBER_ID = F.SUBSCRIBER_ID AND M.MONTH_ID = F.REPORTING_MONTH and M.PROGRAM = F.PROGRAM )
where M.SUBSCRIBER_ID = 'V60010115';


---LTP_IND
select M.SUBSCRIBER_ID,F.SUBSCRIBER_ID, M.MONTH_ID, F.REPORTING_MONTH, M.PROGRAM,F.PROGRAM, M.VPIN, F.VPIN,
        M.RACE, F.RACE, M.LTP_IND, F.LTP_IND, M.REFERRAL_DATE, F.REFERRAL_DATE
from CHOICEBI.FACT_MEMBER_MONTH M
join CHOICE.FACT_MEMBER_MONTH@dlake F on (M.SUBSCRIBER_ID = F.SUBSCRIBER_ID AND M.MONTH_ID = F.REPORTING_MONTH and M.PROGRAM = F.PROGRAM )
where M.LTP_IND!= F.LTP_IND;




--SSN DIFFER: 1000 PATIENTS
select M.SUBSCRIBER_ID,F.SUBSCRIBER_ID, M.MONTH_ID, F.REPORTING_MONTH, M.PROGRAM,F.PROGRAM, M.VPIN, F.VPIN,
       M.LTP_IND, F.LTP_IND, M.REFERRAL_DATE, F.REFERRAL_DATE, M.SSN, F.SSN
from CHOICEBI.FACT_MEMBER_MONTH M
join CHOICE.FACT_MEMBER_MONTH@dlake F on (M.SUBSCRIBER_ID = F.SUBSCRIBER_ID AND M.MONTH_ID = F.REPORTING_MONTH and M.PROGRAM = F.PROGRAM )
where M.SUBSCRIBER_ID IN ('V60010115','V70000045', 'V70000056');


 SELECT DISTINCT SUBSCRIBER_ID FROM (
SELECT SUBSCRIBER_ID, MONTH_ID, PROGRAM, MEMBER_ID,SSN
    -- VPIN, RACE, DATE_OF_BIRTH
FROM CHOICEBI.FACT_MEMBER_MONTH
MINUS
select SUBSCRIBER_ID, REPORTING_MONTH, PROGRAM, MEMBER_ID,SSN
    --  VPIN, RACE, DOB
FROM CHOICE.FACT_MEMBER_MONTH@DLAKE
);


---NAME DIFFERE:
select M.SUBSCRIBER_ID,F.SUBSCRIBER_ID, M.MONTH_ID, F.REPORTING_MONTH, M.PROGRAM,F.PROGRAM, M.VPIN, F.VPIN,
       M.LTP_IND, F.LTP_IND, M.REFERRAL_DATE, F.REFERRAL_DATE, M.SEX, F.GENDER,
       F.SSN,M.FIRST_NAME,F.FIRST_NAME, M.LAST_NAME, F.LAST_NAME
from CHOICEBI.FACT_MEMBER_MONTH M
join CHOICE.FACT_MEMBER_MONTH@dlake F on (M.SUBSCRIBER_ID = F.SUBSCRIBER_ID AND M.MONTH_ID = F.REPORTING_MONTH and M.PROGRAM = F.PROGRAM )
WHERE M.SUBSCRIBER_ID ='V60010115';

---SEX DIFFER:
select M.SUBSCRIBER_ID,F.SUBSCRIBER_ID, M.MONTH_ID, F.REPORTING_MONTH, M.PROGRAM,F.PROGRAM, M.VPIN, F.VPIN,
       M.SEX, F.GENDER
from CHOICEBI.FACT_MEMBER_MONTH M
join CHOICE.FACT_MEMBER_MONTH@dlake F on (M.SUBSCRIBER_ID = F.SUBSCRIBER_ID AND M.MONTH_ID = F.REPORTING_MONTH and M.PROGRAM = F.PROGRAM )
WHERE M.SUBSCRIBER_ID ='V60010115';



--DISENROLLMENT_DATE:
select M.SUBSCRIBER_ID,F.SUBSCRIBER_ID,M.VPIN, F.VPIN,M.RACE, F.RACE, M.SSN, F.SSN,M.FIRST_NAME,F.FIRST_NAME, M.LAST_NAME, F.LAST_NAME,
       M.SEX, F.GENDER, M.ENROLLMENT_DATE, F.ORIG_ENROLLMENT_START_DT, 
       M.PROGRAM, F.PROGRAM,M.LTP_IND, F.LTP_IND, M.REFERRAL_DATE, F.REFERRAL_DATE,
       M.ICS_IND, F.ICS_IND, M.UAS_RECORD_ID, F.UAS_RECORD_ID, M.CARE_STAFF_NAME,  
       M.ENROLLMENT_DATE,  F.ORIG_ENROLLMENT_START_DT 
from CHOICEBI.FACT_MEMBER_MONTH M
join CHOICE.FACT_MEMBER_MONTH@dlake F on (M.SUBSCRIBER_ID = F.SUBSCRIBER_ID AND M.MONTH_ID = F.REPORTING_MONTH and M.PROGRAM = F.PROGRAM )
WHERE M.ENROLLMENT_DATE !=F.ORIG_ENROLLMENT_START_DT;
;


---BENEFIT_REGIN:  142,519RECORDS
select M.SUBSCRIBER_ID,F.SUBSCRIBER_ID, M.MONTH_ID, F.REPORTING_MONTH, M.PROGRAM,F.PROGRAM, M.VPIN, F.VPIN,
       M.SSN, F.SSN,M.FIRST_NAME,F.FIRST_NAME, M.LAST_NAME, F.LAST_NAME,
       M.BENEFIT_REGION , F.BENEFIT_REGION ,
       M.REFERRAL_DATE, F.REFERRAL_DATE
from CHOICEBI.FACT_MEMBER_MONTH M
join CHOICE.FACT_MEMBER_MONTH@dlake F on (M.SUBSCRIBER_ID = F.SUBSCRIBER_ID AND M.MONTH_ID = F.REPORTING_MONTH and M.PROGRAM = F.PROGRAM )
WHERE M.SUBSCRIBER_ID IN ('V60001002','V70000265',
'V70000279',
'V70000301',
'V70000326'
);


SELECT DISTINCT SUBSCRIBER_ID FROM (
SELECT SUBSCRIBER_ID, MONTH_ID, PROGRAM, MEMBER_ID,AGE, DL_LOB_ID, LOB_ID, LINE_OF_BUSINESS, DISENROLLMENT_DATE, ENROLLED_FLAG, DISENROLLED_FLAG, BENEFIT_REGION 
    -- VPIN, RACE, DATE_OF_BIRTH, SSN, FIRST_NAME, LAST_NAME, SEX, ENROLLMENT_DATE, REFERRAL_DATE ,BENEFIT_REGION 
FROM CHOICEBI.FACT_MEMBER_MONTH
MINUS
select SUBSCRIBER_ID, REPORTING_MONTH, PROGRAM, MEMBER_ID, AGE, DL_LOB_ID, DL_LOB_ID, LOB , DISENROLLMENT_DATE, NEW_ENR_IND, NEW_DISENR_IND, BENEFIT_REGION 
    --  VPIN, RACE, DOB, SSN, FIRST_NAME, LAST_NAME, GENDER, ORIG_ENROLLMENT_START_DT, REFERRAL_DATE, BENEFIT_REGION 
FROM CHOICE.FACT_MEMBER_MONTH@DLAKE);




---MEDICAID  MEDICARE No. :
select M.SUBSCRIBER_ID,F.SUBSCRIBER_ID, M.MONTH_ID, F.REPORTING_MONTH, M.PROGRAM,F.PROGRAM, M.VPIN, F.VPIN,
       M.MEDICAID_NUM, F.MEDICAID, M.MEDICARE_NUM, F.CURRENT_MEDICARE
from CHOICEBI.FACT_MEMBER_MONTH M
join CHOICE.FACT_MEMBER_MONTH@dlake F on (M.SUBSCRIBER_ID = F.SUBSCRIBER_ID AND M.MONTH_ID = F.REPORTING_MONTH and M.PROGRAM = F.PROGRAM )
WHERE M.MEDICAID_NUM !=F.MEDICAID OR 
      M.MEDICARE_NUM != F.CURRENT_MEDICARE
;


---DL_COUNTY_SK:
select  M.SUBSCRIBER_ID,F.SUBSCRIBER_ID, M.MONTH_ID, F.REPORTING_MONTH, M.PROGRAM,F.PROGRAM, M.VPIN, F.VPIN,
       M.REFERRAL_DATE, F.REFERRAL_DATE,M.DL_COUNTY_SK, F.DL_COUNTY_SK
from CHOICEBI.FACT_MEMBER_MONTH M
join CHOICE.FACT_MEMBER_MONTH@dlake F on (M.SUBSCRIBER_ID = F.SUBSCRIBER_ID AND M.MONTH_ID = F.REPORTING_MONTH and M.PROGRAM = F.PROGRAM )
WHERE --M.DL_COUNTY_SK != F.DL_COUNTY_SK
    m.SUBSCRIBER_ID in ('V80008771', 'V80032704', 'V80024737','V80184346', 'V80190610','V88002484','V80210372','V80211852','V88000919','V88002484')
;

---referral_date
select  M.SUBSCRIBER_ID,F.SUBSCRIBER_ID, M.MONTH_ID, F.REPORTING_MONTH, M.PROGRAM,F.PROGRAM, M.VPIN, F.VPIN,
       M.REFERRAL_DATE, F.REFERRAL_DATE
from CHOICEBI.FACT_MEMBER_MONTH M
join CHOICE.FACT_MEMBER_MONTH@dlake F on (M.SUBSCRIBER_ID = F.SUBSCRIBER_ID AND M.MONTH_ID = F.REPORTING_MONTH and M.PROGRAM = F.PROGRAM )
WHERE M.REFERRAL_DATE != F.REFERRAL_DATE
;

