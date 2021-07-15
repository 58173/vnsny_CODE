
--CREATE MATERIALIZED VIEW CHOICEBI.MV_FCT_MI_CLAIMS as
select 
MI.Claim_id,clm.adjudication_dt,clm.PAID_DT,clm.PAID_AMT,mi.CLAIM_SERVICE_DT,mi.MST_COPAY_AMT,fdb.genericcd_num,DRG.GENERICPRODUCT_FLAG
,mi.CLIENT_HQ_CD,mi.CLIENT_HQ_DESC,mi.sbsb_id,mi.member_id,mi.FIRSTNAME,mi.lastname,mi.dob,mi.MIDDLEINITIAL,mi.CLAIM_ADJ_SRC_CLAIM_ID
,mi.CLAIM_ADJ_TYPE,mi.CLAIM_PRESCRIPTION_NUM,DRG.PROD_NAME,DRG.LABEL_NAME,drg.qty_dispensed,mi.CLAIM_DAYS_SUPPLY,mi.PDE_COVERED_PLAN_PAID_AMT,
GENDER,mi.CLAIM_STATUS,mi.CLAIM_STATUS_CD_ACTUAL,mi.CLAIM_REFILL_NUM,mi.CLAIM_REF_NUM,mi.claim_refill_num,mi.CLAIM_NUM_REFILLS_AUTHORIZED
,mi.PRV_PRESCRIBER_FIRSTNAME,mi.PRV_PRESCRIBER_LASTNAME,mi.PRV_PRESCRIBER_MI,mi.PRV_PRESCRIBER_PHONE,mi.PRV_PRESCRIBER_ADDR1
,mi.PRV_PRESCRIBER_ADDR2,mi.PRV_PRESCRIBER_CITY,mi.PRV_PRESCRIBER_STATE,mi.PRV_PRESCRIBER_ZIP,mi.PRV_PRESCRIBER_FAX,
mi.PRV_VENDOR_NAME,mi.PRV_VENDOR_PHONE,mi.PRV_VENDOR_ADDR1,mi.PRV_VENDOR_ADDR2,mi.PRV_VENDOR_CITY,mi.PRV_VENDOR_STATE
,mi.PRV_VENDOR_ZIP,mi.PRV_VENDOR_PHONE,mi.PRV_VENDOR_FAX,mi.PRV_VENDOR_COUNTY,DRG.PARTD_REJECTCD,DRG.PARTD_REJECTDESC,
CLR.REJECTREASON_SEQNUM,CLR.REJECT_CD,CLR.REJECTCD_DESC,mi.MST_TOTALMEMBERCOSTS,
MI.PDE_COVERED_PLAN_PAID_AMT,
MI.PDE_NONCOVERED_PLAN_PAID_AMT,
MI.CLAIM_CHECK_REF_NUM,
MI.CLAIM_COMPOUND_INGREDIENT_CNT,
MI.CLAIM_DECIMAL_QTY_DISPENSED,
MI.CLAIM_FORMULARY_FLAG,
MI.CLAIM_REF_NUM,
MI.CLAIM_REFILL_NUM,
MI.CLAIM_RXWRITTEN_DT,
DRG.DEA_CD,
DST.DISPENSING_FEE,
DST.PAID_AMT,
DST.TOTAL_COST,
DST.TOTALINGREDIENT_COST,
MI.INV_MEMBERPAYLINE_COST,
MI.MST_COPAY_AMT,
MI.MST_TOTALMEMBERCOSTS,
MI.PRV_VENDOR_CHAIN_ID,
MI.PRV_VENDOR_CHAIN_NAME,
RX.NDC,
RX.NDC_CODE,
RX.TCC_SPECIFIC_CODE,
RX.TCC_SPECIFIC_CODE_DESC,
mi.sbsb_id||mi.CLAIM_PRESCRIPTION_NUM ||mi.CLAIM_SERVICE_DT  KEY,
rank() over ( partition by mi.sbsb_id,mi.CLAIM_PRESCRIPTION_NUM,mi.CLAIM_SERVICE_DT 
       order by MI.CLAIM_ID DESC ) RK,
CASE 
    WHEN rank() over ( partition by mi.sbsb_id,mi.CLAIM_PRESCRIPTION_NUM,mi.CLAIM_SERVICE_DT 
       order by MI.CLAIM_ID DESC ) != 1 THEN 'Yes'
    ELSE 'No'
    END  AS Reversal   
FROM CHOICEBI.FCT_CLAIM_MI MI
LEFT JOIN CHOICE.FCT_CLAIM_MI_DRG@dlake DRG ON MI.CLAIM_ID = DRG.CLAIM_ID AND MI.CLAIM_STATUS = DRG.CLAIM_STATUS
LEFT JOIN CHOICE.FCT_CLAIM_MI_FDB@dlake FDB ON MI.CLAIM_ID = FDB.CLAIM_ID 
LEFT JOIN CHOICE.FCT_CLAIM_MI_CLR@dlake CLR ON MI.CLAIM_ID = CLR.CLAIM_ID
LEFT JOIN CHOICEBI.FCT_CLAIM_UNIVERSE_CURR CLM ON MI.CLAIM_ID = CLM.CLAIM_ID
LEFT JOIN CHOICE.FCT_CLAIM_MI_DST@dlake DST ON MI.CLAIM_ID = DST.CLAIM_ID AND MI.CLAIM_STATUS = DST.CLAIM_STATUS
LEFT JOIN DIM_RX_DRUG_CODES RX ON RX.NDC_CODE = DRG.PRODID
WHERE CLAIM_SERVICE_DT > '28-Feb-2021' AND CLIENT_HQ_CD = 'VNS03'
