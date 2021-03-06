/*    ==Scripting Parameters==

    Source Server Version : SQL Server 2017 (14.0.3370)
    Source Database Engine Edition : Microsoft SQL Server Enterprise Edition
    Source Database Engine Type : Standalone SQL Server

    Target Server Version : SQL Server 2017
    Target Database Engine Edition : Microsoft SQL Server Standard Edition
    Target Database Engine Type : Standalone SQL Server
*/
USE [VNSNY_BI]
GO
/****** Object:  View [dbo].[fact_medicare_pdgm]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO











create view [dbo].[fact_medicare_pdgm] as
SELECT DISTINCT
		ce.epi_id,
		pp.pp_id,
		pp.pp_periodnumber as period_seq,
		concat('P',pp.pp_periodnumber) as period_seq_desc,
		CASE WHEN getdate() between pp.pp_startDate and pp.pp_endDate THEN 'Current' 
			 WHEN pp.pp_startDate > getdate() THEN 'Future' ELSE 'Ended' END as period_is_current,
		cefs_id,
		pp.pp_ceoid,
		ce.epi_socdate,
		ce.epi_branchcode,
		PatientName = ISNULL(ce.epi_lastname,'') + ', ' + ISNULL(ce.epi_firstname,'') + ' ' + ISNULL(ce.epi_mi,''),							
		ce.epi_mrnum,
		ce.epi_status,
		pp_startDate = CONVERT(VARCHAR(10),pp.pp_startDate,101),
		pp_endDate = CONVERT(VARCHAR(10),pp.pp_endDate,101) ,
		[pp_initialHipps]
        ,[pp_initialPayment]
        ,[pp_currentHipps]
        ,[pp_currentPayment]
        ,[pp_eraHipps]
		,pp_reimbursementType = CASE
									WHEN pp.pp_reimbursementType = 'L' THEN 'LUPA'
									WHEN pp.pp_reimbursementType = 'O' THEN 'Outlier'
									ELSE 'Standard'
								END,
		pp_rapBilled = CASE WHEN pp.pp_rapBilled = 1 THEN 'Y' ELSE 'N' END,
		pp_claimBilled = CASE WHEN pp.pp_claimBilled = 1 THEN 'Y' ELSE 'N' END,
		ph.ph_admissionSource,
		ph.ph_clinicalGrouping,
		CASE WHEN ph.ph_functionalImpairmentLevel = 'High' THEN 1
												WHEN ph.ph_functionalImpairmentLevel = 'Medium' THEN 2
												WHEN ph.ph_functionalImpairmentLevel = 'Low' THEN 3
												ELSE 4
												END as ph_functionalImpairmentLevelSort ,
		ph.ph_functionalImpairmentLevel,
		ph.ph_comorbidityAdjustment,
		ph.ph_timing,
		ph.ph_caseMixWeight,
		ph.ph_lupaThreshold,
		mh.mh_wageIndexValue,
        mh.mh_adjustedCalculatedPayment, 
		mhs.mhs_totalCost, 
		mhs.mhs_grossMargin, 
		mhs.mhs_grossMarginPct, 
		mhs.mhs_supplyCost,
		mhs.mhs_tripCost,
		mhs.mhs_laborCost,
		cefs_ps,
		cefs_psid, 
		cefs_pstid, 
		cefs_ptid,
		ps.ps_id,
		pp_CurrentHipps_clinicalGrouping = SUBSTRING([pp_currentHipps], 2,1)
	FROM hchb.dbo.client_episodes AS ce	 
	JOIN hchb.dbo.client_episode_fs AS cefs ON cefs.cefs_epiid = ce.epi_id AND cefs.cefs_active = 'Y' AND cefs.cefs_ps <> 'I'
	JOIN hchb.PDGM.PDGM_PERIOD AS pp ON pp.pp_cefsId =  cefs.cefs_id AND pp.pp_deleted = 0 
	LEFT JOIN hchb.pdgm.margin_history AS mh ON pp.pp_mhId = mh.mh_id  
	LEFT JOIN hchb.pdgm.margin_history_supplemental AS mhs ON mhs.mhs_mhid=mh.mh_id
	LEFT JOIN hchb.dbo.PDGM_HIPPS AS ph ON ph.ph_hipps = pp.pp_currentHipps AND ph.ph_rpid = mh.mh_rpid	 
	JOIN HCHB.dbo.PAYOR_SOURCES ps ON ps.ps_id = cefs.cefs_psid
	WHERE ce.epi_status <> 'NON-ADMIT' 
	and cefs.cefs_ptid in ( 57, 25007) --PDGM and PDGM Like payers
	--and cefs.cefs_ptid in ( 57) --PDGM only
	AND ce.epi_slid  = 1 
	;


GO
/****** Object:  View [dbo].[VW_CLIENT_ORDER_CALENDAR_BY_WEEK]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[VW_CLIENT_ORDER_CALENDAR_BY_WEEK] AS

/*
		select 
			ISNULL(pdgm.pp_id,-1) as pp_id, 
			cefw.cefw_id, 
			cefw.cefw_startdate, 
			cefw.cefw_enddate, 
			cefw.cefw_wkno,
			sc.sc_code,
			sc.sc_desc,
			sc.sc_discipline,
			sc.sc_visittype,
			coc.*
		from
			HCHB.dbo.Client_Order_Calendar as coc with(NOLOCK)
			JOIN HCHB.dbo.ServiceCodes as sc with(NOLOCK) on sc.sc_id = coc.coc_scid
			JOIN HCHB.dbo.CLIENT_EPISODE_FREQUENCY_WEEKS cefw on coc.coc_epiid = cefw.cefw_epiid
			and coc.coc_calendar_visitdate between cefw.cefw_startdate and cefw.cefw_enddate
			left join VNSNY_BI.[dbo].[fact_medicare_pdgm] pdgm on cefw.cefw_epiid = pdgm.[epi_id]
			and coc.coc_calendar_visitdate between pdgm.[pp_startDate] and pdgm.[pp_endDate] and
where pp_id <> -1
*/

		select 
			ISNULL(pdgm.pp_id,-1) as pp_id, 
			cefw.cefw_id, 
			cefw.cefw_startdate, 
			cefw.cefw_enddate, 
			cefw.cefw_wkno,
			sc.sc_code,
			sc.sc_desc,
			sc.sc_discipline,
			sc.sc_visittype,
			sc.sc_id,
			coc.calendar_id as coc_id,
			coc.*
		from
			HCHB.dbo.CLIENT_CALENDAR as coc with(NOLOCK)
			JOIN HCHB.dbo.ServiceCodes as sc with(NOLOCK) on sc.sc_id = coc.calendar_scid
			JOIN HCHB.dbo.CLIENT_EPISODE_FREQUENCY_WEEKS cefw on coc.calendar_epiid = cefw.cefw_epiid
			and coc.calendar_VisitDate between cefw.cefw_startdate and cefw.cefw_enddate
			left join VNSNY_BI.[dbo].[fact_medicare_pdgm] pdgm on cefw.cefw_epiid = pdgm.[epi_id]
			and coc.calendar_VisitDate between pdgm.[pp_startDate] and pdgm.[pp_endDate]
where pp_id <> -1

GO
/****** Object:  View [dbo].[VW_BRANCHES]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


	create view [dbo].[VW_BRANCHES] AS
	select B.BRANCH_CODE, B.BRANCH_NAME, REPLACE(B.BRANCH_CODE, 'COR', 'ZZZ') AS SORT_ORDER
	from HCHB.dbo.BRANCHES B
	UNION
	SELECT 'N/A', 'UNASSIGNED', 'ZZZZ'
GO
/****** Object:  View [dbo].[VW_BRANCH_GROUPS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create view [dbo].[VW_BRANCH_GROUPS] as
	select *, 
	CASE BG_ID WHEN 25049 THEN 10 
		 WHEN 25048 THEN 20 
		 WHEN 25050 THEN 30 
		 WHEN 25054 THEN 40 
		 WHEN 25051 THEN 50 
		 WHEN 25052 THEN 60
		 WHEN 25053 THEN 70
		 ELSE BG_ID
	END AS SORT_ORDER
	FROM HCHB.dbo.BRANCH_GROUPS where BG_ID not in (25060)
	union 
	select -1, 'TEMPLATE', 'Y', '2019-02-19', '2019-02-19', 100000
	union 
	select -2, 'UNASSIGNED', 'Y', '2019-02-19', '2019-02-19', 200000
GO
/****** Object:  View [dbo].[VW_BRANCH_TEAMS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[VW_BRANCH_TEAMS] AS
select * from HCHB.dbo.BRANCH_TEAMS
union 
select 'N/A', 17, 177, 'Y', '2019-02-19', '2019-02-19'
GO
/****** Object:  View [dbo].[VW_BRANCH_GROUP_BRANCHES]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	CREATE VIEW [dbo].[VW_BRANCH_GROUP_BRANCHES] AS
	select * from HCHB.dbo.BRANCH_GROUP_BRANCHES
	UNION
	select -1, 'COR', '2019-02-19', '2019-02-19'
	UNION
	select -2, 'N/A', '2019-02-19', '2019-02-19'
GO
/****** Object:  View [dbo].[VW_CHHA_BRANCH_GROUPS_TEAMS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[VW_CHHA_BRANCH_GROUPS_TEAMS]
AS 
SELECT  ISNULL(BGB.BGB_BGID,-1) BGB_BGID, 
		ISNULL(BG.BG_ID,-1) BG_ID, 
		B.BRANCH_CODE, 
		T.TEAM_ID,
		CASE WHEN LEFT(B.BRANCH_CODE, 1)='H' THEN 'HOSPICE' WHEN B.BRANCH_CODE = 'COR' THEN 'CORPORATE' ELSE 'CHHA' END AS COMPANY
FROM VNSNY_BI.dbo.VW_BRANCH_GROUP_BRANCHES BGB
JOIN VNSNY_BI.dbo.VW_BRANCH_GROUPS BG on BGB.bgb_branchcode = BG.bg_description
RIGHT JOIN VNSNY_BI.dbo.VW_BRANCHES B on BGB.BGB_BRANCHCODE = B.BRANCH_CODE
JOIN VNSNY_BI.dbo.VW_BRANCH_TEAMS BT on BT.bteam_branchcode = B.branch_code
JOIN HCHB.dbo.TEAMS T on BT.bteam_teamid = T.team_id
WHERE (BGB.BGB_BGID in (25048, 25049, 25050, 25051, 25052, 25053, 25054, --CHHA
						-1, -2,
						25043, 25044, 25045, 25046, 25047 --HOSPICE
						)
	  and  T.TEAM_ID not in (1,2)) or (B.BRANCH_CODE in ('COR','N/A')
	  )
UNION SELECT -1,-1,'-1',-1,'-1'
GO
/****** Object:  View [dbo].[VW_CHHA_BRANCH_GROUPS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [dbo].[VW_CHHA_BRANCH_GROUPS]
AS 
SELECT  distinct [BGB_BGID], [BG_ID], [BRANCH_CODE], [COMPANY] from  [dbo].[VW_CHHA_BRANCH_GROUPS_TEAMS]
GO
/****** Object:  View [dbo].[VW_CLIENT_DIAGNOSES_AND_PROCEDURES_485]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[VW_CLIENT_DIAGNOSES_AND_PROCEDURES_485]
AS
SELECT * FROM HCHB.dbo.CLIENT_DIAGNOSES_AND_PROCEDURES WHERE cdp_DiagnosisProcedureTypeSourceId = 3 --POC/485 order
GO
/****** Object:  View [dbo].[pathway_patient_list]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[pathway_patient_list] as
select * from
(
select 
	distinct a11.epi_id epi_id,
	a14.fa_parentcompany fa_parentcompany,
	CAST(a11.epi_socdate as DATE) epi_socdate,
	CAST(a11.epi_startofepisode as DATE) epi_startofepisode,
	cast(a11.epi_endofepisode as date) epi_endofepisode,
	CAST(a11.epi_dateofreferral as DATE) epi_DateOfReferral_Date,
	CAST(a11.epi_dischargedate as DATE) epi_dischargedate,
	a11.epi_status epi_status,
	a11.epi_paid pa_id,
	a13.pa_firstname pa_firstname,
	a13.pa_lastname pa_lastname,
	a11.epi_agid,
	a12.cdp_diICDCode ICD_Code,
	a15.ICD_Description ICD_Description,
	233 ep_ccid	--Care Category id for all heart patient interventions
	,a12.cdp_SortOrder
	,a12.cdp_diICDCode 
	,case when a12.cdp_SortOrder = 10 then 1 else 0 end Primary_diag_flag
	,row_number() over (partition by a11.epi_paid, a11.epi_id order by a11.epi_id,a12.cdp_SortOrder) seq
from HCHB.dbo.CLIENT_EPISODES_ALL a11
	left outer join VNSNY_BI.dbo.VW_CLIENT_DIAGNOSES_AND_PROCEDURES_485 a12 on (a11.epi_id = a12.cdp_epiid)
	left outer join HCHB.dbo.CLIENTS_ALL a13 on (a11.epi_paid = a13.pa_id)
	left outer join HCHB.dbo.FACILITIES a14 on (a11.epi_referralfaid = a14.FA_ID)
	left outer join SHARED_REFERENCE.icd.ICD_CODES a15 on (a12.cdp_diICDCode = a15.ICD_Code and a12.cdp_diICDTypeCode = a15.ICD_TypeCode)
where 1=1
	and a11.epi_slid in (1)
	and a12.cdp_diICDCode like '%I50%'
) dat 
where seq=1
GO
/****** Object:  View [dbo].[VW_Intervention_Goals_by_client]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE view [dbo].[VW_Intervention_Goals_by_client] as 
with POC_Order as 
(
	select * from
	(
	select 
		cea.epi_id,
		c485o_c485id, 
		c485_oid, 
		c485_calculatedfreq,
		c485_orderfreq, 
		safety_measures, 
		nutritional_req,
		c485o_otext,
		c485o_seqno,
		row_number() over (partition by epi_paid, cea.epi_id order by cea.epi_id) seq
	from hchb.dbo.CLIENT_EPISODES_ALL	cea
		join hchb.dbo.CLIENT_485_ALL c485a on cea.epi_id = c485a.epi_id
		join hchb.dbo.CLIENT_485_ORDERS c485o on c485o_c485id = c485_id
	 where 
		upper(c485o_otext) like upper('Skilled Nursing to provide evaluation of heart failure symptoms, patient/caregiver education of symptoms and criteria for escalation,%')
	) poc_order1
	where seq =1
),
dat as 
(
select 
		ep.pa_id
		,ep.ICD_Code
		,ep.ICD_Description
		,ep.epi_id				epi_id
		,ep.epi_dischargedate	epi_dischargedate
		,ep.epi_DateOfReferral_Date
		,ep.epi_socdate			SOCDate
		,ep.epi_startofepisode	SOEDate
		,ep.epi_endofepisode	EOEDate
		,ep.cdp_SortOrder
		,ep.Primary_diag_flag
		,ep.cdp_diICDCode
		,poc.c485_oid
		,poc.c485_calculatedfreq
		,poc.c485_orderfreq
		,poc.c485o_otext poc_order
		,poc.c485o_seqno
		--,o_desc
		--,o_orderfreq
		,CEV_ID
		,CEV_VISITDATE		cev_visitdate
		,intr.sc_code				VisitType
		,intr.sc_discipline		discipline
		,ep.epi_agid			CaseManager
		,intr.cev_agid			Agent
		,intr.cevi_id
		,intr.cevi_seqno		IntvSeq	
		,intr.cevi_fid
		,intr.cevi_qid
		,intr.Intervention	Intervention
		,intr.cevi_answer		
		,isnull(intr.cc_id,233)				care_category_ID
		,isnull(intr.cc_desc,'CARDIOVASCULAR')	care_category
		,intr.cevg_id
		,intr.cevg_Fid	
		,intr.cevg_answer
		,GoalMet = Case WHEN intr.cevg_aid = 2 THEN 1 ELSE 0 END
		--,frm.F_NAME Intv_Form
		,intr.ec_desc
		,intr.goal
		,row_number() over (partition by ep.pa_id, ep.epi_id, intr.cevi_fid,intr.cevi_qid order by Case WHEN intr.cevg_aid = 2 THEN 1 ELSE 0 END desc) goal_seq
	from 
		vnsny_bi.dbo.pathway_patient_list ep
		left join poc_order poc on (ep.epi_id= poc.epi_id) 
		left JOIN 
		(
			select
				 sc_code
				--,o.o_desc
				--,o.o_orderfreq
				,cev.cev_epiid
				,cev.CEV_ID
				,cev.CEV_VISITDATE		
				,cev.cev_agid			
				,cevi.cevi_id
				,cevi.cevi_seqno
				,cevi.cevi_fid
				,cevi.cevi_qid			
				,q.q_text intervention
				,cevi.cevi_answer		
				,sc.sc_discipline		
				,cc.cc_id				
				,cc.cc_desc		
				,cevg.cevg_aid
				,cevg.cevg_id
				,cevg.cevg_Fid	
				,cevg.cevg_answer
				,GoalMet = Case WHEN cevg.cevg_aid = 2 THEN 1 ELSE 0 end
				,ec.ec_desc
				,q1.q_text	goal
			from
				hchb.dbo.CLIENT_EPISODE_VISITS as CEV
				JOIN hchb.dbo.servicecodes sc on (sc_id = cev.cev_sc_id and sc_discipline='SN')			
				--left join hchb.dbo.CLIENT_ORDERS_ALL o on (o.o_epiid  = cev.cev_epiid and o.o_cevid = cev.cev_id)
				join hchb.dbo.CLIENT_EPISODE_VISIT_INTERVENTIONS cevi on (cev.cev_epiid = cevi.cevi_epiid  and cev.CEV_ID=cevi.cevi_cevid and cevi_qid in (1127928,1127929,1127930,1127931,1127932,1127933,1127933,1127934))--and cevi.cevi_answer <> '-1' AND cevi.cevi_answer is not null
				JOIN hchb.dbo.CLIENT_EPISODE_VISIT_GOALS as cevg on (cev.cev_epiid = cevg.cevg_epiid and cev.CEV_ID = cevg.cevg_cevid)
				left join hchb.dbo.EXCEPTION_CODES as ec on (ec.ec_id = cevg.cevg_ecid)
				JOIN hchb.dbo.forms as frm ON  cevi.cevi_fid = FRM.F_ID
				JOIN hchb.dbo.forms as frm1 ON  cevg.cevg_fid = FRM1.F_ID AND frm.f_id = frm1.F_PRIMARYFID
				JOIN hchb.dbo.questions as q on cevi.cevi_qid = q.Q_ID
				left JOIN hchb.dbo.questions as q1 on cevg.cevg_qid = q1.Q_ID
				join hchb.dbo.CARE_CATEGORIES as cc on cc.cc_id = q.Q_CCID
		) intr on (ep.epi_id  = intr.cev_epiid and ep.ep_ccid = intr.cc_id)
	where 1=1
		and IIF(intr.cev_epiid is null,1, iif(intr.cevi_answer is not null and intr.cevi_answer <> '-1', 1, 0)) > 0
		and epi_dischargedate >= '01-jan-2020'
)
select * from dat where goal_seq=1 
GO
/****** Object:  View [dbo].[VW_BH_DX]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[VW_BH_DX] AS
select distinct t1.CEO_CEO_ID, 1 AS BHDX_IND,
  STUFF((SELECT distinct  t2.CEOA_CEOA_ANSWER + ',' 
         from VNSNY_BI.dbo.AST_DX  t2
         where t1.CEO_CEO_ID = t2.CEO_CEO_ID and
		      replace(CEOA_CEOA_ANSWER,'.','') 
                 in ('F04', 'F060', 'F061', 'F0630','F064','F068', 'F070',
                    'F0789','F09','F200','F201','F202', 'F205', 'F2089',
                    'F209', 'F22', 'F259','F28','F29', 'F3110','F312','F3130',
                    'F3131', 'F3132','F314','F315','F3160','F3161','F3162', 
					'F3174','F3175','F3176','F3181','F319', 'F320','F321',
                    'F322','F323','F324','F325','F328','F329','F330','F331',
                    'F332','F333','F3341', 'F3342','F339','F341','F39','F4001',
                    'F4002','F4010','F40240', 'F408','F409','F410','F411','F418',
                    'F419', 'F42','F430','F4310','F4312','F4320', 'F4321','F4322',
                    'F4323','F444','F449','F450','F451','F4522','F4542','F482',
                    'F488','F489', 'F502','F508','F5101','F5109','F519','F600',
                    'F601', 'F603', 'F605', 'F609', 'F633', 'F6389','F639',
                    'F70','F71', 'F72','F73','F79','F801','F802','F8189','F819',
                    'F840', 'F845','F849', 'F89', 'F900','F908', 'F909',
                    'F919','F989', 'F99', 'R451','R480')
            FOR XML PATH(''), TYPE
            ).value('.', 'NVARCHAR(MAX)') 
        ,1,0,'') BHDX_DTL
from VNSNY_BI.dbo.AST_DX  t1
WHERE replace(CEOA_CEOA_ANSWER,'.','') 
                 in ('F04', 'F060', 'F061', 'F0630','F064','F068', 'F070',
                    'F0789','F09','F200','F201','F202', 'F205', 'F2089',
                    'F209', 'F22', 'F259','F28','F29', 'F3110','F312','F3130',
                    'F3131', 'F3132','F314','F315','F3160','F3161','F3162', 
					'F3174','F3175','F3176','F3181','F319', 'F320','F321',
                    'F322','F323','F324','F325','F328','F329','F330','F331',
                    'F332','F333','F3341', 'F3342','F339','F341','F39','F4001',
                    'F4002','F4010','F40240', 'F408','F409','F410','F411','F418',
                    'F419', 'F42','F430','F4310','F4312','F4320', 'F4321','F4322',
                    'F4323','F444','F449','F450','F451','F4522','F4542','F482',
                    'F488','F489', 'F502','F508','F5101','F5109','F519','F600',
                    'F601', 'F603', 'F605', 'F609', 'F633', 'F6389','F639',
                    'F70','F71', 'F72','F73','F79','F801','F802','F8189','F819',
                    'F840', 'F845','F849', 'F89', 'F900','F908', 'F909',
                    'F919','F989', 'F99', 'R451','R480')
GO
/****** Object:  View [dbo].[VW_BH_WORKLIST]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE view [dbo].[VW_BH_WORKLIST]
as 
SELECT  DISTINCT [CEO_ID]
	  ,EPI_ID -- added by Pavel on 1/30/2019
      ,BHDX_IND
	  ,BHDX_DTL
	  ,EFF_DATE
	  ,SCORE
	  ,CASE WHEN SCORE > 0 THEN 1 ELSE 0 END AS BH_TRIGGER
	  ,CASE WHEN SCORE >0 AND SCORE <=2 THEN 1 
	        WHEN SCORE >2 AND SCORE <=4 THEN 2
			WHEN SCORE >4 THEN 3
		ELSE 0 END AS BH_STRATA
	 ,[M1000_DC_PSYCH_14_DA] 
	 ,[M1018_PRIOR_IMPR_DECSN]
	 ,[M1018_PRIOR_DISRUPTIVE]
	 ,[M1018_PRIOR_MEM_LOSS]
	 ,[M1033_HOSP_RISK_MLTPL_HOSPZTN]
	 ,[M1033_HOSP_RISK_MLTPL_ED_VISIT]
	 ,[M1033_HOSP_RISK_MNTL_BHV_DCLN]
	 ,[M1036_RSK_ALCOHOLISM]
	 ,[M1036_RSK_DRUGS]
	 ,[M1230_SPEECH]
	 ,[M1720_WHEN_ANXIOUS]
	 ,[M1730_PHQ2_DPRSN]
	 ,[M1730_PHQ2_LACK_INTRST]
	 ,[M1740_BD_VERBAL]
	 ,[M1740_BD_PHYSICAL]
	 ,[M1740_BD_DELUSIONS]
	 ,[M1740_BD_SOC_INAPPRO]
	 ,[M1745_BEH_PROB_FREQ]
	 ,[M2250_PLAN_SMRY_DPRSN_INTRVTN]
FROM (
select [CEO_CEO_ID] AS CEO_ID
	  ,[CEO_EPIID] AS  EPI_ID -- added by Pavel on 1/30/2019
      ,[bhdx_ind]
	  ,[BHdx_dtl]
	  ,DL_eff_dt AS EFF_DATE
	  ,rank() over (partition by CEO_EPIID order by DL_eff_dt desc) as oasis_seq
	  ,(case when [M1000_DC_PSYCH_14_DA]  = 1 then 1 else 0 end + case when [M1018_PRIOR_IMPR_DECSN] = 1 then 1 else 0 end 
	  +case when [M1018_PRIOR_DISRUPTIVE] = 1 then 1 else 0 end  +case when [M1018_PRIOR_MEM_LOSS] = 1 then 1 else 0 end
	  +case when [M1033_HOSP_RISK_MLTPL_HOSPZTN]=1 then 1 else 0 END + case when [M1033_HOSP_RISK_MLTPL_ED_VISIT]=1 then 1 else 0 END 
	  +case when [M1033_HOSP_RISK_MNTL_BHV_DCLN]=1 then 1 else 0 END  + case when [M1036_RSK_ALCOHOLISM] = 1 THEN 1 ELSE 0 END 
	  +case when [M1036_RSK_DRUGS] = 1 THEN 1 ELSE 0 END +case when [M1230_SPEECH] = 5 THEN 1 ELSE 0 END  
	  +case when [M1720_WHEN_ANXIOUS] = 3  THEN 1 ELSE 0 END  +case when ([M1730_PHQ2_DPRSN] +[M1730_PHQ2_LACK_INTRST]) >3 THEN 1 ELSE 0 END
	  +case when [M1740_BD_VERBAL] = 1 THEN 1 ELSE 0 END + case when [M1740_BD_PHYSICAL]= 1 THEN 1 ELSE 0 END 
	  +case when [M1740_BD_DELUSIONS]= 1 THEN 1 ELSE 0 END  +case when [M1740_BD_SOC_INAPPRO]= 1 THEN 1 ELSE 0 END
	  + case when [M1745_BEH_PROB_FREQ] >= 4 THEN 1 ELSE 0 END +case when [M2250_PLAN_SMRY_DPRSN_INTRVTN] = 1 THEN 1 ELSE 0 END  ) AS SCORE
	  ,ISNULL([M1000_DC_PSYCH_14_DA],0) [M1000_DC_PSYCH_14_DA] 
      ,ISNULL([M1018_PRIOR_IMPR_DECSN],0) [M1018_PRIOR_IMPR_DECSN]
	  ,ISNULL([M1018_PRIOR_DISRUPTIVE],0) [M1018_PRIOR_DISRUPTIVE]
	  ,ISNULL([M1018_PRIOR_MEM_LOSS],0) [M1018_PRIOR_MEM_LOSS]
	  ,ISNULL([M1033_HOSP_RISK_MLTPL_HOSPZTN],0) [M1033_HOSP_RISK_MLTPL_HOSPZTN]
	  ,ISNULL([M1033_HOSP_RISK_MLTPL_ED_VISIT],0) [M1033_HOSP_RISK_MLTPL_ED_VISIT]
	  ,ISNULL([M1033_HOSP_RISK_MNTL_BHV_DCLN],0) [M1033_HOSP_RISK_MNTL_BHV_DCLN]
      ,ISNULL([M1036_RSK_ALCOHOLISM],0) [M1036_RSK_ALCOHOLISM]
	  ,ISNULL([M1036_RSK_DRUGS],0) [M1036_RSK_DRUGS]
      ,ISNULL([M1230_SPEECH],0) [M1230_SPEECH]
	  ,ISNULL([M1720_WHEN_ANXIOUS],0) [M1720_WHEN_ANXIOUS]
	  ,ISNULL([M1730_PHQ2_DPRSN],0) [M1730_PHQ2_DPRSN]
	  ,ISNULL([M1730_PHQ2_LACK_INTRST],0) [M1730_PHQ2_LACK_INTRST]
	  ,ISNULL([M1740_BD_VERBAL],0) [M1740_BD_VERBAL]
	  ,ISNULL([M1740_BD_PHYSICAL],0) [M1740_BD_PHYSICAL]
	  ,ISNULL([M1740_BD_DELUSIONS],0) [M1740_BD_DELUSIONS]
	  ,ISNULL([M1740_BD_SOC_INAPPRO],0) [M1740_BD_SOC_INAPPRO]
	  ,ISNULL([M1745_BEH_PROB_FREQ],0) [M1745_BEH_PROB_FREQ]
	  ,ISNULL([M2250_PLAN_SMRY_DPRSN_INTRVTN],0) [M2250_PLAN_SMRY_DPRSN_INTRVTN]
from vnsny_bi.[dbo].[VW_BH_DX] 
     join VNSNY_BI.dbo.AST_WIDE 
	      on ceo_ceo_id = ceo_ceoid
where BHDX_DTL is not null
)A
where SCORE > 0 and oasis_seq = 1 
GO
/****** Object:  View [dbo].[VW_FACT_DAILY_WORKER_PRODUCTIVITY]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO









create view [dbo].[VW_FACT_DAILY_WORKER_PRODUCTIVITY]
as
SELECT P.[VISIT_DATE]
      ,P.[AGENT_ID]
      ,P.[WKR_PAYROLL_NO]
      ,P.[PAYMENT_METHOD]
	  ,P.[SERVICE_LINE_ID]
	  ,P.[WORKER_CATEGORY]
	  ,SH.MAX_SHIFT_CODE as SHIFT_CODE
	  /*, P.[SOC_VISIT_IND]    ----- ajusted due to ovetr time issue, add a filter 
	  , P.[RECERT_VISIT_IND]         on visits time base on siff shift code. see below
	  , P.[ADDON_VISIT_IND]
	  , P.[RESUMP_VISIT_IND] 
	  , P.[FOLLOWUP_VISIT_IND]
	  , P.[OTHER_VISIT_IND] 
	  ,P.[TOTAL_TIME]
      ,P.[IN_HOME_TIME]
      ,P.[DOCUMENTATION_TIME]
      ,P.[MILEAGE]
      ,P.[DRIVE_TIME]
      ,P.[GRAND_TOTAL_TIME]
      ,P.[POINTS]
      ,P.[WEEKDAY_POINTS]
	  ,P.[WEEKDAY_VISIT]*/
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[SOC_VISIT_IND] else 0 end SOC_VISIT_IND
      ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[RECERT_VISIT_IND] else 0 end RECERT_VISIT_IND
      ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[ADDON_VISIT_IND] else 0 end ADDON_VISIT_IND
      ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[RESUMP_VISIT_IND] else 0 end RESUMP_VISIT_IND
      ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[FOLLOWUP_VISIT_IND] else 0 end FOLLOWUP_VISIT_IND
      ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[OTHER_VISIT_IND] else 0 end OTHER_VISIT_IND
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.REVISIT_VISIT_IND else 0 end REVISIT_VISIT_IND
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.PHONE_VISIT_IND else 0 end PHONE_VISIT_IND
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.VIDEO_VISIT_IND else 0 end VIDEO_VISIT_IND
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[TOTAL_TIME] else 0 end [TOTAL_TIME]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[IN_HOME_TIME] else 0 end [IN_HOME_TIME]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[DOCUMENTATION_TIME] else 0 end [DOCUMENTATION_TIME]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[MILEAGE] else 0 end [MILEAGE]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[DRIVE_TIME] else 0 end [DRIVE_TIME]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[GRAND_TOTAL_TIME] else 0 end [GRAND_TOTAL_TIME]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[POINTS] else 0 end [POINTS]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[WEEKDAY_POINTS] else 0 end [WEEKDAY_POINTS]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[WEEKDAY_VISIT] else 0 end [WEEKDAY_VISIT]
      ,P.[WEEKEND_POINTS]
	  ,H.[WORKPAID_HRS]
	  ,getdate() as data_as_of
	  ,(select MAX(WB_WORK_DATE) from [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY]) as wb_data_as_of
	  ,datediff(WEEK, (select MAX(WB_WORK_DATE) from [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY]), P.VISIT_DATE) as WB_WEEK_VS_VISIT_DATE
	  ,CASE WHEN SH.MAX_SHIFT_CODE = 'FT3' THEN 12 WHEN SH.MAX_SHIFT_CODE = 'FT4' THEN 9 WHEN SH.MAX_SHIFT_CODE = 'FT5' THEN 7.25 ELSE 1 END as STANDARD_HOURS 
	  ,36.25 as STANDARD_HOURS_WEEKLY
  FROM [VNSNY_BI].[dbo].[FCT_STAFF_PRODUCTIVITY] P LEFT OUTER JOIN
		--(SELECT * FROM [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY] WHERE RPT_HOUR_TYPE like '%Regular Work Hours%') H
		(SELECT WB_WORK_DATE, EMPL_ID, sum([WORKPAID_HRS]) as [WORKPAID_HRS] FROM [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY] WHERE RPT_HOUR_TYPE like '%Regular Work Hours%' group by WB_WORK_DATE, EMPL_ID) H
		ON P.VISIT_DATE = H.WB_WORK_DATE AND P.WKR_PAYROLL_NO = H.EMPL_ID   JOIN
		(SELECT EMPL_ID, CONVERT(CHAR(6), WB_WORK_DATE, 112) WB_WORK_MONTH, MAX(ltrim(rtrim([SHIFT_CODE]))) AS MAX_SHIFT_CODE FROM [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY] GROUP BY EMPL_ID, CONVERT(CHAR(6), WB_WORK_DATE, 112)) SH
		ON P.[WKR_PAYROLL_NO] = SH.EMPL_ID and CONVERT(CHAR(6), P.[VISIT_DATE], 112) = SH.WB_WORK_MONTH
		left outer join	HCHB.dbo.SERVICECODES	S
	    ON 	(P.SERVICE_CODE_ID = S.sc_id)
  WHERE
	  P.SERVICE_CODE not like '%44%'
	  and P.SERVICE_CODE not like '%66%'
	  and P.SERVICE_CODE not like '%88%'
	  and P.SERVICE_CODE not like '%TCM%'
	  and S.sc_billable = 'Y'
	
GO
/****** Object:  View [dbo].[VW_FACT_DAILY_WORKER_PRODUCTIVITY_AGG]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [dbo].[VW_FACT_DAILY_WORKER_PRODUCTIVITY_AGG]
AS
SELECT P.[VISIT_DATE]
      ,P.[WKR_PAYROLL_NO]
	  ,P.[AGENT_ID]
      ,P.[PAYMENT_METHOD]
	  ,P.[SERVICE_LINE_ID]
	  ,P.[WORKER_CATEGORY]
	  --,SH.MAX_SHIFT_CODE as SHIFT_CODE
	  ,Max(P.SHIFT_CODE) as SHIFT_CODE
      ,SUM(P.[SOC_VISIT_IND]) [SOC_VISIT_IND]
      ,SUM(P.[RECERT_VISIT_IND]) [RECERT_VISIT_IND]
      ,SUM(P.[ADDON_VISIT_IND]) [ADDON_VISIT_IND]
      ,SUM(P.[RESUMP_VISIT_IND]) [RESUMP_VISIT_IND]
      ,SUM(P.[FOLLOWUP_VISIT_IND]) [FOLLOWUP_VISIT_IND]
      ,SUM(P.[OTHER_VISIT_IND]) [OTHER_VISIT_IND]
	  ,SUM(P.[REVISIT_VISIT_IND]) [REVISIT_VISIT_IND]
	  ,SUM(P.[PHONE_VISIT_IND]) [PHONE_VISIT_IND]
	  ,SUM(P.[VIDEO_VISIT_IND]) [VIDEO_VISIT_IND]
      ,SUM(P.[TOTAL_TIME]) [TOTAL_TIME]
      ,SUM(P.[IN_HOME_TIME]) [IN_HOME_TIME]
      ,SUM(P.[DOCUMENTATION_TIME]) [DOCUMENTATION_TIME]
      ,SUM(P.[MILEAGE]) [MILEAGE]
      ,SUM(P.[DRIVE_TIME]) [DRIVE_TIME]
      ,SUM(P.[GRAND_TOTAL_TIME]) [GRAND_TOTAL_TIME]
      ,SUM(P.[POINTS]) [POINTS]
      ,SUM(P.[WEEKDAY_POINTS]) [WEEKDAY_POINTS]
	  ,SUM(P.[WEEKDAY_VISIT]) [WEEKDAY_VISIT]
      ,SUM(P.[WEEKEND_POINTS]) [WEEKEND_POINTS]
	  ,MAX(P.[WORKPAID_HRS]) [WORKPAID_HRS]
	  ,getdate() as data_as_of
	 -- ,(select MAX(WB_WORK_DATE) from [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY]) as wb_data_as_of
	  ,MAX(P.wb_data_as_of) wb_data_as_of
	--  ,datediff(WEEK, (select MAX(WB_WORK_DATE) from [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY]), P.[VISIT_DATE]) as WB_WEEK_VS_VISIT_DATE
	  ,MAX(P.WB_WEEK_VS_VISIT_DATE) as WB_WEEK_VS_VISIT_DATE
	--  ,CASE WHEN SH.MAX_SHIFT_CODE = 'FT3' THEN 12 WHEN SH.MAX_SHIFT_CODE = 'FT4' THEN 9 WHEN SH.MAX_SHIFT_CODE = 'FT5' THEN 7.25 ELSE 1 END as STANDARD_HOURS 
	  ,MAX(P.STANDARD_HOURS) as STANDARD_HOURS
	  ,36.25 as STANDARD_HOURS_WEEKLY
  FROM [VNSNY_BI].[dbo].[VW_FACT_DAILY_WORKER_PRODUCTIVITY] P 
  GROUP BY P.[VISIT_DATE]
      ,P.[AGENT_ID]
      ,P.[WKR_PAYROLL_NO]
      ,P.[PAYMENT_METHOD]
	  ,P.[SERVICE_LINE_ID]
	  ,P.[WORKER_CATEGORY]
	  --,CASE WHEN SH.MAX_SHIFT_CODE = 'FT3' THEN 12 WHEN SH.MAX_SHIFT_CODE = 'FT4' THEN 9 WHEN SH.MAX_SHIFT_CODE = 'FT5' THEN 7.25 ELSE 1 END
	  ,P.SHIFT_CODE
GO
/****** Object:  View [dbo].[VW_FACT_WEEKLY_WORKER_PRODUCTIVITY]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO









CREATE VIEW [dbo].[VW_FACT_WEEKLY_WORKER_PRODUCTIVITY]
AS
SELECT D.CHHA_WEEK_ID
	  ,[AGENT_ID]
      ,P.[WKR_PAYROLL_NO]
      ,P.[PAYMENT_METHOD]
	  ,P.[SERVICE_LINE_ID]
	  ,P.[WORKER_CATEGORY]
	  ,SH.MAX_SHIFT_CODE as SHIFT_CODE
      ,SUM(P.[SOC_VISIT_IND]) [SOC_VISIT_IND]
      ,SUM(P.[RECERT_VISIT_IND]) [RECERT_VISIT_IND]
      ,SUM(P.[ADDON_VISIT_IND]) [ADDON_VISIT_IND]
      ,SUM(P.[RESUMP_VISIT_IND]) [RESUMP_VISIT_IND]
      ,SUM(P.[FOLLOWUP_VISIT_IND]) [FOLLOWUP_VISIT_IND]
      ,SUM(P.[OTHER_VISIT_IND]) [OTHER_VISIT_IND] 
	  ,SUM(P.[REVISIT_VISIT_IND]) [REVISIT_VISIT_IND]
	  ,SUM(P.[PHONE_VISIT_IND]) [PHONE_VISIT_IND]
	  ,SUM(P.[VIDEO_VISIT_IND]) [VIDEO_VISIT_IND]
      ,SUM(P.[TOTAL_TIME]) [TOTAL_TIME]
      ,SUM(P.[IN_HOME_TIME]) [IN_HOME_TIME]
      ,SUM(P.[DOCUMENTATION_TIME]) [DOCUMENTATION_TIME]
      ,SUM(P.[MILEAGE]) [MILEAGE]
      ,SUM(P.[DRIVE_TIME]) [DRIVE_TIME]
      ,SUM(P.[GRAND_TOTAL_TIME]) [GRAND_TOTAL_TIME]
      ,SUM(P.[POINTS]) [POINTS]
      ,SUM(P.[WEEKDAY_POINTS]) [WEEKDAY_POINTS]
	  ,SUM(P.[WEEKDAY_VISIT]) [WEEKDAY_VISIT]
      ,SUM(P.[WEEKEND_POINTS]) [WEEKEND_POINTS]
	  ,SUM(H.[WORKPAID_HRS]) [WORKPAID_HRS]
	  ,getdate() as data_as_of
	  ,(select MAX(WB_WORK_DATE) from [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY]) as wb_data_as_of
	  ,datediff(WEEK, (select MAX(WB_WORK_DATE) from [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY]), MAX(P.[VISIT_DATE])) as WB_WEEK_VS_VISIT_DATE
	  ,CASE WHEN SH.MAX_SHIFT_CODE = 'FT3' THEN 12 WHEN SH.MAX_SHIFT_CODE = 'FT4' THEN 9 WHEN SH.MAX_SHIFT_CODE = 'FT5' THEN 7.25 ELSE 1 END as STANDARD_HOURS 
	  ,36.25 as STANDARD_HOURS_WEEKLY
  FROM [VNSNY_BI].[dbo].[VW_FACT_DAILY_WORKER_PRODUCTIVITY_AGG] P LEFT OUTER JOIN
		(SELECT WB_WORK_DATE, EMPL_ID, sum([WORKPAID_HRS]) as [WORKPAID_HRS] FROM [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY] WHERE RPT_HOUR_TYPE like '%Regular Work Hours%' group by WB_WORK_DATE, EMPL_ID) H
		ON P.VISIT_DATE = H.WB_WORK_DATE AND P.WKR_PAYROLL_NO = H.EMPL_ID  JOIN
		(SELECT EMPL_ID, CONVERT(CHAR(6), WB_WORK_DATE, 112) WB_WORK_MONTH, MAX(ltrim(rtrim([SHIFT_CODE]))) AS MAX_SHIFT_CODE FROM [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY] GROUP BY EMPL_ID, CONVERT(CHAR(6), WB_WORK_DATE, 112)) SH
		ON P.[WKR_PAYROLL_NO] = SH.EMPL_ID and CONVERT(CHAR(6), P.[VISIT_DATE], 112) = SH.WB_WORK_MONTH JOIN
		VNSNY_BI.dbo.LU_DAY D
		ON P.[VISIT_DATE] = D.DAY_DATE
  GROUP BY D.CHHA_WEEK_ID
      ,P.[AGENT_ID]
      ,P.[WKR_PAYROLL_NO]
      ,P.[PAYMENT_METHOD]
	  ,P.[SERVICE_LINE_ID]
	  ,SH.[MAX_SHIFT_CODE]
	  ,P.[WORKER_CATEGORY]
GO
/****** Object:  View [dbo].[vw_active_covid19_patient]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[vw_active_covid19_patient]  as
select epi_paid,epi_lastname,epi_firstname, cect_ctypeid
      ,ctype_description,cect_effectivedatefrom
	  ,cect_effectivedateto,DAY_DATE, epi_id,epi_slid,1 as flag
from hchb.[dbo].[CLIENT_EPISODE_CARE_TYPES]
join hchb.dbo.client_episodes_all on epi_id = cect_epiid
join hchb.dbo.care_types on ctype_id = cect_ctypeid
cross join VNSNY_BI.dbo.LU_DAY
where cect_ctypeid  in(25090,25091, 25092, 25094)
      and convert(date,cect_effectivedatefrom) <= DAY_DATE
       AND convert(date,isnull(cect_effectivedateto, '9999-12-31') )>= DAY_DATE
	   AND DAY_DATE  <= getdate()
GO
/****** Object:  View [dbo].[vw_active_covid19_visits]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW
[dbo].[vw_active_covid19_visits]
as
select distinct v.csv_id, v.csv_scheddate, a.epi_id, epi_slid, 
a.cect_effectivedatefrom, a.cect_effectivedateto, a.ctype_description, a.epi_paid,
v.csv_status, a13.sc_billable
from HCHB.dbo.CLIENT_SCHED_VISITS v
join VNSNY_BI.[dbo].[vw_active_covid19_patient] a on v.csv_epiid = a.EPI_ID 
and v.csv_scheddate between a.cect_effectivedatefrom and isnull(a.cect_effectivedateto,getdate())
join	HCHB.dbo.SERVICECODES	a13
	  on 	(csv_scid = a13.sc_id)
where a.cect_ctypeid = 25090 -- only positive COVID-19
--where csv_status = 'C' and sc_billable in ('Y')
GO
/****** Object:  View [dbo].[VW_DXCS_PENG_ANY]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




create view [dbo].[VW_DXCS_PENG_ANY] as
SELECT /*+ parallel( AST_AA, 4) */ AST_AA.*,
       CASE
          WHEN dxpengSEV_any_aids_O = 4 THEN 3
          ELSE dxpengSEV_any_aids_O
       END
          AS dxpengSEV2_any_aids_O,
       CASE
          WHEN dxpengSEV_any_cancer_O = 4 THEN 3
          ELSE dxpengSEV_any_cancer_O
       END
          AS dxpengSEV2_any_cancer_O,
       CASE
          WHEN dxpengSEV_any_diabetes_O = 4 THEN 3
          ELSE dxpengSEV_any_diabetes_O
       END
          AS dxpengSEV2_any_diabetes_O,
       CASE
          WHEN dxpengSEV_any_dementia_O = 4 THEN 3
          ELSE dxpengSEV_any_dementia_O
       END
          AS dxpengSEV2_any_dementia_O,
       CASE
          WHEN dxpengSEV_any_depression_O = 4 THEN 3
          ELSE dxpengSEV_any_depression_O
       END
          AS dxpengSEV2_any_depression_O,
       CASE
          WHEN dxpengSEV_any_cerebral_O = 4 THEN 3
          ELSE dxpengSEV_any_cerebral_O
       END
          AS dxpengSEV2_any_cerebral_O,
       CASE
          WHEN dxpengSEV_any_neurological_O = 4 THEN 3
          ELSE dxpengSEV_any_neurological_O
       END
          AS dxpengSEV2_any_neurological_O,
       CASE
          WHEN dxpengSEV_any_hypertension_O = 4 THEN 3
          ELSE dxpengSEV_any_hypertension_O
       END
          AS dxpengSEV2_any_hypertension_O,
       CASE
          WHEN dxpengSEV_any_acute_mi_O = 4 THEN 3
          ELSE dxpengSEV_any_acute_mi_O
       END
          AS dxpengSEV2_any_acute_mi_O,
       CASE
          WHEN dxpengSEV_any_cardiac_O = 4 THEN 3
          ELSE dxpengSEV_any_cardiac_O
       END
          AS dxpengSEV2_any_cardiac_O,
       CASE
          WHEN dxpengSEV_any_heart_O = 4 THEN 3
          ELSE dxpengSEV_any_heart_O
       END
          AS dxpengSEV2_any_heart_O,
       CASE
          WHEN dxpengSEV_any_stroke_O = 4 THEN 3
          ELSE dxpengSEV_any_stroke_O
       END
          AS dxpengSEV2_any_stroke_O,
       CASE WHEN dxpengSEV_any_pvd_O = 4 THEN 3 ELSE dxpengSEV_any_pvd_O END
          AS dxpengSEV2_any_pvd_O,
       CASE
          WHEN dxpengSEV_any_pulmonary_O = 4 THEN 3
          ELSE dxpengSEV_any_pulmonary_O
       END
          AS dxpengSEV2_any_pulmonary_O,
       CASE
          WHEN dxpengSEV_any_renal_O = 4 THEN 3
          ELSE dxpengSEV_any_renal_O
       END
          AS dxpengSEV2_any_renal_O,
       CASE
          WHEN dxpengSEV_any_skinulcer_O = 4 THEN 3
          ELSE dxpengSEV_any_skinulcer_O
       END
          AS dxpengSEV2_any_skinulcer_O,
       CASE
          WHEN dxpengSEV_any_genitourinary_O = 4 THEN 3
          ELSE dxpengSEV_any_genitourinary_O
       END
          AS dxpengSEV2_any_genitourinary_O,
       CASE
          WHEN dxpengSEV_any_arthritis_O = 4 THEN 3
          ELSE dxpengSEV_any_arthritis_O
       END
          AS dxpengSEV2_any_arthritis_O
  FROM   (select ceo_ceo_id,CEOA_OASISANSWER_EFF_DT,
       max(case  when group_id = 61 then 1 else 0 end) AS dxpeng_any_aids_b,
	   max(case  when group_id = 62 then 1 else 0 end) AS dxpeng_any_cancer_b,
       max(case  when group_id = 63 then 1 else 0 end) AS dxpeng_any_diabetes_b,
	   max(case  when group_id = 64 then 1 else 0 end) AS dxpeng_any_dementia_b,
	   max(case  when group_id = 65 then 1 else 0 end) AS dxpeng_any_depression_b,
	   max(case  when group_id = 66 then 1 else 0 end) AS dxpeng_any_cerebral_b,
	   max(case  when group_id = 67 then 1 else 0 end) AS dxpeng_any_neurological_b,
	   max(case  when group_id = 68 then 1 else 0 end) AS dxpeng_any_hypertension_b,
	   max(case  when group_id = 69 then 1 else 0 end) AS dxpeng_any_acute_mi_b,
	   max(case  when group_id = 70 then 1 else 0 end) AS dxpeng_any_cardiac_b,
	   max(case  when group_id = 71 then 1 else 0 end) AS dxpeng_any_heart_b,
	   max(case  when group_id = 72 then 1 else 0 end) AS dxpeng_any_stroke_b,
	   max(case  when group_id = 73 then 1 else 0 end) AS dxpeng_any_pvd_b,
	   max(case  when group_id = 74 then 1 else 0 end) AS dxpeng_any_pulmonary_b,
	   max(case  when group_id = 75 then 1 else 0 end) AS dxpeng_any_renal_b,
	   max(case  when group_id = 76 then 1 else 0 end) AS dxpeng_any_skinulcer_b,
	   max(case  when group_id = 77 then 1 else 0 end) AS dxpeng_any_genitourinary_b,
	   max(case  when group_id = 78 then 1 else 0 end) AS dxpeng_any_arthritis_b,
	   max(case  when group_id = 61 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_aids_O ,
	   max(case  when group_id = 62 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_cancer_O ,
	   max(case  when group_id = 63 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_diabetes_O ,
	   max(case  when group_id = 64 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_dementia_O ,
	   max(case  when group_id = 65 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_depression_O ,
	   max(case  when group_id = 66 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_cerebral_O ,
	   max(case  when group_id = 67 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_neurological_O ,
	   max(case  when group_id = 68 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_hypertension_O ,
	   max(case  when group_id = 69 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_acute_mi_O ,
	   max(case  when group_id = 70 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_cardiac_O ,
	   max(case  when group_id = 71 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_heart_O ,
	   max(case  when group_id = 72 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_stroke_O ,
	   max(case  when group_id = 73 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_pvd_O ,
	   max(case  when group_id = 74 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_pulmonary_O ,
	   max(case  when group_id = 75 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_renal_O ,
	   max(case  when group_id = 76 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_skinulcer_O ,
	   max(case  when group_id = 77 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_genitourinary_O ,
	   max(case  when group_id = 78 then  isnull(SEVERITY_LEVEL, 2) else 0 end) AS dxpengSEV_any_arthritis_O 
from hchb.dbo.client_episode_oasis
  join VNSNY_BI.dbo.AST_DX on ceo_id = CEO_CEO_ID and CEO_CEO_EPIID = ceo_epiid
  join VNSNY_STAT.dbo.icd_group on REPLACE(CEOA_CEOA_ANSWER,'.','') = ICD_CODE
 group by CEO_CEO_ID,CEOA_OASISANSWER_EFF_DT) AST_AA 


GO
/****** Object:  View [dbo].[VW_MLTC_WORK_LIST]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE view [dbo].[VW_MLTC_WORK_LIST] AS
with adls as
(
select a.* 
from (

SELECT     epi_id
            ,  isnull(dxpeng_any_aids_b, 0)
            + isnull(dxpeng_any_cancer_b, 0)
            + isnull(dxpeng_any_diabetes_b, 0)
            + isnull(dxpeng_any_dementia_b, 0)
            + isnull(dxpeng_any_depression_b, 0)
            + isnull(dxpeng_any_cerebral_b, 0)
            + isnull(dxpeng_any_neurological_b, 0)
            + isnull(dxpeng_any_hypertension_b, 0)
            + isnull(dxpeng_any_acute_mi_b, 0)
            + isnull(dxpeng_any_cardiac_b, 0)
            + isnull(dxpeng_any_heart_b, 0)
            + isnull(dxpeng_any_stroke_b, 0)
            + isnull(dxpeng_any_pvd_b, 0)
            + isnull(dxpeng_any_pulmonary_b, 0)
            + isnull(dxpeng_any_renal_b, 0)
            + isnull(dxpeng_any_skinulcer_b, 0)
            + isnull(dxpeng_any_genitourinary_b, 0)
            + isnull(dxpeng_any_arthritis_b, 0)
                AS dxpeng_score
          , CASE WHEN [M1800_CRNT_GROOMING] >= 2 THEN 1 ELSE 0 END AS             m1800_groom
          , CASE WHEN  [M1810_CRNT_DRESS_UPPER] >= 2 THEN 1 ELSE 0 END AS       m1810_dressupper
          , CASE WHEN [M1820_CRNT_DRESS_LOWER] >= 2 THEN 1 ELSE 0 END AS       m1820_dresslower
          , CASE WHEN [M1830_CRNT_BATHG] >= 2 THEN 1 ELSE 0 END AS                m1830_bathe
          , CASE WHEN [M1840_CRNT_TOILTG] >= 1 THEN 1 ELSE 0 END AS               m1840_toilet
          , CASE WHEN [M1845_CRNT_TOILTG_HYGN] >= 2 THEN 1 ELSE 0 END AS       m1845_toilethyg
          , CASE WHEN [M1850_CRNT_TRNSFRNG] >= 2 THEN 1 ELSE 0 END AS             m1850_xfer
               , CASE WHEN [M1860_CRNT_AMBLTN] >= 2 THEN 1 ELSE 0 END as                m1860_ambltn
               , CASE WHEN [M1870_CRNT_FEEDING] >= 1 THEN 1 ELSE 0 END as         m1870_feeding
        --  , CASE WHEN [M2102_CARE_TYPE_SRC_ADL] >= 1 THEN 1 ELSE 0 END AS m2102a_srcassistadl 
              , CASE WHEN [M1800_CRNT_GROOMING] >= 2 THEN 1 ELSE 0 END 
                    + CASE WHEN  [M1810_CRNT_DRESS_UPPER] >= 2 THEN 1 ELSE 0 END 
                    + CASE WHEN [M1820_CRNT_DRESS_LOWER] >= 2 THEN 1 ELSE 0 END 
                    + CASE WHEN [M1830_CRNT_BATHG] >= 2 THEN 1 ELSE 0 END 
                    + CASE WHEN [M1840_CRNT_TOILTG] >= 1 THEN 1 ELSE 0 END        
                    + CASE WHEN [M1845_CRNT_TOILTG_HYGN] >= 2 THEN 1 ELSE 0 END 
                    + CASE WHEN [M1850_CRNT_TRNSFRNG] >= 2 THEN 1 ELSE 0 END        
                    + CASE WHEN [M1860_CRNT_AMBLTN] >= 2 THEN 1 ELSE 0 END        
                    + CASE WHEN [M1870_CRNT_FEEDING] >= 1 THEN 1 ELSE 0 END        as num_adls
          , isnull(dxpeng_any_aids_b, 0) AS dxpeng_any_aids_b
          , isnull(dxpeng_any_cancer_b, 0) AS dxpeng_any_cancer_b
          , isnull(dxpeng_any_diabetes_b, 0) AS dxpeng_any_diabetes_b
          , isnull(dxpeng_any_dementia_b, 0) AS dxpeng_any_dementia_b
          , isnull(dxpeng_any_depression_b, 0) AS dxpeng_any_depression_b
          , isnull(dxpeng_any_cerebral_b, 0) AS dxpeng_any_cerebral_b
          , isnull(dxpeng_any_neurological_b, 0) AS dxpeng_any_neurological_b
          , isnull(dxpeng_any_hypertension_b, 0) AS dxpeng_any_hypertension_b
          , isnull(dxpeng_any_acute_mi_b, 0) AS dxpeng_any_acute_mi_b
          , isnull(dxpeng_any_cardiac_b, 0) AS dxpeng_any_cardiac_b
          , isnull(dxpeng_any_heart_b, 0) AS dxpeng_any_heart_b
          , isnull(dxpeng_any_stroke_b, 0) AS dxpeng_any_stroke_b
          , isnull(dxpeng_any_pvd_b, 0) AS dxpeng_any_pvd_b
          , isnull(dxpeng_any_pulmonary_b, 0) AS dxpeng_any_pulmonary_b
          , isnull(dxpeng_any_renal_b, 0) AS dxpeng_any_renal_b
          , isnull(dxpeng_any_skinulcer_b, 0) AS dxpeng_any_skinulcer_b
          , isnull(dxpeng_any_genitourinary_b, 0) AS dxpeng_any_genitourinary_b
          , isnull(dxpeng_any_arthritis_b, 0) AS dxpeng_any_arthritis_b
          , dx.[CEOA_OASISANSWER_EFF_DT],
               row_number() OVER (PARTITION BY epi_id ORDER BY dx.[CEOA_OASISANSWER_EFF_DT] DESC)  /*CHANGED FROM SUM(1) TO ROW_NUMBER() */
                  AS oasis_seq
                             ,cefs_psid     
FROM      hchb.dbo.client_episodes_all 
            INNER JOIN  [VNSNY_BI].[dbo].[AST_WIDE]  ON (epi_id = CEO_EPIID)
            INNER JOIN  VNSNY_BI.dbo.vw_dxcs_peng_any dx ON (CEO_CEOID = ceo_ceo_id)
                    join HCHB.dbo.CLIENT_EPISODE_FS  on cefs_epiid = epi_id
WHERE     cefs_active = 'Y'
          AND cefs_psid NOT IN ( 28091,28101,28239,28105,28124,28117
                                                       ,28127,28140,28123,28115,28154
                                                       ,28197,28174,28209,28085,28211,28259
                                                       ,28196,28269,28190,28170,28167,28156,28098
                                               ,28099,28120,28126,28131,28135,28149,28151
                                               ,28123,28119,28115,28139,28154,28235,28158
                                               ,28197,28174,28213,28190,28182,28169,28164
                                                                            ,28156,23161,28234,28159,28231,28128,28233
                                                                            ,28270)
                    and cefs_ps = 'P' 
            AND epi_slid =1
            AND EPI_status NOT IN( 'NON-ADMIT','DELETED','PENDING','HOLD')
         --   AND IN ('A', 'C')
            AND  [M0100_ASSMT_REASON] IN (1, 3)
          --  AND am.cms_epi_no is not null
            AND ([M1800_CRNT_GROOMING] >= 2
                 OR [M1810_CRNT_DRESS_UPPER] >= 2
                 OR [M1820_CRNT_DRESS_LOWER] >= 2
                 OR [M1830_CRNT_BATHG] >= 2
                 OR [M1840_CRNT_TOILTG] >= 1
                 OR [M1845_CRNT_TOILTG_HYGN] >= 2
                 OR [M1850_CRNT_TRNSFRNG] >= 2
             OR [M1860_CRNT_AMBLTN] >= 2
                    OR [M1870_CRNT_FEEDING] >= 1)
             AND DL_ACTIVE_REC_IND='Y' /*take most recent edit for each assessment from AST_WIDE table*/
) a
where a.oasis_seq = 1
)
select *  from adls 
       where (dxpeng_any_dementia_b = 1 and num_adls >= 2)
             or (coalesce(dxpeng_any_dementia_b,0) = 0 and num_adls >= 3)

GO
/****** Object:  View [dbo].[VW_CMO_PDGM]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW  [dbo].[VW_CMO_PDGM]
AS
(
select DISTINCT pdgm_episodes.epi_id, pdgm_episodes.EPI_PAID,
CASE WHEN CONVERT(DATE, CMO_LST.CMSBAR_SCRIPT_DATE) IS NOT NULL and CONVERT(DATE, CMSBAR_SCRIPT_DATE) > CONVERT(DATE,epi_SocDate)  THEN 1 ELSE 0
END CMO_FLAG
FROM 
(select DISTINCT PDGM1.epi_id, PDGM1.EPI_PAID,  UPPER(PAYOR_INFO.ps_desc) PAY_DESC, PDGM1.epi_StartOfEpisode, PDGM1.epi_EndOfEpisode,epi_SocDate,
case when UPPER(PAYOR_INFO.ps_desc) in ('HUMANA PDGM' , 'UNITED HEALTHCARE MEDICARE PDGM- INTERNAL AUTH CMO') then dateadd(day, 61, PDGM1.epi_StartOfEpisode) 
else PDGM1.epi_EndOfEpisode end  epi_EndOfEpisode_new
from 
(select distinct PDGM.epi_id , ce.epi_paid, ce.epi_StartOfEpisode, ce.epi_EndOfEpisode, ce.epi_SocDate
from hchb.dbo.client_episodes_all CE, vnsny_bi.[dbo].[fact_medicare_pdgm] PDGM
WHERE CE.EPI_ID = PDGM.EPI_ID) PDGM1
LEFT JOIN (select distinct cefs_epiid, PS_ID, ps_desc  
						from hchb.dbo.client_episode_fs 
						join hchb.dbo.payor_sources on ps_id = cefs_psid
						join hchb.dbo.payor_types on pt_id = cefs_ptid
					where cefs_ps = 'P'and  UPPER(ps_desc) like '%PDGM%') PAYOR_INFO
on PDGM1.epi_id = PAYOR_INFO.cefs_epiid
) pdgm_episodes
LEFT JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY GC_PATIENT_ID ORDER BY CONVERT(DATE, CMSBAR_SCRIPT_DATE) DESC) RN 
from vnsny_bi.DBO.CASE_RATE_CMO_PATIENT_LST) CMO_LST
ON CONVERT(VARCHAR, pdgm_episodes.EPI_PAID) = CMO_LST.GC_PATIENT_ID
AND  rn = 1 
);
GO
/****** Object:  View [dbo].[VW_TIME_BASED_EPISODE_DTL_CMO]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE VIEW  [dbo].[VW_TIME_BASED_EPISODE_DTL_CMO]
AS
(
SELECT DISTINCT TBED.TBED_ID, TBED.TBED_TBEID, TBED.TBED_EPIID, TBED.EPI_PAID, TBED_EPISODE_START_DATE, 
CASE WHEN TBED_PAYOR_CD IN (28226, 28228) THEN dateadd(day, 59, TBED_EPISODE_START_DATE) ELSE TBED_EPISODE_END_DATE END AS TBED_EPISODE_END_DATE,
TBED_EPI_SOCDATE,EPISODE_END_DATE_ACTUAL, TBED_PAYOR_CD,TBED_EPI_PAID, TBED.TBED_LVL_NUM,
CASE WHEN CONVERT(DATE, CMO_LST.CMSBAR_SCRIPT_DATE) IS NOT NULL and CONVERT(DATE, CMSBAR_SCRIPT_DATE) > TBED_EPI_SOCDATE  THEN 1 ELSE 0
END CMO_FLAG
FROM vnsny_bi.[dbo].[TIME_BASED_EPISODE_DTL] TBED
left join (SELECT *, ROW_NUMBER() OVER (PARTITION BY GC_PATIENT_ID ORDER BY CONVERT(DATE, CMSBAR_SCRIPT_DATE) DESC) RN 
from vnsny_bi.DBO.CASE_RATE_CMO_PATIENT_LST) CMO_LST
ON TBED.EPI_PAID = CMO_LST.GC_PATIENT_ID 
AND  rn = 1
);
GO
/****** Object:  View [dbo].[VW_TIME_BASED_EPISODE_CAREPORT_EMERGENCY_VISITS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE   view [dbo].[VW_TIME_BASED_EPISODE_CAREPORT_EMERGENCY_VISITS]
as 
SELECT * FROM 
(select DISTINCT VPIN ,CE.EPI_ID,CE.EPI_PAID,TBED_TBEID, 
CAREPORT_VISIT_ID ,ADMIT_DATE,DISCHARGE_DATE,
TBED_EPISODE_START_DATE,
CASE WHEN TBED_PAYOR_CD IN (28226, 28228) THEN dateadd(day, 59, TBED_EPISODE_START_DATE) ELSE TBED_EPISODE_END_DATE END AS TBED_EPISODE_END_DATE, 
TBED_EPI_SOCDATE ,TBED.EPISODE_END_DATE_ACTUAL,[TBED_PAYOR_CD],datediff(day,TBED_EPISODE_START_DATE,ADMIT_DATE) + 1 as EPISODE_DAYS_TO_HOSP, 
PRIMARY_DIAGNOSIS_CODE,  'CRT' AS TBE_TYPE, TBED.CMO_FLAG, 1 as FLAG
from vnsny_bi.[dbo].[VW_TIME_BASED_EPISODE_DTL_CMO] TBED
join (SELECT distinct epi_id ,  epi_paid , VPIN, PID, CONVERT(DATE,ADMIT_DATE) as ADMIT_DATE, CONVERT(DATE,DISCHARGE_DATE) as DISCHARGE_DATE,
epi_SocDate, VISIT_ID as CAREPORT_VISIT_ID, CH.PRIMARY_DIAGNOSIS_CODE
				from hchb.dbo.client_episodes_all CL_E,
					vnsny_bi.[dbo].CAREPORT_WEEKLY_HOPSITALIZATION CH
				where convert(VARCHAR(30), epi_paid) = CH.PID
				AND CL_E.epi_status not in ('DELETED', 'NON-ADMIT')
				AND ADMIT_TYPE IN ( 'Emergency')
				and epi_slid = 1
	)CE
on CE.epi_id = tbed_epiid
WHERE  TBED_LVL_NUM>0
)A
where ADMIT_DATE between TBED_EPISODE_START_DATE and TBED_EPISODE_END_DATE

union all 

SELECT * FROM 
(SELECT distinct ch.VPIN, c_epi.EPI_ID AS EPI_ID, c_epi.EPI_PAID , null TBED_TBEID,
VISIT_ID as CAREPORT_VISIT_ID,CONVERT(DATE,ADMIT_DATE) as ADMIT_DATE, CONVERT(DATE,DISCHARGE_DATE) as DISCHARGE_DATE, 
c_epi.EPI_STARTOFEPISODE AS TBED_EPISODE_START_DATE, 
case when UPPER(PAYOR_INFO.ps_desc) in ('HUMANA PDGM' , 'UNITED HEALTHCARE MEDICARE PDGM- INTERNAL AUTH CMO') then dateadd(day, 60, c_epi.epi_StartOfEpisode)
else c_epi.EPI_ENDOFEPISODE end as TBED_EPISODE_END_DATE, 
c_epi.EPI_SOCDATE AS TBED_EPI_SOCDATE,
c_epi.EPI_ENDOFEPISODE as EPISODE_END_DATE_ACTUAL, PAYOR_INFO.PS_ID AS TBED_PAYOR_CD,
datediff(day,epi_startofepisode,ADMIT_DATE) + 1 as EPISODE_DAYS_TO_HOSP, CH.PRIMARY_DIAGNOSIS_CODE, 'PDGM' AS TBE_TYPE, PDGM.CMO_FLAG, 1 FLAG
	 From hchb.dbo.client_episodes_all c_epi
		JOIN vnsny_bi.[dbo].CAREPORT_WEEKLY_HOPSITALIZATION CH ON convert(VARCHAR(30), c_epi.epi_paid) = CH.PID
		JOIN vnsny_bi.[dbo].[VW_CMO_PDGM] PDGM ON pdgm.epi_id = c_epi.epi_id
		LEFT JOIN (select distinct cefs_epiid, PS_ID, ps_desc  
						from hchb.dbo.client_episode_fs 
						join hchb.dbo.payor_sources on ps_id = cefs_psid
						join hchb.dbo.payor_types on pt_id = cefs_ptid
					where cefs_ps = 'P'and  UPPER(ps_desc) like '%PDGM%') PAYOR_INFO
		ON c_epi.epi_id = PAYOR_INFO.cefs_epiid
	    where c_epi.epi_status not in ('DELETED', 'NON-ADMIT')
			AND ADMIT_TYPE IN ( 'Emergency')
			and epi_slid = 1
			and PAYOR_INFO.PS_ID is not null 
) A
WHERE ADMIT_DATE between TBED_EPISODE_START_DATE and TBED_EPISODE_END_DATE
;
GO
/****** Object:  View [dbo].[VNSNY_PCRS_HCHB_BRANCH_XREF]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
  create view [dbo].[VNSNY_PCRS_HCHB_BRANCH_XREF]
  AS
  select distinct [PCRS_PRT_TEAM_CD], BG_ID, BRANCH_CODE 
  from [VNSNY_BI].[dbo].[VW_CHHA_BRANCH_GROUPS_TEAMS]
  where [PCRS_PRT_TEAM_CD] <> '-1'
GO
/****** Object:  View [dbo].[VW_CLIENT_CASE]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view  [dbo].[VW_CLIENT_CASE] as
select c.epi_id, 
	   c.epi_paid,
	   CONCAT(c.epi_paid,convert(INT, c.epi_SocDate)) as 'Case_num' ,
	   c.epi_mrnum,
	   c.epi_lastname, 
	   c.epi_firstname, 
	   c.epi_status, 
	   c.epi_DateOfReferral,
	   c.epi_SocDate,
	   c.epi_DischargeDate,
	   c.epi_AdmitType, 
	   c.epi_RecertFlag, 
	   c.epi_branchcode, 
	   c.epi_NonAdmitCode, 
	   c.epi_NonAdmitDate,
	   c.epi_StartOfEpisode,
	   c.epi_EndOfEpisode,
	   c.epi_slid,
	   ps.ps_id,
	   ps.ps_desc,
	   pt.pt_id,
	   pt.pt_desc,
	   bg.bg_description,
	   na.nac_desc,
	   c.epi_ReferralSource,
	   CASE 
		WHEN c.epi_ReferralSource in('FACILITY','OTHER') THEN c.epi_ReferralFaId
		WHEN c.epi_ReferralSource='PHYSICIAN' THEN left(p.PH_ZIPCODE,5)
		END as 'Referral_ID',
	   cdp.cdp_diICDCode,
	   ICD.ICD_Description,
	   sq.Case_Discharge_Date
from hchb.dbo.client_episodes_all c
left join HCHB.dbo.CLIENT_EPISODE_FS fs
on c.epi_id=fs.cefs_epiid and fs.cefs_active='Y' and fs.cefs_ps='P'
left join HCHB.dbo.PAYOR_SOURCES ps
on ps.ps_id=fs.cefs_psid
left join HCHB.dbo.PAYOR_TYPES pt
on pt.pt_id=fs.cefs_ptid
left join [VNSNY_BI].[dbo].[VW_CHHA_BRANCH_GROUPS_TEAMS] b
on b.branch_code=c.epi_branchcode and b.team_id=c.epi_teamid
left join [VNSNY_BI].[dbo].[VW_BRANCH_GROUPS] bg
on bg.bg_id=b.BGB_BGID
left join hchb.dbo.NONADMIT_REASONS na
on na.nac_code=c.epi_NonAdmitCode
left join HCHB.dbo.physician p
on c.epi_phid=p.ph_id and c.epi_poid=p.po_id
left join [HCHB].[dbo].[client_diagnoses_and_procedures] cdp
on c.epi_id=cdp.cdp_epiid
left join [HCHB].[dbo].[DIAGNOSIS_INFO] di
on cdp.cdp_diICDVersionID = di.di_ICDVersionId AND cdp.cdp_diICDTypeCode = di.di_ICDTypeCode AND cdp.cdp_diICDCode = di.di_ICDCode
left join [SHARED_REFERENCE].[ICD].[ICD_CODES] ICD
on ICD.ICD_VersionId = di.di_ICDVersionId AND ICD.ICD_TypeCode  = di.di_ICDTypeCode AND ICD.ICD_Code = di.di_ICDCode
left join (
select epi_paid
		, epi_DateOfReferral
       , epi_socdate
,max(epi_dischargedate) as 'Case_Discharge_Date'
from hchb.dbo.client_episodes_all
where epi_admittype <> 'BEREAVEMENT'
and epi_status in ('discharged', 'recertified', 'current', 'hold')
group by epi_paid, epi_socdate, epi_DateOfReferral) sq
on c.epi_paid=sq.epi_paid and c.epi_socdate=sq.epi_socdate
where cdp.cdp_diICDTypeCode = 'D'
and cdp.cdp_DiagnosisProcedureTypeSourceId=1
and icd.ICD_VersionId=10
and cdp.cdp_SortOrder=10


GO
/****** Object:  View [dbo].[VW_VNSNY_PCRS_HCHB_BRANCH_XREF]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

  create view [dbo].[VW_VNSNY_PCRS_HCHB_BRANCH_XREF]
  AS
  select distinct [BGB_BGID],[PCRS_PRT_TEAM_CD], BG_ID, BRANCH_CODE 
  from [VNSNY_BI].[dbo].[VW_CHHA_BRANCH_GROUPS_TEAMS]
  where [PCRS_PRT_TEAM_CD] <> '-1'
GO
/****** Object:  View [dbo].[pdgm_visits]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[pdgm_visits]	 as
	select ordr_vst.pp_id,ordr_vst.ph_timing
 	      ,ordr_vst.Status as ordered_status,ordr_vst.visits as ordered_visits
	      ,sche_vst.Status as scheduled_status,sche_vst.Visits as scheduled_visits
		  ,miss_vst.Status as missed_status,miss_vst.Visits as missed_visits
	from
	(SELECT 
		pp.pp_id, 
		pp.ph_timing,
		'Ordered' AS [Status],
		COUNT(*) AS [Visits]
	FROM
	VNSNY_BI.dbo.fact_medicare_pdgm AS pp
	INNER JOIN HCHB.dbo.CLIENT_CALENDAR AS c ON c.calendar_epiid = pp.Epi_id
	INNER JOIN HCHB.dbo.SERVICECODES AS sc ON sc.sc_id = c.calendar_scid
	INNER JOIN HCHB.dbo.DISCIPLINES AS d ON d.dsc_code = sc.sc_discipline
	WHERE
	c.calendar_visitdate BETWEEN pp.pp_startDate AND pp.pp_endDate
	AND sc.sc_pointcareformat NOT IN ('R' , '22') ---R Medical Treatment and 22 for phone visits
	AND sc.sc_billable = 'Y'	
	GROUP BY
	pp.pp_id, 
	pp.ph_timing	
	) ordr_vst 
left join
	(SELECT 
		pp.pp_id, 
		pp.ph_timing,
		'Scheduled' AS [Status],
		COUNT(*) AS [Visits]
	FROM
	VNSNY_BI.dbo.fact_medicare_pdgm as pp
	INNER JOIN hchb.dbo.SCHED AS s ON s.epiid = pp.epi_id
	INNER JOIN hchb.dbo.SERVICECODES AS sc ON sc.sc_code = s.flattype
	INNER JOIN hchb.dbo.DISCIPLINES AS d ON d.dsc_code = sc.sc_discipline
	LEFT JOIN hchb.pdgm.V_PDGM_MSP AS m ON m.PdgmBillingPeriodId = pp.pp_id
	WHERE 
	(
		(m.PrimaryPayorCefsID IS NULL AND s.PrimID = pp.cefs_id) -- non-msp
		OR (m.PrimaryPayorCefsID > 0 AND s.PrimID = m.PrimaryPayorCefsID)  --msp
	)
	AND s.ShiftDate BETWEEN pp.pp_startDate AND pp.pp_endDate
	AND s.WKRID > 0 
	AND s.CLTID > 0
	AND sc.sc_pointcareformat NOT IN ('R' , '22')---R Medical Treatment and 22 for phone visits
	AND ((s.freq in (0,9) AND s.difpay IN (1,3)) OR (s.freq = 10 AND s.ExcludeFROMBilling = 0))
	GROUP BY
	pp.pp_id, 	
	pp.ph_timing
	) sche_vst	
    on sche_vst.pp_id = ordr_vst.pp_id
left join
    (SELECT 
		pp.pp_id, 
		pp.ph_timing,
		'Missed' AS [Status],
		COUNT(*) AS [Visits]
	FROM
	VNSNY_BI.dbo.fact_medicare_pdgm  AS pp
	INNER JOIN HCHB.dbo.SCHED AS s ON s.epiid = pp.epi_id
	INNER JOIN HCHB.dbo.SERVICECODES AS sc ON sc.sc_code = s.flattype
	INNER JOIN HCHB.dbo.DISCIPLINES AS d ON d.dsc_code = sc.sc_discipline
	LEFT JOIN HCHB.pdgm.V_PDGM_MSP AS m ON m.PdgmBillingPeriodId = pp.pp_id
	WHERE 
	(
		(m.PrimaryPayorCefsID IS NULL AND s.PrimID = pp.cefs_id) -- non-msp
		OR (m.PrimaryPayorCefsID > 0 AND s.PrimID = m.PrimaryPayorCefsID)  --msp
	)
	AND 
	s.ShiftDate BETWEEN pp.pp_startdate AND pp.pp_enddate
	AND s.WKRID = -99
	AND s.NoNeedID > 0
	AND s.CLTID > 0
	AND sc.sc_pointcareformat NOT IN ('R' , '22')---R Medical Treatment and 22 for phone visits
	AND ((s.freq in (0,9) AND s.difpay IN (1,3)) OR (s.freq = 10 AND s.ExcludeFROMBilling = 0))	
	GROUP BY
	pp.pp_id, 		
	pp.ph_timing
	) miss_vst on miss_vst.pp_id = ordr_vst.pp_id
GO
/****** Object:  View [dbo].[VW_CLIENT_SCHED_VISITS_BY_WEEK]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_CLIENT_SCHED_VISITS_BY_WEEK] AS
select ISNULL(pdgm.pp_id,-1) as pp_id, cefw.cefw_id, cefw.cefw_startdate, cefw.cefw_enddate, cefw.cefw_wkno,
csv.*
from HCHB.dbo.CLIENT_EPISODE_FREQUENCY_WEEKS cefw
join HCHB.dbo.CLIENT_SCHED_VISITS csv on cefw.cefw_epiid = csv.csv_epiid
and csv.csv_scheddate between cefw.cefw_startdate and cefw.cefw_enddate
left join VNSNY_BI.[dbo].[fact_medicare_pdgm] pdgm on cefw.cefw_epiid = pdgm.[epi_id]
and csv.csv_scheddate between pdgm.[pp_startDate] and pdgm.[pp_endDate]
GO
/****** Object:  View [dbo].[VW_TIME_BASED_EPISODE_CAREPORT_HOSPITALIZATIONS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO







CREATE view [dbo].[VW_TIME_BASED_EPISODE_CAREPORT_HOSPITALIZATIONS]
as 
SELECT * FROM 
(select DISTINCT VPIN ,CE.EPI_ID,CE.EPI_PAID,TBED_TBEID, 
CAREPORT_VISIT_ID ,ADMIT_DATE,DISCHARGE_DATE,
TBED_EPISODE_START_DATE,
CASE WHEN TBED_PAYOR_CD IN (28226, 28228) THEN dateadd(day, 59, TBED_EPISODE_START_DATE) ELSE TBED_EPISODE_END_DATE END AS TBED_EPISODE_END_DATE, 
TBED_EPI_SOCDATE ,TBED.EPISODE_END_DATE_ACTUAL,[TBED_PAYOR_CD],datediff(day,TBED_EPISODE_START_DATE,ADMIT_DATE) + 1 as EPISODE_DAYS_TO_HOSP, 
PRIMARY_DIAGNOSIS_CODE, 'CRT' AS TBE_TYPE, TBED.CMO_FLAG, 1 as FLAG
from vnsny_bi.[dbo].[VW_TIME_BASED_EPISODE_DTL_CMO] TBED
join (SELECT distinct epi_id ,  epi_paid , VPIN, PID, CONVERT(DATE,ADMIT_DATE) as ADMIT_DATE, CONVERT(DATE,DISCHARGE_DATE) as DISCHARGE_DATE,
epi_SocDate, VISIT_ID as CAREPORT_VISIT_ID, CH.PRIMARY_DIAGNOSIS_CODE
				from hchb.dbo.client_episodes_all CL_E,
					vnsny_bi.[dbo].CAREPORT_WEEKLY_HOPSITALIZATION CH
				where convert(VARCHAR(30), epi_paid) = CH.PID
				AND CL_E.epi_status not in ('DELETED', 'NON-ADMIT')
				AND ADMIT_TYPE = 'Inpatient'
				and epi_slid = 1
	)CE
on CE.epi_id = tbed_epiid
WHERE  TBED_LVL_NUM>0
)A
where ADMIT_DATE between TBED_EPISODE_START_DATE and TBED_EPISODE_END_DATE

union all 

SELECT * FROM 
(SELECT distinct ch.VPIN, c_epi.EPI_ID AS EPI_ID, c_epi.EPI_PAID , null TBED_TBEID,
VISIT_ID as CAREPORT_VISIT_ID,CONVERT(DATE,ADMIT_DATE) as ADMIT_DATE, CONVERT(DATE,DISCHARGE_DATE) as DISCHARGE_DATE, 
c_epi.EPI_STARTOFEPISODE AS TBED_EPISODE_START_DATE, 
case when UPPER(PAYOR_INFO.ps_desc) in ('HUMANA PDGM' , 'UNITED HEALTHCARE MEDICARE PDGM- INTERNAL AUTH CMO') then dateadd(day, 60, c_epi.epi_StartOfEpisode)
else c_epi.EPI_ENDOFEPISODE end as TBED_EPISODE_END_DATE, 
c_epi.EPI_SOCDATE AS TBED_EPI_SOCDATE,c_epi.EPI_ENDOFEPISODE as EPISODE_END_DATE_ACTUAL, PAYOR_INFO.PS_ID AS TBED_PAYOR_CD,
datediff(day,epi_startofepisode,ADMIT_DATE) + 1 as EPISODE_DAYS_TO_HOSP, CH.PRIMARY_DIAGNOSIS_CODE, 'PDGM' AS TBE_TYPE, PDGM.CMO_FLAG, 1 FLAG
	 From hchb.dbo.client_episodes_all c_epi
		JOIN vnsny_bi.[dbo].CAREPORT_WEEKLY_HOPSITALIZATION CH ON convert(VARCHAR(30), c_epi.epi_paid) = CH.PID
		JOIN vnsny_bi.[dbo].[VW_CMO_PDGM] PDGM ON pdgm.epi_id = c_epi.epi_id
		LEFT JOIN (select distinct cefs_epiid, PS_ID, ps_desc  
						from hchb.dbo.client_episode_fs 
						join hchb.dbo.payor_sources on ps_id = cefs_psid
						join hchb.dbo.payor_types on pt_id = cefs_ptid
					where cefs_ps = 'P'and  UPPER(ps_desc) like '%PDGM%') PAYOR_INFO
		ON c_epi.epi_id = PAYOR_INFO.cefs_epiid
	    where c_epi.epi_status not in ('DELETED', 'NON-ADMIT')
			AND ADMIT_TYPE = 'Inpatient'
			and epi_slid = 1
			and PAYOR_INFO.PS_ID is not null 
) A
WHERE ADMIT_DATE between TBED_EPISODE_START_DATE and TBED_EPISODE_END_DATE
;
GO
/****** Object:  View [dbo].[VW_EPISODE_FLAGS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[VW_EPISODE_FLAGS] 
AS
SELECT 
	c.epi_paid, c.epi_id,
	i.DL_AST_WIDE_SK, i.DL_ANS_ASSESSMENT_SK,
	i.IE_EVENT_DATE as OASIS_SOC_HOSPITALIZATION_DATE, 
	datediff(day, c.epi_SocDate, i.IE_EVENT_DATE)+1 as DAYS_TO_HOSPITALIZATION, 
	(CASE WHEN datediff(day, c.epi_SocDate, i.IE_EVENT_DATE)+1 BETWEEN 1 and 30 THEN 1 ELSE 0 END) as HOSPITALIZATION_30_DAY,
	(CASE WHEN datediff(day, c.epi_SocDate, i.IE_EVENT_DATE)+1 BETWEEN 1 and 60 THEN 1 ELSE 0 END) as HOSPITALIZATION_60_DAY,
	(CASE WHEN datediff(day, c.epi_SocDate, i.IE_EVENT_DATE)+1 BETWEEN 1 and 90 THEN 1 ELSE 0 END) as HOSPITALIZATION_90_DAY,
	ROW_NUMBER() OVER(PARTITION BY epi_paid ORDER BY epi_StartOfEpisode ASC) AS epi_sequence_number,
	ROW_NUMBER() OVER(PARTITION BY epi_paid ORDER BY epi_StartOfEpisode desc) AS epi_sequence_number_descending,
	a.M1005_INP_DISCHARGE_DT,
	case when len(trim(f.cef_value)) = 0 then null else trim(f.cef_value) end as 'PRIMARY_LANGUAGE'
	,CASE 
       WHEN M0140_ETHNIC_AI_AN=1 then 'American Indian'
       WHEN M0140_ETHNIC_ASIAN=1 then 'Asian'
       WHEN M0140_ETHNIC_BLACK=1 then 'Black or African-American'
       WHEN M0140_ETHNIC_HISP=1 then 'Hispanic or Latino'
       WHEN M0140_ETHNIC_NH_PI=1 then 'Native Hawaiian or Pacific Islander'
       WHEN M0140_ETHNIC_WHITE=1 then 'White'
       WHEN M0140_ETHNIC_UK=1 then 'White UK'
END as Race,
CASE
       WHEN M0140_ETHNIC_HISP=1 then '1'
       ELSE '0'
END as Hispanic
FROM HCHB.dbo.CLIENT_EPISODES_ALL c
left outer join	VNSNY_BI.[dbo].[INPATIENT_EVENTS] i on c.epi_id = i.IE_EPIID and i.IE_START_USED = 'SOC' and i.IE_HOSPITALIZATION = 1
left outer join 
(SELECT ceo_epiid, MIN(M1005_INP_DISCHARGE_DT) M1005_INP_DISCHARGE_DT
     ,M0140_ETHNIC_AI_AN,M0140_ETHNIC_ASIAN,M0140_ETHNIC_BLACK,M0140_ETHNIC_HISP,M0140_ETHNIC_NH_PI,M0140_ETHNIC_WHITE,M0140_ETHNIC_UK
		FROM VNSNY_BI.[dbo].[AST_WIDE] 
		WHERE M0100_ASSMT_REASON = 1 and DL_ACTIVE_REC_IND = 'Y' 
		GROUP BY ceo_epiid,M0140_ETHNIC_AI_AN,M0140_ETHNIC_AI_AN,M0140_ETHNIC_ASIAN,M0140_ETHNIC_BLACK,M0140_ETHNIC_HISP,M0140_ETHNIC_NH_PI,M0140_ETHNIC_WHITE,M0140_ETHNIC_UK
) a on c.epi_id = a.ceo_epiid 
left outer join  HCHB.dbo.[CLIENT_EPISODE_FLAGS] f on c.epi_id = f.cef_epiid and f.cef_flag = 'PrimaryLanguage';
GO
/****** Object:  View [dbo].[VW_FACT_ACTIVE_PATIENTS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[VW_FACT_ACTIVE_PATIENTS] as
SELECT DAY_DATE , A.* from( 
select	 cea.epi_id,
		cea.epi_paid,
      -- CASE WHEN epi_RecertFlag =  'F' THEN convert(date,epi_SocDate) ELSE convert(date,epi_StartOfEpisode) END AS START_DATE,
          cea.epi_SocDate,
		  cea.epi_StartOfEpisode,
		  cea.epi_EndOfEpisode,
		  cea.epi_DischargeDate,
          cea.epi_AdmitType,
		  cea.epi_status,
          cea.epi_slid, 
		  1 as FLAG,
          RANK() over (partition by cea.epi_paid order by cea.epi_StartOfEpisode ) as epi_sequence_number
from HCHB.dbo.client_episodes_all  cea
	JOIN vnsny_bi.dbo.VW_EPISODE_FLAGS f on cea.epi_id = f.epi_id
where epi_AdmitType in('NEW ADMISSION','RECERTIFICATION','READMISSION')
      and epi_status not in ('NON-ADMIT','DELETED')--, 'PENDING')
         and epi_slid = 1
) A  CROSS JOIN VNSNY_BI.dbo.LU_DAY b
WHERE convert(date,epi_StartOfEpisode) <= DAY_DATE
       AND convert(date,epi_EndOfEpisode) >= DAY_DATE
	   AND DAY_DATE  < CONVERT(date, getdate())

union
SELECT DAY_DATE , A.* from( 
select	 cea.epi_id,
		cea.epi_paid,
      -- CASE WHEN epi_RecertFlag =  'F' THEN convert(date,epi_SocDate) ELSE convert(date,epi_StartOfEpisode) END AS START_DATE,
          cea.epi_SocDate,
		  cea.epi_StartOfEpisode,
		  cea.epi_EndOfEpisode,
		  cea.epi_DischargeDate,
          cea.epi_AdmitType,
		  cea.epi_status,
          cea.epi_slid, 
		  1 as FLAG,
          RANK() over (partition by cea.epi_paid order by cea.epi_StartOfEpisode ) as epi_sequence_number
from HCHB.dbo.client_episodes_all  cea
	JOIN vnsny_bi.dbo.VW_EPISODE_FLAGS f on cea.epi_id = f.epi_id
where epi_AdmitType in('NEW ADMISSION','RECERTIFICATION','READMISSION')
      and epi_status not in ('NON-ADMIT','DELETED')--, 'PENDING')
         and epi_slid = 2
) A  CROSS JOIN VNSNY_BI.dbo.LU_DAY b
WHERE convert(date,epi_StartOfEpisode) <= DAY_DATE
       AND convert(date,epi_EndOfEpisode) >= DAY_DATE
	   AND DAY_DATE  < CONVERT(date, getdate())

GO
/****** Object:  View [dbo].[auditreport_client_485_orders]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[auditreport_client_485_orders] as
select 
  ar_id,
  ar_c485o_id ,
  ar_c485o_c485id,
  ar_colname,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby ,
  ar_datemodified ,
  ar_login ,
  ar_appname,
  ar_action ,
  convert(varchar(50), ar_datemodified, 121) dl_ar_datemodified
  from hchb.dbo.auditreport_client_485_orders;

GO
/****** Object:  View [dbo].[auditreport_client_address]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[auditreport_client_address] as
select
  ar_id            ,
  ar_ca_id         ,
  ar_ca_cslid      ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified   ,
  ar_login         ,
  ar_appname       ,
  ar_action       ,
  convert(varchar(50), ar_datemodified, 121) dl_ar_datemodified
  from hchb.dbo.auditreport_client_address;


GO
/****** Object:  View [dbo].[auditreport_client_diagnoses_and_procedures]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[auditreport_client_diagnoses_and_procedures] as 
select 
  ar_id            ,
  ar_cdp_id        ,
  ar_cdp_epiid     ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified   ,
  ar_login         ,
  ar_appname       ,
  ar_action        ,
  ar_cdp_oid       ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified
  from hchb.dbo.auditreport_client_diagnoses_and_procedures;


GO
/****** Object:  View [dbo].[auditreport_client_episode_date_of_death]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[auditreport_client_episode_date_of_death] as 
select 
  ar_id            ,
  ar_cedd_id       ,
  ar_cedd_epiid    ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified  ,
  ar_login         ,
  ar_appname       ,
  ar_action       ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified 
 from hchb.dbo.auditreport_client_episode_date_of_death;


GO
/****** Object:  View [dbo].[auditreport_client_episode_inpatient_events]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[auditreport_client_episode_inpatient_events] as 
select 
  ar_id            ,
  ar_ceie_id       ,
  ar_ceie_epiid    ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified   ,
  ar_login         ,
  ar_appname       ,
  ar_action        ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified
 from hchb.dbo.auditreport_client_episode_inpatient_events;

GO
/****** Object:  View [dbo].[auditreport_client_episode_oasis]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[auditreport_client_episode_oasis] as
select 
  ar_id            ,
  ar_ceo_id        ,
  ar_ceo_epiid     ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified   ,
  ar_login         ,
  ar_appname       ,
  ar_action       ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified
 from hchb.dbo.auditreport_client_episode_oasis;

GO
/****** Object:  View [dbo].[auditreport_client_episode_oasis_answers]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[auditreport_client_episode_oasis_answers] as
select 
  ar_id            ,
  ar_ceoa_id       ,
  ar_ceoa_ceoid    ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified  ,
  ar_login         ,
  ar_appname       ,
  ar_action        ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified
 from hchb.dbo.auditreport_client_episode_oasis_answers;

GO
/****** Object:  View [dbo].[auditreport_client_episode_visit_notes]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[auditreport_client_episode_visit_notes] as
select 
  ar_id            ,
  ar_cevn_id       ,
  ar_cevn_epiid    ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified ,
  ar_login         ,
  ar_appname       ,
  ar_action     ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified 
 from hchb.dbo.auditreport_client_episode_visit_notes;

GO
/****** Object:  View [dbo].[auditreport_client_episode_visits]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[auditreport_client_episode_visits] as 
select
  ar_id            ,
  ar_cev_id        ,
  ar_cev_epiid     ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified   ,
  ar_login         ,
  ar_appname       ,
  ar_action    ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified
 from hchb.dbo.auditreport_client_episode_visits;

GO
/****** Object:  View [dbo].[auditreport_client_log]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[auditreport_client_log] as
select
  ar_id            ,
  ar_clog_id       ,
  ar_clog_epiid    ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified  ,
  ar_login         ,
  ar_appname       ,
  ar_action  ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified 
from hchb.dbo.auditreport_client_log;

GO
/****** Object:  View [dbo].[auditreport_client_meds]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[auditreport_client_meds] as
select
  ar_id            ,
  ar_cm_id         ,
  ar_cm_epiid      ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified  ,
  ar_login         ,
  ar_appname       ,
  ar_action       ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified
from hchb.dbo.auditreport_client_meds;


GO
/****** Object:  View [dbo].[auditreport_client_orders_all]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[auditreport_client_orders_all] as
select 
  ar_id            ,
  ar_o_id          ,
  ar_o_epiid       ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified ,
  ar_login         ,
  ar_appname       ,
  ar_action  ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified 
from hchb.dbo.auditreport_client_orders_all;


GO
/****** Object:  View [dbo].[auditreport_client_sched_visits]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[auditreport_client_sched_visits] as
select
  ar_id            ,
  ar_csv_id        ,
  ar_csv_epiid     ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified  ,
  ar_login         ,
  ar_appname       ,
  ar_action  ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified
from hchb.dbo.auditreport_client_sched_visits;

GO
/****** Object:  View [dbo].[auditreport_client_supplies]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[auditreport_client_supplies] as
select 
  ar_id            ,
  ar_csu_id        ,
  ar_csu_epiid     ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified   ,
  ar_login         ,
  ar_appname       ,
  ar_action ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified
from hchb.dbo.auditreport_client_supplies;

GO
/****** Object:  View [dbo].[auditreport_invoices]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[auditreport_invoices]
as
SELECT  ar_id
      ,ar_iid
      ,ar_colname
      ,cast(ar_oldval as varchar(8000)) ar_oldval
      ,cast(ar_newval as varchar(8000))  ar_newval
      ,ar_modifiedby
      ,ar_datemodified
      ,ar_login
      ,ar_appname
      ,ar_action
	  ,CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified 
  FROM HCHB.dbo.AUDITREPORT_INVOICES;

GO
/****** Object:  View [dbo].[auditreport_line_items]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[auditreport_line_items] as
select
  ar_id            ,
  ar_liid          ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified   , 
  ar_login         ,
  ar_appname       ,
  ar_action,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified
from hchb.dbo.auditreport_line_items;


GO
/****** Object:  View [dbo].[auditreport_referral_requests]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create  view [dbo].[auditreport_referral_requests] as
select 
  ar_id            ,
  ar_rr_id         ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified  , 
  ar_login         ,
  ar_appname       ,
  ar_action		   ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified
from hchb.dbo.auditreport_referral_requests;

GO
/****** Object:  View [dbo].[auditreport_startofcare_scheduling]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[auditreport_startofcare_scheduling] as
select 
  ar_id            ,
  ar_socs_id       ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified  , 
  ar_login         ,
  ar_appname       ,
  ar_action        ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified 
from hchb.dbo.auditreport_startofcare_scheduling;


GO
/****** Object:  View [dbo].[auditreport_startofcare_scheduling_request]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[auditreport_startofcare_scheduling_request] as
select 
  ar_id            ,
  ar_ssr_id        ,
  ar_ssr_epiid     ,
  ar_colname       ,
  cast(ar_oldval as varchar(8000)) ar_oldval
  ,cast(ar_newval as varchar(8000))  ar_newval,
  ar_modifiedby    ,
  ar_datemodified  ,
  ar_login         ,
  ar_appname       ,
  ar_action        ,
  CONVERT(varchar(50), ar_datemodified, 121) dl_ar_datemodified 
from hchb.dbo.auditreport_startofcare_scheduling_request;

GO
/****** Object:  View [dbo].[EPS_WAGEINFO]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[EPS_WAGEINFO]  as
SELECT
		CEFS_ID = z.vez_cefsid,
		PS_ID = z.vez_psid,
		AUTH_ID = z.vez_authid, 
		EPI_ID = z.vez_epiid,
		RP_ID = r.rp_id,
		WAGE_VALUE = w.wi_value,
		RURAL = (CASE WHEN wtc.wtc_rural = 'Y' THEN 1 ELSE 0 END),
		WAGE_CODE = wtc.wtc_code,
		WAGE_PARITY = w.wi_wageparity		
	FROM  hchb.dbo.V_EPS_ZONES z 
		INNER JOIN hchb.dbo.payor_sources ps ON ps.ps_id = z.vez_psid
		INNER JOIN hchb.dbo.rate_period r ON r.rp_rptid = ps.ps_rptid AND r.rp_sltid =1	
		INNER JOIN hchb.dbo.branches b ON b.branch_code = vez_epibranchcode 		--Use patient's branch address for this NY Medicaid payor
		
		INNER JOIN hchb.dbo.ZIPCODES zc ON zc.zc_zipcode = SUBSTRING(b.branch_zip, 1,5) AND zc.zc_city = b.branch_city --Noticed more than one row in the zipcodes table
																											   --for the same zipcode but different cities
		INNER JOIN hchb.dbo.ZIPCODE_WAGE_TYPE_CODES zwtc ON zwtc.zwtc_zcid = zc.zc_id AND zwtc.zwtc_rpid = r.rp_id 
		INNER JOIN hchb.dbo.wage_indexes w ON w.wi_rpid = r.rp_id and w.wi_wtcid = zwtc.zwtc_wtcid 
		INNER JOIN hchb.dbo.wage_type_codes wtc ON wtc.wtc_id = w.wi_wtcid 		
	WHERE  w.wi_active = 'Y' AND wtc.wtc_active = 'Y' AND zc.zc_active = 'Y' AND b.branch_active = 'Y'
		AND (
					((z.vez_authstartdate  >= r.rp_soe_effectivefrom AND z.vez_authstartdate  <= r.rp_soe_effectiveto) --Always use start of episode date to get rate period for both interim and final claims. 
																													 -- This is specific to this payor.
					AND (z.vez_authstartdate >= r.rp_eoe_effectivefrom and z.vez_authstartdate <= r.rp_eoe_effectiveto))
			)
	    and  z.vez_cefsid in (select cefs_id from hchb.dbo.CLIENT_EPISODE_FS join hchb.dbo.PAYOR_SOURCES on ps_id = cefs_psid where ps_freq = 8)

GO
/****** Object:  View [dbo].[fact_medicare_pps]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




create view [dbo].[fact_medicare_pps] as
select  pps_id,pps_startdate
        ,case when pps_enddate > epi_endofepisode then epi_endofepisode else pps_enddate end as pps_enddate
        ,pps_rap,pps_lupa,pps_outlier, pps_pep 
	    ,pps_msp,pps_therapyvisits,epi_id, epi_startofepisode,epi_endofepisode,epi_status
	    ,cefs_id ,cefs_ps,ps_id,ps_desc,ps_freq,epi_paid ,HHRG,CMSHIPPS ,FinalHIPPS
		,HIPPS,PPSFSID,Payment,EstimatedPayment,ceoid ,epi_RecertFlag ,M2200_THER_NEED_NBR
		,ssh_soecasemix,ssh_adjustedeoecasemix,ssh_soccasemix,ssh_initialHHRG,ssh_finalHHRG,ssh_initialHIPPS
		,ssh_revenue,ssh_cost,ssh_profit,ssh_cashreceived,ssh_adjustments,ssh_zone
		,ISNULL(HHA_VST_SCHD,0)HHA_VST_SCHD,ISNULL(MSW_VST_SCHD,0)MSW_VST_SCHD,ISNULL(SN_VST_SCHD,0)SN_VST_SCHD
		,ISNULL(OT_VST_SCHD,0)OT_VST_SCHD,ISNULL(PT_VST_SCHD,0)PT_VST_SCHD,ISNULL(ST_VST_SCHD,0)ST_VST_SCHD
	    ,ISNULL(HHA_VST_MIS,0)HHA_VST_MIS,ISNULL(MSW_VST_MIS,0)MSW_VST_MIS,ISNULL(SN_VST_MIS,0)SN_VST_MIS
		,ISNULL(OT_VST_MIS,0)OT_VST_MIS,ISNULL(PT_VST_MIS,0)PT_VST_MIS,ISNULL(ST_VST_MIS,0)ST_VST_MIS
		,HHA_VST_ORD ,MSW_VST_ORD ,SN_VST_ORD, OT_VST_ORD ,PT_VST_ORD ,ST_VST_ORD
		,HHA_VST_CMP ,MSW_VST_CMP,SN_VST_CMP, OT_VST_CMP ,PT_VST_CMP , ST_VST_CMP
from
	(
		SELECT pps_id =auth_id , pps_startdate =auth_startdate,pps_enddate=auth_enddate
			   ,pps_rap= auth_rap ,pps_lupa=auth_lupa,pps_outlier=auth_outlier, pps_pep =auth_pep
			   ,pps_msp = auth_msp,pps_therapyvisits=auth_ppstherapyvisits 
			   ,epi_id, epi_startofepisode,epi_endofepisode
			   ,cefs_id = auth_cefsid,cefs_ps,ps_id,ps_desc,ps_freq
			   ,epi_paid ,HHRG,CMSHIPPS ,FinalHIPPS,HIPPS,PPSFSID,Payment,EstimatedPayment,ceoid 
			   ,epi_RecertFlag,epi_status
		FROM         HCHB.dbo.Authorizations auth
			  INNER JOIN HCHB.dbo.Client_Episode_FS cefs ON auth.auth_cefsid = cefs.cefs_id
			  INNER JOIN HCHB.dbo.Payor_Sources ps ON ps.ps_id = cefs_psid
			  INNER JOIN HCHB.dbo.Client_Episodes_all ce ON ce.epi_id = cefs.cefs_epiid 
			  join HCHB.dbo.PPSFS p on CltBillID = auth_id
		WHERE PS_FREQ = 5 
			  AND active =1    ----CHANGE IN 04/02/2019
			  and auth_active = 'Y'
			  and cefs_active = 'Y'
			  AND p.ppstype = 'S' 
		      AND p.TransferStatus <> 3 
			  AND auth_startdate = epi_startofepisode 
			  AND PS_ID = 56   ----CHANGE IN 04/02/2019
			  --and epi_status in('RECERTIFIED','DISCHARGED')
			  --and epi_status in('RECERTIFIED','DISCHARGED','CURRENT') --PAVEL 3/5/2019
	) PPS_EPI
join hchb.dbo.SUMMARY_STATISTIC_HEADERS on PPS_EPI.epi_id = ssh_epiid
left join
   (
        SELECT MISS_VISIT.cltbillid as cltbillid,HHA_VST_SCHD,MSW_VST_SCHD,SN_VST_SCHD,OT_VST_SCHD,PT_VST_SCHD,ST_VST_SCHD
	          ,HHA_VST_MIS,MSW_VST_MIS,SN_VST_MIS,OT_VST_MIS,PT_VST_MIS,ST_VST_MIS
		FROM (
			(Select cltbillid,isnull(HHA,0) as HHA_VST_SCHD ,isnull(MSW,0) AS MSW_VST_SCHD, isnull(SN,0) AS SN_VST_SCHD
				   ,isnull(OT,0) AS OT_VST_SCHD ,isnull(PT,0) AS PT_VST_SCHD,isnull(ST,0) AS ST_VST_SCHD
			 from(
			select cltbillid,sc_discipline,COUNT(*)  as a
			from HCHB.dbo.SCHED 
			LEFT join HCHB.DBO.servicecodes svc with(nolock) on svc.sc_code = sched.flattype
			where Freq = 5 
				  and sched.wkrid > 0 
						and sched.ExcludeFromBilling=0
						and svc.sc_visittype <> 'MEDICAL TREATMENT'
			group by cltbillid,sc_discipline
			) sourcetable
			pivot
			( avg( a )
			  for sc_discipline in(HHA ,MSW ,SN, OT  ,PT ,ST)
			) as pivottable
			) SCHED_VISIT
		JOIN 
			(select cltbillid,isnull(HHA,0) as HHA_VST_MIS ,isnull(MSW,0) AS MSW_VST_MIS, isnull(SN,0) AS SN_VST_MIS 
				   ,isnull(OT,0) AS OT_VST_MIS ,isnull(PT,0) AS PT_VST_MIS,isnull(ST,0) AS ST_VST_MIS
			 from(
			select cltbillid,sc_discipline,COUNT(*)  as a
			from HCHB.dbo.SCHED 
			LEFT join HCHB.DBO.servicecodes svc with(nolock) on svc.sc_code = sched.flattype
			where Freq = 5 
				  and sched.wkrid = -99
						and sched.ExcludeFromBilling=0
						and svc.sc_visittype <> 'MEDICAL TREATMENT'
			group by cltbillid,sc_discipline
			) sourcetable
			pivot
			( avg( a )
			  for sc_discipline in(HHA ,MSW ,SN, OT  ,PT ,ST)
			) as pivottable
			) MISS_VISIT
		 on MISS_VISIT.cltbillid = SCHED_VISIT.cltbillid 
		 )
	) VST_SCHD_MISS
on PPS_EPI.pps_id = VST_SCHD_MISS.cltbillid
left join 
    (select cefw_epiid,isnull(HHA,0) HHA_VST_ORD ,isnull(MSW,0)MSW_VST_ORD ,isnull(SN,0)SN_VST_ORD, 
	        ISNULL(OT,0) OT_VST_ORD ,ISNULL(PT,0) PT_VST_ORD ,ISNULL(ST,0) ST_VST_ORD
	from(
		  select cefw_epiid,ceov_discipline
			   ,SUM(ceov_visits) as sum_visits
		  from HCHB.dbo.CLIENT_EPISODE_ORDERED_VISITS
		  join HCHB.dbo.CLIENT_EPISODE_FREQUENCY_WEEKS
		     on  ceov_cefwid = cefw_id 
		  group by cefw_epiid,ceov_discipline
		  ) sourcetable
	 pivot
	     ( avg( sum_visits )
	      for ceov_discipline in(HHA ,MSW ,SN, OT  ,PT ,ST, SCC)
	     ) as pivottable) VST_ORD
on PPS_EPI.epi_id = VST_ORD.cefw_epiid
LEFT JOIN
    (select CEV_EPIID,isnull(HHA,0) HHA_VST_CMP ,isnull(MSW,0)MSW_VST_CMP,isnull(SN,0)SN_VST_CMP, 
	        ISNULL(OT,0) OT_VST_CMP ,ISNULL(PT,0) PT_VST_CMP ,ISNULL(ST,0) ST_VST_CMP
	from(
		  select SC_DISCIPLINE,CEV_EPIID,COUNT(CEV_ID) AS VST_CONT
		  from hchb.dbo.client_episode_visits
			 join hchb.dbo.servicecodes on sc_id = CEV_SC_ID
			 --JOIN VNSNY_BI.DBO.fact_medicare_pps ON EPI_ID = CEV_EPIID
		  where sc_billable  = 'Y'
		  GROUP by  SC_DISCIPLINE,CEV_EPIID
		  ) sourcetable
	 pivot
	     ( avg( VST_CONT )
	      for SC_DISCIPLINE in(HHA ,MSW ,SN, OT  ,PT ,ST, SCC)
	     ) as pivottable)  VST_CMP
ON PPS_EPI.epi_id = VST_CMP.CEV_EPIID
left join
	(select CEO_EPIID,M2200_THER_NEED_NBR from 
		(
		select rank() over ( partition by ceo_epiid order by asst_date desc,ceoa_oasisanswer_eff_dt desc) as rank_num,CEO_EPIID,
		 M2200_THER_NEED_NBR
		from 
		vnsny_bi.dbo.AST_WIDE a
		 where DL_ACTIVE_REC_IND = 'Y' 
		   and M0100_ASSMT_REASON IN (1,3,4,5)
		) oasis_m2200
		where rank_num = 1 ) OASIS
	on OASIS.CEO_EPIID = PPS_EPI.epi_id

GO
/****** Object:  View [dbo].[LINE_ITEMS_REVENUE]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[LINE_ITEMS_REVENUE]  
AS  
	select
		lir_lineitemid = li.li_id, 
		lir_lineitemtypeid = li.li_litid,
		lir_revenueid = rt.rt_id, 
		lir_revenuetypeid = rt.rt_rttid, 
		lir_from_revenueid = rt.rt_from_rtid, 	
		lir_calculatedamount = li_calculatedamount, 
		lir_revenueamount = rt.rt_amount, 
		lir_revenueadjustedamount = rt.rt_adjustedamount, 
		lir_branchcode = rt.rt_branchcode, 
		lir_payorsourceid = rt.rt_psid, 
		lir_payorsourcefrequency = rt.rt_pfid, 
		lir_clientid = rt.rt_paid, 
		lir_scheduleid = rt.rt_scheduleid, 
		lir_ppsfsid = rt.rt_ppsfsid, 
		lir_hnsrdid = li.li_hnsrdid,
		lir_finalclaim = rt.rt_finalclaim,
		lir_epilevelofcareid = rt.rt_celocid, 
		lir_roomandboarddetailid = rt.rt_cmrbdid, 
		lir_physicianserviceid = rt.rt_phsid, 
		lir_supplyid = rt.rt_suid, 
		lir_epilabtestid = rt.rt_celtid, 
		lir_cevclaimcodeid = rt.rt_cevccid, 
		lir_miscchargetypeid = rt.rt_mctid, 
		lir_workerid = li_wkrid,
		lir_servicedate = li_servicedate, 
		lir_begintime = li_begintime, 
		lir_durationinseconds = li_durationinseconds, 
		lir_durationinhours = li_durationinhours, 
		lir_begintime1900 = cast(li_begintime as datetime),
		lir_endtime1900 = dateadd(ss, li_durationinseconds, cast(li_begintime as datetime)),
		lir_ratetype = 
			case 
				when rt_deleted = 1 then 'DELETED' 
				when rt_rttid = 1 and li_calculatebyrateonly = 1 then 'INTVIS' 
				when rt_rttid = 1 then 'HRVISIT'
				when rt_rttid = 2 then 'PPS'
				else 'CurMisc' 
			end,
		lir_units = li_units, 
		lir_rate = li_rate, 
		lir_calculatebyrateonly = li_calculatebyrateonly, 
		lir_visithours = 
			case 
				when li.li_litid in (27,28) then li.li_durationinhours --case rate bundled visits
				when rt.rt_rttid = 1 THEN isnull(li.li_units, li.li_durationinhours)
				else cast(0 as decimal(15, 4))
			end, 
		lir_visitrate = 
			case 
				when rt.rt_rttid = 1 then cast(li.li_rate as money)
				else cast(0 as money)
			end, 
		lir_miscunits = 
			case 
				when rt.rt_rttid = 1 then cast(0 as decimal(15, 4))
				else li.li_units  
			end, 
		lir_miscrate = 
			case 
				when rt.rt_rttid = 1 then cast(0 as money)
				else cast(li.li_rate as money)
			end,
		lir_cltrtid = li_cltrtid, 
		lir_shift = li_shift, 
		lir_description = li_description, 
		lir_notes = li_notes,
		lir_descriptionandnotes = li_description + isnull(' / ' + li_notes, ''),
		lir_invoiceid = li_iid, 
		lir_invoicepostdate = li_invoicepostdate,
		lir_contractualadjustmentdate = rt.rt_contractualadjustmentdate, 
		lir_original_invoiceid = li_original_iid, 
		lir_rebill = li.li_rebill,
		lir_transfertypeid = li.li_transfertypeid,
		lir_transferredfrom_lineitemid = li.li_transferredfrom_liid,
		lir_programid = li_pgid, 
		lir_authorizationid = li_authid, 
		lir_epifundingsourceid = li_cefsid, 
		lir_payorsourcebranchid = li_psbid,
		lir_episodeid = li_epiid, 
		lir_servicelineid = li_slid,
		lir_jobdescriptionid = li_jdid, 
		lir_servicecodeid = li_scid,
		lir_patientpayorbillingperiodid = li.li_ppbpid, 
		lir_revenuecode = li_revenuecode, 
		lir_hcpcs = li_hcpcs, 
		lir_routinesupply = li_routinesupply,
		lir_woundcaresupply = li_woundcaresupply,
		lir_icd1 = li_icd1, 
		lir_icd2 = li_icd2, 
		lir_icd3 = li_icd3, 
		lir_icd4 = li_icd4,
		lir_edibatchid = li_edibatchid,
		lir_ediexportdate = li_ediexportdate,
		lir_processdate = li_processdate,
		lir_covered = rt.rt_covered,
        lir_void = li.li_void,
		lir_includeonclaim = li_includeonclaim, 
		--OutputOnClaims attempts to consolidate the 837 output logic and combine it with LineItem logic in this view
		--This field is a work in progress and will be built on in later tickets - CN - 8/30/2013
		lir_outputonclaim = 
			CASE
				WHEN rt.rt_deleted = 0
					AND
					(
						(rt.rt_pfid = 9) --case rate payors can allow $0 amounts
						OR
						(rt.rt_rttid = 1 AND isnull(li.li_units, li.li_durationinseconds) > 0 and li.li_rate > 0)
						OR 
						(rt.rt_rttid in (5, 6, 7, 9) AND li.li_units > 0 and li.li_rate > 0) --- xsort logic -- Missing logic for LOC and CMRB	
						OR 
						(rt.rt_rttid = 10 AND (li.li_units > 0 OR li.li_transferredfrom_liid is not null)) --does not care if transfer balance was undone
					)
					AND rt.rt_covered = 1   --current logic only pulls covered items
				THEN cast(1 as bit)
				ELSE cast(0 as bit)
			END,
		lir_insertedby = li.li_insertedby, 
		lir_insertedbyworkerid = li.li_insertedbywkrid, 
		lir_insertdate = li.li_insertdate, 
		lir_lastupdatedby = li.li_lastupdatedby, 
		lir_lastupdatedbyworkerid = li.li_lastupdatedbywkrid, 
		lir_lastupdate = li.li_lastupdate
	from hchb.billing.LINE_ITEMS li
	join hchb.accounting.REVENUE_TRANSACTIONS rt on rt.rt_id = li.li_rtid
	where rt.rt_deleted = 0
GO
/****** Object:  View [dbo].[pc_patients1]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[pc_patients1] as
select 
	 agid ,
	sessnum ,
	active ,
	csvid ,
	visitstatus ,
	patientid ,
	visitdate ,
	visitnumber ,
	epiid ,
	medrecord ,
	firstname ,
	lastname ,
	address ,
	city ,
	county ,
	state ,
	zip ,
	homephone ,
	sex ,
	dob ,
	svccode ,
	cast(directions as varchar(8000)) directions ,
	physid ,
	pharmname ,
	pharmphone ,
	reasoncodeid ,
	reasoncodedate ,
	reschedindays ,
	startodo ,
	endodo ,
	soc ,
	mi ,
	nickname ,
	ssn ,
	workphone ,
	altphone ,
	email ,
	evaldisc ,
	newordertypeid ,
	medreleasecode ,
	admissionsource ,
	dcstatus ,
	dccondition ,
	signatureby ,
	caregiverreason ,
	hospadmitdate ,
	hospdcdate ,
	hospreason ,
	altphysid ,
	raceid ,
	billable ,
	hospitalname ,
	episodestartdate ,
	episodeenddate ,
	facilitytype ,
	hospmrnumber ,
	newepiid ,
	starttime ,
	endtime ,
	tripfees ,
	mileagepaymethod ,
	medidsource ,
	insertdate ,
	processeddate ,
	serviceline ,
	slfloor ,
	slroom ,
	cast(slcomments as varchar(8000)) slcomments ,
	unabletocollectallvs ,
	vsoc ,
	intakeheight ,
	intakeweight ,
	abnanswer ,
	mdid ,
	datetimeofdeath ,
	casemanagerid ,
	episodetiming ,
	completionrequired ,
	episodetimingchanged ,
	latevisit ,
	setsocdateflag ,
	enablehospiceinpatientencounter ,
	earlierbillablevisit ,
	takegpsatvisitstart ,
	takegpsatvisitend ,
	medconsultbypharmacy ,
	totalcareminutes ,
	fsheaderid ,
	demographicschanges ,
	therapyreassessmenttype ,
	icdcodeversionid ,
	dcreasonid ,
	schemaversion ,
	convert(varchar(50), insertdate, 121) dl_insertdate
	from hchb.dbo.pc_patients1;

GO
/****** Object:  View [dbo].[PDGM_TARGETS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create  view [dbo].[PDGM_TARGETS]
AS 
SELECT TEAM, MONTHS, CASE WHEN MONTHS ='JAN' THEN '202101' 
 WHEN MONTHS ='FEB' THEN '202102'
 WHEN MONTHS ='MAR' THEN '202103'
 WHEN MONTHS ='APR' THEN '202104'
 WHEN MONTHS ='MAY' THEN '202105'
 WHEN MONTHS ='JUN' THEN '202106'
 WHEN MONTHS ='JUL' THEN '202107'
 WHEN MONTHS ='AUG' THEN '202108'
 WHEN MONTHS ='SEP' THEN '202109'
 WHEN MONTHS ='OCT' THEN '202110'
 WHEN MONTHS ='NOV' THEN '202111'
 WHEN MONTHS ='DEC' THEN '202112'
END MONTH_ID, '2021' YEAR,
CONVERT(DECIMAL(10,2),TARGETS) AS TARGETS, 'Admission Budget' AS SCORE_CARD_TYPE,
NULL ADJUSTMENT, null ADMISSION_SOURCE, null TIMING, null DISCIPLINE, null PAYOR_TYPE, NULL PAYOR_ID
FROM
(SELECT *
FROM VNSNY_BI.[dbo].[STG_PDGM_ADMISSION_2021]) T
UNPIVOT (TARGETS FOR MONTHS IN (JAN, FEB, MAR,APR,MAY,JUN, JUL, AUG, SEP,OCT,NOV,DEC)
) AS UNPVT

union all 


SELECT TEAM, MONTHS, CASE WHEN MONTHS ='JAN' THEN '202101' 
 WHEN MONTHS ='FEB' THEN '202102'
 WHEN MONTHS ='MAR' THEN '202103'
 WHEN MONTHS ='APR' THEN '202104'
 WHEN MONTHS ='MAY' THEN '202105'
 WHEN MONTHS ='JUN' THEN '202106'
 WHEN MONTHS ='JUL' THEN '202107'
 WHEN MONTHS ='AUG' THEN '202108'
 WHEN MONTHS ='SEP' THEN '202109'
 WHEN MONTHS ='OCT' THEN '202110'
 WHEN MONTHS ='NOV' THEN '202111'
 WHEN MONTHS ='DEC' THEN '202112'
END MONTH_ID, '2021' YEAR,
convert(DECIMAL(10, 2), TARGETS ) as TARGETS, 'Referral Budget' AS SCORE_CARD_TYPE ,
NULL ADJUSTMENT, NULL Admission_source, NULL Timing, null Discipline, null PAYOR_TYPE, NULL PAYOR_ID
FROM
(SELECT *
FROM VNSNY_BI.[dbo].[STG_PDGM_Referral_2021]) T
UNPIVOT (TARGETS FOR MONTHS IN (JAN, FEB, MAR,APR,MAY,JUN, JUL, AUG, SEP,OCT,NOV,DEC)
) AS UNPVT

union all 

SELECT TEAM, MONTHS, CASE WHEN MONTHS ='JAN' THEN '202101' 
 WHEN MONTHS ='FEB' THEN '202102'
 WHEN MONTHS ='MAR' THEN '202103'
 WHEN MONTHS ='APR' THEN '202104'
 WHEN MONTHS ='MAY' THEN '202105'
 WHEN MONTHS ='JUN' THEN '202106'
 WHEN MONTHS ='JUL' THEN '202107'
 WHEN MONTHS ='AUG' THEN '202108'
 WHEN MONTHS ='SEP' THEN '202109'
 WHEN MONTHS ='OCT' THEN '202110'
 WHEN MONTHS ='NOV' THEN '202111'
 WHEN MONTHS ='DEC' THEN '202112'
END MONTH_ID, '2021' YEAR,
TARGETS, 'LUPA Budget' AS SCORE_CARD_TYPE ,
NULL ADJUSTMENT, NULL Admission_source, NULL Timing, null Discipline, lupa_TYPE as PAYOR_TYPE, 
case when LUPA_TYPE LIKE 'HUMANA%' THEN 28297
	WHEN LUPA_TYPE LIKE 'Medicare%' THEN 28348
	WHEN LUPA_TYPE LIKE 'UHC%' THEN 28296 END PAYOR_ID
FROM
(SELECT *
FROM VNSNY_BI.[dbo].[STG_PDGM_LUPA_Budget-2021]) T
UNPIVOT (TARGETS FOR MONTHS IN (JAN, FEB, MAR,APR,MAY,JUN, JUL, AUG, SEP,OCT,NOV,DEC)
) AS UNPVT

UNION ALL 

SELECT TEAM, MONTHS, CASE WHEN MONTHS ='JAN' THEN '202101' 
 WHEN MONTHS ='FEB' THEN '202102'
 WHEN MONTHS ='MAR' THEN '202103'
 WHEN MONTHS ='APR' THEN '202104'
 WHEN MONTHS ='MAY' THEN '202105'
 WHEN MONTHS ='JUN' THEN '202106'
 WHEN MONTHS ='JUL' THEN '202107'
 WHEN MONTHS ='AUG' THEN '202108'
 WHEN MONTHS ='SEP' THEN '202109'
 WHEN MONTHS ='OCT' THEN '202110'
 WHEN MONTHS ='NOV' THEN '202111'
 WHEN MONTHS ='DEC' THEN '202112'
END MONTH_ID, '2021' YEAR,
TARGETS, 'LUPA Budget' AS SCORE_CARD_TYPE ,
NULL ADJUSTMENT,NULL Admission_source, NULL Timing, null Discipline, CMI_TYPE As PAYOR_TYPE, 
case when CMI_TYPE LIKE 'HUMANA%' THEN 28297
	WHEN CMI_TYPE LIKE 'Medicare%' THEN 28348
	WHEN CMI_TYPE LIKE 'UHC%' THEN 28296 END PAYOR_ID
FROM
(SELECT *
FROM VNSNY_BI.[dbo].[STG_PDGM_CMI_2021]) T
UNPIVOT (TARGETS FOR MONTHS IN (JAN, FEB, MAR,APR,MAY,JUN, JUL, AUG, SEP,OCT,NOV,DEC)
) AS UNPVT

UNION ALL 

SELECT TEAM, MONTHS, CASE WHEN MONTHS ='JAN' THEN '202101' 
 WHEN MONTHS ='FEB' THEN '202102'
 WHEN MONTHS ='MAR' THEN '202103'
 WHEN MONTHS ='APR' THEN '202104'
 WHEN MONTHS ='MAY' THEN '202105'
 WHEN MONTHS ='JUN' THEN '202106'
 WHEN MONTHS ='JUL' THEN '202107'
 WHEN MONTHS ='AUG' THEN '202108'
 WHEN MONTHS ='SEP' THEN '202109'
 WHEN MONTHS ='OCT' THEN '202110'
 WHEN MONTHS ='NOV' THEN '202111'
 WHEN MONTHS ='DEC' THEN '202112'
END MONTH_ID, '2021' YEAR,
TARGETS, 'Comorbidity Adjustment Budget' AS SCORE_CARD_TYPE ,
ADJUSTMENT,NULL Admission_source, NULL Timing,null Discipline, ADJUSTMENT_TYPE  As PAYOR_TYPE, 
case when ADJUSTMENT_TYPE LIKE 'HUMANA%' THEN 28297
	WHEN ADJUSTMENT_TYPE LIKE 'Medicare%' THEN 28348
	WHEN ADJUSTMENT_TYPE LIKE 'UHC%' THEN 28296 END PAYOR_ID
FROM
(SELECT *
FROM VNSNY_BI.[dbo].[STG_PDGM_Comorbidity Adjustment_2021]) T
UNPIVOT (TARGETS FOR MONTHS IN (JAN, FEB, MAR,APR,MAY,JUN, JUL, AUG, SEP,OCT,NOV,DEC)
) AS UNPVT

UNION ALL 

SELECT TEAM, MONTHS, CASE WHEN MONTHS ='JAN' THEN '202101' 
 WHEN MONTHS ='FEB' THEN '202102'
 WHEN MONTHS ='MAR' THEN '202103'
 WHEN MONTHS ='APR' THEN '202104'
 WHEN MONTHS ='MAY' THEN '202105'
 WHEN MONTHS ='JUN' THEN '202106'
 WHEN MONTHS ='JUL' THEN '202107'
 WHEN MONTHS ='AUG' THEN '202108'
 WHEN MONTHS ='SEP' THEN '202109'
 WHEN MONTHS ='OCT' THEN '202110'
 WHEN MONTHS ='NOV' THEN '202111'
 WHEN MONTHS ='DEC' THEN '202112'
END MONTH_ID, '2021' YEAR,
TARGETS, 'Functional Impairment Lvl Budget' AS SCORE_CARD_TYPE ,
Adjustment, NULL Admission_source, NULL Timing, null Discipline, payor_type,
case when payor_type LIKE 'HUMANA%' THEN 28297
	WHEN payor_type LIKE 'Medicare%' THEN 28348
	WHEN payor_type LIKE 'UHC%' THEN 28296 END PAYOR_ID
FROM
(SELECT *
FROM VNSNY_BI.[dbo].[STG_PDGM_Functional Impairment Lvl_2021]) T
UNPIVOT (TARGETS FOR MONTHS IN (JAN, FEB, MAR,APR,MAY,JUN, JUL, AUG, SEP,OCT,NOV,DEC)
) AS UNPVT

UNION ALL 

SELECT TEAM, MONTHS, CASE WHEN MONTHS ='JAN' THEN '202101' 
 WHEN MONTHS ='FEB' THEN '202102'
 WHEN MONTHS ='MAR' THEN '202103'
 WHEN MONTHS ='APR' THEN '202104'
 WHEN MONTHS ='MAY' THEN '202105'
 WHEN MONTHS ='JUN' THEN '202106'
 WHEN MONTHS ='JUL' THEN '202107'
 WHEN MONTHS ='AUG' THEN '202108'
 WHEN MONTHS ='SEP' THEN '202109'
 WHEN MONTHS ='OCT' THEN '202110'
 WHEN MONTHS ='NOV' THEN '202111'
 WHEN MONTHS ='DEC' THEN '202112'
END MONTH_ID, '2021' YEAR,
TARGETS, 'Admission Timing Budget' AS SCORE_CARD_TYPE ,
NULL Adjustment, Admission_source,Timing, null Discipline, payor_type,
case when payor_type LIKE 'HUMANA%' THEN 28297
	WHEN payor_type LIKE 'Medicare%' THEN 28348
	WHEN payor_type LIKE 'UHC%' THEN 28296 END PAYOR_ID
FROM
(SELECT *
FROM VNSNY_BI.[dbo].[STG_PDGM_Admission Timing_2021]) T
UNPIVOT (TARGETS FOR MONTHS IN (JAN, FEB, MAR,APR,MAY,JUN, JUL, AUG, SEP,OCT,NOV,DEC)
) AS UNPVT

union all 

SELECT TEAM, MONTHS, CASE WHEN MONTHS ='JAN' THEN '202101' 
 WHEN MONTHS ='FEB' THEN '202102'
 WHEN MONTHS ='MAR' THEN '202103'
 WHEN MONTHS ='APR' THEN '202104'
 WHEN MONTHS ='MAY' THEN '202105'
 WHEN MONTHS ='JUN' THEN '202106'
 WHEN MONTHS ='JUL' THEN '202107'
 WHEN MONTHS ='AUG' THEN '202108'
 WHEN MONTHS ='SEP' THEN '202109'
 WHEN MONTHS ='OCT' THEN '202110'
 WHEN MONTHS ='NOV' THEN '202111'
 WHEN MONTHS ='DEC' THEN '202112'
END MONTH_ID, '2021' YEAR,
TARGETS, 'Non-LUPA VPP Budget' AS SCORE_CARD_TYPE ,
NULL Adjustment, null ADMISSION_SOURCE, null TIMING, DISCIPLINE, payor_type,
case when payor_type LIKE 'HUMANA%' THEN 28297
	WHEN payor_type LIKE 'Medicare%' THEN 28348
	WHEN payor_type LIKE 'UHC%' THEN 28296 END PAYOR_ID
FROM
(SELECT *
FROM VNSNY_BI.[dbo].STG_PDGM_Non_LUPA_VPP_2021) T
UNPIVOT (TARGETS FOR MONTHS IN (JAN, FEB, MAR,APR,MAY,JUN, JUL, AUG, SEP,OCT,NOV,DEC)
) AS UNPVT
;
GO
/****** Object:  View [dbo].[risk_score_with_all]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[risk_score_with_all]  as
 with t
 as
 (
 select epi_paid,epi_id,cev_id,ceo_ceoid,m0100_assmt_reason,rsh_lrlid,r ,asst_date
 from zzz_risk_score_show_priv  where rsh_lrlid is not null
 union all
 select  t1.epi_paid,t1.epi_id,t1.cev_id,t1.ceo_ceoid,t1.m0100_assmt_reason,t.rsh_lrlid,t1.r ,t.asst_date
 from t inner join zzz_risk_score_show_priv　t1
 on t.r+1= t1.r  and t1.epi_paid = t.epi_paid 
 where t1.rsh_lrlid is null
 )
 select * from t
GO
/****** Object:  View [dbo].[v_careport_patient]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[v_careport_patient] as
select
	distinct
	PID,
	CA.pa_id,
	admit_type,
	ADMIT_DATE,
	DISCHARGE_DATE,
	VPIN,
	VISIT_ID
from vnsny_bi.[dbo].CAREPORT_WEEKLY_HOPSITALIZATION CH WITH (INDEX(0)) 
 , hchb.dbo.CLIENTS_ALL ca  WITH (INDEX(0))  
	where  CH.PID like concat('%|', ca.pa_id ,'|%') 
GO
/****** Object:  View [dbo].[VW_CLIENT_DIAGNOSES_AND_PROCEDURES_NON_485]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[VW_CLIENT_DIAGNOSES_AND_PROCEDURES_NON_485]
AS
SELECT * FROM HCHB.dbo.CLIENT_DIAGNOSES_AND_PROCEDURES WHERE cdp_DiagnosisProcedureTypeSourceId = 2 --non-POC/485 order
GO
/****** Object:  View [dbo].[VW_CLIENT_DIAGNOSES_AND_PROCEDURES_PRIMARY]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



 CREATE VIEW [dbo].[VW_CLIENT_DIAGNOSES_AND_PROCEDURES_PRIMARY]
 AS
  -- before adding CHHA/Hospice logic--
/*
 select * 
 from HCHB.dbo.CLIENT_DIAGNOSES_AND_PROCEDURES
 where cdp_SortOrder = 10 and cdp_DiagnosisProcedureTypeSourceId = 3
 and cdp_id in (Select max(cdp_id) from HCHB.dbo.CLIENT_DIAGNOSES_AND_PROCEDURES
group by cdp_epiid, cdp_SortOrder, cdp_DiagnosisProcedureTypeSourceId)
*/

select a.* 
from HCHB.dbo.CLIENT_DIAGNOSES_AND_PROCEDURES a  LEFT JOIN 
		HCHB.dbo.CLIENT_EPISODES_ALL b ON a.cdp_epiid = b.epi_id
where	a.cdp_SortOrder =  10 and 
		a.cdp_DiagnosisProcedureTypeSourceId = CASE b.epi_slid WHEN 1 THEN 3 ELSE 1 END
 and a.cdp_id in (Select max(cdp_id) from HCHB.dbo.CLIENT_DIAGNOSES_AND_PROCEDURES
group by cdp_epiid, cdp_SortOrder, cdp_DiagnosisProcedureTypeSourceId)

GO
/****** Object:  View [dbo].[VW_CLIENT_DIAGNOSES_AND_PROCEDURES_REFERRAL]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[VW_CLIENT_DIAGNOSES_AND_PROCEDURES_REFERRAL]
AS
SELECT * FROM HCHB.dbo.CLIENT_DIAGNOSES_AND_PROCEDURES WHERE cdp_DiagnosisProcedureTypeSourceId = 1 --At the Referral
GO
/****** Object:  View [dbo].[VW_CLIENT_EPISODE_CLAIMSAUDIT_RANK]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


  create view [dbo].[VW_CLIENT_EPISODE_CLAIMSAUDIT_RANK]
  AS
Select rank_flag ,cec_epiid,cec_chrid,cec_chcomment,cec_insertdate,cec_lastupdate,cec_modifiedby,cec_priority,cec_id
from (select rank() over (partition by cec_epiid order by cec_priority) as rank_flag
       ,cec_epiid,cec_chrid,cec_chcomment,cec_insertdate,cec_lastupdate,cec_modifiedby,cec_priority,cec_id
from hchb.[dbo].[CLIENT_EPISODE_CLAIMSAUDIT]) sub
where rank_flag = 1
GO
/****** Object:  View [dbo].[VW_CLIENT_EPISODE_FS_PRIMARY_PAYER]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[VW_CLIENT_EPISODE_FS_PRIMARY_PAYER]   as
	select  * from hchb.dbo.CLIENT_EPISODE_FS 
	where cefs_ps= 'P' and cefs_active = 'Y' 
GO
/****** Object:  View [dbo].[VW_CLIENT_EPISODE_FS_SECONDARY_PAYER]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



create view [dbo].[VW_CLIENT_EPISODE_FS_SECONDARY_PAYER]   as
  	select  a.* from hchb.dbo.CLIENT_EPISODE_FS a
	INNER JOIN
        (
            SELECT  cefs_epiid, MAX(cefs_lastupdate) as cefs_lastupdate
            FROM    hchb.dbo.CLIENT_EPISODE_FS 
			where cefs_ps= 'S' and cefs_active = 'Y'
            GROUP BY cefs_epiid
        ) b ON  a.cefs_epiid = b.cefs_epiid AND
                a.cefs_lastupdate = b.cefs_lastupdate
	where a.cefs_ps= 'S' and a.cefs_active = 'Y' 
GO
/****** Object:  View [dbo].[VW_CLIENT_EPISODE_TEAM_MEMBERS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[VW_CLIENT_EPISODE_TEAM_MEMBERS] as
select * from HCHB.dbo.CLIENT_EPISODE_CASE_MANAGERS where cecm_primary <> 'Y'
GO
/****** Object:  View [dbo].[VW_COVID19_SCREENING]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE view [dbo].[VW_COVID19_SCREENING] as
select epi_id,epi_paid,epi_slid,CEV_ID,CEV_VISITDATE
         ,CASE when [1127645]=1582 or [1127671]=1582  
               then case when ([1127647] = 1582 or [1127673]=1582) 
			                   and ([1127648] = 1582 or [1127674]=1582) 
						 then 1
                         else  0
                         end
               else 0
           end as trv_smpt
      ,CASE when ([1127662]=1 OR [1127675]=1)
	             and ([1127645]=1200760 OR [1127671]=1200760)
               then CASE when ([1127647] = 1582 OR [1127673]=1582)
			                  and ([1127663]=910036 OR [1127676]=910036)
                               then 1
                                    else 0
                           end
                     else 0
              end as no_cntct_smpt  --No contact and exposure
       ,CASE when ([1127662]=1200386 and [1127665]=1200386)
	              or ([1127646] = 1582 and [1127649]=1200386)
				  OR [1127675] = 1200386
             then CASE when ([1127647] = 1582 OR [1127673]=1582)
			             then  1
			             when ([1127663] = 910036 OR [1127676] =910036)
                         then 1
                         else 0
                         end
              else 0
              end as cntct_smpt  --Contact and Symptoms
		,CASE when( ([1127663]=910036)
	              or ([1127647] = 1582)
				  OR ([1127648] = 1582)
				  or ([1127673] = 1582)
				  or ([1127674] = 1582)
				  or ([1127676] = 910036))
			             then  1
              else 0
              end as symptoms  --Symptoms
from (
       select * from(
       select epi_id,epi_slid,epi_paid,CEV_ID,cevaa_ceva_aid,Q_ID,CEV_VISITDATE
       from   HCHB.dbo.CLIENT_EPISODE_VISIT_ASSESSITEMS a11
              left outer join      HCHB.dbo.CLIENT_EPISODE_VISIT_ASSESSITEMS_ANSWERS      a12
                on   (a11.ceva_id = a12.cevaa_ceva_id)
              left outer join      HCHB.dbo.QUESTIONS   a13
                on   (a11.ceva_qid = a13.Q_ID)
              left outer join      HCHB.dbo.CLIENT_EPISODES_ALL      a14
                on   (a11.ceva_epiid = a14.epi_id)
              left outer join      HCHB.dbo.CLIENT_EPISODE_VISITS_ALL a15
                on   (a11.ceva_cevid = a15.CEV_ID)
       where a13.Q_AICID = 25064  --
              and CAST(a15.CEV_VISITDATE as date) >=  '2020-02-06'  --they started screening on this date
              and cevaa_ceva_aid is not null 
              --and epi_paid in ( 192980, 205654)
              --and cev_id in (2118220,2119122)
       ) a 
       PIVOT(
              avg(cevaa_ceva_aid)
              FOR Q_ID IN (
                           [1127645],  --HAVE YOU RETURNED FROM CHINA, SOUTH KOREA, ITALY, IRAN OR JAPAN IN THE PAST 14 DAYS?
                           [1127646],  --HAVE YOU HAD CLOSE CONTACT WITH A PERSON WHO HAS THE 2019 NOVEL CORONAVIRUS?
                           [1127647],  --DO YOU HAVE A FEVER?
                           [1127648],  --DO YOU HAVE A COUGH OR SHORTNESS OF BREATH?
                           [1127649],  --HAS THE 2019 NOVEL CORONAVIRUS BEEN CONFIRMED IN THE CONTACT PERSON?
						   [1127662],  --HAVE YOU HAD CLOSE CONTACT WITH A PERSON KNOWN TO HAVE COVID-19 ILLNESS?
                           [1127663],  --DO YOU HAVE SEVERE SYMPTOMS OF LOWER RESPIRATORY ILLNESS (PNEUMONIA, ACUTE RESPIRATORY DISTRESS SYNDROME [ARDS])?
                           [1127665],  --THE CONTACT PERSON UNDER INVESTIGATION (PUI) FOR COVID-19 - OR - HAS COVID19 BEEN CONFIRMED IN THE CONTACT PERSON?
                           [1127671],  --HAVE YOU OR ANYONE RESIDING IN THE HOUSEHOLD HAD INTERNATIONAL TRAVEL WITHIN THE LAST 14 DAYS TO COUNTRIES WITH  SUSTAINED COMMUNITY TRANSMISSION?
	                       [1127672],  --HAVE YOU, OR A HOUSEHOLD MEMBER, HAD CLOSE CONTACT WITH A PERSON WHO HAS THE 2019 NOVEL CORONAVIRUS?
						   [1127673],  --DO YOU, OR A HOUSEHOLD MEMBER, HAVE A FEVER?
						   [1127674],  --DO YOU, OR A HOUSEHOLD MEMBER, HAVE A COUGH OR SHORTNESS OF BREATH?)
						   [1127675],  --HAVE YOU, OR A HOUSEHOLD MEMBER, HAD CLOSE CONTACT WITH A PERSON KNOWN TO HAVE COVID-19 ILLNESS?
	                       [1127676],  --DO YOU, OR A HOUSEHOLD MEMBER, HAVE SEVERE SYMPTOMS OF LOWER RESPIRATORY ILLNESS (PNEUMONIA, ACUTE RESPIRATORY DISTRESS SYNDROME [ARDS])?
						   [1127688],  --HAS THE COVID-19 PRE SCREEN BEEN DONE TODAY?
                           [1127696]   --HAVE YOU OR ANYONE IN THE HOUSEHOLD BEEN TESTED AND CONFIRMED TO HAVE COVID-19?
	   )) AS pivot_table
       ) a

GO
/****** Object:  View [dbo].[VW_COVID19_SYMPTOMS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






CREATE view [dbo].[VW_COVID19_SYMPTOMS] as
       select epi_id,epi_slid,epi_paid,CEV_ID,ceva_id, Q_ID,Q_TEXT,cevaa_id, cevaa_ceva_aid, cevaa_ceva_answer,
	          case when cevaa_ceva_answer ='YES' then 1
			       when cevaa_ceva_answer ='NO'  then 0
				   else -99
		      end symptoms_flag
			  ,CEV_VISITDATE
       from   HCHB.dbo.CLIENT_EPISODE_VISIT_ASSESSITEMS a11
              left outer join      HCHB.dbo.CLIENT_EPISODE_VISIT_ASSESSITEMS_ANSWERS      a12
                on   (a11.ceva_id = a12.cevaa_ceva_id)
              left outer join      HCHB.dbo.QUESTIONS   a13
                on   (a11.ceva_qid = a13.Q_ID)
              left outer join      HCHB.dbo.CLIENT_EPISODES_ALL      a14
                on   (a11.ceva_epiid = a14.epi_id)
              left outer join      HCHB.dbo.CLIENT_EPISODE_VISITS_ALL a15
                on   (a11.ceva_cevid = a15.CEV_ID)
       where a13.Q_AICID in (25064, 25068)  --
              and CAST(a15.CEV_VISITDATE as date) >=  '2020-02-06'  --they started screening on this date
              and cevaa_ceva_aid is not null 
			  and Q_ID in(1127663,1127647,1127648,1127673,1127674,1127676, 1127702, 1127699, 1127703, 1127704, 1127809, 1127811, 1127812, 1127813, 1127815, 1127817, 1127818,1127935, 1127937)
              --and epi_paid in ( 192980, 205654)
              --and cev_id in (2118220,2119122)

     
GO
/****** Object:  View [dbo].[VW_CURRENT_CLIENT_ADDRESS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[VW_CURRENT_CLIENT_ADDRESS]
AS
SELECT CA_ID, CA_PAID,  ca_description, ca_address1, ca_address2, CA_CITY, ca_state, ca_zip, CA_COUNTY, ca_IsCurrentAddress  FROM HCHB.DBO.CLIENT_ADDRESS
WHERE ca_IsCurrentAddress = 1
GO
/****** Object:  View [dbo].[VW_DIM_SHIFT]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create view [dbo].[VW_DIM_SHIFT] AS
select distinct SHIFT_CODE
from [VNSNY_BI].[dbo].[FCT_WB_SMRY_HRS_PRODUCTIVITY]
GO
/****** Object:  View [dbo].[VW_EVENT_STAGE_DEFAULT_RP]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[VW_EVENT_STAGE_DEFAULT_RP]
AS
SELECT cees_stid, rp_id, rp_desc, esrp_allottedtime
FROM  (
   SELECT cees_stid, rp_id, rp_desc, esrp_allottedtime, count(*) as count_of_stages, rank() OVER (PARTITION BY CEES.cees_stid ORDER BY count(*) DESC) AS rnk
   FROM HCHB.dbo.CLIENT_EPISODE_EVENT_STAGES CEES
   join HCHB.dbo.CLIENT_EPISODE_EVENTS cee
		on (CEES.cees_ceeid = cee.cee_id)
	join	HCHB.dbo.CLIENT_EPISODES_ALL CE
	  on 	(cee.cee_epiid = CE.epi_id)
	JOIN HCHB.dbo.EVENT_STAGES evst ON evst.evst_evid = cee.cee_evid 
								AND cees.cees_stid = evst.evst_stid 
								AND evst.evst_active = 'Y' 
								AND evst.evst_branchcode = ce.epi_branchcode
	JOIN HCHB.dbo.EVENT_STAGE_RP esrp ON esrp.esrp_evstid = evst.evst_id	
								   AND esrp.esrp_branchcode = evst.evst_branchcode 
								   AND esrp.esrp_active = 'Y'
   join	HCHB.dbo.RESPONSIBLE_POSITION	RP
		  on 	(CEES.cees_rpid = RP.rp_id and CEES.cees_rpid = esrp.esrp_rpid)
where CEES.cees_active = 'Y' and CE.epi_slid = 1 --and CEES.cees_businesshours is not null
group by cees_stid, rp_id, rp_desc, esrp_allottedtime
   ) sub
WHERE  rnk = 1
GO
/****** Object:  View [dbo].[VW_FACT_DAILY_WORKER_PRODUCTIVITY_VISITS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






create view [dbo].[VW_FACT_DAILY_WORKER_PRODUCTIVITY_VISITS]
as
SELECT P.[VISIT_DATE]
      ,P.[AGENT_ID]
      ,P.[WKR_PAYROLL_NO]
      ,P.[PAYMENT_METHOD]
	  ,P.[SERVICE_LINE_ID]
	  ,P.[WORKER_CATEGORY]
	  ,P.SERVICE_CODE
	  ,P.SERVICE_CODE_ID
	  ,P.VISIT_TYPE
	  ,P.VISIT_ID
	  ,P.PATIENT_ID
	  ,P.START_TIME
	  ,SH.MAX_SHIFT_CODE as SHIFT_CODE
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then 1 else 0 end PRODUCTIVITY_VISITS
		,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	    OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
		OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	then 0 else 1 end OVERTIME_VISITS
	,1 ALL_VISITS
     	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[TOTAL_TIME] else 0 end [TOTAL_TIME]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[IN_HOME_TIME] else 0 end [IN_HOME_TIME]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[DOCUMENTATION_TIME] else 0 end [DOCUMENTATION_TIME]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[MILEAGE] else 0 end [MILEAGE]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[DRIVE_TIME] else 0 end [DRIVE_TIME]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[GRAND_TOTAL_TIME] else 0 end [GRAND_TOTAL_TIME]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[POINTS] else 0 end [POINTS]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[WEEKDAY_POINTS] else 0 end [WEEKDAY_POINTS]
	  ,case when (SH.MAX_SHIFT_CODE ='FT5' and p.start_time between ('08:30:00') and ('16:30:00'))
	          OR (SH.MAX_SHIFT_CODE ='FT4' and p.start_time between ('08:00:00') and ('18:00:00'))
			  OR (SH.MAX_SHIFT_CODE ='FT3' and p.start_time between ('08:00:00') and ('20:00:00'))
	        then P.[WEEKDAY_VISIT] else 0 end [WEEKDAY_VISIT]
      ,P.[WEEKEND_POINTS]
	  ,H.[WORKPAID_HRS]
	  ,getdate() as data_as_of
	  ,(select MAX(WB_WORK_DATE) from [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY]) as wb_data_as_of
	  ,datediff(WEEK, (select MAX(WB_WORK_DATE) from [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY]), P.VISIT_DATE) as WB_WEEK_VS_VISIT_DATE
	  ,CASE WHEN SH.MAX_SHIFT_CODE = 'FT3' THEN 12 WHEN SH.MAX_SHIFT_CODE = 'FT4' THEN 9 WHEN SH.MAX_SHIFT_CODE = 'FT5' THEN 7.25 ELSE 1 END as STANDARD_HOURS 
	  ,36.25 as STANDARD_HOURS_WEEKLY
  FROM [VNSNY_BI].[dbo].[FCT_STAFF_PRODUCTIVITY] P LEFT OUTER JOIN
		(SELECT * FROM [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY] WHERE RPT_HOUR_TYPE like '%Regular Work Hours%') H
		ON P.VISIT_DATE = H.WB_WORK_DATE AND P.WKR_PAYROLL_NO = H.EMPL_ID  JOIN
		(SELECT EMPL_ID, CONVERT(CHAR(6), WB_WORK_DATE, 112) WB_WORK_MONTH, MAX(ltrim(rtrim([SHIFT_CODE]))) AS MAX_SHIFT_CODE FROM [VNSNY_BI].[dbo].[FCT_WD_SMRY_HRS_PRODUCTIVITY] GROUP BY EMPL_ID, CONVERT(CHAR(6), WB_WORK_DATE, 112)) SH
		ON P.[WKR_PAYROLL_NO] = SH.EMPL_ID and CONVERT(CHAR(6), P.[VISIT_DATE], 112) = SH.WB_WORK_MONTH
		left outer join	HCHB.dbo.SERVICECODES	S
	    ON 	(P.SERVICE_CODE_ID = S.sc_id)
  WHERE
	  P.SERVICE_CODE not like '%44%'
	  and P.SERVICE_CODE not like '%66%'
	  and P.SERVICE_CODE not like '%88%'
	  and P.SERVICE_CODE not like '%TCM%'
	  and S.sc_billable = 'Y'
	
GO
/****** Object:  View [dbo].[VW_FACT_LOS_And_INTAKE]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create VIEW [dbo].[VW_FACT_LOS_And_INTAKE]
as
SELECT epi_id, epi_paid, epi_status, epi_AdmitType, epi_SocDate, epi_DischargeDate, epi_DateOfReferral, 
       datediff(day, [epi_SocDate], ISNULL([epi_DischargeDate],GETDATE()) )+1 as length_of_stay,
      datediff(day, [epi_DateOfReferral],[epi_SocDate] ) as length_of_intake
  from [hchb].[dbo].[CLIENT_EPISODES_ALL]
  where epi_status not in ('DELETED', 'NON-ADMIT')
GO
/****** Object:  View [dbo].[VW_FCT_CMS_MEASURE]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[VW_FCT_CMS_MEASURE] AS
select * from 
[dbo].[FCT_CMS_MEASURE] M
JOIN [dbo].[DIM_CMS_EPISODE] E ON M.FCM_CMS_EPISODE_ID = E.CMS_EPISODE_ID
GO
/****** Object:  View [dbo].[VW_HCHB_PCRS_PAYER_XREF]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[VW_HCHB_PCRS_PAYER_XREF]
AS
SELECT * FROM [VNSNY_BI].[dbo].[XREF_PAYOR]
GO
/****** Object:  View [dbo].[VW_HCHB_TIME_BASED_EPISODE]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



create view [dbo].[VW_HCHB_TIME_BASED_EPISODE]
 AS 
 SELECT * FROM vnsny_bi.[dbo].[TIME_BASED_EPISODE]
GO
/****** Object:  View [dbo].[VW_HCHB_TIME_BASED_EPISODE_dtl]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



create view [dbo].[VW_HCHB_TIME_BASED_EPISODE_dtl]
 AS 
 SELECT * FROM vnsny_bi.[dbo].[TIME_BASED_EPISODE_DTL]
GO
/****** Object:  View [dbo].[vw_hosp_risk_score_cur]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[vw_hosp_risk_score_cur] as
select * from 
 (select epi_paid,epi_id ,epi_StartOfEpisode,epi_EndOfEpisode
  from hchb.dbo.client_episodes_all 
  where epi_slid = 1 
        and epi_status in ('CURRENT' ,'DISCHARGED','RECERTIFIED')) epi
left join 
 (select rsh_epiid,rsh_lmid,model_desc,rsh_pred_val,rsh_lrlid,risk_level_desc,upd_ts
  from (
	  SELECT  a.rsh_epiid,
			a.rsh_ceo_cevid,
			a.rsh_astrsn,
			a.rsh_lmid, 
			b.model_desc,	
      		a.rsh_pred_val,
			a.rsh_lrlid,
			c.risk_level_desc,
			a.upd_ts,
			rank() over(partition by rsh_epiid order by a.upd_ts desc) as flag
 	  FROM [VNSNY_STAT].[dbo].[risk_score_history]	a
     	  join VNSNY_STAT.dbo.lu_model b on b.lm_id = a.rsh_lmid	
        	 join VNSNY_STAT.dbo.lu_risk_level c on a.rsh_lrlid = c.lrl_id	
	  where rsh_lmid = 25100
      ) a
  where flag = 1
  ) risk_epi
on epi.epi_id = risk_epi.rsh_epiid
GO
/****** Object:  View [dbo].[VW_HOSPICE_30_HOSPITALIZATION]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



 CREATE VIEW [dbo].[VW_HOSPICE_30_HOSPITALIZATION] AS
select e.epi_id,epi_lastname,epi_firstname,epi_SocDate,epi_DischargeDate,epi_dcRid,epi_status
 ,  datediff(day, [epi_SocDate], ISNULL([epi_DischargeDate],GETDATE()) )+1 AS LENGTH_OF_STAY, 1 AS FLAG
from hchb.dbo.client_episodes_all e 
where  datediff(day, [epi_SocDate], ISNULL([epi_DischargeDate],GETDATE()) )+1 < 31
      AND epi_slid = 2
	  and e.epi_status NOT in ('PENDING', 'NON-ADMIT','DELETED') 	  
	  AND epi_dcrid IN ('25009','25015')
	  and epi_AdmitType not in ('BEREAVEMENT','RECERTIFICATION')
	  --and e.epi_SocDate between ('01-JAN-2019') and ('31-JAN-2019') 
	 -- ORDER BY epi_lastname,epi_firstname
GO
/****** Object:  View [dbo].[VW_LU_MODEL]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_LU_MODEL] AS
SELECT * FROM VNSNY_STAT.dbo.lu_model;
GO
/****** Object:  View [dbo].[VW_LU_RISK_LEVEL]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_LU_RISK_LEVEL] AS
SELECT * FROM VNSNY_STAT.dbo.lu_risk_level;
GO
/****** Object:  View [dbo].[VW_LU_RISK_LEVEL_RANGE]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_LU_RISK_LEVEL_RANGE] AS
SELECT * FROM VNSNY_STAT.dbo.LU_RISK_LEVEL_RANGE;
GO
/****** Object:  View [dbo].[VW_NONADMIT_REASONS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[VW_NONADMIT_REASONS]
as select nac_id, nac_code, nac_desc from HCHB.dbo.NONADMIT_REASONS
--where  nac_active = 'Y'  -- Jessica 3/20/2020 Ka Ho,Pavel request
GO
/****** Object:  View [dbo].[VW_PHYSICIAN]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[VW_PHYSICIAN] as
SELECT * FROM HCHB.DBO.PHYSICIAN
GO
/****** Object:  View [dbo].[VW_PHYSICIAN_OFFICES_ACTIVE]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[VW_PHYSICIAN_OFFICES_ACTIVE] as
SELECT * FROM HCHB.DBO.PHYSICIAN_OFFICES
WHERE po_active = 'Y' 
GO
/****** Object:  View [dbo].[VW_REFERRALS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
--drop view  [dbo].[VW_REFERRALS]

CREATE view  [dbo].[VW_REFERRALS]
as
SELECT CONCAT('P',PH_ID, '-', PO_ID) AS 'REF_ID' , 
PH_ID as 'SRC_ID',  
CONCAT(PH_FIRSTname,'-', PH_LASTNAME) AS 'NAME',
'PHYSICIAN' REF_DESC, 
'N/A' AS FACILITY_TYPE,
ph_active AS ACTIVE_FLAG, --,'||', 'PO-',po_active
PO_ID AS 'OFFICE', 
po_address AS 'ADDRESS', 
po_city AS 'CITY', 
po_state AS 'STATE', 
po_zip AS 'ZIPCODE',
LEFT(PO.PO_ZIP, 5) AS 'ZIP5',
ISNULL(OFC_ZIP.ZIP,'Unassigned')  AS 'SRC_ACC_ID', 
ISNULL(OFC_ZIP.ZIP,'Unassigned')  AS 'SRC_ACC_NAME', 
ISNULL(OFC_ZIP.ZIP,'Unassigned')  AS 'ReferralID', 
ISNULL(OFC_ZIP.ZIP,'Unassigned')  AS 'ReferralName', 
'PHYSICIAN' AS Segment, 
ISNULL(OFC_ZIP.SFDC_VP, 'Unassigned') AS 'VP'
FROM HCHB.[dbo].[PHYSICIANS] P
JOIN HCHB.[dbo].[PHYSICIAN_OFFICES] PO
ON P.PH_ID = PO.PO_PHID
LEFT JOIN VNSNY_BD.[dbo].DIM_XWALK_ZIPS OFC_ZIP
ON 'ZIP' + LEFT(PO.PO_ZIP, 5) = OFC_ZIP.ZIP

union all 

SELECT CONCAT( 'O', REL_ID) AS 'REF_ID', 
REL_ID as 'SRC_ID', 
rel_RelationDesc AS 'NAME',
'OTHER' REF_DESC , 
rel_RelationDesc AS FACILITY_TYPE,
rel_active AS ACTIVE_FLAG ,
0 AS 'OFFICE', 
'' AS 'ADDRESS', 
'' AS 'CITY', 
'' AS 'STATE', 
'' AS 'ZIPCODE',
'' AS 'ZIP5',
'Unassigned' AS 'SRC_ACC_ID', 
'Other' AS 'SRC_ACC_NAME', 
'Unassigned' AS 'ReferralID', 
'Other' AS 'ReferralName', 
'Non-core Facility' AS Segment, 
'Unassigned' AS VP
FROM HCHB.[dbo].[RELATIONSHIPS]

UNION ALL 

SELECT CONCAT('F', FA_ID) AS 'REF_ID', 
FA_ID as 'SRC_ID',  
fa_name AS 'NAME',
'FACILITY' REF_DESC, 
rft.rft_desc AS FACILITY_TYPE,
fa_active AS ACTIVE_FLAG ,
0 AS 'OFFICE',
fa_street AS 'ADDRESS',
fa_city AS 'CITY', 
fa_state AS 'STATE',
fa_zip AS 'ZIPCODE',
LEFT(fa_zip, 5) AS 'ZIP5',
ISNULL(xr.AccountHCHBID,'Unassigned') AS 'SRC_ACC_ID', 
ISNULL(xr.AccountName,'Unassigned') AS 'SRC_ACC_NAME', 
ISNULL(xr.ReferralID,'Unassigned') AS 'ReferralID',
ISNULL(xr.ReferralName,'Unassigned') AS 'ReferralName', 
ISNULL(xr.Segment,'Unassigned') AS Segment,
ISNULL(xr.VP,'Unassigned') AS VP
FROM HCHB.[dbo].[FACILITIES] F
LEFT JOIN hchb.dbo.REFERRING_FACILITY_TYPES rft 
ON f.fa_rftid = rft.rft_id 
LEFT JOIN VNSNY_BD.dbo.DIM_XWALK_REFERRAL xr 
ON xr.HCHBFacilityID = f.fa_id;
GO
/****** Object:  View [dbo].[VW_SERVICECODE_FLAG]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[VW_SERVICECODE_FLAG] as
	select sc_id, sc_code, sc_discipline, sc_desc, sc_visittype,
	case when  sc_desc like '%COVID%' then 1 else 0   --1 is for COVID19 pre-screening
	end as flag,
	case when  sc_desc like '%COVID%' then 'COVID19 Pre-screening' else 'N/A'   --1 is for COVID19 pre-screening
	end as flag_desc
	from hchb.dbo.servicecodes
GO
/****** Object:  View [dbo].[VW_SUNCOAST_TRANSITION_PATIENTS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create VIEW [dbo].[VW_SUNCOAST_TRANSITION_PATIENTS]
AS
SELECT        distinct(ca.pa_id), TRY_CAST(LTRIM(RTRIM(CASE WHEN CHARINDEX('SC#', ca.pa_legacymrnum) > 0 THEN LTRIM(SUBSTRING(ca.pa_legacymrnum, CHARINDEX('SC#', ca.pa_legacymrnum) + 3, 7)) 
                         WHEN CHARINDEX('SC', ca.pa_legacymrnum) > 0 THEN SUBSTRING(ca.pa_legacymrnum, CHARINDEX('SC#', ca.pa_legacymrnum) + 3, 6) ELSE NULL END)) AS NUMERIC) AS suncoast_id, 
                         ca.pa_legacymrnum, 
						 tRY_CAST(LTRIM(RTRIM(CASE WHEN CHARINDEX('CASE#', ca.pa_legacymrnum) > 0 THEN LTRIM(SUBSTRING(ca.pa_legacymrnum, CHARINDEX('CASE#', ca.pa_legacymrnum) + 5, 8)) 
                         WHEN CHARINDEX('CASE', ca.pa_legacymrnum) > 0 THEN  SUBSTRING(ca.pa_legacymrnum, CHARINDEX('CASE#', ca.pa_legacymrnum) + 5, 7) ELSE NULL END)) AS NUMERIC) AS suncoast_CASE_NUM, 
						 convert(date,(b.epi_SocDate)) as epi_SOCDate
FROM            HCHB.dbo.CLIENTS_ALL ca
join			hchb.dbo.client_episodes_all b on b.epi_paid = ca.pa_id
WHERE        (CHARINDEX('SC#', ca.pa_legacymrnum) > 0) OR
                         (CHARINDEX('SC', pa_legacymrnum) > 0) AND
						 TRY_CAST(LTRIM(RTRIM(CASE WHEN CHARINDEX('SC#', ca.pa_legacymrnum) > 0 THEN LTRIM(SUBSTRING(ca.pa_legacymrnum, CHARINDEX('SC#', ca.pa_legacymrnum) + 3, 7)) 
                         WHEN CHARINDEX('SC', ca.pa_legacymrnum) > 0 THEN SUBSTRING(ca.pa_legacymrnum, CHARINDEX('SC#', ca.pa_legacymrnum) + 3, 6) ELSE NULL END)) AS NUMERIC) IS NOT NULL
						 and b.epi_status <> 'DELETED'
					
GO
/****** Object:  View [dbo].[VW_TIME_BASED_EPISODE_HOSPITALIZATIONS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE view [dbo].[VW_TIME_BASED_EPISODE_HOSPITALIZATIONS]
as 
select IE_EPIID,IE_TIF_CEOID,IE_SOCROC_EFF_DATE,IE_EVENT_DATE,TBED_TBEID,TBED_EPISODE_START_DATE,TBED_EPISODE_END_DATE
      ,[EPISODE_END_DATE_ACTUAL],[TBED_PAYOR_CD],TBED_EPI_PAID
      ,datediff(day,TBED_EPISODE_START_DATE,IE_EVENT_DATE) + 1 as EPISODE_DAYS_TO_HOSP,1 as flag
from vnsny_bi.[dbo].[TIME_BASED_EPISODE_DTL]
  join vnsny_bi.[dbo].[INPATIENT_EVENTS] on ie_epiid = tbed_epiid
where IE_EVENT_DATE between TBED_EPISODE_START_DATE and [EPISODE_END_DATE_ACTUAL]


GO
/****** Object:  View [dbo].[VW_TIME_BASED_EPISODE_RISK_SCORE]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE VIEW [dbo].[VW_TIME_BASED_EPISODE_RISK_SCORE] AS
select distinct a.TBED_TBEID,b.RSH_LRLID AS START_RISK_SCORE
       ,a.rsh_lrlid AS END_RISK_SCORE 
from 
			   (SELECT * FROM
			   (select distinct TBED_TBEID,TBED_EPIID,rsh_lrlid ,crt_ts,[rsh_pred_val],rsh_ceo_cevid,rsh_astrsn,tbed_lvl_num
						,rank() over ( partition by TBED_TBEID order by tbed_epiid,crt_ts desc,rsh_ceo_cevid desc) as flag1
				from  VNSNY_BI.dbo.TIME_BASED_EPISODE_DTL
				join VNSNY_STAT.[dbo].[risk_score_history]
					 on TBED_EPIID = rsh_epiid
				where rsh_lmid = 25100  and crt_ts is not null and tbed_lvl_num >0  
				)  a
				WHERE A.FLAG1 = 1)A
			join 	
               (SELECT * FROM 
			   (select distinct TBED_TBEID,TBED_EPIID,rsh_lrlid ,crt_ts,[rsh_pred_val],rsh_ceo_cevid,rsh_astrsn,tbed_lvl_num
						,rank() over ( partition by TBED_TBEID order by tbed_epiid,crt_ts ,rsh_ceo_cevid) as flag1
				from  VNSNY_BI.dbo.TIME_BASED_EPISODE_DTL
				join VNSNY_STAT.[dbo].[risk_score_history]
					 on TBED_EPIID = rsh_epiid
				where rsh_lmid = 25100  and crt_ts is not null and tbed_lvl_num >0  
				) b
				WHERE B.FLAG1 = 1) B
on A.TBED_EPIID = B.TBED_EPIID  
GO
/****** Object:  View [dbo].[VW_TIME_BASED_EPISODE_RISK_SCORE_OLD]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_TIME_BASED_EPISODE_RISK_SCORE_OLD] AS
select distinct a.TBED_TBEID,a.RSH_LRLID AS START_RISK_SCORE
       ,b.rsh_lrlid AS END_RISK_SCORE 
from 
				(select * from (
				select  TBED_TBEID,TBED_EPIID,rsh_lrlid ,crt_ts
						,rank() over ( partition by TBED_TBEID order by tbed_epiid,crt_ts desc) as flag1
				from  VNSNY_BI.dbo.TIME_BASED_EPISODE_DTL
				join VNSNY_STAT.[dbo].[risk_score_history]
					 on TBED_EPIID = rsh_epiid
				where rsh_lmid = 25100  and crt_ts is not null
				)  a
				where a.flag1 = 1 ) a
join
				(select * from (
				select  TBED_TBEID,TBED_EPIID,rsh_lrlid ,crt_ts
					   ,rank() over ( partition by TBED_TBEID order by tbed_epiid,crt_ts) as flag1
				from  VNSNY_BI.dbo.TIME_BASED_EPISODE_DTL
				join VNSNY_STAT.[dbo].[risk_score_history]
					 on TBED_EPIID = rsh_epiid
				where rsh_lmid = 25100  and crt_ts is not null
				) b
				where b.flag1 = 1) b
on a.TBED_EPIID = b.TBED_EPIID  
GO
/****** Object:  View [dbo].[VW_TIME_BASED_EPISODE_UTILIZATION]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE view [dbo].[VW_TIME_BASED_EPISODE_UTILIZATION]
as 
select TBED_EPIID,VISIT_ID,Visit_date,TBED_TBEID,TBED_EPISODE_START_DATE,TBED_EPISODE_END_DATE
       ,[EPISODE_END_DATE_ACTUAL] ,[TBED_PAYOR_CD],TBED_EPI_PAID
       ,SERVICE_CODE_ID,service_code_desc,VISIT_DURATION_HOURS AS VISIT_DURATION_HRS,VISIT_DURATION_SECONDS AS VISIT_DURATION_SEC
	   ,Billable_ind
from vnsny_bi.[dbo].[TIME_BASED_EPISODE_DTL]
  join vnsny_bi.[dbo].[FCT_VISIT_UNIVERSE] on Episode_id = tbed_epiid and PAYOR_SOURCE_ID = TBED_PAYOR_CD
where Visit_date between TBED_EPISODE_START_DATE and [EPISODE_END_DATE_ACTUAL] and Billable_ind = 1 and VISIT_STATUS = 'COMPLETED'
GO
/****** Object:  View [dbo].[VW_WORKER_SERVICE_LINE_DEFAULT]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[VW_WORKER_SERVICE_LINE_DEFAULT]
AS
SELECT wsl_slid, wsl_wkrid, wsl_id, wsl_active
FROM HCHB.dbo.WORKER_SERVICE_LINES 
WHERE wsl_default = 'Y'
GO
/****** Object:  View [dbo].[VW_WORKER_WORKPAID_HOURS]    Script Date: 10/13/2021 10:10:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[VW_WORKER_WORKPAID_HOURS]
AS
  SELECT P.[EMPL_ID]
      ,W.[WKR_ID]
      ,P.[WB_WORK_DATE]
      ,P.[STD_HOURS]
      ,P.[SHIFT_CODE]
      ,P.[RPT_HOUR_TYPE]
      ,P.[WORKPAID_HRS]
      ,P.[UNKNOWN_HRS]
  FROM [VNSNY_BI].[dbo].[FCT_WB_SMRY_HRS_PRODUCTIVITY] P JOIN HCHB.dbo.WORKER_BASE W
  ON P.EMPL_ID = W.wkr_payrollno
GO
