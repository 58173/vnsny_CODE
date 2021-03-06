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
/****** Object:  StoredProcedure [dbo].[DELTA_LOAD]    Script Date: 10/13/2021 10:15:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[DELTA_LOAD]
AS
BEGIN
DECLARE @LoopCounter VARCHAR(100) , @MaxDeltaId VARCHAR(100), 
        @TableName NVARCHAR(100),@tableArchive NVARCHAR(100),@TableDelta NVARCHAR(100),@columnName NVARCHAR(100),@Deltacolumn1 NVARCHAR(100),@Deltacolumn2 NVARCHAR(100),
		@id NVARCHAR(100)
SELECT @LoopCounter = min(id) , @MaxDeltaId = max(id) 

FROM VNSNY_BI.dbo.LOAD_DETAILS
WHILE(  @LoopCounter IS NOT NULL
      AND @LoopCounter <= @MaxDeltaId)
BEGIN
   SELECT @TableName = Table_name, @TableArchive=Table_name_archive,@TableDelta=Table_Name_delta,@columnName=PK_Column_name,
   @Deltacolumn1=Delta_Column_1,@Deltacolumn2=Delta_Column_2,@id=@LoopCounter
   FROM VNSNY_BI.dbo.LOAD_DETAILS WHERE id = @LoopCounter
   
   if @id = @LoopCounter
   begin
    DECLARE @sql NVARCHAR(MAX);
	DECLARE @sql2 NVARCHAR(MAX);
    -- construct SQL
    SET @sql = N'insert into '+@TableDelta+' 
	SELECT B.*, ''DELETED'' AS ''CHANGE_TYPE''
FROM '+@TableArchive+' B
LEFT JOIN ' +@TableName+' A  ON B.'+@columnName+' = A.'+@columnName+'
WHERE A.'+@columnName+' IS NULL
UNION SELECT A.*, ''NEW''  AS ''CHANGE_TYPE''
FROM ' +@TableName+'   A
LEFT JOIN '+@tableArchive+'  B ON B.'+@columnName+' = A.'+@columnName+'
WHERE B.'+@columnName+' IS NULL
UNION
SELECT B.*, ''MODIFIED''  AS ''CHANGE_TYPE''
FROM ' +@TableName+'   A
INNER JOIN '+@tableArchive+'  B ON A.'+@columnName+' = B.'+@columnName+'
AND (A.'+@Deltacolumn1+' > B.'+@Deltacolumn1+' or A.'+@Deltacolumn2+' > B.'+@Deltacolumn2+') '; 
 
 -- execute the SQL
    EXEC sp_executesql @sql;
	set @sql2 =N'truncate table '+ @tableArchive+'
	insert into '+ @tableArchive+'
	select *  from '+ @TableName+' ';
	

--	 EXEC sp_executesql @sql2;
	end
    SET @LoopCounter  = @LoopCounter  + 1        
END
END;

GO
/****** Object:  StoredProcedure [dbo].[DeltaTable]    Script Date: 10/13/2021 10:15:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE  PROC [dbo].[DeltaTable] (
    @table NVARCHAR(128),@columnName  NVARCHAR(128),@tableArchive NVARCHAR(128)
)
AS
BEGIN
IF  OBJECT_ID(N'[VNSNY_BI].[dbo].[UniqueRecords]', 'U') IS NULL
        BEGIN
            CREATE TABLE [VNSNY_BI].[dbo].UniqueRecords
            (    
                [id]  nchar(10)     NOT NULL,
                [type]  nchar(10)     NOT NULL,
                [cost]   nchar(10)    NOT NULL,
                [Status]  varchar(max)    NOT NULL,
            ) ON [PRIMARY];

        END;
    DECLARE @sql NVARCHAR(MAX);
    -- construct SQL
    SET @sql = N'insert into VNSNY_BI.dbo.test_delta 
	SELECT B.*, ''DELETED'' AS ''CHANGE_TYPE''
FROM '+@tableArchive+' B
LEFT JOIN ' +@table+'   A  ON B.'+@columnName+' = A.'+@columnName+'
WHERE A.'+@columnName+' IS NULL
UNION SELECT A.*, ''NEW''  AS ''CHANGE_TYPE''
FROM ' +@table+'   A
LEFT JOIN '+@tableArchive+'  B ON B.'+@columnName+' = A.'+@columnName+'
WHERE B.'+@columnName+' IS NULL
UNION
SELECT B.*, ''MODIFIED''  AS ''CHANGE_TYPE''
FROM (
        SELECT * FROM '+@table+'
        EXCEPT
        SELECT * FROM '+@tableArchive+' 
    ) S1
INNER JOIN '+@tableArchive+'  B ON S1.'+@columnName+' = B.'+@columnName+' ';
    -- execute the SQL
    EXEC sp_executesql @sql;
    
END;
GO
/****** Object:  StoredProcedure [dbo].[p_unbilled_reasons]    Script Date: 10/13/2021 10:15:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






-- =============================================
-- Author:		Jessica
-- Create date: 6/19/19
-- Description:	unbilled reasons
-- =============================================
CREATE PROCEDURE [dbo].[p_unbilled_reasons] 

AS
BEGIN

        -- a. declare scalar variables
        declare @defaultInferredDate datetime = cast('1/1/1900' as datetime)
		-- b. declare temp tables
		CREATE TABLE #PayorAudits     (ps_id int NOT NULL, bai_id int NOT NULL ,warn bit NOT NULL, hold bit NOT NULL ) 
		CREATE TABLE #PayorAudits_All  (ps_id int NOT NULL, bai_id int NOT NULL,[bai_name] varchar(256) not null,[bat_id] int NOT NULL, [bat_desc] varchar(32) not null,[bap_id] int NOT NULL,[bap_name] varchar(32) not null, [parent_bapid] int NOT NULL,[parent_bapname] varchar(128) not null,warn bit NOT NULL, hold bit NOT NULL, [inherit] bit NOT NULL,[default_warn]bit NOT NULL, [default_hold] bit NOT NULL,[defaultable] bit NOT NULL, [bai_readonly]  bit NOT NULL,[bai_visible]  bit NOT NULL,[bai_disableable] bit NOT NULL)
		 -- (ps_id int NOT NULL, bai_id int NOT NULL,bai_name varchar(256),bat_id int not null, bat_desc varchar(32),bap_id int not null,bap_anme varchar(128), parent_bapid int not null, parent_bapname varchar(128), warn bit NOT NULL, hold bit NOT NULL,inherit int,default_warn bit,default_hold bit,bai_readonly bit,bai_visible bit, bai_disableable bit) 
		CREATE TABLE #PayorAudits_Get (ps_id int, bai_id int, warn bit, hold bit)
		--CREATE TABLE #PS (ps_id int primary key)               -- Holds all inputted ps_id's
		CREATE TABLE #BR (branch_code varchar(3) primary key)  -- Holds all inputted branch_codes's
		CREATE TABLE #PA (pa_id int primary key, pa_lastname varchar(100), pa_firstname varchar(100), pa_mi varchar(5), pa_status varchar(15), cst_id int)               -- Holds inputted clients ID's
		CREATE TABLE #TEAMs (team_id int primary key, teamcode varchar(10))          -- Holds inputted teams
		CREATE TABLE #SL (sl_id int primary key, serviceline varchar(25))               -- Holds inputted service lines
		CREATE TABLE #CS (cst_id int primary key)              -- Holds inputted client statuses
		CREATE TABLE #audits(pk bigint identity(1,1) primary key, bai_id int, bai_name varchar(128), bat_id int, bat_desc varchar(128), ErrorMessage varchar(2048) NOT NULL, pa_name varchar(101), ps_desc varchar(50), pt_desc varchar(50),
						Branch varchar(3), bai_helptext varchar(2048), ps_id int, ID int, ID_TYPE int, audit_level varchar(max))  -- Audit Staging table (holds all audits)
		CREATE TABLE #audits_failure_warn                        (bai_id int, bai_name varchar(128), bat_id int, bat_desc varchar(128), ErrorMessage varchar(2048) NOT NULL, pa_name varchar(101), ps_desc varchar(50), pt_desc varchar(50),
						Branch varchar(3), bai_helptext varchar(2048), ps_id int, ID int, ID_TYPE int)
		CREATE TABLE #audits_failure_hold                        (bai_id int, bai_name varchar(128), bat_id int, bat_desc varchar(128), ErrorMessage varchar(2048) NOT NULL, pa_name varchar(101), ps_desc varchar(50), pt_desc varchar(50),
						Branch varchar(3), bai_helptext varchar(2048), ps_id int, ID int, ID_TYPE int)
		CREATE TABLE #mapping (pk bigint identity(1,1) primary key,s_fk bigint, a_fk bigint, audit_type int, audit_level varchar(max)) -- Mapping staging table (holds all mappings)
		CREATE TABLE #output_audit (pk bigint, bai_id int, audit_level varchar(max), bai_name varchar(128), bat_id int, bat_desc varchar(128), ErrorMessage varchar(2048), pa_name varchar(101), ps_desc varchar(50), pt_desc varchar(50),
						Branch varchar(3), bai_helptext varchar(2048))  -- audits to output to application
		CREATE TABLE #output_mapping (pk bigint identity(1,1) primary key,s_fk bigint, a_fk bigint, audit_type int) -- mappings to output to application
		CREATE TABLE #claim_profiles (cp_id int not null, cp_payorsourceid int  null,[ps_id] int not null,   cp_branchcode varchar(3) not null,cp_itid int null,cp_name varchar(500), profile_branch_IsAssigned bit)
		CREATE TABLE #claim_profile_formats (cp_id int, [157] bit, [163] bit, [162] bit,[153] bit, [161] bit, [164] bit, [213] bit, [151] bit,[211] bit,[216] bit,[217] bit,[212] bit,[196] varchar(1000),[187] int,
						[184] varchar(100),[167] varchar(100),[169] varchar(100), [328] DATE)
		CREATE TABLE #PhysicianInfo(ph_lastname varchar(50),ph_firstname varchar(50),PPlace1Type char(2),PPlace1 varchar(50),PPlace2Type char(2),PPlace2 varchar(50),ph_UPIN varchar(50),ph_medicaidnumber varchar(50),epiid int,
						authid int,poid int,sortorder int,pcp bit,secondaryid varchar(50), HasPCP bit, HasSecondary bit, UsePCP bit, OtherProvider bit, SecondaryPhysician bit, LAMedicaid bit, UseMe bit, CheckMedicaid bit, CheckUPIN bit
						,isReferringPhysician BIT, ph_statelicense VARCHAR(50), IsCertifyingPhysician BIT DEFAULT (0), ph_id int)
		CREATE TABLE #invoice_grouping(invoice_grouping int primary key, startdate datetime, enddate datetime)

		CREATE TABLE #AUTHORIZATIONS(auth_id INT, pn_description VARCHAR(100), cefs_id INT, cp_id INT, auth_pending CHAR(1), epi_id INT, epi_startofepisode DATETIME, epi_endofepisode DATETIME, epi_branchcode CHAR(3), pt_desc VARCHAR(50), 
								     ps_desc VARCHAR(50), ps_id INT, epi_fullname VARCHAR(120), auth_no VARCHAR(20), ps_supports837 BIT DEFAULT(0), invoice_grouping INT, maxshiftdate DATE)

		CREATE TABLE #DepositNumbers(InvNum INT, DepositNumber INT)
		
		--*********************************************************************************
		-- parse claim profiles
		--*********************************************************************************
----111		 
		CREATE TABLE #CLAIM_PROVIDERS (cp_id int, name nvarchar(250), status bit)

		INSERT INTO #CLAIM_PROVIDERS (cp_id, name, status)
                select distinct  cp.cp_id as cp_id, [Name] = cp.cp_name, [Status] = cp.cp_active
				from hchb.dbo.CLAIM_PROFILES cp
				join hchb.dbo.CLAIM_PROFILE_INFO cpi on cp.cp_id = cpi.cp_id
				join hchb.dbo.payor_sources ps on cpi.cp_psid =ps.ps_id	
				join hchb.dbo.payor_source_branches psb  on cpi.cp_psid = psb.psb_psid 
				join hchb.dbo.invoice_types it on ps.ps_itid = it.it_id
				join hchb.dbo.payor_types pt on ps.ps_ptid = pt.pt_id
				join hchb.dbo.VI_CPCF c on CP.cp_id = c.cp_id
				where  psb.psb_active = 'Y'

		--*********************************************************************************
		-- get applicable audits
		--*********************************************************************************
	    -- check for specific inputs of audits (not likely to do anything)
		create TABLE #payors_unbilledreasons (BAP_ID INt, PS_ID INT NOT NULL)
          --#payors  [usp_BillingAudit_PayorAudits_Get]
		INSERT INTO #payors_unbilledreasons
                          SELECT bap_id = 
								CASE
									WHEN bap.bap_id IS NOT NULL THEN bap.bap_id
									WHEN ps.ps_freq = 8 THEN 5
									WHEN ps.ps_freq = 9 THEN (SELECT bap.bap_id from hchb.dbo.BILLING_AUDIT_PROFILES bap WITH(NOLOCK) where bap.bap_name = 'CASE RATES')
									WHEN ps.ps_sltid IN (1,3) AND pt.pt_wfid IN (2,3,4) THEN 1
									WHEN ps.ps_hrtid = 2 AND ps.ps_freq = 0 AND ps.ps_sltid = 2 THEN 1
									WHEN ps.ps_freq = 6 AND ps.ps_sltid = 2 THEN 2
									WHEN ps.ps_freq = 5 AND ps.ps_sltid = 1 THEN 3
									ELSE 0
								END	,ps_id
							FROM hchb.dbo.PAYOR_SOURCES ps WITH(NOLOCK)
							JOIN hchb.dbo.PAYOR_TYPES pt WITH(NOLOCK) on ps.ps_ptid = pt.pt_id 
							LEFT JOIN hchb.dbo.BILLING_AUDIT_PROFILES bap WITH(NOLOCK) on bap.bap_id = ps.ps_bapid and bap.bap_active = 'Y'

      	create table #payorProfiles_unbilledreasons (bai_id int not null, bai_name varchar(256) not null, bat_id int not null,bat_desc varchar(32) NOT NULL,BAP_ID INT NOT NULL ,BAP_NAME VARCHAR(128) NOT NULL,
		                                            PARENT_BAPID INT NOT NULL, PARENT_BAPNAME VARCHAR(128) NOT NULL, WARN BIT, HOLD BIT ,INHERIT BIT,DEFULT_WARN BIT NOT NULL, DEFAULT_HOLD BIT NOT NULL,
													defaultable BIT, BAI_READONLY BIT NOT NULL, BAI_VISIBLE BIT NOT NULL, bai_disableable BIT NOT NULL)
        --#payorProfiles   [dbo].[usp_BillingAudit_PayorAudits_Get]
		insert into #payorProfiles_unbilledreasons
        select 
		bai_id = bai.bai_id,
		bai_name = bai.bai_name,
		bat_id = bat.bat_id,
		bat_desc = bat.bat_desc,
		bap_id = bap.bap_id,
		bap_name = bap.bap_name,
		parent_bapid = baptop.bap_id,
		parent_bapname = baptop.bap_name,
		warn =
			case
				when bap.bap_bapid is null then bapbaidefault.bapbai_warn 
				when bapbaioverride.bapbai_bapid is not null then bapbaioverride.bapbai_warn 
				else bapbaidefault.bapbai_warn 
			end,
		hold =
			case
				when bap.bap_bapid is null then bapbaidefault.bapbai_hold
				when bapbaioverride.bapbai_bapid is not null then bapbaioverride.bapbai_hold 
				else bapbaidefault.bapbai_hold
			end,--bap.bap_bapid ,bapbaioverride.bapbai_bapid,
		inherit =
			cast(case
				when bap.bap_bapid is null and 0 = 0 then 0
				when bap.bap_bapid is null and 0 = 1 then 1
				when bapbaioverride.bapbai_bapid is not null then 0 
				else 1
			end as bit),
		default_warn = bapbaidefault.bapbai_warn,
		default_hold = bapbaidefault.bapbai_hold,
		defaultable =
			cast(case
				when bap.bap_bapid is null and not 0 = 1 then 0
				else 1
			end as bit),
		bai_readonly,
		bai_visible,
		bai_disableable
	from
		(select bap_id from #payors_unbilledreasons) p
		inner join hchb.dbo.billing_audit_profiles bap on bap.bap_id = p.bap_id
		inner join hchb.dbo.billing_audit_profiles baptop on baptop.bap_id = isnull(bap.bap_bapid,bap.bap_id)
		inner join hchb.dbo.billing_audit_profile_billing_audit_items bapbaidefault on bapbaidefault.bapbai_bapid = baptop.bap_id
		inner join hchb.dbo.billing_audit_items bai on bai.bai_id = bapbaidefault.bapbai_baiid and bai.bai_active = 'Y'
		inner join hchb.dbo.billing_audit_types bat on bat.bat_id = bai.bai_batid
		left join hchb.dbo.billing_audit_profile_billing_audit_items bapbaioverride on bapbaioverride.bapbai_bapid = bap.bap_id and bapbaioverride.bapbai_baiid = bai.bai_id
	union all
	select
		bai_id = bai.bai_id,
		bai_name = bai.bai_name,
		bat_id = bat.bat_id,
		bat_desc = bat.bat_desc,
		bap_id = bap.bap_id,
		bap_name = bap.bap_name,
		parent_bapid = baptop.bap_id,
		parent_bapname = baptop.bap_name,
		warn = bapbai.bapbai_warn,
		hold = bapbai.bapbai_hold,--bap.bap_bapid ,bapbaitop.bapbai_bapid,
		inherit = cast(0 as bit),
		default_warn = bapbai.bapbai_warn,
		default_hold = bapbai.bapbai_hold,
		defaultable = cast(0 as bit),
		bai_readonly,
		bai_visible,
		bai_disableable
	from
		(select bap_id from #payors_unbilledreasons)  p
		inner join hchb.dbo.billing_audit_profiles bap on bap.bap_id = p.bap_id
		inner join hchb.dbo.billing_audit_profiles baptop on baptop.bap_id = bap.bap_bapid
		inner join hchb.dbo.billing_audit_profile_billing_audit_items bapbai on bapbai.bapbai_bapid = bap.bap_id
		inner join hchb.dbo.billing_audit_items bai on bai.bai_id = bapbai.bapbai_baiid and bai.bai_active = 'Y'
		inner join hchb.dbo.billing_audit_types bat on bat.bat_id = bai.bai_batid
		left join hchb.dbo.billing_audit_profile_billing_audit_items bapbaitop on bapbaitop.bapbai_baiid = bapbai.bapbai_baiid and bapbaitop.bapbai_bapid = baptop.bap_id
	where
		bapbaitop.bapbai_baiid is null			

		INSERT INTO #PayorAudits_All 
			select 
				ps.ps_id,
				pp.bai_id as bai_id,
				pp.bai_name as bai_name,
				pp.bat_id as bat_id,
				pp.bat_desc as bat_desc,
				pp.bap_id as bap_id,
				pp.bap_name as bap_name,
				pp.parent_bapid as parent_bapid,
				pp.parent_bapname as parent_bapname,
				isnull(baips.baips_warn, pp.warn) as warn,
				isnull(baips.baips_hold, pp.hold) as hold,
				cast((case when baips.baips_baiid is null then 1 else 0 end) as bit) as inherit,
				pp.warn as default_warn,
				pp.hold as default_hold,
				cast(1 as bit) as defaultable,
				pp.bai_readonly as bai_readonly,
				pp.bai_visible as bai_visible,
				pp.bai_disableable as bai_disableable
			from
				#payors_unbilledreasons ps
				inner join #payorProfiles_unbilledreasons pp on pp.bap_id = ps.bap_id
				left join hchb.dbo.billing_audit_items_payor_sources baips on baips.baips_baiid = pp.bai_id and baips.baips_psid = ps.ps_id
				union
				select
				ps.ps_id,
				bai.bai_id as bai_id,
				bai.bai_name as bai_name,
				bat.bat_id as bat_id,
				bat.bat_desc as bat_desc,
				bap.bap_id as bap_id,
				bap.bap_name as bap_name,
				baptop.bap_id as parent_bapid,
				baptop.bap_name as parent_bapname,
				baips.baips_warn as warn,
				baips.baips_hold as hold,
				cast(0 as bit) as inherit,
				baips.baips_warn as warn,
				baips.baips_hold as hold,
				cast(0 as bit) as defaultable,
				bai.bai_readonly as bai_readonly,
				bai.bai_visible as bai_visible,
				bai.bai_disableable as bai_disableable
			from
				#payors_unbilledreasons  ps
				inner join hchb.dbo.billing_audit_profiles bap on bap.bap_id = ps.bap_id
				inner join hchb.dbo.billing_audit_profiles baptop on baptop.bap_id = isnull(bap.bap_bapid, bap.bap_id)
				inner join hchb.dbo.billing_audit_items_payor_sources baips on baips.baips_psid = ps.ps_id
				inner join hchb.dbo.billing_audit_items bai on bai.bai_id = baips.baips_baiid
				inner join hchb.dbo.billing_audit_types bat on bat.bat_id = bai.bai_batid
				left join #payorProfiles_unbilledreasons  pp on pp.bai_id = bai.bai_id and pp.bap_id = ps.bap_id
			where
				pp.bai_id is null
				and 
					(
					 bai.bai_id not in 
					 (
						select
							c.bai_id
						from
							#payorProfiles_unbilledreasons  c
							left join #payorProfiles_unbilledreasons n on n.bai_id = c.bai_id --and n.bap_id = @bap_id
						where
							 c.defaultable = 0
							and n.bai_id is null
					))

            INSERT INTO #PayorAudits
			SELECT  ps_id,bai_id,warn,hold
			FROM #PayorAudits_All
			WHERE 0=1 or warn = 1 or hold = 1
        --*********************************************************************************
		-- figure out which clients to look at
		--*********************************************************************************
		print('PA')
            insert into #PA (pa_id,pa_lastname,pa_firstname,pa_mi,pa_status,cst_id)     	
			select c.pa_id,c.pa_lastname,c.pa_firstname,c.pa_mi,c.pa_status,cst_id
			from  hchb.dbo.CLIENTS c
			      join hchb.dbo.CLIENT_STATUSES on cst_status = pa_status
			where cst_active='Y'
			
		--*********************************************************************************
		-- figure out which Providers to look at
		-- legacy id is passed in, but we are only storing the cp_id so the only reference to remove will be this one
		-- (this is out legacy_id mapping)
		--*********************************************************************************
			PRINT 'claim_profiles'
			    ;with cp as (
				select 
					cp.cp_id as profile_id,
					cp.cp_name as profile_name,
					cp.cp_active as profile_active,
					cpi.cp_psid as profile_payorsource,
					ps.ps_desc as profile_payorsourcename,
					case when ps.ps_default_cpid = cp.cp_id then cast(1 as bit) else cast(0 as bit) end as profile_payorsource_default,
					cp.cp_parent_cpid as profile_parentprofile
				from hchb.dbo.CLAIM_PROFILES cp
				join hchb.dbo.CLAIM_PROFILE_INFO cpi on cp.cp_id = cpi.cp_id
				left join hchb.dbo.PAYOR_SOURCES ps on cpi.cp_psid = ps.ps_id)
			insert into #claim_profiles	(cp_id, cp_payorsourceid,ps_id, cp_branchcode,profile_branch_IsAssigned,cp_itid,cp_name)
			select  CP_ID = ISNULL(cp.profile_id, -1),
					CP_payorsourceid = cp.profile_payorsource,
					ps.ps_id,
					CP_branchcode = psb.psb_branchcode, 
					profile_branch_IsAssigned = case when cp.profile_id = psb.psb_cpid then CAST(1 as bit) else CAST(0 as bit) end,
					CP_itid = ps.ps_itid,
					ISNULL(profile_name, -1) CP_NAME 
				from
				cp 
				right join hchb.dbo.payor_sources ps with(nolock) on cp.profile_payorsource = ps.ps_id
				join hchb.dbo.payor_source_branches psb with(nolock) on ps.ps_id = psb.psb_psid and psb.psb_active = 'Y'


			create  index IX_CP_UNIQUE  on #claim_profiles(cp_payorsourceid,cp_branchcode)



			--*********************************************************************************
			-- find applicable settings for the above providers
			--*********************************************************************************			
			SELECT cpcf_cpid,
				   CASE WHEN cpcf_cfid = 157 THEN cpcf_value END AS [157],
				   CASE WHEN cpcf_cfid = 163 THEN cpcf_value END AS [163],
				   CASE WHEN cpcf_cfid = 162 THEN cpcf_value END AS [162],
				   CASE WHEN cpcf_cfid = 153 THEN cpcf_value END AS [153],
				   CASE WHEN cpcf_cfid = 161 THEN cpcf_value END AS [161],
				   CASE WHEN cpcf_cfid = 164 THEN cpcf_value END AS [164],
				   CASE WHEN cpcf_cfid = 213 THEN cpcf_value END AS [213],
				   CASE WHEN cpcf_cfid = 151 THEN cpcf_value END AS [151],
				   CASE WHEN cpcf_cfid = 211 THEN cpcf_value END AS [211],
				   CASE WHEN cpcf_cfid = 216 THEN cpcf_value END AS [216],
				   CASE WHEN cpcf_cfid = 217 THEN cpcf_value END AS [217],
				   CASE WHEN cpcf_cfid = 212 THEN cpcf_value END AS [212],
				   CASE WHEN cpcf_cfid = 196 THEN cpcf_value END AS [196],
				   CASE WHEN cpcf_cfid = 187 THEN cpcf_value END AS [187],
				   CASE WHEN cpcf_cfid = 184 THEN cpcf_value END AS [184],
				   CASE WHEN cpcf_cfid = 167 THEN cpcf_value END AS [167],
				   CASE WHEN cpcf_cfid = 169 THEN cpcf_value END AS [169],
				   CASE WHEN cpcf_isformaton = 1 AND cpcf_cfid = 328 THEN cpcf_value ELSE NULL END AS [328]
			INTO #claim_profiles_claim_formats_grouping
			FROM hchb.dbo.CLAIM_PROFILES_CLAIM_FORMATS 
			WHERE cpcf_cfid IN (157, 163, 162,153,161,164,213,151,211,216,217,212,196,187,184,167,169,328)
			AND EXISTS(SELECT 1 FROM #claim_profiles c WHERE c.cp_id  = cpcf_cpid)				

			INSERT INTO #claim_profile_formats (cp_id,[157],[163],[162],[153],[161],[164],[213],[151],[211],[216],[217],[212],[196],[187],[184],[167],[169],[328])
			SELECT 
				cpcf_cpid,
				CAST(MAX([157]) AS BIT) AS [157],
				CAST(MAX([163]) AS BIT) AS [163],
				CAST(MAX([162]) AS BIT) AS [162],
				CAST(MAX([153]) AS BIT) AS [153],
				CAST(MAX([161]) AS BIT) AS [161],
				CAST(MAX([164]) AS BIT) AS [164],
				CAST(MAX([213]) AS BIT) AS [213],
				CAST(MAX([151]) AS BIT) AS [151],
				CAST(MAX([211]) AS BIT) AS [211],
				CAST(MAX([216]) AS BIT) AS [216],
				CAST(MAX([217]) AS BIT) AS [217],
				CAST(MAX([212]) AS BIT) AS [212],
				CAST(MAX([196]) AS VARCHAR(100)) AS [196],
				CAST(MAX([187]) AS INT) AS [187],
				CAST(MAX([184]) AS VARCHAR(100)) AS [184],
				CAST(MAX([167]) AS VARCHAR(100))AS [167],
				CAST(MAX([169]) AS VARCHAR(100)) AS [169],
				CAST(MAX([328]) AS DATE) AS [328]
			FROM #claim_profiles_claim_formats_grouping
			GROUP BY cpcf_cpid			
			
			IF EXISTS (select 1 from #claim_profiles where cp_id <= 0)
			BEGIN
				insert into #claim_profile_formats (cp_id,[196],[187]) values (-1,'',1)
			END

			CREATE UNIQUE INDEX IX_CPF_UNIQUE ON #claim_profile_formats(cp_id)
-----222
    -- Get all applicable line items to check against the above named billing audits.
	-- All data should be pulled at this point, no need to hit base tables again in audits...
	-- if you need more info in the audit, add it to the shared temp tables prepared below

		-- Mini-Entities to break up the Main Data Join
			-- Payor Source
			print('payor_entity')
			SELECT ps.ps_id
                   ,ps.ps_ptid
                   ,ps.ps_active
                   ,ps.ps_lastupdate
                   ,ps.ps_desc
                   ,ps.ps_address
                   ,ps.ps_city
                   ,ps.ps_state
                   ,ps.ps_zip
                   ,ps.ps_phone
                   ,ps.ps_itid
                   ,ps.ps_freq
                   ,ps.ps_sltid
                   ,ps.ps_hrtid
                   ,ps.ps_bapid
                   ,ps.ps_billbymonth
                   ,ps.ps_spanacrossepisodes
                   ,ps.ps_default_cpid
                   ,ps.ps_splitbyauthno
                   ,ps.ps_enableF2FEncounterFeature
                   ,ps.ps_allowReplacementClaims
                   ,ps.ps_billByDay
                   ,ps.ps_billByDiscipline
                   ,ps.ps_billByBranch
                   ,ps.ps_combinePhysicianServiceAndLocClaims
                   ,ps.ps_combineRoomAndBoardAndLocClaims
                   ,ps.ps_rptid
                   ,ps.ps_bypassRAPBilling
                   ,ps.ps_enablePECOSVerification
                   ,ps.ps_PECOSVerificationStartDate
				   ,ps.ps_F2FEncounterEffectiveDate
                   ,pt.pt_id
                   ,pt.pt_desc
                   ,pt.pt_wfid
			INTO #PAYOR_ENTITY
			FROM hchb.dbo.PAYOR_SOURCES ps 
				INNER JOIN hchb.dbo.PAYOR_TYPES pt ON ps.ps_ptid = pt.pt_id

			-- Agency
			PRINT('#AGENCY_ENTITY')
			SELECT a.agency_id ,
                   a.agency_companyid ,
                   a.agency_name ,
                   a.agency_ProviderNumber ,
                   a.agency_street ,
                   a.agency_city ,
                   a.agency_state ,
                   a.agency_zip ,
                   a.agency_phone ,
                   a.agency_fax ,
                   a.FED_ID ,
                   a.ST_ID ,
                   a.HHA_AGENCY_ID ,
                   a.ACY_NAME ,
                   a.ACY_ADDR_1 ,
                   a.ACY_ADDR_2 ,
                   a.ACY_CITY ,
                   a.ACY_ST ,
                   a.ACY_ZIP ,
                   a.ACY_CNTCT ,
                   a.ACY_PHONE ,
                   a.ACY_EXTEN ,
                   a.AGT_ID ,
                   a.AGT_NAME ,
                   a.AGT_ADDR_1 ,
                   a.AGT_ADDR_2 ,
                   a.AGT_CITY ,
                   a.AGT_ST ,
                   a.AGT_ZIP ,
                   a.AGT_CNTCT ,
                   a.AGT_PHONE ,
                   a.AGT_EXTEN ,
                   a.SFW_ID ,
                   a.SFW_NAME ,
                   a.SFW_ADDR_1 ,
                   a.SFW_ADDR_2 ,
                   a.SFW_CITY ,
                   a.SFW_ST ,
                   a.SFW_ZIP ,
                   a.SFW_CNTCT ,
                   a.SFW_PHONE ,
                   a.SFW_EXTEN ,
                   a.TEST_SW ,
                   a.agency_active ,
                   a.agency_insertdate ,
                   a.agency_lastupdate ,
                   a.agency_rslid ,
                   a.agency_GL ,
                   a.agency_legalname ,
                   a.agency_email ,
                   a.agency_webaddress ,
                   a.agency_CLIA ,
                   a.agency_his_stCd ,
                   a.agency_his_facId ,
                   a.agency_his_ccn,
				   asb.asb_agencyid ,
				   asb.asb_slid ,
				   asb.asb_branchcode ,
				   asb.asb_id ,
				   asb.asb_insertdate ,
				   asb.asb_lastupdate ,
				   asb.asb_active ,
				   asb.asb_branchparent ,
				   asb.asb_npi ,
				   asb.asb_activelastupdate
			INTO #AGENCY_ENTITY
			FROM hchb.dbo.AGENCIES a
				INNER JOIN hchb.dbo.V_AGENCIES_SERVICELINES_BRANCHES asb on asb.asb_agencyid = a.agency_id

			-- Programs (Dependant on Payor Sources)
			PRINT'#PROGRAMS_ENTITY'
			SELECT pg.pg_id ,
                   pg.pg_psid ,
                   pg.pg_insertdate ,
                   pg.pg_lastupdate ,
                   pg.pg_active ,
                   pg.pg_required ,
                   pg.pg_daterange ,
                   pg.pg_hrsbyperiod ,
                   pg.pg_hrsbyday ,
                   pg.pg_hrsbyweek ,
                   pg.pg_hrsbymonth ,
                   pg.pg_hrsbyyear ,
                   pg.pg_visbyperiod ,
                   pg.pg_visbyday ,
                   pg.pg_visbyweek ,
                   pg.pg_visbymonth ,
                   pg.pg_visbyyear ,
                   pg.pg_auth ,
                   pg.pg_pnid ,
                   pg.pg_pdid ,
                   pg.pg_pstid ,
                   pg.pg_abtid ,
                   pg.pg_autid ,
                   pg.pg_reauthorization ,
                   pg.pg_pending ,
                   pg.pg_useforpps ,
                   pg.pg_adjbytype,
				   pn.pn_id ,
				   pn.pn_insertdate ,
				   pn.pn_lastupdate ,
				   pn.pn_description ,
				   pn.pn_active ,
				   pn.pn_required
			INTO #PROGRAMS_ENTITY
			FROM hchb.dbo.PAYOR_SOURCES ps
			JOIN hchb.dbo.PROGRAMS pg on ps.ps_id = pg.pg_psid
			JOIN hchb.dbo.PROGRAM_NAMES pn on pg.pg_pnid = pn.pn_id

			-- Billing Codes (Dependant on Payor Sources,Programs)
			--Pull all active codes here so can pull by effective date below
			PRINT '#BILLING_CODES_ENTITY'
			SELECT bc.bc_id ,
                   bc.bc_pgid ,
                   bc.bc_insertdate ,
                   bc.bc_lastupdate ,
                   bc.bc_active ,
                   bc.bc_required ,
                   bc.bc_jdid ,
                   bc.bc_proccode ,
                   bc.bc_modifier ,
                   bc.bc_revcode ,
                   bc.bc_HCPCS ,
                   bc.bc_modifier2 ,
                   bc.bc_modifier3 ,
                   bc.bc_modifier4 ,
                   bc.bc_maxdefaultqty ,
                   bc.bc_effectivefrom ,
                   bc.bc_effectiveto ,
                   bc.bc_valuecode ,
                   bc.bc_valuecodeamount
			INTO #BILLING_CODES_ENTITY
			FROM #PROGRAMS_ENTITY pe
			INNER JOIN hchb.dbo.BILLING_CODES bc on pe.pg_id = bc.bc_pgid
			WHERE bc.bc_active = 'Y'
			
			CREATE UNIQUE INDEX IX_NoDuplicateBillingCodes ON #BILLING_CODES_ENTITY(bc_pgid,bc_jdid, bc_effectivefrom, bc_effectiveto)

		-- Get Contractual Adjustment Info
		CREATE TABLE #ContractualAdjustmentTransactions (AdjustmentID int, DestID char(3), parent_tcid int, MisQty decimal(15,4), CltMisChrg decimal(15,4), InvNum int, ac_desc varchar(50), postdate datetime, ps_id int, visamt decimal(9,2),
														 houramt decimal(9,2), pplan varchar(100), subplan varchar(100), locname varchar(50), scheduleid int)
		CREATE INDEX IX_ContractualAdjustmentTransactions ON #ContractualAdjustmentTransactions (parent_tcid)
		
		CREATE TABLE #LineItemsMatchingContractualCriteria([DestID] char(3) not null ,[parent_tcid] int not null,[MisQty] decimal(38,15),[CltMisChrg] decimal(38,15),[InvNum] int,[ac_desc] varchar(50) not null,[Postdate] date ,[ps_id] int,[visamt] decimal(9,2),[houramt] decimal(9,2),[pplan] varchar(100) ,[subplan] varchar(100),[locname] varchar(1),[scheduleId] int)
		create table #AvailableLineItemsToPostContractuals ([DestID] char(3) not null ,[parent_tcid] int not null,[RateType] varchar(7) not null, cltrt decimal(15,4) not null, clthrs  decimal(15,4) not null,[InvNum] int,[Postdate] date ,[ps_id] int, cltbillid int, progid int, jobdid int, shiftdate date, cltbranchid char(3) not null,[pd_description] varchar(1) not null,[pst_description] varchar(1) not null, locname  varchar(1) not null,tc_charge decimal(15,2) not null, [lir_scheduleId] int)
		create table #authorization_contractuals_shared(cefs_id int not null, pg_id int, auth_id int not null, jd_id int not null , ac_desc varchar(50) not null,ac_id int not null, caut_id int not null, capt_id int not null, ca_units decimal(9,2) not null, cca_startdate date, cca_enddate date)
		
		insert into  #AvailableLineItemsToPostContractuals
		SELECT      
			DestID = t.lir_branchcode,  
			parent_tcid = t.lir_lineitemid,  
			RateType = t.lir_ratetype,  
			cltrt = t.lir_rate, 
			CltHrs = t.lir_visithours,  
			InvNum = t.lir_invoiceid,     
			Postdate = t.lir_invoicepostdate,  
			ps_id = t.lir_payorsourceid, 
			CltBillID = t.lir_authorizationid,  
			ProgID = t.lir_programid,  
			JobDID = t.lir_jobdescriptionid,  
			ShiftDate = t.lir_servicedate,  
			CltBranchID = t.lir_branchcode,  
			pd_description = '''',  
			pst_description = '''',  
			LocName = '''',
			tc_charge = t.lir_calculatedamount,
			t.lir_scheduleId
		FROM hchb.billing.LINE_ITEMS_REVENUE t 
		WHERE
			t.lir_covered = 1
			AND t.lir_contractualadjustmentdate IS NULL
			AND t.lir_lineitemtypeid in (1, 2) /*support contractuals for visits and shift splits*/
			AND t.lir_rate > 0
			AND t.lir_void = 0
			and t.lir_authorizationid > 0
  		
		----[dbo].[usp_GetUnappliedContractualAdjustmentsTransactions]
		;with temp_common as---common date 
		(select
				 a.auth_cefsid
				,a.auth_programid
				,a.auth_id
				,ac.ac_desc
				,ac.ac_id
				,caut.caut_id
				,capt.capt_id
				,cefs_c.cefsc_units
				,bc.bc_jdid
				,b.abt_id
				,cefs_c.cefsc_id --- added for TFS 47554
				,cefs_c.cefsc_effectiveFrom cca_startdate
				,cefs_c.cefsc_effectiveto cca_enddate
			--into #temp_common
			from hchb.dbo.authorizations a with(nolock)
				--join #authids a_t on a.auth_id = a_t.auth_id
				join hchb.dbo.client_episode_fs cefs with(nolock) on cefs.cefs_id = a.auth_cefsid
				join hchb.dbo.payor_sources ps with(nolock) on cefs.cefs_psid = ps.ps_id
				join hchb.dbo.programs p with(nolock) on a.auth_programid = p.pg_id
					and p.pg_active = 'Y'		
				join hchb.[dbo].[udf_BillingCode_GetActiveByDate]() bc on p.pg_id = bc.bc_pgid
				join hchb.dbo.authorization_budget_types b with(nolock) on a.auth_abtid = b.abt_id
					and b.abt_active = 'Y'
				join hchb.dbo.client_episode_fs_contractuals cefs_c with(readcommitted) on cefs_c.cefsc_cefsid = a.auth_cefsid
					and cefs_c.cefsc_active = 'Y'
				join hchb.dbo.adjustment_codes ac with(nolock) on cefs_c.cefsc_acid = ac.ac_id
				join hchb. dbo.contractual_adjustments_unit_types caut on caut.caut_id = cefs_c.cefsc_cautid
					and caut.caut_active = 'Y'
				join hchb.dbo.contractual_adjustments_period_types capt on capt.capt_id = cefs_c.cefsc_captid
					and capt.capt_active = 'Y'
			where 0=0
				and a.auth_active = 'Y'
				and cefs_c.cefsc_units <> 0
				and IsNull(ps.ps_contractualadjustmentsbyclient,'N') = 'Y'
		)
	insert into  #authorization_contractuals_shared
		select DISTINCT
				 tc.auth_cefsid cefs_id
				,tc.auth_programid pg_id
				,tc.auth_id auth_id
				,jd.jd_id jd_id
				,tc.ac_desc ac_desc
				,tc.ac_id ac_id
				,tc.caut_id caut_id
				,tc.capt_id capt_id
				,tc.cefsc_units ca_units
				,tc.cca_startdate  cca_startdate
				,tc.cca_enddate cca_enddate
			from temp_common tc
				join hchb.dbo.authorization_disciplines ad with(nolock) on tc.auth_id = ad.ad_authid
					and ad.ad_active = 'Y'
				join hchb.dbo.disciplines d with(nolock) on ad.ad_dscid = d.dsc_id
					and d.dsc_active = 'Y'
				join hchb.dbo.job_descriptions jd with(nolock) on ad.ad_dscid = jd.jd_dscid and tc.bc_jdid = jd.jd_id
					and jd.jd_active = 'Y'
				join hchb.dbo.authorization_discipline_contractuals ad_c with(readcommitted) on ad_c.adc_cefscid = tc.cefsc_id -- changed for TFS 47554
					and ad_c.adc_adid = ad.ad_id
			where 0=0
				and tc.abt_id = 2
			union
			select DISTINCT
				 tc.auth_cefsid
				,tc.auth_programid
				,tc.auth_id
				,jd.jd_id
				,tc.ac_desc
				,tc.ac_id
				,tc.caut_id
				,tc.capt_id
				,tc.cefsc_units
				,tc.cca_startdate
				,tc.cca_enddate
			from temp_common tc
				join hchb.dbo.authorization_jobdescriptions aj with(nolock) on tc.auth_id = aj.ajd_authid
					and aj.ajd_active = 'Y'
				join hchb.dbo.job_descriptions jd with(nolock) on aj.ajd_jdid = jd.jd_id and tc.bc_jdid = jd.jd_id
					and jd.jd_active = 'Y'
				join hchb.dbo.authorization_jobdescription_contractuals ajd_c with(readcommitted) on ajd_c.ajdc_cefscid = tc.cefsc_id -- changed for tfs 47554
					and ajd_c.ajdc_ajdid = aj.ajd_id
			where 0=0
				and tc.abt_id = 3

       insert into #LineItemsMatchingContractualCriteria	
		SELECT 				
				DestID = t.DestID,
				parent_tcid = t.parent_tcid,
				MisQty = CASE WHEN t.RateType = 'INTVIS' AND ca.caut_id = 1 THEN -1
										WHEN t.RateType = 'HRVISIT' AND ca.caut_id = 1 THEN -1 * t.CltHrs
										WHEN ca.caut_id = 2 THEN -1
								END * (ca.ca_units / abs(ca.ca_units)),
				CltMisChrg = CASE WHEN t.RateType = 'HRVISIT' AND ca.caut_id = 2 THEN ((t.tc_charge) * (ca.ca_units / 100))
										WHEN t.RateType = 'INTVIS'  AND ca.caut_id = 2 THEN ((t.cltrt * 1) * (ca.ca_units / 100))
										WHEN ca.caut_id = 1 THEN ca.ca_units 
									   END * (ca.ca_units / abs(ca.ca_units)),
				t.InvNum,
				ca.ac_desc,
				t.Postdate,
				t.ps_id,
				visamt  = (CASE WHEN ca.capt_id = 2 AND ca.caut_id = 1 THEN ca.ca_units ELSE 0 END), 
				houramt = (CASE WHEN ca.capt_id = 1 AND ca.caut_id = 1 THEN ca.ca_units ELSE 0 END),
				pplan =pd.pd_description, 
				subplan = pst.pst_description, 
				locname = '',
				scheduleId = t.lir_scheduleId		
			FROM #AvailableLineItemsToPostContractuals t
			JOIN #authorization_contractuals_shared ca on t.progid = ca.pg_id 
					AND t.jobdid = ca.jd_id 
					AND t.CltBillID = ca.auth_id
					AND t.shiftdate >= isnull(ca.CCA_startdate, '1/1/1753') 
					AND t.shiftdate <= isnull(ca.CCA_enddate, '12/31/9999')
			JOIN hchb.dbo.Programs pg WITH(NOLOCK) on t.ProgID = pg.pg_id
			LEFT JOIN hchb.dbo.Program_Disciplines pd WITH(NOLOCK) ON pd.pd_id = pg.pg_pdid
			LEFT JOIN hchb.dbo.Program_Shift_Types pst WITH(NOLOCK) ON pg.pg_pstid = pst.pst_id
			WHERE 0=1
				OR (t.RateType = 'HRVISIT' AND ca.capt_id = 1)	
				OR (t.RateType = 'INTVIS'  AND ca.capt_id = 2)	


		INSERT INTO #ContractualAdjustmentTransactions
		--create view [dbo].[zzz_ContractualAdjustmentTransactions] as
         select (ROW_NUMBER() OVER (PARTITION BY t.DestID ORDER BY t.parent_tcid ,ac_desc ,CltMisChrg)) as AdjustmentID,* 
		from #LineItemsMatchingContractualCriteria t
		
		-- group line items in #ContractualAdjustmentTransactions, adding the visitamount and hour amount for multiple adjustments
		select parent_tcid as tc_id, SUM(visamt) as visamt,SUM(houramt) as houramt ,pplan, subplan, locname , sum(CltMisChrg) as CAMisChrg
		into #Contractuals
		from #ContractualAdjustmentTransactions
		group by parent_tcid,pplan,subplan,locname

		CREATE UNIQUE INDEX IX_Contractuals ON #Contractuals(tc_id)

		
		create table #AllLineItems_BillingAuditPerform ([pk] [int] NOT NULL,[tc_id] [int] NOT NULL,[pa_lastname] [varchar](50) NOT NULL,
		                                                [pa_firstname] [varchar](50) NOT NULL,[pa_mi] [char](1) NULL,[ps_desc] [varchar](50) NOT NULL,
	                                                    [shiftdate] [datetime] NULL,[BeginTime] [datetime] NOT NULL,[EndTime] [datetime] NOT NULL,
	                                                    [jd_description] [varchar](35) NULL,[jd_code] [varchar](4) NOT NULL,[CltServices] [varchar](503) NOT NULL,
	                                                    [FlatType] [varchar](10) NOT NULL,[cltbillid] [int] NOT NULL,[CltRt] [money] NULL,[PrimID] [int] NOT NULL,
	                                                    [epiid] [int] NOT NULL,[WorkerName] [varchar](102) NOT NULL,[InsID] [int] NOT NULL,
	                                                    [ServiceLineID] [int] NOT NULL,[CltBranchID] [char](3) NOT NULL,[JobDID] [int] NOT NULL,
	                                                    [GroupID] [int] NOT NULL,[ProgID] [int] NOT NULL,[XSort] [varchar](1) NOT NULL,[SupplyFlag] [varchar](1) NOT NULL,
	                                                    [agency_id] [int] NOT NULL,[RateType] [varchar](7) NOT NULL,[CltHrs] [decimal](15, 4) NULL,[TotHrs] [decimal](15, 4) NULL,
	                                                    [v_OriginalCharge] [decimal](15, 2) NOT NULL,[v_TotalAdjustments] [decimal](18, 2) NULL,
	                                                    [RevCode] [varchar](10) NULL,[HCPCS] [varchar](10) NULL,[pn_description] [varchar](100) NULL,
	                                                    [CltID] [int] NOT NULL,[cst_status] [varchar](15) NULL,[pt_id] [int] NOT NULL,[pt_desc] [varchar](50) NOT NULL,
	                                                    [BillableCharge] [int] NOT NULL,[team_NAME] [varchar](10) NOT NULL,[formID] [int] NULL,
	                                                    [Modifier] [varchar](5) NULL,[visamt] [decimal](38, 2) NULL,[houramt] [decimal](38, 2) NULL,
                                                        [pplan] [varchar](100) NULL,[subplan] [varchar](100) NULL,[misqty] [decimal](15, 4) NULL,
	                                                    [cltmischrg] [money] NULL,[milenumb] [decimal](15, 2) NULL,[cltmilert] [decimal](15, 2) NULL,
	                                                    [nopaybill] [int] NOT NULL,[locname] [varchar](1) NULL,[CAMisChrg] [decimal](38, 7) NULL,
	                                                    [contadjdate] [date] NULL,[hicpic] [varchar](10) NOT NULL,[revenuecode] [varchar](10) NOT NULL,
	                                                    [SplitByEpisode] [int] NOT NULL,[SplitByMonth] [bit] NOT NULL,[InvoiceType] [int] NULL,
	                                                    [invnum] [int] NOT NULL,[invoice_grouping] [int] NULL,[inferred_epi] [int] NULL,
	                                                    [inferred_month] [int] NULL,[inferred_day] [datetime] NULL,[inferred_discipline] [int] NULL,
	                                                    [inferred_auth] [varchar](20) NULL,[IsCPTCharge] [bit] NULL,[cp_id] [int] NOT NULL,
	                                                    [vicefs_ps] [char](1) NOT NULL,[epi_StartOfEpisode] [datetime] NULL,[epi_EndOfEpisode] [datetime] NULL,
	                                                    [auth_pending] [char](1) NULL,[t_owed] [decimal](38, 2) NULL,[TimecardID] [int] NOT NULL,
	                                                    [BillType] [int] NOT NULL,[ReBill] [bit] NOT NULL,[cp_name] [nvarchar](250) NOT NULL,[epi_SocDate] [datetime] NULL,
	                                                    [tc_labtest] [bit] NULL,[ClaimBillTypeID] [int] NOT NULL,[AllowReplacementClaims] [int] NOT NULL,
	                                                    [SplitByDay] [bit] NOT NULL,[SplitByDiscipline] [bit] NOT NULL,[SplitByAuthNo] [bit] NOT NULL,
	                                                    [AuthNumber] [varchar](20) NULL,[DisciplineID] [int] NOT NULL,[IsMergeLineItem] [bit] NULL,
	                                                    [CPT_Code] [char](1) NULL,[tc_phsid] [int] NULL,[tc_charge] [decimal](15, 2) NOT NULL,[scheduleId] [int] NULL)

        print ('#AllLineItems_BillingAuditPerform')

		insert INTO #AllLineItems_BillingAuditPerform
	     SELECT 0 as pk
		       ,t.lir_lineitemid as tc_id
			   ,p.pa_lastname
			   ,p.pa_firstname
			   ,p.pa_mi
			   ,pe.ps_desc
			   ,shiftdate = DATEADD(dd, 0, DATEDIFF(dd, 0, t.lir_servicedate))
			   ,IsNull(t.lir_begintime1900,'1/1/1753') as BeginTime
			   ,IsNull(t.lir_endtime1900,'12/31/9999') as EndTime
			   ,jd.jd_description
			   ,IsNull(jd.jd_code,'-1') as jd_code
			   ,t.lir_descriptionandnotes as CltServices
			   ,IsNull(sc.sc_code,'') as FlatType
			   ,IsNull(t.lir_authorizationid, 0) as cltbillid
			   ,t.lir_visitrate as CltRt
			   ,IsNull(t.lir_epifundingsourceid, 0) as PrimID
			   ,IsNull(t.lir_episodeid, 0) as epiid
			   ,IsNull(w.wkr_lastname + ', ' + w.wkr_firstname, '') as WorkerName
			   ,IsNull(t.lir_payorsourcebranchid, 0) as InsID
			   ,IsNull(t.lir_servicelineid, 1) as ServiceLineID
			   ,t.lir_branchcode as CltBranchID
			   ,IsNull(t.lir_jobdescriptionid, 0) as JobDID
			   ,t.lir_payorsourceid as GroupID
			   ,IsNull(t.lir_programid, 0) as ProgID
			   ,case
					WHEN t.lir_revenuetypeid = 1 AND t.lir_rebill = 1 THEN 'P'
					WHEN t.lir_revenuetypeid = 1 THEN 'A'
					WHEN t.lir_revenuetypeid = 10 THEN 'P'
					else 'C'
				end as XSort --this is deprecated, remove once we get capacity to remove from report report rdl
			   ,IsNull(case when t.lir_routinesupply = 1 then 'R' when t.lir_routinesupply = 0 then 'N' else null end, '') as SupplyFlag
			   ,a.agency_id
			   ,t.lir_ratetype as RateType
			   ,t.lir_visithours as CltHrs
			   ,t.lir_durationinhours as TotHrs
			   ,t.lir_calculatedamount as v_OriginalCharge
			   ,(t.lir_revenueamount - t.lir_revenueadjustedamount) * -1 as v_TotalAdjustments
			   ,case
			      when t.lir_revenuetypeid = 8 then t.lir_revenuecode
				  else coalesce(t.lir_revenuecode, be.bc_revcode)
				end as RevCode
			   ,coalesce(t.lir_hcpcs, be.bc_hcpcs) as HCPCS
			   ,pge.pn_description
			   ,t.lir_clientid as CltID
			   ,p.pa_status as cst_status
			   ,pe.pt_id
			   ,pe.pt_desc
			   ,CASE
			      WHEN t.lir_covered = 1 THEN 1
				  ELSE 0
				END as BillableCharge
			   ,team_NAME
			   ,pe.ps_itid as formID
			   ,be.bc_modifier as Modifier
			   ,t_c.visamt
			   ,t_c.houramt
			   ,t_c.pplan
			   ,t_c.subplan
			   ,t.lir_miscunits as misqty
			   ,t.lir_miscrate as cltmischrg
			   ,cast(0 as decimal(15, 2)) as milenumb
			   ,cast(0 as decimal(15, 2)) as cltmilert
			 --  ,sl.serviceline
			   ,(case when t.lir_covered = 1 then 1 else 2 end) as nopaybill
			   ,t_c.locname
			   ,CAMisChrg
			   ,t.lir_contractualadjustmentdate as contadjdate
			   ,IsNull(t.lir_hcpcs,'') as hicpic
			   ,ISNULL(t.lir_revenuecode,'') as revenuecode
			   ,pe.ps_spanacrossepisodes^1 as SplitByEpisode
			   ,pe.ps_billbymonth as SplitByMonth
			   ,pe.ps_itid as InvoiceType
			   ,IsNull(t.lir_invoiceid, 0) as invnum
			   ,CAST(0 as int) as invoice_grouping
			   ,CAST(-1 as int) as inferred_epi
			   ,CAST(-1 as int) as inferred_month
			   ,cast('1/1/1753' as datetime) as inferred_day
			   ,CAST(-1 as int) as inferred_discipline
			   ,cast(-1 as varchar(20)) as inferred_auth
			   ,cast((CASE WHEN t.lir_revenuetypeid = 8 then 1 ELSE 0 END) as bit) as IsCPTCharge
			   ,cp.cp_id
			   ,cefs.vicefs_ps
			   ,ce.epi_StartOfEpisode
			   ,ce.epi_EndOfEpisode
			   ,au.auth_pending
			   ,t.lir_revenueadjustedamount - isnull(lica.lica_balance, 0) as t_owed
			   ,0 as TimecardID
			   ,case
			      when t.lir_ediexportdate is null and t.lir_edibatchid is null then 0
				  else 1
				end as BillType
			   ,t.lir_rebill as ReBill
			   ,cp.cp_name
			   ,ce.epi_SocDate
			   ,cast((CASE WHEN t.lir_revenuetypeid = 7 THEN 1 ELSE 0 END) as bit) as tc_labtest
			   ,ClaimBillTypeID = 1
			   ,AllowReplacementClaims = pe.ps_allowReplacementClaims & 1
			   ,SplitByDay = pe.ps_billByDay
			   ,SplitByDiscipline = pe.ps_billByDiscipline
			   ,SplitByAuthNo = pe.ps_splitbyauthno
			   ,AuthNumber = cast(au.auth_no as varchar(20))
			   ,DisciplineID = isnull(jd.jd_dscid,0)
			   ,IsMergeLineItem = cast(0 as bit)
			   ,cast((case when t.lir_revenuetypeid = 8 then 'Y' else null end) as char(1)) as CPT_Code
			   ,tc_phsid = t.lir_physicianserviceid
			   ,t.lir_calculatedamount as tc_charge
			   ,scheduleId = t.lir_scheduleid
		FROM hchb.billing.LINE_ITEMS_REVENUE t
		JOIN #PAYOR_ENTITY pe on t.lir_payorsourceid = pe.ps_id
		--JOIN #BR br_i on br_i.branch_code = t.lir_branchcode
		JOIN #PA p on t.lir_clientid = p.pa_id
		--JOIN #CS i_cs on p.cst_id = i_cs.cst_id
		--JOIN #SL sl on IsNull(t.lir_servicelineid, 1) = sl.sl_id
		JOIN #AGENCY_ENTITY a on a.asb_slid = IsNull(t.lir_servicelineid,1) and t.lir_branchcode = a.asb_branchcode
		JOIN hchb.dbo.VI_EPISODE_FS cefs WITH(NOEXPAND) on cefs.vicefs_id = t.lir_epifundingsourceid
		JOIN hchb.dbo.VI_CLIENT_EPISODES ce WITH(NOEXPAND) on cefs.vicefs_epiid = ce.epi_id
		JOIN hchb.dbo.TEAMs team on team.team_id = ce.epi_TeamID
		JOIN #claim_profiles cp on cp.cp_payorsourceid = pe.ps_id and cp.cp_branchcode = t.lir_branchcode
		LEFT JOIN #PROGRAMS_ENTITY pge on t.lir_programid = pge.pg_id                                  -- LEFT JOIN REASON: An invalid/missing Program could cause an audit failure 
		LEFT JOIN #BILLING_CODES_ENTITY be on t.lir_programid = be.bc_pgid and t.lir_jobdescriptionid = be.bc_jdid   -- LEFT JOIN REASON: An invalid/missing billing code could cause an audit failure
					AND (ISNULL(be.bc_effectivefrom, '01/01/1900') <= ISNULL(t.lir_servicedate, '12/31/2099'))
					AND (ISNULL(be.bc_effectiveto, '12/31/2099') >= ISNULL(t.lir_servicedate, '12/31/2099'))
		LEFT JOIN hchb.dbo.JOB_DESCRIPTIONS jd on t.lir_jobdescriptionid = jd.jd_id                       -- LEFT JOIN REASON: An invalid/missing Job Description could cause an audit failure
		LEFT JOIN hchb.dbo.SERVICECODES sc on sc.sc_id = t.lir_servicecodeid
		LEFT JOIN hchb.dbo.WORKERS w on t.lir_workerid = w.wkr_id                                  -- LEFT JOIN REASON: An invalid/missing Worker could cause an audit failure
		LEFT JOIN #Contractuals t_c on t.lir_lineitemid = t_c.tc_id                                      -- LEFT JOIN REASON: Not all line items have a contractual
		LEFT JOIN hchb.dbo.AUTHORIZATIONS au on t.lir_authorizationid = au.auth_id                    -- LEFT JOIN REASON: An invalid/missing Authorization could cause an audit failure
		--LEFT JOIN #PayorAttachmentRequirements x ON x.epi_id = ce.epi_id and x.par_psid = pe.ps_id
		LEFT JOIN hchb.billing.LINE_ITEM_CASH_AMOUNTS lica on lica.lica_lineitemid = t.lir_lineitemid
		WHERE t.lir_payorsourcefrequency = 0
		    and t.lir_servicelineid = 1
			AND t.lir_includeonclaim = 1
			AND
			(
				t.lir_invoicepostdate is null
			)
			AND t.lir_invoiceid is null
			AND t.lir_authorizationid > 0
			AND t.lir_covered = 1
			--AND @AlreadyInvoiced = 0
			AND ce.epi_DischargeDate IS NOT NULL
			AND t.lir_servicedate BETWEEN '1/1/1900' and '12/31/2099'
	    union
			SELECT
				0 as pk,
				t.lir_lineitemid,
				p.pa_lastname,
				p.pa_firstname,
				p.pa_mi,
				pe.ps_desc,
				shiftdate = DATEADD(dd, 0, DATEDIFF(dd, 0, t.lir_servicedate)),
				IsNull(t.lir_begintime1900,'1/1/1753') as BeginTime,
				IsNull(t.lir_endtime1900,'12/31/9999') as EndTime,
				jd.jd_description,
				IsNull(jd.jd_code,'-1') as jd_code,
				t.lir_descriptionandnotes,
				IsNull(sc.sc_code,'') as FlatType,
				IsNull(t.lir_authorizationid, 0),
				t.lir_visitrate,
				IsNull(t.lir_epifundingsourceid, 0),
				IsNull(t.lir_episodeid, 0),
				IsNull(w.wkr_lastname + ', ' + w.wkr_firstname, '') as WorkerName,
				IsNull(t.lir_payorsourcebranchid, 0),
				IsNull(t.lir_servicelineid,1) as ServiceLineID,
				t.lir_branchcode,
				IsNull(t.lir_jobdescriptionid, 0),
				t.lir_payorsourceid,
				IsNull(t.lir_programid, 0),
				case
					WHEN t.lir_revenuetypeid = 1 AND t.lir_rebill = 1 THEN 'P'
					WHEN t.lir_revenuetypeid = 1 THEN 'A'
					WHEN t.lir_revenuetypeid = 10 THEN 'P'
					else 'C'
				end as XSort, --this is deprecated, remove once we get capacity to remove from report report rdl
				IsNull(case when t.lir_routinesupply = 1 then 'R' when t.lir_routinesupply = 0 then 'N' else null end, '') as SupplyFlag,
				a.agency_id,
				t.lir_ratetype,
				t.lir_visithours,
				t.lir_durationinhours as TotHrs,
				t.lir_calculatedamount as v_OriginalCharge,
				(t.lir_revenueamount - t.lir_revenueadjustedamount) * -1 as v_TotalAdjustments,
				case when t.lir_revenuetypeid = 8 then t.lir_revenuecode else  coalesce(t.lir_revenuecode, be.bc_revcode) end as RevCode,
				coalesce(t.lir_hcpcs, be.bc_HCPCS) as HCPCS,
				pge.pn_description,
				t.lir_clientid as CltID,
				p.pa_status as cst_status,
				pe.pt_id,
				pe.pt_desc,
				CASE WHEN t.lir_covered = 1 THEN 1 ELSE 0 END as BillableCharge,
				team_name,
				pe.ps_itid as formID,
				be.bc_modifier as Modifier,
				t_c.visamt,
				t_c.houramt,
				t_c.pplan,
				t_c.subplan,
				t.lir_miscunits as misqty,
				t.lir_miscrate as cltmischrg,
				cast(0 as decimal(15, 2)) as milenumb,
				cast(0 as decimal(15, 2)) as cltmilert,
				--sl.serviceline,
				(case when t.lir_covered = 1 then 1 else 2 end) as nopaybill,
				t_c.locname,
				CAMisChrg,
				t.lir_contractualadjustmentdate as contadjdate,
				IsNull(t.lir_hcpcs,'') as hicpic,
				ISNULL(t.lir_revenuecode,'') as revenuecode,
				pe.ps_spanacrossepisodes^1 as SplitByEpisode, 
				pe.ps_billbymonth as SplitByMonth,
				pe.ps_itid as InvoiceType,
				IsNull(t.lir_invoiceid, 0) as invnum,
				y.i_id as invoice_grouping,
				IsNull(t.lir_episodeid, 0) as inferred_epi,
				(YEAR(t.lir_servicedate) * 100) + MONTH(t.lir_servicedate) as inferred_month,
				DATEADD(dd, 0, DATEDIFF(dd, 0, t.lir_servicedate)) as inferred_day,
				jd_dscid as inferred_discipline,
				au.auth_no as inferred_auth,
				cast((CASE WHEN t.lir_revenuetypeid = 8 then 1 ELSE 0 END) as bit) as IsCPTCharge,
				cp.cp_id,
				cefs.vicefs_ps,
				ce.epi_StartOfEpisode,
				ce.epi_EndOfEpisode,
				au.auth_pending,
				t.lir_revenueadjustedamount - isnull(lica.lica_balance, 0) as t_owed,
				t.lir_lineitemid as TimecardID,
				case when t.lir_ediexportdate is null and t.lir_edibatchid is null then 0 else 1 end as BillType,
				t.lir_rebill as ReBill,
				cp.cp_name,
				ce.epi_SocDate,
				cast((CASE WHEN t.lir_revenuetypeid = 7 THEN 1 ELSE 0 END) as bit) as tc_labtest,
				ClaimBillTypeID = y.i_claimbilltypeid,
				AllowReplacementClaims = pe.ps_allowReplacementClaims & 1,
				SplitByDay = pe.ps_billByDay,
				SplitByDiscipline = pe.ps_billByDiscipline,
				SplitByAuthNo = pe.ps_splitbyauthno,
				AuthNumber = cast(au.auth_no as varchar(20)),
				DisciplineID = isnull(jd.jd_dscid,0),
				IsMergeLineItem = cast(0 as bit),
				cast((case when t.lir_revenuetypeid = 8 then 'Y' else null end) as char(1)) as CPT_Code,
				t.lir_physicianserviceid as tc_phsid,
				t.lir_calculatedamount as tc_charge
				,scheduleId = t.lir_scheduleid
			FROM hchb.[billing].[INVOICES] y
				JOIN hchb.billing.LINE_ITEMS_REVENUE t on t.lir_invoiceid = y.i_id
				JOIN  #PAYOR_ENTITY pe on t.lir_payorsourceid = pe.ps_id
				--JOIN #BR br_i on br_i.branch_code = t.lir_branchcode
				JOIN #PA p on t.lir_clientid = p.pa_id
				--JOIN #CS i_cs on p.cst_id = i_cs.cst_id
				--JOIN #SL sl on IsNull(t.lir_servicelineid,1) = sl.sl_id
				JOIN #AGENCY_ENTITY a on a.asb_slid = IsNull(t.lir_servicelineid,1) and t.lir_branchcode = a.asb_branchcode
				JOIN hchb.dbo.VI_EPISODE_FS cefs WITH(NOEXPAND) on cefs.vicefs_id = t.lir_epifundingsourceid
				JOIN hchb.dbo.VI_CLIENT_EPISODES ce WITH(NOEXPAND) on cefs.vicefs_epiid = ce.epi_id
				JOIN hchb.dbo.TEAMs team on team.team_id = ce.epi_TeamID
				JOIN #claim_profiles cp on cp.cp_payorsourceid = pe.ps_id and cp.cp_branchcode =  t.lir_branchcode
				LEFT JOIN #PROGRAMS_ENTITY pge on t.lir_programid = pge.pg_id                                  -- LEFT JOIN REASON: An invalid/missing Program could cause an audit failure 
				LEFT JOIN hchb.dbo.AUTHORIZATIONS au on t.lir_authorizationid = au.auth_id                    -- LEFT JOIN REASON: An invalid/missing Authorization could cause an audit failure 
				LEFT JOIN #BILLING_CODES_ENTITY  be on t.lir_programid = be.bc_pgid and t.lir_jobdescriptionid = be.bc_jdid   -- LEFT JOIN REASON: An invalid/missing billing code could cause an audit failure
						AND (ISNULL(be.bc_effectivefrom, '01/01/1900') <= ISNULL(t.lir_servicedate, '12/31/2099'))
						AND (ISNULL(be.bc_effectiveto, '12/31/2099') >= ISNULL(t.lir_servicedate, '12/31/2099'))
				LEFT JOIN hchb.dbo.JOB_DESCRIPTIONS jd on t.lir_jobdescriptionid = jd.jd_id                       -- LEFT JOIN REASON: An invalid/missing Job Description could cause an audit failure
				LEFT JOIN hchb.dbo.SERVICECODES sc on sc.sc_id = t.lir_servicecodeid
				LEFT JOIN hchb.dbo.WORKERS w on t.lir_workerid = w.wkr_id                                  -- LEFT JOIN REASON: An invalid/missing Worker could cause an audit failure
				LEFT JOIN  #Contractuals  t_c on t.lir_lineitemid = t_c.tc_id                                      -- LEFT JOIN REASON: Not all line items have a contractual
			    --LEFT JOIN #PayorAttachmentRequirements x ON x.epi_id = ce.epi_id and x.par_psid = pe.ps_id
				LEFT JOIN hchb.billing.LINE_ITEM_CASH_AMOUNTS lica on lica.lica_lineitemid = t.lir_lineitemid
			WHERE
				y.i_pfid = 0
				and t.lir_servicelineid = 1
				AND t.lir_invoiceid is not null
				AND pe.ps_itid IN (5,6,8) --ONLY SUPPORT CMS1500 and UB04 FOR ALREADY INVOICE OPTION AT THE MOMENT
			--	AND ((@AlreadyInvoiced = 1 AND t.lir_rebill in (@Rebill, ~@OriginalClaim)) OR @AlreadyInvoiced = 0) --if you are loading to check for mergeable line items, then don look at rebill flag
			--	AND ((@AlreadyInvoiced = 1 AND (@IncludeDischargeOnly = 0 OR ce.epi_DischargeDate IS NOT NULL)) OR @AlreadyInvoiced = 0) --if you are loading to check for mergeable line items, then don look at epi dischargedate
			--	AND (@AlreadyInvoiced = 1 OR (@AlreadyInvoiced = 0 AND pe.ps_allowReplacementClaims = 1 AND @AllowReplacementClaimsSetting = 1)) --if you are loading to check for mergeable line items, then only bring back payors that allow replacement
			--	AND (isnull(@EDIBatchNumbers, '') = '' OR t.lir_edibatchid in (select s.stringname from dbo.fn_SplitString(@EDIBatchNumbers,',') as s))
				AND  t.lir_servicedate BETWEEN '1/1/1900' and '12/31/2099'
				AND EXISTS ( select 1 from hchb.dbo.failed_electronic_claims fec where fec.fec_invoiceid = y.i_id)


		SELECT
			tc_id = at.tc_id,
			MergeToInvoiceNo = MAX(t.InvNum)
	    INTO #MergeableLineItems_BillingAuditPerform
		FROM #AllLineItems_BillingAuditPerform at
		join #AllLineItems_BillingAuditPerform t on t.CltID = at.CltID and t.CltBranchID = at.CltBranchID and t.GroupID = at.GroupID and t.AllowReplacementClaims = 1 and t.InvNum > 0
		join hchb.dbo.YrInvo y on y.InvNum = t.invnum
		join
		(
			select
				invnum = a.invnum,
				MinShiftDate = MIN(shiftdate),
				MaxShiftDate = MAX(shiftdate)
			from #AllLineItems_BillingAuditPerform a
			where a.invnum > 0
			group by a.invnum

		) minMaxDates on y.InvNum = minMaxDates.invnum
		left join hchb.dbo.JOB_DESCRIPTIONS jd on jd_id = t.JobDID
		WHERE 0=0
			and at.invnum = 0 --unbilled line items
			and at.BillableCharge = 1 --you can only merge billable charges
			and at.AllowReplacementClaims = 1 --that allow replacement claims
			and at.ShiftDate between minMaxDates.MinShiftDate and minMaxDates.MaxShiftDate
			and y.ClaimBillTypeID in (1,2) --you can only merge into a new or existing replacement claim, cant merge into a voided claim
			and not exists(select 1 from hchb.billing.V_LINE_ITEMS t where t.li_original_invoiceid = y.InvNum) --cant merge into any invoice that has been rebilled
			--specific split rules
			and t.epiid = case when at.SplitByEpisode = 1 then at.epiid else t.epiid end
			and (YEAR(t.shiftdate) * 100) + MONTH(t.shiftdate) = case when at.SplitByMonth = 1 then (YEAR(at.shiftdate) * 100) + MONTH(at.shiftdate) else (YEAR(t.shiftdate) * 100) + MONTH(t.shiftdate) end
			and dateadd(dd, 0, datediff(dd, 0, t.ShiftDate)) = case when at.SplitByDay = 1 then at.ShiftDate else dateadd(dd, 0, datediff(dd, 0, t.ShiftDate)) end
			and isnull(jd.jd_dscid,0) = case when at.SplitByDiscipline = 1 then isnull(at.DisciplineID,0) else isnull(jd.jd_dscid,0) end
		GROUP BY at.tc_id
		
			--set the invoice number on the unbilled line items to the one it will merge with, this will allow all the logic below to follow suit (charge summation, billing audits, etc)
			update at
			set
				at.invnum = mt.MergeToInvoiceNo,
				invoice_grouping = mt.MergeToInvoiceNo,
				inferred_epi = at.epiid,
				inferred_month = (YEAR(at.shiftdate) * 100) + MONTH(at.shiftdate),
				inferred_day = at.ShiftDate,
				inferred_discipline = at.DisciplineID,
				inferred_auth = cast(at.AuthNumber as varchar(20)),
				IsMergeLineItem = 1
			from #AllLineItems_BillingAuditPerform at
			join #MergeableLineItems_BillingAuditPerform mt on mt.tc_id = at.tc_id

			--update claim bill type to "Replacement Claims" on the screen
			update at
			set at.ClaimBillTypeID = 2
			from #AllLineItems_BillingAuditPerform at
			join (select distinct MergeToInvoiceNo = MergeToInvoiceNo from #MergeableLineItems_BillingAuditPerform) mt on mt.MergeToInvoiceNo = at.invnum
		

		;WITH InvoiceGrouping AS
			(
				select
					ROW_NUMBER() OVER (order by s.CltID) as invoice_grouping,
					s.CltID,
					s.CltBranchID,
					s.GroupID,
					s.cst_status,
					s.pt_id,
					s.epi_socdate,
					case when s.SplitByEpisode = 1 then s.epiid else 0 end as inferred_epi,
					case when s.SplitByMonth = 1 then (YEAR(s.shiftdate) * 100) + MONTH(s.shiftdate) else 0 end as inferred_month,
					case when s.SplitByDay = 1 then s.ShiftDate else @defaultInferredDate end as inferred_day,
					case when s.SplitByDiscipline = 1 then s.DisciplineID else 0 end as inferred_discipline,
					case when s.SplitByAuthNo = 1 then cast(s.AuthNumber as varchar(20)) else cast(0 as varchar(20)) end as inferred_auth
				from #AllLineItems_BillingAuditPerform s
				where s.invnum = 0
				group by s.CltID, s.CltBranchID, s.GroupID, s.cst_status, s.pt_id
				,s.epi_socdate
				,case when s.SplitByEpisode = 1 then s.epiid else 0 end
				,case when s.SplitByMonth = 1 then (YEAR(shiftdate) * 100) + MONTH(shiftdate) else 0 end
				,case when s.SplitByDay = 1 then s.ShiftDate else @defaultInferredDate end
				,case when s.SplitByDiscipline = 1 then s.DisciplineID else 0 end
				,case when s.SplitByAuthNo = 1 then cast(s.AuthNumber as varchar(20)) else cast(0 as varchar(20)) end
			)

			update s
				set invoice_grouping    = (i.invoice_grouping) * -1 --so we dont conflict with invoice numbers that are positive
				   ,inferred_epi        = i.inferred_epi
				   ,inferred_month      = i.inferred_month
				   ,inferred_day        = i.inferred_day
				   ,inferred_discipline = i.inferred_discipline
				   ,inferred_auth		= cast(i.inferred_auth as varchar(20))
			from #AllLineItems_BillingAuditPerform s
			join InvoiceGrouping i on s.CltID=i.CltID and  s.CltBranchID = i.CltBranchID and  s.GroupID=i.GroupID and s.cst_status=i.cst_status and s.pt_id=i.pt_id
				and isnull(s.epi_socdate,'1/1/1900') = isnull(i.epi_socdate,'1/1/1900')
			where 0=0
				AND s.invnum = 0
				AND i.inferred_epi = case when s.SplitByEpisode = 1 then s.epiid else i.inferred_epi end
				AND i.inferred_month = case when s.SplitByMonth = 1 then (YEAR(s.shiftdate) * 100) + MONTH(s.shiftdate) else i.inferred_month end
				AND i.inferred_day = case when s.SplitByDay = 1 then s.ShiftDate else i.inferred_day end
				AND i.inferred_discipline = case when s.SplitByDiscipline = 1 then s.DisciplineID else i.inferred_discipline end
				AND i.inferred_auth = case when s.SplitByAuthNo = 1 then cast(s.AuthNumber as varchar(20)) else cast(i.inferred_auth as varchar(20)) end

			 --check constraint on temp table so that invoice_grouping <> 0
			ALTER TABLE #AllLineItems_BillingAuditPerform WITH CHECK ADD CHECK (invoice_grouping <> 0)
			ALTER TABLE #AllLineItems_BillingAuditPerform WITH CHECK ADD CHECK (inferred_epi <> -1)
			ALTER TABLE #AllLineItems_BillingAuditPerform WITH CHECK ADD CHECK (inferred_month <> -1)
			ALTER TABLE #AllLineItems_BillingAuditPerform WITH CHECK ADD CHECK (inferred_day <> cast('1/1/1753' as datetime))
			ALTER TABLE #AllLineItems_BillingAuditPerform WITH CHECK ADD CHECK (inferred_discipline <> -1)
		-- b.  Get all auth 
		PRINT('Get all authorizations')
		    INSERT INTO #AUTHORIZATIONS
	        ( auth_id ,
	          pn_description ,
	          cefs_id ,
			  cp_id,
	          auth_pending ,
	          epi_id ,
	          epi_startofepisode ,
	          epi_endofepisode ,
	          epi_branchcode ,
	          pt_desc ,
	          ps_desc ,
	          ps_id ,
	          epi_fullname ,
	          auth_no ,
	          ps_supports837 ,
	          invoice_grouping ,
	          maxshiftdate
	        )
		SELECT DISTINCT 
			t.CltBillID AS auth_id, 
			t.pn_description, 
			auth_cefsid AS cefs_id, 
			t.cp_id, 
			a.auth_pending, 
			t.epiid AS epi_id,
			ce.epi_startofepisode, 
			ce.epi_endofepisode,
			ce.epi_branchcode, 
			t.pt_desc, 
			t.ps_desc, 
			t.GroupID,
			c.pa_lastname + ', ' + c.pa_firstname + CASE WHEN c.pa_mi = '' OR c.pa_mi IS NULL THEN '' ELSE ' ' + c.pa_mi END AS epi_fullname,
			a.auth_no, 
			0,
			t.invoice_grouping, CAST('01/01/1900' AS DATE) AS maxshiftdate
		FROM
			#AllLineItems_BillingAuditPerform t
			JOIN hchb.dbo.AUTHORIZATIONS a ON t.CltBillID = auth_id
			JOIN hchb.dbo.CLIENT_EPISODES ce ON ce.epi_id = t.epiid
			JOIN hchb.dbo.CLIENTS_ALL c ON ce.epi_paid = c.pa_id
			WHERE t.BillableCharge = 1		
		
		UPDATE a
		SET 
		ps_supports837 =  1
		FROM #AUTHORIZATIONS a
		WHERE EXISTS(SELECT 1 
					 FROM hchb.dbo.CLIENT_EPISODE_FS cefs 
					 JOIN #AUTHORIZATIONS b ON cefs.cefs_id = b.cefs_id
					 JOIN hchb.dbo.PAYOR_SOURCE_BILLING_OPTIONS psbo  ON psbo.psbo_psid = cefs.cefs_psid	AND psbo.psbo_boid = 1 AND psbo.psbo_Active = 'Y'					  
					 JOIN #claim_profiles cp ON cp.cp_payorsourceid = psbo.psbo_psid
					 JOIN #claim_profile_formats cpf ON cpf.cp_id = cp.cp_id					 
					 WHERE psbo.psbo_id IS NOT NULL AND cpf.[184] IS NOT NULL AND cpf.[167] IS NOT NULL AND cpf.[169] IS NOT NULL)		
			
		print 'COMPLETE: auth'
		-- c.  Get all episodes 
		print 'get episodes'
		SELECT DISTINCT
		       c.pa_lastname + ', ' + c.pa_firstname + CASE WHEN c.pa_mi = '' or c.pa_mi is null THEN '' ELSE ' ' + c.pa_mi END as epi_fullname
		       ,ce.epi_startofepisode
			   ,ce.epi_endofepisode
			   ,csl.csl_phone as HomePhone
			   ,te.team_name
			   ,cs.cst_status as ClientStatus
			   ,c.pa_id
			   ,ce.epi_id
			   ,t.cp_id
			   ,ce.epi_branchcode
			   ,t.pt_desc
			   ,t.ps_desc
			   ,t.GroupID as ps_id
			   ,ce.epi_paperworkreceived
			   ,IsNull(cefs.cefs_assignmentofbenefits,'N') as cefs_assignmentofbenefits
			   ,t.vicefs_ps
			   ,t.PrimID as cefs_id
			   ,ce.epi_ssn
			   ,ce.epi_SocDate
			   ,IsNull(cefs.cefs_authonfile,'N') as cefs_authonfile
			   ,cefs.cefs_medicareNo
			   ,cefs.cefs_policyno
			   ,cefs.cefs_claimorder
			   ,c.pa_dob
			   ,cefs.cefs_medicaidno
			   ,ce.epi_mrccode
			   ,ps_supports837 = (CASE WHEN psbo.psbo_id IS NOT NULL AND cpf.[184] IS NOT NULL AND cpf.[167] IS NOT NULL AND cpf.[169] IS NOT NULL THEN 1 ELSE 0 END)
			   ,ce.epi_slid
			   ,IsRecert =
			     CASE
				   WHEN ce.epi_recertflag IS NOT NULL THEN 1
				   ELSE 0
				 END
		INTO #Episodes
		FROM
			#AllLineItems_BillingAuditPerform t
			INNER JOIN hchb.dbo.CLIENT_EPISODES ce on ce.epi_id = t.epiid
			LEFT  JOIN hchb.dbo.CLIENT_EPISODE_FS cefs on t.PrimID = cefs.cefs_id
			LEFT  JOIN hchb.dbo.CLIENT_EPISODE_SERVICE_LOCATIONS cesl on ce.epi_id = cesl.cesl_epiid and cesl.cesl_currentaddress = 'Y'
			LEFT  JOIN hchb.dbo.CLIENT_SERVICE_LOCATIONS csl on cesl.cesl_cslid = csl.csl_id
			LEFT  JOIN hchb.dbo.CLIENTS c on ce.epi_paid = c.pa_id
			LEFT  JOIN hchb.dbo.CLIENT_STATUSES cs on c.pa_status = cs.cst_status
			LEFT  JOIN hchb.dbo.TEAMS te on ce.epi_teamid = te.team_id
			LEFT  JOIN hchb.dbo.PAYOR_SOURCE_BILLING_OPTIONS psbo on psbo.psbo_psid = cefs.cefs_psid
				and psbo.psbo_boid = 1
				and psbo.psbo_Active = 'Y'
			LEFT  JOIN #claim_profile_formats cpf ON cpf.cp_id = t.cp_id
		WHERE t.BillableCharge = 1
		-- episode events
		create table #epi_events (epiid int not null, st_id int not null, StageEnded bit not null)

		insert into #epi_events (epiid,st_id,StageEnded)
		select
			 epi.epi_id
			,cees.cees_stid
			,StageEnded = CASE WHEN cees.cees_enddate IS NOT NULL THEN 1 ELSE 0 END
		from #Episodes epi
			inner join hchb.dbo.CLIENT_EPISODE_EVENTS cee ON cee.cee_epiid = epi.epi_id
			inner join hchb.dbo.CLIENT_EPISODE_EVENT_STAGES cees ON cees.cees_ceeid = cee.cee_id
		where 0=0
			and cees.cees_active = 'Y'

		-- episode visits
		create table #epi_visits (epiid int not null, VisitHours float null, VisitDate datetime not null, sct_id int not null, sc_pointcareformat varchar(64) null, sc_code varchar(10) not null, sc_visittype varchar(50) null, scheduleid int null)

		insert into #epi_visits (epiid,VisitHours,VisitDate,sct_id,sc_pointcareformat,sc_code,sc_visittype, scheduleid)
		select
			 epi.epi_id
			,ts.ts_inhometime
			,cev.CEV_VISITDATE
			,sc.sc_sctid
			,sc.sc_pointcareformat
			,sc.sc_code
			,sc.sc_visittype
			,sch.scheduleid
		from #Episodes epi
			inner join hchb.dbo.CLIENT_EPISODE_VISITS cev ON cev.CEV_EPIID = epi.epi_id
				and cev.cev_billable = 1
			inner join hchb.dbo.services s on cev_sid = s_id
			inner join hchb.dbo.sched sch on s.s_scheduleid = sch.scheduleid
				and sch.DifPay in (1,3) -- billable
			inner join #AllLineItems_BillingAuditPerform li ON li.scheduleid = sch.scheduleid
			inner join hchb.dbo.SERVICECODES sc ON sc.sc_id = cev.CEV_SC_ID
			inner join
			(	-- may be worthwhile to make this calculation its own view, instead of using V_TIMESLIP_VISITTIME everywhere
				select
					 ts_sid
					,ts_inhometime = SUM(DATEDIFF(ss, ts_start, ts_stop)) / 3600.0
				from hchb.dbo.TIME_SLIPS
				where ts_tstid = 1
				group by ts_sid
			) ts on ts.ts_sid = cev.cev_sid

		-- episode face to face
		CREATE TABLE #EPI_FACETOFACE
		(
			epiid INT NOT NULL, payorSourceId INT NOT NULL, ceftf_id INT NULL, ceftf_personnelresponsibleftfptid INT NULL, ceftf_encounterdate DATETIME NULL,
			ceftf_signaturedate DATETIME NULL, ceftf_required BIT, ceftf_active BIT, ceftf_f2frsid INT NULL, ceftf_insertdate DATETIME NULL,
			SocEpisodeStart DATETIME NOT NULL, SocEpisodeEnd DATETIME NOT NULL, ceftf_certifyingphysicianonclaimpoid INT NULL, DaysBeforeFaceToFace INT NOT NULL,
			DaysAfterFaceToFace INT NOT NULL
		)

		INSERT INTO #EPI_FACETOFACE (epiid, payorSourceId, ceftf_id, ceftf_personnelresponsibleftfptid, ceftf_encounterdate, ceftf_signaturedate, ceftf_required,ceftf_active,
									ceftf_f2frsid,ceftf_insertdate,SocEpisodeStart, SocEpisodeEnd, ceftf_certifyingphysicianonclaimpoid, DaysBeforeFaceToFace, DaysAfterFaceToFace)
		SELECT
			 epi.epi_id
			,epi.ps_id
			,ceftf.ceftf_id
			,ceftf_personnelresponsibleftfptid
			,ceftf_encounterdate
			,ceftf_signaturedate
			,ceftf_required
			,ceftf_active
			,ceftf_f2frsid
			,ceftf_insertdate
			,ce.epi_StartOfEpisode
			,ce.epi_EndOfEpisode
			,ceftf.ceftf_certifyingphysicianonclaimpoid
			,psftfs.psftfs_daysBefore
			,psftfs.psftfs_daysAfter
		FROM #Episodes epi
			INNER JOIN #PAYOR_ENTITY pe ON pe.ps_id = epi.ps_id
				AND pe.ps_enableF2FEncounterFeature = 'Y'
			INNER JOIN hchb.dbo.CLIENT_EPISODES ce ON ce.epi_paid = epi.pa_id
				AND ce.epi_StartOfEpisode = epi.epi_SocDate
				AND ce.epi_slid = epi.epi_slid
		        AND ce.epi_status <> 'NON-ADMIT'
		        AND ce.epi_nonadmitdate IS NULL
			LEFT JOIN hchb.dbo.CLIENT_EPISODE_FACETOFACE ceftf ON ceftf.ceftf_f2fappliestoepiid = ce.epi_id
			    AND ceftf.ceftf_active = 1
			INNER JOIN hchb.dbo.PAYOR_SOURCE_FACE_TO_FACE_SETUP AS psftfs
			  ON epi.ps_id = psftfs.psftfs_psid
			 AND psftfs.psftfs_active = 1
			 AND epi.epi_SocDate BETWEEN psftfs.psftfs_effectiveFrom AND ISNULL(psftfs.psftfs_effectiveTo, '01/01/3000')


		create table #epi_order_info --added for commercial hospice needing to check items int he orders for unsigned
		(
			epiid int not null, o_id int not null, o_otid int not null, o_physiciansigneddate datetime NULL, o_meddirsigneddate datetime null,
			o_datevoided datetime null, o_datedeclined datetime null, o_dateapproved datetime null, IsSignedByPhysician bit not null,
			IsSignedByMedicalDirector bit not null, IsSignedByPhysicianAndMedicalDirector bit not null, o_orderdate datetime not null,
			o_sendtophysician char(1) NOT NULL, o_meddirsentdate datetime NULL, o_meddir varchar(100) NULL
		)

		insert into #epi_order_info
		(
			epiid, o_id, o_otid, o_physiciansigneddate, o_meddirsigneddate, o_datevoided, o_datedeclined, o_dateapproved,
			IsSignedByPhysician, IsSignedByMedicalDirector, IsSignedByPhysicianAndMedicalDirector, o_orderdate, o_sendtophysician,
			o_meddirsentdate, o_meddir
		)
		select
			e.epi_id,
			o.o_id,
			o.o_otid,
			o.o_physiciansigneddate,
			o.o_meddirsigneddate,
			o.o_datevoided,
			o.o_datedeclined,
			o.o_dateapproved,
			case when o.o_physiciansigneddate is null then 0 else 1 end,
			case when o.o_meddirsigneddate is null then 0 else 1 end,
			case when o.o_meddirsigneddate is not null and o.o_physiciansigneddate is not null then 1 else 0 end,
			o_orderdate,
			o_sendtophysician,
			o_meddirsentdate,
			o_meddir
		from #Episodes e
		join hchb.dbo.CLIENT_ORDERS o on e.epi_id = o.o_epiid

		create table #epi_funding_sources
		(
			epiid int not null, cefs_id int not null, ps_id int not null, ps_responsibility char(1) not null, epi_policyNo varchar(26),
			ps_itid int not null, IsMedicaid bit not null, IsPerDiemHospice bit not null, psb_id int not null, ps_desc varchar(50) not null,
			pt_desc varchar(50) not null
		)
		print 'COMPLETE: episode'
		-- d.  Tone down the number of columns in line items so the set isn't so wide (may be un-necessary)


		select
			tc_id, CltServices, CltRt, epiid, PrimID, WorkerName, ShiftDate, BeginTime, EndTime, FlatType, InsID, ServiceLineID, CltBranchID, JobDID, GroupID
			,ProgID, jd_code, SupplyFlag, hicpic, invoice_grouping, IsCPTCharge, cltbillid, tc_labtest
			,CltMisChrg, ps_desc, R.pa_lastname + ', ' + R.pa_firstname + CASE WHEN R.pa_mi = '' or R.pa_mi is null THEN '' ELSE ' ' + R.pa_mi END as epi_fullName
			,R.epi_startofepisode, R.epi_endofepisode, R.pt_desc,R.auth_pending, R.invnum, R.ClaimBillTypeID, CPT_Code, revcode, IsMergeLineItem
			,epi_SocDate, cltid, tc_phsid, tc_charge, r.cp_id, r.scheduleid
        into #LineItems_BillingAuditPerform
		from #AllLineItems_BillingAuditPerform R
		WHERE R.BillableCharge = 1  -- only run audits on things that are billable

		INSERT INTO #invoice_grouping(invoice_grouping, startdate, enddate)
		SELECT invoice_grouping, MIN(tc.ShiftDate), MAX(tc.ShiftDate)
		FROM #LineItems_BillingAuditPerform tc
		GROUP BY tc.invoice_grouping


		-- PECOS
		CREATE TABLE #epi_PayorSource_PECOSPhysicianCategories(epiid INT NOT NULL, ps_id INT NOT NULL, pc_id INT NOT NULL, maxshiftdate datetime )
        CREATE TABLE #epi_PECOS_Physicians(epiid INT NOT NULL, ps_id INT NOT NULL, pc_id INT NOT NULL, ph_id INT NULL, referenceTableID INT NOT NULL, validationDate date)

		IF EXISTS (SELECT 1 FROM hchb.dbo.SYSTEM_SETTINGS WHERE SS_SETTING = 'PecosVerificationHomeHealth' AND SS_VALUE = 'Y')
		 begin
		  INSERT INTO #epi_PayorSource_PECOSPhysicianCategories(epiid, ps_id, pc_id, maxshiftdate)
          SELECT DISTINCT epiid, GroupID, pspc_pcid, ig.enddate
            FROM #LineItems_BillingAuditPerform as t
           INNER JOIN #invoice_grouping ig ON ig.invoice_grouping = t.invoice_grouping
           INNER JOIN hchb.dbo.PAYOR_SOURCE_PHYSICIAN_CATEGORIES
              ON GroupID = pspc_psid
             AND pspc_active = 1
		   INNER JOIN hchb.dbo.PAYOR_SOURCES as ps
		      ON GroupID = ps.ps_id
			 AND ps_enablePECOSVerification = 1
		   	
			-- 1 - F2F PHYSICIAN
			INSERT INTO #epi_PECOS_Physicians(epiid, ps_id, pc_id, ph_id, referenceTableID, validationDate)
			SELECT epp.epiid, epp.ps_id, epp.pc_id, po.po_phid, ceftf.ceftf_id, ceftf_encounterdate
			  FROM #epi_PayorSource_PECOSPhysicianCategories AS epp
			 INNER JOIN hchb.dbo.CLIENT_EPISODE_FACETOFACE AS ceftf
				ON epp.epiid = ceftf.ceftf_f2fappliestoepiid
			   AND ceftf.ceftf_required = 1
			   AND ceftf.ceftf_active = 1
			   AND epp.pc_id = 1
			 INNER JOIN hchb.dbo.Physician_Offices as po
				ON po.po_id = ceftf.ceftf_documentcompletedbypoid

			-- 2 - CERTIFYING PHYSICIAN ON CLAIM
			INSERT INTO #epi_PECOS_Physicians(epiid, ps_id, pc_id, ph_id, referenceTableID, validationDate)
			SELECT epp.epiid, epp.ps_id, epp.pc_id, po.po_phid, ceftf.ceftf_id, ce.epi_SocDate
			  FROM #epi_PayorSource_PECOSPhysicianCategories AS epp
			 INNER JOIN hchb.dbo.CLIENT_EPISODE_FACETOFACE AS ceftf
				ON epp.epiid = ceftf.ceftf_f2fappliestoepiid
			   AND ceftf.ceftf_required = 1
			   AND ceftf.ceftf_active = 1
			   AND epp.pc_id = 2
			 INNER JOIN hchb.dbo.CLIENT_EPISODES AS ce
				ON epp.epiid = epi_id
			 INNER JOIN hchb.dbo.Physician_Offices as po
				ON po.po_id = ceftf.ceftf_certifyingphysicianonclaimpoid
	
			-- 3 - POC PHYSICIAN
			INSERT INTO #epi_PECOS_Physicians(epiid, ps_id, pc_id, ph_id, referenceTableID, validationDate)
			SELECT epp.epiid, epp.ps_id, epp.pc_id, po.po_phid, c4.c485_id, ce.epi_StartOfEpisode
			  FROM #epi_PayorSource_PECOSPhysicianCategories AS epp
			 INNER JOIN hchb.dbo.CLIENT_485 AS c4 -- All POCs are stored in this table.
				ON epp.epiid = c4.epi_id
			   AND epp.pc_id = 3
			 INNER JOIN hchb.dbo.CLIENT_EPISODES AS ce
				ON epp.epiid = ce.epi_id
			 INNER JOIN hchb.dbo.Physician_Offices as po
				ON po.po_id = c4.epi_poid1
	
			-- 4 - REFERRING PHYSICIAN
			INSERT INTO #epi_PECOS_Physicians(epiid, ps_id, pc_id, ph_id, referenceTableID, validationDate)
			SELECT epp.epiid, epp.ps_id, epp.pc_id, po.po_phid, ce.epi_id, ce.epi_StartOfEpisode
			  FROM #epi_PayorSource_PECOSPhysicianCategories AS epp
			 INNER JOIN hchb.dbo.CLIENT_EPISODES AS ce
				ON epp.epiid = ce.epi_id
			   AND epp.pc_id = 4
			 INNER JOIN hchb.dbo.Physician_Offices as po
				ON po.po_id = ce.epi_poid
	
			-- 5 - ORDERING PHYSICIAN (PRIMARY)
			INSERT INTO #epi_PECOS_Physicians(epiid, ps_id, pc_id, ph_id, referenceTableID, validationDate)
			SELECT epp.epiid, epp.ps_id, epp.pc_id, po.po_phid, co.o_id, co.o_physiciansigneddate
			  FROM #epi_PayorSource_PECOSPhysicianCategories AS epp
			 INNER JOIN hchb.dbo.CLIENT_ORDERS AS co
				ON epp.epiid = co.o_epiid
			   AND co.o_otid NOT IN (1, 30)
			   AND epp.pc_id = 5
			   AND co.o_datedeclined IS NULL
			   AND co.o_datevoided IS NULL
			 INNER JOIN hchb.dbo.Physician_Offices as po
				ON po.po_id = co.o_poid
	
			-- 6 - ORDERING PHYSICIAN (SECONDARY)
			INSERT INTO #epi_PECOS_Physicians(epiid, ps_id, pc_id, ph_id, referenceTableID, validationDate)
			SELECT epp.epiid, epp.ps_id, epp.pc_id, po.po_phid, co.o_id, co.o_physiciansigneddate
			  FROM #epi_PayorSource_PECOSPhysicianCategories AS epp
			 INNER JOIN hchb.dbo.CLIENT_ORDERS AS co
				ON epp.epiid = co.o_epiid
			   AND co.o_otid NOT IN (1, 30)
			   AND epp.pc_id = 6
			   AND co.o_datedeclined IS NULL
			   AND co.o_datevoided IS NULL
			 INNER JOIN hchb.dbo.Physician_Offices as po
				ON po.po_id = co.o_secpoid
	
			-- 7 - ORDERING PHYSICIAN (MEDICAL DIRECTOR)
			INSERT INTO #epi_PECOS_Physicians(epiid, ps_id, pc_id, ph_id, referenceTableID, validationDate)
			SELECT epp.epiid, epp.ps_id, epp.pc_id, po.po_phid, co.o_id, co.o_meddirsigneddate
			  FROM #epi_PayorSource_PECOSPhysicianCategories AS epp
			 INNER JOIN hchb.dbo.CLIENT_ORDERS AS co
				ON epp.epiid = co.o_epiid
			   AND co.o_otid NOT IN (1, 30)
			   AND epp.pc_id = 7
			   AND co.o_datedeclined IS NULL
			   AND co.o_datevoided IS NULL
			 INNER JOIN hchb.dbo.Physician_Offices as po
				ON po.po_id = co.o_meddirpoid
	
			-- 8 - PRIMARY PHYSICIAN
			INSERT INTO #epi_PECOS_Physicians(epiid, ps_id, pc_id, ph_id, referenceTableID, validationDate)
			SELECT epp.epiid, epp.ps_id, epp.pc_id, po.po_phid, cep.cep_id, ce.epi_StartOfEpisode
			  FROM #epi_PayorSource_PECOSPhysicianCategories AS epp
			 INNER JOIN hchb.dbo.CLIENT_EPISODE_PHYSICIANS AS cep
				ON epp.epiid = cep.cep_epiid
			   AND epp.pc_id = 8
			   AND cep.cep_sortorder = 0 -- Primary Physician
			 INNER JOIN hchb.dbo.CLIENT_EPISODES AS ce
				ON epp.epiid = ce.epi_id
			 INNER JOIN hchb.dbo.Physician_Offices as po
				ON po.po_id = cep.cep_poid
	
			-- 9 - SECONDARY PHYSICIAN
			INSERT INTO #epi_PECOS_Physicians(epiid, ps_id, pc_id, ph_id, referenceTableID, validationDate)
			SELECT epp.epiid, epp.ps_id, epp.pc_id, po.po_phid, cep.cep_id, ce.epi_StartOfEpisode
			  FROM #epi_PayorSource_PECOSPhysicianCategories AS epp
			 INNER JOIN hchb.dbo.CLIENT_EPISODE_PHYSICIANS AS cep
				ON epp.epiid = cep.cep_epiid
			   AND epp.pc_id = 9
			   AND cep.cep_sortorder > 0 -- Secondary Physician(s)
 			 INNER JOIN hchb.dbo.CLIENT_EPISODES AS ce
				ON epp.epiid = ce.epi_id
			 INNER JOIN hchb.dbo.Physician_Offices as po
				ON po.po_id = cep.cep_poid
	
			-- 10 - MEDICAL DIRECTOR
			INSERT INTO #epi_PECOS_Physicians(epiid, ps_id, pc_id, ph_id, referenceTableID, validationDate)
			SELECT epp.epiid, epp.ps_id, epp.pc_id, po.po_phid, cep.cep_id, ce.epi_StartOfEpisode
			  FROM #epi_PayorSource_PECOSPhysicianCategories AS epp
			 INNER JOIN hchb.dbo.CLIENT_EPISODE_PHYSICIANS AS cep
				ON epp.epiid = cep.cep_epiid
			   AND epp.pc_id = 10
			   AND cep.cep_hospicemedicaldirector = 'Y'
			 INNER JOIN hchb.dbo.CLIENT_EPISODES AS ce
				ON epp.epiid = ce.epi_id
			 INNER JOIN hchb.dbo.Physician_Offices as po
				ON po.po_id = cep.cep_poid

			-- 11 - PCP PHYSICIAN/FACILITY
			INSERT INTO #epi_PECOS_Physicians(epiid, ps_id, pc_id, ph_id, referenceTableID, validationDate)
			SELECT epp.epiid, epp.ps_id, epp.pc_id, po.po_phid, cpcp.cpcp_id, maxshiftdate
			  FROM #epi_PayorSource_PECOSPhysicianCategories AS epp
			  INNER JOIN hchb.dbo.CLIENT_EPISODES as ce
				 ON epp.epiid = ce.epi_id
				AND epp.pc_id = 11
			  INNER JOIN hchb.dbo.CLIENT_PRIMARY_CARE_PROVIDERS cpcp
				 ON	cpcp.cpcp_paid = ce.epi_paid  
				AND cpcp.cpcp_active = 'Y' 
				AND cpcp.cpcp_effectivefrom <= maxshiftdate 
				AND ISNULL(cpcp.cpcp_effectiveto, '12/31/2099') >= maxshiftdate 
			  INNER JOIN hchb.dbo.Physician_Offices as po
				 ON po.po_id = cpcp.cpcp_poid
        end

		-- FFS HIPPS
		SELECT  t.epiid
				, MAX(t.ShiftDate) as maxDate
				, MAX(t.epi_SocDate) epiSocDate
				, MAX(t.CltId) cltid
				, MAX(t.ServiceLineID) ServiceLineID
		into #maxDateOnEpiid
		FROM #LineItems_BillingAuditPerform t
		INNER JOIN hchb.dbo.PAYOR_SOURCE_BRANCHES psb ON psb.psb_psid = t.groupid
			AND psb.psb_branchcode = t.cltbranchid
			and t.epi_socdate is not null
		INNER JOIN hchb.dbo.CLAIM_PROFILES_CLAIM_FORMATS cpcf ON psb.psb_cpid = cpcf.cpcf_cpid
			AND cpcf.cpcf_IsFormatOn = 1
			AND cpcf.cpcf_cfid = 281 --SEND HIPPS CODE ON FFS CLAIM
		GROUP BY t.epiid,CAST(cpcf.cpcf_value AS DATE)
		HAVING CAST(cpcf.cpcf_value AS DATE) <= MIN(t.ShiftDate)

		CREATE TABLE #FFSGetNewestHipps(idCol int identity,  paid int NOT NULL, socDate datetime NOT NULL, maxShiftDate datetime NOT NULL
			,slid int NOT NULL DEFAULT 1, hipps VARCHAR(5) NULL, revCode VARCHAR(4) NULL)

		INSERT INTO #FFSGetNewestHipps(paid, socDate, maxShiftDate, slid)
		SELECT DISTINCT m.cltid, m.epiSocDate, m.maxDate, m.ServiceLineID
		FROM #maxDateOnEpiid m
		WHERE m.epiSocDate IS NOT NULL

		--EXEC usp_FFS_GetNewestHipps
		SELECT rank() over  (PARTITION BY f.idCol ORDER BY ceo.ceo_insertdate DESC, ceo.ceo_id DESC) as hippsRank, idCol, ceo.ceo_hipps hipps, '0023' as revCode
		INTO #ranked_hipps
		FROM #FFSGetNewestHipps f
		INNER JOIN hchb.dbo.client_episodes epi
			ON epi.epi_paid = f.paid
			AND epi.epi_SocDate = f.socDate
			AND epi.epi_slid = f.slid
			AND f.maxShiftDate BETWEEN epi.epi_StartOfEpisode AND epi.epi_EndOfEpisode
		INNER JOIN hchb.dbo.client_episode_oasis ceo on epi.epi_id = ceo.ceo_epiid
		WHERE 0=0
			AND isnull(ceo.ceo_hipps,'') <> ''

		--if we still haven't found the HIPPS and the episode is a recert check the re-certified episode.
		-- logic matches usp_DoesEpisodeHaveOASIS
		INSERT INTO #ranked_hipps(hippsRank, idCol, hipps, revCode)
		SELECT rank() over  (PARTITION BY f.idCol ORDER BY ceo.ceo_insertdate DESC, ceo.ceo_id DESC) as hippsRank, idCol, ceo.ceo_hipps hipps, '0023' as revCode
		FROM #FFSGetNewestHipps f
		INNER JOIN hchb.dbo.client_episodes epi
			ON epi.epi_paid = f.paid
			AND epi.epi_SocDate = f.socDate
			AND epi.epi_slid = f.slid
			AND f.maxShiftDate BETWEEN epi.epi_StartOfEpisode AND epi.epi_EndOfEpisode
		INNER JOIN hchb.dbo.Client_485 c485 ON c485.epi_id = epi.epi_id
		INNER JOIN hchb.dbo.Client_Orders ON o_id = c485_oid  
		INNER JOIN hchb.dbo.Client_Episode_Visits ON cev_id = o_cevid -- visit 485 was created from  
		INNER JOIN hchb.dbo.Client_Episode_OASIS ceo ON ceo_cevid = cev_id -- oasis tied to the visit the 485 was created from  
		INNER JOIN hchb.dbo.ServiceCodes ON sc_id = cev_sc_id AND sc_visittype = 'ROC/RECERT'
		WHERE ISNULL(ceo.ceo_hipps,'') <> ''
			AND NOT EXISTS
				(
					SELECT 1 FROM #ranked_hipps r WHERE r.idCol = f.idCol
				)
	
		UPDATE f
		SET f.hipps = r.hipps, f.revCode = r.revCode
		FROM #FFSGetNewestHipps f
		INNER JOIN #ranked_hipps r on r.idCol = f.idCol
		AND r.hippsRank = 1
	
		print 'COMPLETE: 2. c'
		print 'END: 2. Main Data Fetch'



----  Audit Preparation
---- Get other relevant information for auding
---- phsician info by auth
	
     CREATE TABLE #AUTH (auth_id INT, cp_id INT, epiid INT, cefs_SecondaryIdentification VARCHAR(MAX), maxshiftdate date)  
	 CREATE TABLE #OutputOptions ( cp_id INT,npitt_NPIRequired BIT,npitt_NPIOptional BIT,npitt_ProviderIDRequired BIT,npitt_ProviderIDOptional BIT)

			update a SET a.maxshiftdate = i.enddate
			FROM #Authorizations a
			JOIN #invoice_grouping i
			ON a.invoice_grouping = i.invoice_grouping
	    
	--	insert into #PhysicianInfo (ph_lastname,ph_firstname,PPlace1Type,PPlace1,PPlace2Type,PPlace2,ph_UPIN,ph_medicaidnumber,epiid,authid,poid,sortorder,pcp,secondaryid
	--							,isReferringPhysician, ph_statelicense, IsCertifyingPhysician, ph_id)
	 --exec usp_Get837PhysicianInfoByAuthorization NULL,0,0,1
	
		 	INSERT INTO #AUTH (auth_id,maxshiftdate,cp_id)  
		    SELECT distinct auth_id, maxshiftdate, cp_id
				FROM #Authorizations a	
	
	
			UPDATE A1 SET
				epiid = ce.epi_id
				,cefs_SecondaryIdentification = cefs.cefs_SecondaryIdentification
			FROM #Auth a1
			JOIN hchb.dbo.AUTHORIZATIONS a2 WITH(NOLOCK) ON a1.auth_id = a2.auth_id
			JOIN hchb.dbo.VI_EPISODE_FS v_cefs WITH(NOLOCK,NOEXPAND) ON v_cefs.vicefs_id = a2.auth_cefsid
			JOIN hchb.dbo.CLIENT_EPISODES ce WITH(NOLOCK) ON v_cefs.vicefs_epiid = ce.epi_id
			JOIN hchb.dbo.CLIENT_EPISODE_FS CEFS ON cefs.cefs_id = v_cefs.vicefs_id 
	
	        ;with f as (select 	
						cpcf_cpid = v.cp_id,
						cpcf_parent_cpid =  v.cp_parent_cpid ,
						cpcf_IsFormatOn = cast(case 
												when cast(getdate() as date) < v.cf_effectiveDate then 0
												when coalesce(L0.cpcf_value,L1.cpcf_value) is null then 0 
												else 1 
											   end as bit),
						cpcf_ValueCameFrom = case 
												when cast(getdate() as date) < v.cf_effectiveDate then null
												when L0.cpcf_value is not null then v.cp_id
												when L1.cpcf_value is not null  then v.cp_parent_cpid		
												when L1.cpcf_value is not null and ISNULL(0,0) <> 0 then 0
												else NULL
											 end,				 
						cpcf_cfid = v.cf_id,
						cpcf_stid = v.cf_stid,
						cpcf_id = coalesce(L0.cpcf_id,L1.cpcf_id),
						cpcf_value = case 
										when cast(getdate() as date) < v.cf_effectiveDate then (case when v.cf_stid = 1 then CAST(cast(0 as bit) as SQL_VARIANT) else null end)
										when v.cf_id = 204 then coalesce(L0.cpcf_value,L1.cpcf_value,24)
										when v.cf_stid = 1 and coalesce(L0.cpcf_id,L1.cpcf_id) is null then CAST(cast(0 as bit) as SQL_VARIANT)
										when v.cf_stid = 1 and coalesce(L0.cpcf_id,L1.cpcf_id) is not null then CAST(cast(coalesce(L0.cpcf_value,L1.cpcf_value,0) as bit) as SQL_VARIANT)
										else coalesce(L0.cpcf_value,L1.cpcf_value)
									 end 
					from hchb.dbo.VI_CPCF v with(NOEXPAND)
					--join hchb.dbo.CLAIM_FORMATS f on = f.cf_id
					left join hchb.dbo.CLAIM_PROFILES_CLAIM_FORMATS_BASE L0 on L0.cpcf_cpid = v.cp_id and L0.cpcf_cfid = v.cf_id 
					left join hchb.dbo.CLAIM_PROFILES_CLAIM_FORMATS_BASE L1 on L1.cpcf_cpid = case when ISNULL(0,0) = 0 then v.cp_parent_cpid else ISNULL(0,0) end and L1.cpcf_cfid = v.cf_id and L0.cpcf_id is null
					where v.cf_id  = 202
			          )
			INSERT INTO #OutputOptions
			SELECT 
				a.cp_id 
				,npitt_NPIRequired = e.npitt_NPIRequired
				,npitt_NPIOptional = e.npitt_NPIOptional
				,npitt_ProviderIDRequired = e.npitt_ProviderIDRequired
				,npitt_ProviderIDOptional = e.npitt_ProviderIDOptional	
			FROM (SELECT DISTINCT cp_id FROM #AUTH) a
			JOIN f ON f.cpcf_cpid = a.cp_id
			JOIN hchb.dbo.EDI_NPITypes e ON e.npitt_id = CAST(f.cpcf_value AS INT)

			IF OBJECT_ID('tempdb..#P_STAGE') IS NOT NULL
			BEGIN
				DROP TABLE #P_STAGE
			END
	
			CREATE TABLE #P_STAGE
			(
				epiid INT, authid INT, poid INT, sortorder INT, pcp BIT DEFAULT(0), secondaryid VARCHAR(MAX), cp_id INT
				, isReferringPhysician BIT DEFAULT(0), IsCertifyingPhysician BIT DEFAULT(0), maxshiftdate DATE, IsPlanOfCarePhysician BIT DEFAULT(0)
			)

	
			INSERT INTO #P_STAGE (epiid, authid, poid, sortorder, pcp, secondaryid, cp_id, maxshiftdate)  
			SELECT epi_id, a1.auth_id, epi_poid1,0,0,a1.cefs_SecondaryIdentification, a1.cp_id, a1.maxshiftdate  
			FROM #AUTH a1 
			JOIN hchb.dbo.Client_Episodes CE ON a1.epiid = ce.epi_id
			WHERE 0=0
				AND epi_poid1 IS NOT NULL


			INSERT INTO #P_STAGE (epiid, authid, poid, sortorder, pcp, secondaryid, cp_id)
			SELECT p.epiid, p.authid, cep.cep_poid, cep.cep_sortorder,0, p.secondaryid,p.cp_id
			FROM hchb.dbo.CLIENT_EPISODE_PHYSICIANS cep WITH(NOLOCK)
			JOIN #P_STAGE p ON cep_epiid = p.epiid
			WHERE 0=0
				AND cep_poid IS NOT NULL
				AND cep.cep_sortorder <> 0  -- Don't re-get the primary
		    
			
		   SELECT a.auth_id, ce.epi_id, ce.epi_paid, cpcp.cpcp_poid, cpcp.cpcp_faid, secondaryid = a.cefs_SecondaryIdentification, a.cp_id
			 INTO #auth_pcp
			FROM #AUTH a
			JOIN hchb.dbo.CLIENT_EPISODES ce WITH(NOLOCK) ON ce.epi_id = a.epiid
			JOIN hchb.dbo.CLIENT_PRIMARY_CARE_PROVIDERS cpcp ON 
			cpcp.cpcp_paid = ce.epi_paid AND	 
			cpcp.cpcp_active = 'Y' AND
			cpcp.cpcp_effectivefrom <= maxshiftdate AND
			ISNULL(cpcp.cpcp_effectiveto, '12/31/2099') >= maxshiftdate
	
			--Check if PCP physician already exists in stage table

			UPDATE p
				SET pcp = 1 
			FROM #P_STAGE p
			JOIN #auth_pcp pcp ON p.authid = pcp.auth_id AND p.poid = pcp.cpcp_poid
	
			--otherwise add to stage table
	
			INSERT INTO #P_STAGE(epiid, authid, poid, sortorder, pcp, secondaryid, cp_id)
			SELECT pcp.epi_id,pcp.auth_id,pcp.cpcp_poid, 20, 1,pcp.secondaryid,pcp.cp_id
			FROM #auth_pcp pcp
			LEFT JOIN #P_STAGE p ON p.authid = pcp.auth_id AND p.poid = pcp.cpcp_poid
			WHERE p.poid IS NULL
			AND pcp.cpcp_poid IS NOT NULL
			
			--IF @getPlanOfCareOrderingPhysician = 1
		

			SELECT distinct a.auth_id, po.po_id
			into #PlanOfCarePhysicians
			FROM #AUTH a
			INNER JOIN hchb.dbo.CLIENT_ORDERS o on o_epiid = a.epiid
			INNER JOIN hchb.dbo.client_485 c485 on o.o_id = c485.c485_oid
			INNER JOIN hchb.dbo.PHYSICIAN_OFFICES po on po.po_id = o.o_poid

			--flag if the physician has been fetched
			UPDATE P 
			SET	IsPlanOfCarePhysician = 1
			FROM #P_STAGE p
			join #PlanOfCarePhysicians poc on p.authid = poc.auth_id 
				AND p.poid = poc.po_id

			--insert if needed
			INSERT INTO #P_STAGE (epiid, authid, poid, sortorder, secondaryid, cp_id, IsPlanOfCarePhysician)
			SELECT a.epiid, a.auth_id, poc.po_id, 0, null, a.cp_id, 1
			FROM #AUTH a
			join #PlanOfCarePhysicians poc on a.auth_id = poc.auth_id
			WHERE NOT EXISTS
				(
					SELECT 1
					FROM #P_STAGE P
					WHERE a.auth_id = p.authid
					AND p.poid = poc.po_id
				)

				 

		   SELECT 
				p.ph_id,
				ph_lastname = CASE WHEN ISNULL(null,0) = 1 THEN ISNULL(p.ph_PECOSLastname,p.ph_lastname) ELSE p.ph_lastname END, 
				ph_firstname = CASE WHEN ISNULL(null,0) = 1 THEN ISNULL(p.ph_PECOSFirstName,p.ph_firstname) ELSE p.ph_firstname END,
				p.ph_UPIN,
				p.ph_medicaidnumber,
				REPLACE(p.ph_SSN,'-','') AS ph_SSN,
				REPLACE(p.ph_EIN,'-','') AS ph_EIN,
				p.ph_NPI,
				p.ph_statelicense,
				ps.*,po.*
			INTO #tmpout
			FROM #P_STAGE ps
			JOIN hchb.dbo.PHYSICIAN_OFFICES po WITH(NOLOCK) ON ps.poid = po.po_id
			JOIN hchb.dbo.PHYSICIANS p WITH(NOLOCK) ON p.ph_id = po.po_phid
			--LEFT JOIN dbo.fn_Claim_Profiles_Claim_Formats_Get(@ProfileID, NULL, '227') cp ON @profileID IS NOT NULL AND cpcf_isformaton = 1	--Physician Legal Name

			CREATE TABLE #PhysicianInformation
				(
					ph_lastname VARCHAR(50),ph_firstname VARCHAR(50),PPlace1Type CHAR(2),PPlace1 VARCHAR(50),PPlace2Type CHAR(2),PPlace2 VARCHAR(50),
					ph_UPIN VARCHAR(50),ph_medicaidnumber VARCHAR(50),epiid INT,authid INT,poid INT,sortorder INT,pcp BIT,secondaryid VARCHAR(50),
					isReferringPhysician BIT, ph_statelicense VARCHAR(50), IsCertifyingPhysician BIT DEFAULT (0), IsPlanOfCarePhysician BIT DEFAULT (0), ph_id INT
				)
		 print('#PhysicianInformation')
			INSERT INTO #PhysicianInformation(ph_lastname,ph_firstname,PPlace1Type,PPlace1,PPlace2Type,PPlace2,
				   ph_UPIN,ph_medicaidnumber,epiid,authid,poid,sortorder,pcp,secondaryid,
				   isReferringPhysician, ph_statelicense, IsCertifyingPhysician, IsPlanOfCarePhysician, ph_id)
			SELECT 
				t.ph_lastname, 
				t.ph_firstname,
				i.PPlace1Type,
				i.PPlace1, 
				i.PPlace2Type,
				i.PPlace2, 
				ISNULL(t.ph_UPIN, '') AS ph_UPIN,
				t.ph_medicaidnumber,
				t.epiid, 
				t.authid, 
				t.poid, 
				t.sortorder, 
				t.pcp,
				t.secondaryid,
				t.isReferringPhysician,
				t.ph_statelicense,
				t.IsCertifyingPhysician,
				t.IsPlanOfCarePhysician,
				t.ph_id
			FROM #tmpout t
			JOIN #OutputOptions oo ON oo.cp_id = t.cp_id
			CROSS APPLY [dbo].[f_Get_Physician_Id](t.ph_SSN,t.ph_EIN,t.ph_NPI,oo.npitt_NPIRequired,oo.npitt_NPIOptional,oo.npitt_ProviderIDRequired,oo.npitt_ProviderIDOptional,t.ph_id,t.cp_id,0) AS i	 
	
	 ---------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--			              final																																				        --
--                                                                                                                                                                                  --
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
print 'FINAL'
TRUNCATE TABLE UNBILLED_REASON
--INTSER INTO table unbilled_reason as

INSERT INTO UNBILLED_REASON
---- 1
		SELECT DISTINCT 
			t.epi_id as epi_id,
			t.epi_branchcode as Branch,
			t.epi_fullname as ClientName,
			t.epi_startofepisode as StartDate,
			t.epi_endofepisode as EndDate,
			bat.bat_id,
			bat.bat_desc,
			bai.bai_id,
			bai.bai_name,
			REPLACE(REPLACE(REPLACE(REPLACE(bai_message,'CLIENTNAME',t.epi_fullname),'STARTDATE',CONVERT(varchar(10),t.epi_startofepisode,101)),'ENDDATE',CONVERT(varchar(10),t.epi_endofepisode,101)),'PHYSICIANNAME','') as bai_message,	
			t.ps_desc,
			t.pt_desc,
			bai.bai_helptext,
			t.ps_id,
			t.epi_id as id,
			2 AS ID_TYPE
		FROM 
			#Authorizations t			
			LEFT OUTER JOIN hchb.dbo.CLIENT_485 c WITH(NOLOCK) on c.epi_id = t.epi_id
			INNER JOIN  hchb.dbo.Billing_Audit_Items bai WITH(NOLOCK) on bai.bai_id = 1
			INNER JOIN  hchb.dbo.Billing_Audit_Types bat WITH(NOLOCK) on bat.bat_id = bai.bai_batid
		where 
			c.c485_id is null	
					
INSERT INTO UNBILLED_REASON
----2	
		SELECT DISTINCT 
			t.epi_id as epi_id,
			t.epi_branchcode as Branch,
			t.epi_fullname as ClientName,
			t.epi_startofepisode as StartDate,
			t.epi_endofepisode as EndDate,
			bat.bat_id,
			bat.bat_desc,
			bai.bai_id,
			bai.bai_name,
			REPLACE(REPLACE(REPLACE(REPLACE(bai_message,'CLIENTNAME',t.epi_fullname),'STARTDATE',CONVERT(varchar(10),t.epi_startofepisode,101)),'ENDDATE',CONVERT(varchar(10),t.epi_endofepisode,101)),'PHYSICIANNAME','') as bai_message,	
			t.ps_desc,
			t.pt_desc,
			bai.bai_helptext,
			t.ps_id,
			t.epi_id  as id,
			2 as ID_TYPE
		FROM 
			#Authorizations t			
			LEFT OUTER JOIN hchb.dbo.CLIENT_485 c WITH(NOLOCK) on c.epi_id = t.epi_id
			INNER JOIN hchb.dbo.CLIENT_ORDERS co with (NOLOCK) on c.c485_oid = co.o_id
			INNER JOIN  hchb.dbo.Billing_Audit_Items bai WITH(NOLOCK) on bai.bai_id =2
			INNER JOIN  hchb.dbo.Billing_Audit_Types bat WITH(NOLOCK) on bat.bat_id = bai.bai_batid
		where 
			co.o_physiciansigneddate is null		

INSERT INTO UNBILLED_REASON
---- 3
	SELECT DISTINCT 
			epi.epi_id as epi_id,
			epi.epi_branchcode,
			epi.epi_fullname as ClientName,
			epi.epi_startofepisode as StartDate,
			epi.epi_endofepisode as EndDate,
			bat.bat_id,
			bat.bat_desc,
			bai.bai_id,
			bai.bai_name,
			REPLACE(REPLACE(REPLACE(REPLACE(bai_message,'CLIENTNAME',epi.epi_fullname),'STARTDATE',CONVERT(varchar(10),epi.epi_startofepisode,101)),'ENDDATE',CONVERT(varchar(10),epi.epi_endofepisode,101)),'PHYSICIANNAME','') as bai_message,
			bai.bai_helptext,
			epi.ps_desc,
			epi.pt_desc,			
			epi.ps_id,
			epi.epi_id as id,
			2 as ID_TYPE
		FROM 
			#Episodes epi		
			INNER JOIN hchb.dbo.Billing_Audit_Items bai WITH(NOLOCK) on bai.bai_id = 3
			INNER JOIN hchb.dbo.Billing_Audit_Types bat WITH(NOLOCK) on bat.bat_id = bai.bai_batid
			LEFT JOIN hchb.dbo.authorizations a on a.auth_cefsid = epi.cefs_id
		where			
			a.auth_id is null
			and epi.vicefs_ps <> 'I'

INSERT INTO UNBILLED_REASON
---- 4	
    	SELECT DISTINCT 
			t.epi_id as epi_id,
			t.epi_branchcode as Branch,
			t.epi_fullname as ClientName,
			t.epi_startofepisode as StartDate,
			t.epi_endofepisode as EndDate,
			bat.bat_id,
					bat.bat_desc,
					bai.bai_id,
					bai.bai_name,
			REPLACE(REPLACE(REPLACE(REPLACE(bai.bai_message,'CLIENTNAME',t.epi_fullname),'STARTDATE',CONVERT(varchar(10),t.epi_startofepisode,101)),'ENDDATE',CONVERT(varchar(10),t.epi_endofepisode,101)),'PHYSICIANNAME','') as bai_message,
			bai.bai_helptext,
			t.ps_desc,
			t.pt_desc,
			t.ps_id,
			at.invoice_grouping as id,
			6 AS ID_TYPE
		FROM 
			#AllLineItems_BillingAuditPerform at
			INNER JOIN #Authorizations t ON at.cltbillid = t.auth_id
			INNER JOIN hchb.dbo.Billing_Audit_Items bai WITH(NOLOCK) on bai.bai_id = 4
			INNER JOIN hchb.dbo.Billing_Audit_Types bat WITH(NOLOCK) on bat.bat_id = bai.bai_batid
		where 			
			t.auth_pending = 'Y'

INSERT INTO UNBILLED_REASON
---- 5
        	SELECT DISTINCT 
			t.epiid as epi_id,
			t.CltBranchID,
			t.epi_fullname as ClientName,
			t.epi_startofepisode as StartDate,
			t.epi_endofepisode as EndDate,
			bat.bat_id,
			bat.bat_desc,
			bai.bai_id,
			bai.bai_name,
			REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
				bai.bai_message,'CLIENTNAME',isnull(t.epi_fullname,'')),
				'SHIFTDATE',CONVERT(varchar(10),t.ShiftDate,101)),
				'FLATTYPE',isnull(t.FlatType,'')),
				'BEGINTIME',LTRIM(RIGHT(CONVERT(VARCHAR(25), t.BeginTime,100),7))),
				'ENDTIME',LTRIM(RIGHT(CONVERT(VARCHAR(25), t.EndTime,100),7))),
				'WORKERNAME',isnull(t.WorkerName,'')),
				'AGENCYNAME',''),
				'PAYORTYPENAME',isnull(t.pt_desc,'')),
				'PAYORSOURCENAME',isnull(t.ps_desc,'')),
				'PROGRAMNAME',''),
				'JOBDESCRIPTIONNAME',isnull(t.jd_code,'')) as bai_message,		
			t.ps_desc,
			t.pt_desc,
			bai.bai_helptext,
			--t.WorkerName as WorkerName,
			--t.ShiftDate as ShiftDate,
			--t.BeginTime as BeginTime,
			--t.EndTime as EndTime,
			--t.FlatType as FlatType,
			t.GroupID as ps_id,
			t.tc_id as id,
			1 as id_type
		FROM 
			#LineItems_BillingAuditPerform t 				
			INNER JOIN hchb.dbo.Billing_Audit_Items bai WITH(NOLOCK) on bai.bai_id = 5
			INNER JOIN hchb.dbo.Billing_Audit_Types bat WITH(NOLOCK) on bat.bat_id = bai.bai_batid
		where 0=0
			AND t.CltRt = 0
			--Allow supplies (SupplyFlag=N,R) or miscellaneous charges with a non-zero charge to pass
			AND NOT (t.SupplyFlag <> '' OR t.cltmischrg <> 0)			 -- supplyflag was isnull(,'')ed earlier
			AND t.IsCPTCharge = 0  -- don't check CPT codes, they have different charges			

---- 6
	    ;with 
			MinDateOnEpiid AS 
			(
				SELECT  t.epiid
				FROM  #LineItems_BillingAuditPerform t
				INNER JOIN hchb.dbo.PAYOR_SOURCE_BRANCHES psb ON psb.psb_psid = t.groupid 
													AND psb.psb_branchcode = t.cltbranchid
				INNER JOIN hchb.dbo.CLAIM_PROFILES_CLAIM_FORMATS cpcf ON psb.psb_cpid = cpcf.cpcf_cpid 
															AND cpcf.cpcf_IsFormatOn = 1 
															AND cpcf.cpcf_cfid = 328
				GROUP BY t.epiid,CAST(cpcf.cpcf_value AS DATE) 
				HAVING CAST(cpcf.cpcf_value AS DATE) <= MIN(t.ShiftDate) 
			)
			INSERT INTO UNBILLED_REASON
				select DISTINCT 
					epi.epi_id as epi_id,
					epi_branchcode,
					epi.epi_fullname as ClientName,
					epi.epi_startofepisode as StartDate,
					epi.epi_endofepisode as EndDate,
					bat_id,
					bat_desc,
					bai_id,
					bai_name,
					REPLACE(
						REPLACE(bai_message,'PHYSICIANTYPE ',
							case when ph.sortorder = 0 then 'Primary '
								 when ph.sortorder = 1 then 'Secondary '
								 when ph.pcp = 1 then 'Primary Care '
								 when ph.IsCertifyingPhysician =1 then 'Certify '
								 else ''
							end 
						),'PHYSICIANNAME',ph.ph_lastname + ', ' + ph.ph_firstname) as bai_message,
					bai_helptext,
					ps_desc,
					pt_desc,					
					--ph.ph_lastname + ', ' + ph.ph_firstname as PhysicianName,	
					ps_id	,
					epi.epi_id as [ID] ,
                    2 as [ID_TYPE]	
				FROM  #Episodes epi
					INNER JOIN #PhysicianInformation ph on epi.epi_id = ph.epiid	
			
					LEFT JOIN MinDateOnEpiid md ON md.epiid = epi.epi_id
			
					INNER JOIN hchb.dbo.Billing_Audit_Items bai WITH(NOLOCK) on bai.bai_id = 6
					INNER JOIN hchb.dbo.Billing_Audit_Types bat WITH(NOLOCK) on bat.bat_id = bai.bai_batid		
				where 0=0	
					AND ph.PPlace1Type = 'XX'
					AND (select result from hchb.dbo.fn_NPIValidator(ph.PPlace1)) = 0
					AND (
						(ph.IsCertifyingPhysician = 0)
						OR
						(ph.IsCertifyingPhysician = 1 and md.epiid is NOT null and sortorder = 10)
						)		
                        
---- 7          
			 ;WITH  temptc AS
			 (	SELECT DISTINCT t.ServiceLineID as slid, t.CltBranchID as brid, 
								t.InsID, t.GroupId, t.ps_desc, 
								t.pt_desc,epiid,epi_startofepisode,epi_endofepisode,
						isnull((SELECT (SELECT RESULT FROM hchb.dbo.fn_NPIValidator(result)) 
								 FROM hchb.dbo.fn_GetAgencyNPI_TVF(t.insid, t.ServiceLineID, t.CltBranchID)),0) as isvalidnpi
				FROM #lineItems_BillingAuditPerform t
			)
		INSERT INTO UNBILLED_REASON
			SELECT DISTINCT 
						t.epiid as epi_id,	
						'N/A' AS Branch,
						'Not Applicable' AS ClientName,
						t.epi_startofepisode as startdate,
						t.epi_endofepisode as enddate,		
						bat_id,
						bat_desc,
						bai_id,
						bai_name,
						REPLACE(bai_message,'AGENCYNAME',agency_name) AS bai_message,	
						bai_helptext,
						--agency_name AS AgencyName,
						t.ps_desc,	 
						t.pt_desc,
						t.GroupID AS ps_id,
						agency_id as id,			
						3 AS ID_TYPE
					FROM 
						temptc t
						INNER JOIN  hchb.dbo.AGENCIES_SERVICELINES_BRANCHES asb WITH(NOLOCK) on asb.asb_slid =  t.SLID AND asb.asb_branchcode = t.BrID
							AND asb.asb_active = 'Y'
						INNER JOIN  hchb.dbo.AGENCIES a WITH(NOLOCK) ON asb.asb_agencyid = a.agency_id
						INNER JOIN hchb.dbo.Billing_Audit_Items bai WITH(NOLOCK) ON bai.bai_id = 7
						INNER JOIN  hchb.dbo.Billing_Audit_Types bat WITH(NOLOCK) ON bat.bat_id = bai.bai_batid
					WHERE 0=0
						AND t.isvalidnpi = 0

---- 8
INSERT INTO UNBILLED_REASON
           SELECT DISTINCT 
				t.epiid as epi_id,
				t.CltBranchID,
				t.epi_fullname as ClientName,
				t.epi_startofepisode as StartDate,
				t.epi_endofepisode as EndDate,
				bat.bat_id,
				bat.bat_desc,
				bai.bai_id,
				bai.bai_name,
				REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
					bai.bai_message,'CLIENTNAME',isnull(t.epi_fullname,'')),
					'SHIFTDATE',CONVERT(varchar(10),t.ShiftDate,101)),
					'FLATTYPE',isnull(t.FlatType,'')),
					'BEGINTIME',LTRIM(RIGHT(CONVERT(VARCHAR(25), t.BeginTime,100),7))),
					'ENDTIME',LTRIM(RIGHT(CONVERT(VARCHAR(25), t.EndTime,100),7))),
					'WORKERNAME',isnull(t.WorkerName,'')),
					'AGENCYNAME',''),
					'PAYORTYPENAME',isnull(t.pt_desc,'')),
					'PAYORSOURCENAME',isnull(t.ps_desc,'')),
					'PROGRAMNAME',isnull(pn_description,'')),
					'JOBDESCRIPTIONNAME',isnull(t.jd_code,'')) as bai_message,
				t.ps_desc,
				t.pt_desc,
				bai.bai_helptext,
				--t.WorkerName as WorkerName,
				--t.ShiftDate as ShiftDate,
				--t.BeginTime as BeginTime,
				--t.EndTime as EndTime,
				--t.FlatType as FlatType,
				--pn_description,
				--t.jd_code as JobDescription,
				t.GroupID,
				t.tc_id as id,
				1 as ID_TYPE
			FROM 
				ZZZ_LineItems_BillingAuditPerform t			
				INNER JOIN hchb.dbo.Billing_Audit_Items bai WITH(NOLOCK) on bai.bai_id = 8
				INNER JOIN hchb.dbo.Billing_Audit_Types bat WITH(NOLOCK) on bat.bat_id = bai.bai_batid
				INNER JOIN hchb.dbo.PROGRAMS pg WITH(NOLOCK) ON pg.pg_psid = t.GroupID and pg.pg_id = t.ProgID	
				INNER JOIN hchb.dbo.PROGRAM_NAMES pn WITH(NOLOCK) on pg.pg_pnid = pn.pn_id
			
				LEFT JOIN hchb.dbo.BILLING_CODES bc
					ON bc_active = 'Y' AND bc_pgid = pg.pg_id AND bc_jdid = t.JobDID
						AND (ISNULL(bc_effectivefrom, '01/01/1900') <= ISNULL(t.shiftdate, '12/31/2099'))
						AND (ISNULL(bc_effectiveto, '12/31/2099') >= ISNULL(t.shiftdate, '12/31/2099'))

			where 0=0
				AND (
						(t.CPT_Code is null     and (LEN(LTRIM(RTRIM(IsNull(bc.bc_revcode,'')))) = 0))
					 or (t.CPT_Code is NOT null and (LEN(LTRIM(RTRIM(IsNull(revcode      ,'')))) = 0))
					)
				AND NOT (t.SupplyFlag <> '')			 -- supplyflag was isnull(,'')ed earlier		
				AND  t.tc_labtest <> 1
				AND t.tc_phsid is null -- don't run for physician services

---- 9 
INSERT INTO UNBILLED_REASON
           		SELECT DISTINCT 
					t.epiid as epi_id,
					t.CltBranchID,
					t.epi_fullname as ClientName,
					t.epi_startofepisode as StartDate,
					t.epi_endofepisode as EndDate,
					bat.bat_id,
					bat.bat_desc,
					bai.bai_id,
					bai.bai_name,
					REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						bai_message,'CLIENTNAME',isnull(t.epi_fullname, '')),
						'SHIFTDATE',CONVERT(varchar(10),t.ShiftDate,101)),
						'FLATTYPE',isnull(t.FlatType, '')),
						'BEGINTIME',LTRIM(RIGHT(CONVERT(VARCHAR(25), t.BeginTime,100),7))),
						'ENDTIME',LTRIM(RIGHT(CONVERT(VARCHAR(25), t.EndTime,100),7))),
						'WORKERNAME',isnull(t.WorkerName, '')),
						'AGENCYNAME',''),
						'PAYORTYPENAME',isnull(t.pt_desc, '')),
						'PAYORSOURCENAME',isnull(t.ps_desc, '')),
						'PROGRAMNAME',isnull(pn.pn_description, '')),
						'JOBDESCRIPTIONNAME',isnull(t.jd_code,'')) as bai_message,
					t.ps_desc,
					t.pt_desc,
					bai.bai_helptext,
					--t.WorkerName as WorkerName,
					--t.ShiftDate as ShiftDate,
					--t.BeginTime as BeginTime,
					--t.EndTime as EndTime,
					--t.FlatType as FlatType,
					--pn.pn_description,
					--t.jd_code as JobDescription,
					t.GroupID as ps_id,
					t.tc_id as id,
				    1 as id_type
				FROM 
					#LineItems_BillingAuditPerform t 			
					INNER JOIN hchb.dbo.Billing_Audit_Items bai WITH(NOLOCK) on bai.bai_id = 9
					INNER JOIN hchb.dbo.Billing_Audit_Types bat WITH(NOLOCK) on bat.bat_id = bai.bai_batid
					INNER JOIN hchb.dbo.PROGRAMS pg WITH(NOLOCK) ON pg.pg_psid = t.GroupID and pg.pg_id = t.ProgID	
					INNER JOIN hchb.dbo.PROGRAM_NAMES pn WITH(NOLOCK) on pg.pg_pnid = pn.pn_id
					INNER JOIN #claim_profiles cp WITH(NOLOCK) ON cp.cp_payorsourceid = t.GroupID AND cp.cp_branchcode = t.CltBranchID
					INNER JOIN hchb.dbo.CLAIM_PROFILES_CLAIM_FORMATS cpcf WITH(NOLOCK) ON cpcf.cpcf_cpid = cp.cp_id AND cpcf.cpcf_cfid = 208
					LEFT JOIN hchb.dbo.BILLING_CODES bc WITH(NOLOCK)
						ON bc_active = 'Y' AND bc_pgid = pg.pg_id AND bc_jdid = t.JobDID
							AND (ISNULL(bc_effectivefrom, '01/01/1900') <= ISNULL(t.shiftdate, '12/31/2099'))
							AND (ISNULL(bc_effectiveto, '12/31/2099') >= ISNULL(t.shiftdate, '12/31/2099'))
			
				where 0=0
					AND (
							(t.CPT_Code is null     and (LEN(LTRIM(RTRIM(IsNull(bc.bc_hcpcs,'')))) = 0))
						 or (t.CPT_Code is NOT null and (LEN(LTRIM(RTRIM(IsNull(hicpic,'')))) = 0))
						)
					AND NOT (t.SupplyFlag <> '')			 -- supplyflag was isnull(,'')ed earlier	
					AND cpcf.cpcf_IsFormatOn = 1
					AND t.tc_labtest <> 1
					AND t.tc_phsid is null -- don't run this audit for physician services

/*
---- 10
PRINT'10'
INSERT INTO UNBILLED_REASON
				SELECT DISTINCT 
					t.epiid as epi_id,
					t.CltBranchID,
					t.epi_fullname as ClientName,
					t.epi_startofepisode as StartDate,
					t.epi_endofepisode as EndDate,
					bat.bat_id,
					bat.bat_desc,
					bai.bai_id,
					bai.bai_name,
					REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						bai.bai_message,'CLIENTNAME',isnull(t.epi_fullname,'')),
						'SHIFTDATE',CONVERT(varchar(10),t.ShiftDate,101)),
						'FLATTYPE',isnull(t.FlatType,'')),
						'BEGINTIME',LTRIM(RIGHT(CONVERT(VARCHAR(25), t.BeginTime,100),7))),
						'ENDTIME',LTRIM(RIGHT(CONVERT(VARCHAR(25), t.EndTime,100),7))),
						'WORKERNAME',isnull(t.WorkerName,'')),
						'AGENCYNAME',''),
						'PAYORTYPENAME',isnull(t.pt_desc,'')),
						'PAYORSOURCENAME',isnull(t.ps_desc,'')),
						'PROGRAMNAME',isnull(pn.pn_description,'')),
						'JOBDESCRIPTIONNAME',isnull(t.jd_code,'')) as bai_message,
					t.ps_desc,
					t.pt_desc,
					bai.bai_helptext,
					--t.WorkerName as WorkerName,
					--t.ShiftDate as ShiftDate,
					--t.BeginTime as BeginTime,
					--t.EndTime as EndTime,
					--t.FlatType as FlatType,
					--pn.pn_description,
					--t.jd_code as JobDescription,
					t.GroupID as ps_id,
					t.tc_id as id,
				    1 as id_type
				FROM 
					#LineItems_BillingAuditPerform t 		
					INNER JOIN hchb.dbo.Billing_Audit_Items bai WITH(NOLOCK) on bai.bai_id = 10
					INNER JOIN hchb.dbo.Billing_Audit_Types bat WITH(NOLOCK) on bat.bat_id = bai.bai_batid
					INNER JOIN hchb.dbo.PROGRAMS pg WITH(NOLOCK) ON pg.pg_psid = t.GroupID and pg.pg_id = t.ProgID	
					INNER JOIN hchb.dbo.PROGRAM_NAMES pn WITH(NOLOCK) on pg.pg_pnid = pn.pn_id
			
					LEFT JOIN hchb.dbo.BILLING_CODES bc WITH(NOLOCK)
						ON bc_active = 'Y' AND bc_pgid = pg.pg_id AND bc_jdid = t.JobDID
							AND (ISNULL(bc_effectivefrom, '01/01/1900') <= ISNULL(t.shiftdate, '12/31/2099'))
							AND (ISNULL(bc_effectiveto, '12/31/2099') >= ISNULL(t.shiftdate, '12/31/2099'))

				where 0=0
					AND (LEN(LTRIM(RTRIM(IsNull(bc.bc_proccode,'')))) = 0)
					AND NOT (t.SupplyFlag <> '')			 -- supplyflag was isnull(,'')ed earlier
					AND t.tc_labtest <> 1 --don't run if this is a CLIA lab test
					AND t.tc_phsid is null -- don't run for physician services
   

---- 11
INSERT INTO UNBILLED_REASON
              	SELECT DISTINCT 
					e.epi_id as epi_id,
					e.epi_fullname as ClientName,
					e.epi_startofepisode as StartDate,
					e.epi_endofepisode as EndDate,
					bat.bat_id,
					bat.bat_desc,
					bai.bai_id,
					bai.bai_name,
					REPLACE(REPLACE(REPLACE(REPLACE(bai_message,'CLIENTNAME',e.epi_fullname),'STARTDATE',CONVERT(varchar(10),ce.epi_startofepisode,101)),'ENDDATE',CONVERT(varchar(10),ce.epi_endofepisode,101)),'PHYSICIANNAME','') as bai_message,
					bai.bai_helptext,
					e.ps_desc,
					e.pt_desc,
					e.epi_branchcode,
					e.ps_id,
					e.epi_id as id,
					2 as id_type
				FROM 
					#Episodes e
					INNER JOIN hchb.dbo.VI_CLIENT_EPISODES ce WITH(NOLOCK,NOEXPAND)
						ON ce.epi_id = e.epi_id
					INNER JOIN hchb.dbo.Billing_Audit_Items bai WITH(NOLOCK) 
						ON bai.bai_id = 11
					INNER JOIN hchb.dbo.Billing_Audit_Types bat WITH(NOLOCK) 
						ON bat.bat_id = bai.bai_batid
					INNER JOIN hchb.dbo.client_episode_fs cefs WITH(NOLOCK)
						ON cefs.cefs_epiid = ce.epi_id AND cefs.cefs_ps = 'P' AND cefs.cefs_active = 'Y'
					INNER JOIN hchb.dbo.PAYOR_SOURCES ps WITH(NOLOCK)
						ON cefs.cefs_psid = ps.ps_id
					INNER JOIN hchb.dbo.PAYOR_TYPES pt WITH(NOLOCK)
						ON ps.ps_ptid = pt.pt_id 
					LEFT JOIN hchb.dbo.V_PAYOR_SOURCE_PAPER_CLAIM_FORMATS psif WITH(NOLOCK) 
						ON cefs.cefs_psid = psif.pspcf_psid AND psif.pspcf_cfid = 81
					LEFT JOIN hchb.dbo.edi_insprofiles ei WITH(NOLOCK)
						ON cefs.cefs_psid = GroupID
					LEFT JOIN hchb.dbo.client_episode_fs cefs1 WITH(NOLOCK)
						ON cefs1.cefs_epiid = ce.epi_id AND cefs1.cefs_claimorder = 1 AND cefs1.cefs_active = 'Y'
				WHERE (ei.OtherPayors = 1 OR ISNULL(psif.pspcf_isFormatOn,0) = 1)
				  AND cefs1.cefs_id is NULL	
				  */



end
GO
/****** Object:  StoredProcedure [dbo].[p_update_tran_episode]    Script Date: 10/13/2021 10:15:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



-- =============================================
-- Author:		Pavel Chibisov
-- Create date: 8/27/2018
-- Description:	Procedure is used by MicroStrategy transaction services to update / insert information at the episode level

--To test execution of procedure--
/*
execute dbo.p_update_tran_episode @content_id = 1003, 
								@content_desc = 'CHOICE Members in CHHA (Transition of Care)',
								@epiid = 1,
								@response1 = 'test3',
								@response2 = null,
								@response3 = null,
								@response4 = null,
								@response5 = null,
								@response6 = null,
								@response7 = null,
								@response8 = null,
								@response9 = null,
								@response10 = null,
								@v_usr = 'pavel',
								@completed = 1

SELECT * FROM VNSNY_BI.[dbo].[TRAN_EPISODE]

--truncate table VNSNY_BI.[dbo].[TRAN_EPISODE]
*/

-- =============================================
CREATE PROCEDURE [dbo].[p_update_tran_episode]
(
	@content_id		  int,
    @epiid			  int,
    @response1        VARCHAR(255),
    @response2        VARCHAR(255),
    @response3        VARCHAR(255),
    @response4        VARCHAR(255),
    @response5        VARCHAR(255),
    @response6        VARCHAR(255),
    @response7        VARCHAR(255),
    @response8        VARCHAR(255),
    @response9        VARCHAR(255),
    @response10       VARCHAR(255),
    @v_usr			  VARCHAR(64),
    @completed		  int
)

AS
BEGIN

	SET NOCOUNT ON;

	declare @cnt int, 
			@q_num    int,
			@q_desc   VARCHAR(50),
			@content_desc VARCHAR(250)


---------------------------Question / Response 1-----------------------
	SET @cnt = (SELECT COUNT (*)
    FROM VNSNY_BI.[dbo].[TRAN_EPISODE]
    WHERE [TE_LTE_CONTENT_ID] = @content_id AND [TE_EPIID] = @epiid AND [TE_LTE_QUESTION_NUM] = 1)

	select @cnt, @response1
  
   IF @cnt > 0 and @response1 is not null

		BEGIN
   
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = @response1,  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 1;

		END

	ELSE IF @cnt > 0 and (@response1 is null )

			BEGIN     
			
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = 'Comments Deleted',  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 1;
			END

	ELSE IF @cnt = 0 and @response1 is not null

		BEGIN

			SELECT @q_num = [LTE_QUESTION_NUM], @q_desc = [LTE_QUESTION_DESC], @content_desc = [LTE_CONTENT_DESC]
			FROM   VNSNY_BI.[dbo].[LU_TRAN_EPISODE]  
			WHERE   [LTE_CONTENT_ID] = @content_id and [LTE_QUESTION_NUM] = 1
   
			INSERT INTO VNSNY_BI.[dbo].[TRAN_EPISODE] (
											[TE_LTE_CONTENT_ID],
											[TE_LTE_CONTENT_DESC],
											[TE_EPIID],
											[TE_LTE_QUESTION_NUM],
											[TE_LTE_QUESTION_DESC],
											[TE_RESPONSE],
											[TE_CRT_TS],
											[TE_UPD_TS],
											[TE_COMPLETED],
											[TE_UPD_ID])
			VALUES (@content_id, @content_desc, @epiid, @q_num, @q_desc, @response1, SYSDATETIME(), SYSDATETIME(), ISNULL(@completed,0), @v_usr)
		END

-----------------------End of Question / Response 1--------------------

---------------------------Question / Response 2-----------------------
	SET @cnt = (SELECT COUNT (*)
    FROM VNSNY_BI.[dbo].[TRAN_EPISODE]
    WHERE [TE_LTE_CONTENT_ID] = @content_id AND [TE_EPIID] = @epiid AND [TE_LTE_QUESTION_NUM] = 2)

  
   IF @cnt > 0 and @response2 is not null


		BEGIN      
			UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
			SET	    [TE_RESPONSE] = @response2,  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
			WHERE   [TE_LTE_CONTENT_ID] = @content_id
				AND [TE_EPIID] = @epiid
				AND [TE_LTE_QUESTION_NUM] = 2;
		END

   ELSE IF @cnt > 0 and @response2 is null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = 'Deleted',  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 2;
			END

	ELSE IF @cnt = 0 and @response2 is not null

		BEGIN
			SELECT @q_num = [LTE_QUESTION_NUM], @q_desc = [LTE_QUESTION_DESC], @content_desc = [LTE_CONTENT_DESC]
			FROM   VNSNY_BI.[dbo].[LU_TRAN_EPISODE]  
			WHERE   [LTE_CONTENT_ID] = @content_id and [LTE_QUESTION_NUM] = 2
   
			INSERT INTO VNSNY_BI.[dbo].[TRAN_EPISODE] (
											[TE_LTE_CONTENT_ID],
											[TE_LTE_CONTENT_DESC],
											[TE_EPIID],
											[TE_LTE_QUESTION_NUM],
											[TE_LTE_QUESTION_DESC],
											[TE_RESPONSE],
											[TE_CRT_TS],
											[TE_UPD_TS],
											[TE_COMPLETED],
											[TE_UPD_ID])
			VALUES (@content_id, @content_desc, @epiid, @q_num, @q_desc, @response2, SYSDATETIME(), SYSDATETIME(), ISNULL(@completed,0), @v_usr)

			select @q_num, @q_desc
		END

-----------------------End of Question / Response 2--------------------

---------------------------Question / Response 3-----------------------
	SET @cnt = (SELECT COUNT (*)
    FROM VNSNY_BI.[dbo].[TRAN_EPISODE]
    WHERE [TE_LTE_CONTENT_ID] = @content_id AND [TE_EPIID] = @epiid AND [TE_LTE_QUESTION_NUM] = 3)

  
   IF @cnt > 0 and @response3 is not null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = @response3,  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 3;
			END

	ELSE IF @cnt > 0 and @response3 is null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = 'Deleted',  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 3;
			END

	ELSE IF @cnt = 0 and @response3 is not null

		BEGIN
			SELECT @q_num = [LTE_QUESTION_NUM], @q_desc = [LTE_QUESTION_DESC], @content_desc = [LTE_CONTENT_DESC]
			FROM   VNSNY_BI.[dbo].[LU_TRAN_EPISODE]  
			WHERE   [LTE_CONTENT_ID] = @content_id and [LTE_QUESTION_NUM] = 3
   
			INSERT INTO VNSNY_BI.[dbo].[TRAN_EPISODE] (
											[TE_LTE_CONTENT_ID],
											[TE_LTE_CONTENT_DESC],
											[TE_EPIID],
											[TE_LTE_QUESTION_NUM],
											[TE_LTE_QUESTION_DESC],
											[TE_RESPONSE],
											[TE_CRT_TS],
											[TE_UPD_TS],
											[TE_COMPLETED],
											[TE_UPD_ID])
			VALUES (@content_id, @content_desc, @epiid, @q_num, @q_desc, @response3, SYSDATETIME(), SYSDATETIME(), ISNULL(@completed,0), @v_usr)
		END

-----------------------End of Question / Response 3--------------------

---------------------------Question / Response 4-----------------------
	SET @cnt = (SELECT COUNT (*)
    FROM VNSNY_BI.[dbo].[TRAN_EPISODE]
    WHERE [TE_LTE_CONTENT_ID] = @content_id AND [TE_EPIID] = @epiid AND [TE_LTE_QUESTION_NUM] = 4)

  
   IF @cnt > 0 and @response4 is not null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = @response4,  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 4;
			END

	ELSE IF @cnt > 0 and @response4 is null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = 'Deleted',  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 4;
			END

	ELSE IF @cnt = 0 and @response4 is not null

		BEGIN
			SELECT @q_num = [LTE_QUESTION_NUM], @q_desc = [LTE_QUESTION_DESC], @content_desc = [LTE_CONTENT_DESC]
			FROM   VNSNY_BI.[dbo].[LU_TRAN_EPISODE]  
			WHERE   [LTE_CONTENT_ID] = @content_id and [LTE_QUESTION_NUM] = 4
   
			INSERT INTO VNSNY_BI.[dbo].[TRAN_EPISODE] (
											[TE_LTE_CONTENT_ID],
											[TE_LTE_CONTENT_DESC],
											[TE_EPIID],
											[TE_LTE_QUESTION_NUM],
											[TE_LTE_QUESTION_DESC],
											[TE_RESPONSE],
											[TE_CRT_TS],
											[TE_UPD_TS],
											[TE_COMPLETED],
											[TE_UPD_ID])
			VALUES (@content_id, @content_desc, @epiid, @q_num, @q_desc, @response4, SYSDATETIME(), SYSDATETIME(), ISNULL(@completed,0), @v_usr)
		END

-----------------------End of Question / Response 4--------------------

---------------------------Question / Response 5-----------------------
	SET @cnt = (SELECT COUNT (*)
    FROM VNSNY_BI.[dbo].[TRAN_EPISODE]
    WHERE [TE_LTE_CONTENT_ID] = @content_id AND [TE_EPIID] = @epiid AND [TE_LTE_QUESTION_NUM] = 5)

  
   IF @cnt > 0 and @response5 is not null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = @response5,  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 5;
			END

	ELSE IF @cnt > 0 and @response5 is null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = 'Comments Deleted',  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 5;
			END

	ELSE IF @cnt = 0 and @response5 is not null

		BEGIN
			SELECT @q_num = [LTE_QUESTION_NUM], @q_desc = [LTE_QUESTION_DESC], @content_desc = [LTE_CONTENT_DESC]
			FROM   VNSNY_BI.[dbo].[LU_TRAN_EPISODE]  
			WHERE   [LTE_CONTENT_ID] = @content_id and [LTE_QUESTION_NUM] = 5
   
			INSERT INTO VNSNY_BI.[dbo].[TRAN_EPISODE] (
											[TE_LTE_CONTENT_ID],
											[TE_LTE_CONTENT_DESC],
											[TE_EPIID],
											[TE_LTE_QUESTION_NUM],
											[TE_LTE_QUESTION_DESC],
											[TE_RESPONSE],
											[TE_CRT_TS],
											[TE_UPD_TS],
											[TE_COMPLETED],
											[TE_UPD_ID])
			VALUES (@content_id, @content_desc, @epiid, @q_num, @q_desc, @response5, SYSDATETIME(), SYSDATETIME(), ISNULL(@completed,0), @v_usr)
		END

-----------------------End of Question / Response 5--------------------

---------------------------Question / Response 6-----------------------
	SET @cnt = (SELECT COUNT (*)
    FROM VNSNY_BI.[dbo].[TRAN_EPISODE]
    WHERE [TE_LTE_CONTENT_ID] = @content_id AND [TE_EPIID] = @epiid AND [TE_LTE_QUESTION_NUM] = 6)
  
   IF @cnt > 0 and @response6 is not null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = @response6,  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 6;
			END

	ELSE IF @cnt > 0 and @response6 is null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = 'Deleted',  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 6;
			END

	ELSE IF @cnt = 0 and @response6 is not null

		BEGIN
			SELECT @q_num = [LTE_QUESTION_NUM], @q_desc = [LTE_QUESTION_DESC], @content_desc = [LTE_CONTENT_DESC]
			FROM   VNSNY_BI.[dbo].[LU_TRAN_EPISODE]  
			WHERE   [LTE_CONTENT_ID] = @content_id and [LTE_QUESTION_NUM] = 6
   
			INSERT INTO VNSNY_BI.[dbo].[TRAN_EPISODE] (
											[TE_LTE_CONTENT_ID],
											[TE_LTE_CONTENT_DESC],
											[TE_EPIID],
											[TE_LTE_QUESTION_NUM],
											[TE_LTE_QUESTION_DESC],
											[TE_RESPONSE],
											[TE_CRT_TS],
											[TE_UPD_TS],
											[TE_COMPLETED],
											[TE_UPD_ID])
			VALUES (@content_id, @content_desc, @epiid, @q_num, @q_desc, @response6, SYSDATETIME(), SYSDATETIME(), ISNULL(@completed,0), @v_usr)
		END

-----------------------End of Question / Response 6--------------------

---------------------------Question / Response 7-----------------------
	SET @cnt = (SELECT COUNT (*)
    FROM VNSNY_BI.[dbo].[TRAN_EPISODE]
    WHERE [TE_LTE_CONTENT_ID] = @content_id AND [TE_EPIID] = @epiid AND [TE_LTE_QUESTION_NUM] = 7)
  
   IF @cnt > 0 and @response7 is not null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = @response7,  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 7;
			END

	ELSE IF @cnt > 0 and @response7 is null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = 'Deleted',  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 7;
			END

	ELSE IF @cnt = 0 and @response7 is not null

		BEGIN
			SELECT @q_num = [LTE_QUESTION_NUM], @q_desc = [LTE_QUESTION_DESC], @content_desc = [LTE_CONTENT_DESC]
			FROM   VNSNY_BI.[dbo].[LU_TRAN_EPISODE]  
			WHERE   [LTE_CONTENT_ID] = @content_id and [LTE_QUESTION_NUM] = 7
   
			INSERT INTO VNSNY_BI.[dbo].[TRAN_EPISODE] (
											[TE_LTE_CONTENT_ID],
											[TE_LTE_CONTENT_DESC],
											[TE_EPIID],
											[TE_LTE_QUESTION_NUM],
											[TE_LTE_QUESTION_DESC],
											[TE_RESPONSE],
											[TE_CRT_TS],
											[TE_UPD_TS],
											[TE_COMPLETED],
											[TE_UPD_ID])
			VALUES (@content_id, @content_desc, @epiid, @q_num, @q_desc, @response7, SYSDATETIME(), SYSDATETIME(), ISNULL(@completed,0), @v_usr)
		END

-----------------------End of Question / Response 7--------------------

---------------------------Question / Response 8-----------------------
	SET @cnt = (SELECT COUNT (*)
    FROM VNSNY_BI.[dbo].[TRAN_EPISODE]
    WHERE [TE_LTE_CONTENT_ID] = @content_id AND [TE_EPIID] = @epiid AND [TE_LTE_QUESTION_NUM] = 8)
  
   IF @cnt > 0 and @response8 is not null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = @response8,  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 8;
			END

	ELSE IF @cnt > 0 and @response8 is null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = 'Deleted',  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 8;
			END

	ELSE IF @cnt = 0 and @response8 is not null

		BEGIN
			SELECT @q_num = [LTE_QUESTION_NUM], @q_desc = [LTE_QUESTION_DESC], @content_desc = [LTE_CONTENT_DESC]
			FROM   VNSNY_BI.[dbo].[LU_TRAN_EPISODE]  
			WHERE   [LTE_CONTENT_ID] = @content_id and [LTE_QUESTION_NUM] = 8
   
			INSERT INTO VNSNY_BI.[dbo].[TRAN_EPISODE] (
											[TE_LTE_CONTENT_ID],
											[TE_LTE_CONTENT_DESC],
											[TE_EPIID],
											[TE_LTE_QUESTION_NUM],
											[TE_LTE_QUESTION_DESC],
											[TE_RESPONSE],
											[TE_CRT_TS],
											[TE_UPD_TS],
											[TE_COMPLETED],
											[TE_UPD_ID])
			VALUES (@content_id, @content_desc, @epiid, @q_num, @q_desc, @response8, SYSDATETIME(), SYSDATETIME(), ISNULL(@completed,0), @v_usr)
		END

-----------------------End of Question / Response 8--------------------

---------------------------Question / Response 9-----------------------
	SET @cnt = (SELECT COUNT (*)
    FROM VNSNY_BI.[dbo].[TRAN_EPISODE]
    WHERE [TE_LTE_CONTENT_ID] = @content_id AND [TE_EPIID] = @epiid AND [TE_LTE_QUESTION_NUM] = 9)
  
   IF @cnt > 0 and @response9 is not null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = @response9,  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 9;
			END

	ELSE IF @cnt > 0 and @response9 is null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = 'Comments Deleted',  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 9;
			END

	ELSE IF @cnt = 0 and @response9 is not null

		BEGIN
			SELECT @q_num = [LTE_QUESTION_NUM], @q_desc = [LTE_QUESTION_DESC], @content_desc = [LTE_CONTENT_DESC]
			FROM   VNSNY_BI.[dbo].[LU_TRAN_EPISODE]  
			WHERE   [LTE_CONTENT_ID] = @content_id and [LTE_QUESTION_NUM] = 9
   
			INSERT INTO VNSNY_BI.[dbo].[TRAN_EPISODE] (
											[TE_LTE_CONTENT_ID],
											[TE_LTE_CONTENT_DESC],
											[TE_EPIID],
											[TE_LTE_QUESTION_NUM],
											[TE_LTE_QUESTION_DESC],
											[TE_RESPONSE],
											[TE_CRT_TS],
											[TE_UPD_TS],
											[TE_COMPLETED],
											[TE_UPD_ID])
			VALUES (@content_id, @content_desc, @epiid, @q_num, @q_desc, @response9, SYSDATETIME(), SYSDATETIME(), ISNULL(@completed,0), @v_usr)
		END

-----------------------End of Question / Response 9--------------------

---------------------------Question / Response 10-----------------------
	SET @cnt = (SELECT COUNT (*)
    FROM VNSNY_BI.[dbo].[TRAN_EPISODE]
    WHERE [TE_LTE_CONTENT_ID] = @content_id AND [TE_EPIID] = @epiid AND [TE_LTE_QUESTION_NUM] = 10)
  
   IF @cnt > 0 and @response10 is not null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = @response10,  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 10;
			END

	ELSE IF @cnt > 0 and @response10 is null

			BEGIN      
				UPDATE  VNSNY_BI.[dbo].[TRAN_EPISODE]
				SET	    [TE_RESPONSE] = 'Comments Deleted',  [TE_UPD_TS] =  SYSDATETIME(), [TE_COMPLETED] = ISNULL(@completed,0), [TE_UPD_ID] = @v_usr
				WHERE   [TE_LTE_CONTENT_ID] = @content_id
					AND [TE_EPIID] = @epiid
					AND [TE_LTE_QUESTION_NUM] = 10;
			END

	ELSE IF @cnt = 0 and @response10 is not null

		BEGIN
			SELECT @q_num = [LTE_QUESTION_NUM], @q_desc = [LTE_QUESTION_DESC], @content_desc = [LTE_CONTENT_DESC]
			FROM   VNSNY_BI.[dbo].[LU_TRAN_EPISODE]  
			WHERE   [LTE_CONTENT_ID] = @content_id and [LTE_QUESTION_NUM] = 10
   
			INSERT INTO VNSNY_BI.[dbo].[TRAN_EPISODE] (
											[TE_LTE_CONTENT_ID],
											[TE_LTE_CONTENT_DESC],
											[TE_EPIID],
											[TE_LTE_QUESTION_NUM],
											[TE_LTE_QUESTION_DESC],
											[TE_RESPONSE],
											[TE_CRT_TS],
											[TE_UPD_TS],
											[TE_COMPLETED],
											[TE_UPD_ID])
			VALUES (@content_id, @content_desc, @epiid, @q_num, @q_desc, @response10, SYSDATETIME(), SYSDATETIME(), ISNULL(@completed,0), @v_usr)
		END

-----------------------End of Question / Response 10--------------------


END
GO
/****** Object:  StoredProcedure [dbo].[rpt_PDGMUnbilledReport]    Script Date: 10/13/2021 10:15:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



--============================================================
 --Author:		Sabitha Boorsu
 --Create date: 8/6/2019\
   
 --Description:	Retrieves info for PDGM Ubilled Report
 --============================================================

CREATE PROCEDURE [dbo].[rpt_PDGMUnbilledReport]
--(
--	@rptreqrpt_id INT
--)
AS	
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;	
	
/* BEGIN TEMPORARY TESTING INPUTS */
	--DECLARE @rptreqrpt_id INT
	--SET @rptreqrpt_id = 190116344
	--(
	--	SELECT TOP 1 rptreqrpt_id
	--	FROM [dbo].[REPORT_REQUESTS] 
	--	JOIN [dbo].[REPORT_REQUEST_REPORTS] ON rptreqrpt_rptreqid = rptreq_id
	--	WHERE 1=1
	--	AND rptreq_description  = 'PDGM Unbilled Report'
	--	AND rptreq_username = 'HCHBCORP\jgrubs'
	--	AND rptreq_INSERTDate >= '03/06/2020'  
	--	ORDER BY rptreqrpt_InsertDate DESC
	--)
	IF OBJECT_ID('tempdb..#params') IS NOT NULL BEGIN DROP TABLE #params END;
	IF OBJECT_ID('tempdb..#ReasonHeld') IS NOT NULL BEGIN DROP TABLE #ReasonHeld END;
	IF OBJECT_ID('tempdb..#period') IS NOT NULL BEGIN DROP TABLE #period END;
	IF OBJECT_ID('tempdb..#cltbill') IS NOT NULL BEGIN DROP TABLE #cltbill END;
	IF OBJECT_ID('tempdb..#LastSubmissonDate') IS NOT NULL BEGIN DROP TABLE #LastSubmissonDate END;
	IF OBJECT_ID('tempdb..#SOCworkflow') IS NOT NULL BEGIN DROP TABLE #SOCworkflow END;
	IF OBJECT_ID('tempdb..#EOPWorkflow') IS NOT NULL BEGIN DROP TABLE #EOPWorkflow END;
	IF OBJECT_ID('tempdb..#ClaimHeld') IS NOT NULL BEGIN DROP TABLE #ClaimHeld END;
	IF OBJECT_ID('tempdb..#MaxOasisHistoryID') IS NOT NULL BEGIN DROP TABLE #MaxOasisHistoryID END;
	IF OBJECT_ID('tempdb..#PeriodsWIthVerifiedFirstVisit') IS NOT NULL BEGIN	DROP TABLE #PeriodsWIthVerifiedFirstVisit END;
	IF OBJECT_ID('tempdb..#FaceToFace_Details') IS NOT NULL BEGIN	DROP TABLE #FaceToFace_Details END;
	IF OBJECT_ID('tempdb..#SCHEDSubset') IS NOT NULL BEGIN	DROP TABLE #SCHEDSubset END;
	IF OBJECT_ID('tempdb..#PeriodWithMSPPayor') IS NOT NULL BEGIN	DROP TABLE #PeriodWithMSPPayor END;
	
	
	
/* END TEMPORARY TESTING INPUTS */


	DECLARE 
		@lpid INT,
		@EOPDate DATETIME,
		@ARGroup INT,
		@GroupBy VARCHAR(200),
		@SortBy VARCHAR(200),
		@HeldReasonstoDisplay INT

	/*
	DECLARE	@PayorSources dbo.IdList,
			@Branches dbo.BranchList,
			@Patients dbo.IDList		
	*/
	DECLARE @PayorOrder TABLE (PayorOrder CHAR(1) NOT NULL)
	DECLARE @BillingStatus TABLE (BillingStatus VARCHAR(25) NOT NULL)


	CREATE TABLE #params 
	(
		pname VARCHAR(100) NULL, 
		pvalue VARCHAR(MAX) NULL
	)
	CREATE TABLE #ReasonHeld 
	(
		chr_id INT NULL
	)
	CREATE TABLE #period 
	(
		pp_id INT NULL, 
		pp_cefsId INT NULL, 
		pp_startDate DATETIME NULL, 
		pp_enddate DATETIME NULL, 
		pp_msp INT NULL,
		cefs_ps CHAR(1) NULL, 
		cefs_psid INT NULL, 
		cefs_epiid INT NULL, 						  
		InsuredID VARCHAR(26) NULL, 
		ps_desc VARCHAR(50) NULL, 
		Revenue DECIMAL(9,2) NULL, 
		pp_periodEnded BIT NULL, 
		pp_drCode CHAR(2) NULL,  
		LastSubmissonDate DATETIME NULL, 
		BillingStatus VARCHAR(25) NULL, 
		ClaimHeldReasons VARCHAR(MAX) NULL, 
		ClaimHeldComments VARCHAR(MAX) NULL,
		cefs_ManualHold CHAR(1) NULL,
		pp_ceoid INT NULL,
		ceoh_TransType VARCHAR(30) NULL,
		PrimaryPhysician VARCHAR(101) NULL,
		[PECOS status] VARCHAR(50) NULL
		/*INDEX IX_#Period_cefsepiid_ppceoid NONCLUSTERED (cefs_epiid, pp_ceoid)*/

	);
	CREATE TABLE #cltbill 
	(
		pp_id INT NULL, 
		Branchid CHAR(3) NULL, 
		cltid INT NULL, 
		primid INT NULL, 
		PatientName VARCHAR(150) NULL, 
		MRNumber VARCHAR(14) NULL,
		SOP DATETIME NULL, 
		EOP DATETIME NULL, 
		SOE DATETIME NULL, 
		ps_desc VARCHAR(50) NULL, 
		pp_msp INT NULL,
		cefs_ps CHAR(1) NULL,
		cefs_psid INT NULL, 
		epi_id INT NULL, 
		epi_RecertFlag VARCHAR(50) NULL,
		InsuredID VARCHAR(26) NULL, 
		Revenue DECIMAL(9,2) NULL, 
		pp_periodEnded BIT NULL, 
		pp_drCode CHAR(2) NULL, 
		LastSubmissonDate DATETIME NULL, 
		BillingStatus VARCHAR(25) NULL, 
		ClaimHeldReasons VARCHAR(MAX) NULL, 
		ClaimHeldComments VARCHAR(MAX) NULL,
		chr_id INT NULL,
		CHPriority INT NULL,
		cefs_ManualHold CHAR(1) NULL,
		ceoh_TransType VARCHAR(30) NULL,
		F2FAllowBilling CHAR(1) NULL,
		PaperWorkAcceptedFlag CHAR(1) NULL,
		PrimaryPhysician VARCHAR(101) NULL,
		[PECOS status] VARCHAR(50) NULL,
		FirstVisitBillable CHAR(1) NULL,
		EOPWorkflowStage VARCHAR(100) NULL,
		EOPWorkflowStartDate DATETIME NULL
	);

	 CREATE TABLE #SCHEDSubset 
	(
		PeriodId INT NULL, 
		ShiftDate DATETIME NULL
	);

	CREATE TABLE #LastSubmissonDate 
	(
		pp_id INT NULL, 
		LastSubmissonDate DATETIME NULL
	);

	CREATE TABLE #SOCworkflow 
	(		
		rownum INT NULL,
		EpisodeID INT NULL, 
		pp_id INT NULL,										
		EpisodePayorId INT NULL,
		WorkflowStage VARCHAR(1000) NULL,
		cees_enddate DATETIME NULL
	); 
	
	CREATE TABLE #ClaimHeld 
	(
		pp_id INT NULL, 
		chr_id INT NULL,
		ClaimHeldReason VARCHAR(50) NULL, 
		ClaimHeldComment VARCHAR(7000) NULL,
		CHPriority INT NULL
	);
	CREATE TABLE #MaxOasisHistoryID 
	(
		ceoh_ceoid INT NULL, 
		ceoh_id INT NULL
	);

	CREATE TABLE #PeriodsWIthVerifiedFirstVisit
    (
        periodid INT PRIMARY KEY CLUSTERED NOT NULL
    );

	CREATE TABLE #EOPWorkflow 
	(		
		rownum INT NULL,
		pp_id INT NULL,										
		stageid INT NULL,
		cees_startdate DATETIME NULL,
		cees_enddate DATETIME NULL
	); 

	CREATE TABLE #FaceToFace_Details 
	(
		epiid INT NOT NULL, ceftf_id INT NULL,  
		ceftf_required INT NULL, faceTofaceDayBeforeSetup INT NOT NULL,
		faceTofaceDayAfterSetup INT NOT NULL, f2frs_allowBilling CHAR(1) NULL
	)
	CREATE TABLE #PeriodWithMSPPayor (pp_id INT NULL, cefs_id INT NULL)


	/* GET REPORT PARAMETERS */
	/*INSERT INTO #params (pname,pvalue)
	SELECT rrrp.RPTREQRPTP_NAME, rrrpv.rptreqrptpv_value
	FROM dbo.REPORT_REQUEST_REPORT_PARAMETERS AS rrrp
	INNER JOIN dbo.REPORT_REQUEST_REPORT_PARAMETER_VALUES AS rrrpv ON rrrpv.RPTREQRPTPV_RPTREQRPTPID = rrrp.RPTREQRPTP_ID
	WHERE rrrp.RPTREQRPTP_RPTREQRPTID = @rptreqrpt_id
	*/

	/* GET VALUES FROM REPORT REQUEST TABLES */
	/*
	SELECT @lpid = p.pvalue FROM #params AS p WHERE p.pname = 'lpid';
	SELECT @EOPDate = p.pvalue FROM #params AS p WHERE p.pname = 'EOPDate'
	SELECT @ARGroup = p.pvalue FROM #params AS p WHERE p.pname = 'ARGroup'
	SELECT @GroupBy = p.pvalue FROM #params AS p WHERE p.pname = 'GroupByOption'
	SELECT @SortBy = p.pvalue FROM #params AS p WHERE p.pname = 'SortByOption'
	SELECT @HeldReasonstoDisplay = p.pvalue FROM #params AS p WHERE p.pname = 'parmHeldReasonstoDisplay'
	*/	

	/* IF THE (ALL) VALUE EXISTS IN THE REPORT REQUEST TABLES FOR BRANCHES, RETURN LIST OF SECURITY-FILTERED BRANCHES. */
	/*
	IF EXISTS (SELECT 1 FROM #params AS p WHERE p.pname = 'parmBranch' AND p.pvalue = '-99999999')
	BEGIN
		INSERT @Branches SELECT branch_code FROM dbo.fn_GetBranchesByLPID (@lpid, DEFAULT, 'B')		
	END
	ELSE
	BEGIN
		INSERT @Branches SELECT p.pvalue FROM #params AS p WHERE p.pname = 'parmBranch'AND p.pvalue <> '-99999990';
	END

	INSERT @Patients SELECT p.pvalue FROM #PARAMS AS p WHERE p.pname = 'parmClient'
	INSERT @PayorSources SELECT p.pvalue FROM #PARAMS AS p WHERE p.pname = 'parmPayorSource'
	INSERT @PayorOrder SELECT p.pvalue FROM #PARAMS AS p WHERE p.pname  = 'parmPayorOrder'
	INSERT @BillingStatus SELECT p.pvalue FROM #PARAMS AS p WHERE p.pname = 'parmBillingStatus'
	
	INSERT INTO #ReasonHeld (chr_id)
	SELECT pvalue FROM #params WHERE pname = 'parmReasonHeld'
	*/
		
	INSERT INTO #period (pp_id, pp_cefsId, pp_startDate, pp_enddate, pp_msp, cefs_ps, cefs_psid, cefs_epiid, InsuredID, ps_desc, 
						 Revenue, pp_periodEnded, pp_drCode, BillingStatus, cefs_ManualHold, pp_ceoid,PrimaryPhysician,[PECOS status])
	SELECT 			
		pp.pp_id,
		pp.pp_cefsId,
		pp.pp_startDate,
		pp.pp_enddate,
		pp.pp_msp,
		cefs.cefs_ps,
		cefs.cefs_psid,
		cefs.cefs_epiid,
		COALESCE(cefs.cefs_policyno, cefs.cefs_MedicareNo),
		ps.ps_desc,
		pp.pp_currentPayment,
		pp.pp_periodEnded,
		pp.pp_drCode,
		CASE 
			WHEN pp.pp_rapCancelPending = 1 THEN 'PENDING RAP CANCEL'	
			WHEN pp.pp_claimAdjustmentPending = 1 THEN 'PENDING CLAIM ADJUSTMENT'	
			WHEN pp.pp_rapBilled = 0 THEN 'RAP UNBILLED'
			ELSE 'FINAL UNBILLED' 		 
		END AS BillingStatus
		,cefs.cefs_ManualHold
		,pp.pp_ceoid
		,PrimaryPhysician = NULL
		,[PECOS status] = NULL
	FROM hchb.PDGM.PDGM_PERIOD AS pp 
		 JOIN HCHB.dbo.CLIENT_EPISODE_FS cefs ON cefs.cefs_id = pp.pp_cefsId
		 AND cefs.cefs_active = 'Y'
		 AND cefs.cefs_ps IN ('P', 'S')
	JOIN HCHB.dbo.PAYOR_SOURCES AS ps ON cefs.cefs_psid = ps.ps_id AND ps.ps_freq = 10
	--JOIN @PayorOrder AS po ON po.PayorOrder = cefs.cefs_ps	
	WHERE 
	pp.pp_deleted = 0
	AND pp.pp_claimBilled  = 0	
	--AND pp.pp_enddate < @EOPDate 
	AND EXISTS 
		(
			SELECT 1 
			FROM HCHB.dbo.CLIENT_EPISODES AS ce 
			--INNER JOIN @Branches AS b ON ce.epi_branchcode = b.BranchCode
			WHERE cefs.cefs_epiid = ce.epi_id
		)
	--AND EXISTS (SELECT 1 FROM @PayorSources WHERE ID IN (-99999999, ISNULL(cefs.cefs_psid, -99999990)));										

	--select * from #period

	--select * from hchb.PDGM.PDGM_PERIOD
	/* DETERMINE FACE TO FACE STATUS. */
	INSERT INTO #FaceToFace_Details (epiid, ceftf_id, ceftf_required,faceTofaceDayBeforeSetup, faceTofaceDayAfterSetup, f2frs_allowBilling)
	SELECT DISTINCT p.cefs_epiid
		   ,ceftf.ceftf_id 
		   ,ceftf.ceftf_required
		   ,psftfs.psftfs_daysBefore
		   ,psftfs.psftfs_daysAfter
		   ,f2frs.f2frs_allowBilling
	  FROM #period p
	  JOIN HCHB.dbo.CLIENT_EPISODES ce ON p.cefs_epiid = ce.epi_id
	  JOIN HCHB.dbo.PAYOR_SOURCES ps ON ps.ps_id = p.cefs_psid
	  JOIN HCHB.dbo.PAYOR_SOURCE_FACE_TO_FACE_SETUP psftfs ON psftfs.psftfs_psid = ps.ps_id AND psftfs.psftfs_active = 1
	  LEFT JOIN HCHB.dbo.CLIENT_EPISODE_FACETOFACE ceftf ON ceftf.ceftf_f2fappliestoepiid = p.cefs_epiid AND ceftf.ceftf_active = 1
	  LEFT JOIN HCHB.dbo.FACETOFACE_REVIEW_STATUSES f2frs ON ceftf.ceftf_f2frsid = f2frs.f2frs_id
	  WHERE ce.epi_status <> 'NON-ADMIT'
	   AND ce.epi_nonadmitdate IS NULL
	   AND ce.epi_SocDate BETWEEN psftfs.psftfs_effectiveFrom AND ISNULL(psftfs.psftfs_effectiveTo, '01/01/3000')
	   AND ps.ps_enableF2FEncounterFeature = 'Y'
	   AND ps.ps_sltid = 1
	   AND ps.ps_F2FEncounterEffectiveDate <= ce.epi_SocDate
	/* DETERMINE FACE TO FACE STATUS. */
	        
	--select * from #FaceToFace_Details		
			  
	/* DETERMINE OAISIS STATUS */
		/* GET THE MOST RECENT HISTORY ID PER OASIS ID */
			INSERT #MaxOasisHistoryID (ceoh_ceoid, ceoh_id)
			SELECT ceoh.ceoh_ceoid, MAX(ceoh.ceoh_id) AS ceoh_id
			FROM HCHB.dbo.CLIENT_EPISODE_OASIS_HISTORY AS ceoh
			WHERE EXISTS(SELECT 1 FROM #period AS p WHERE p.pp_ceoid = ceoh.ceoh_ceoid) 
			GROUP BY ceoh.ceoh_ceoid

	--select * from #MaxOasisHistoryID

		/* PROVIDE STATUS FOR THOSE RECORDS WHICH HAVE A CEOID. */
			UPDATE #Period
			SET ceoh_TransType = ceoh.ceoh_transtype
			FROM HCHB.dbo.CLIENT_EPISODE_OASIS_HISTORY AS ceoh
			WHERE #Period.pp_ceoid = ceoh.ceoh_ceoid
			AND #Period.pp_ceoid IS NOT NULL
			AND EXISTS(SELECT 1 FROM #MaxOasisHistoryID AS m WHERE m.ceoh_ceoid = ceoh.ceoh_ceoid AND m.ceoh_id = ceoh.ceoh_id)
			

	/*DETERMINE PRIMARY PHYSICIAN AND PECOS STATUS*/

	UPDATE p SET p.PrimaryPhysician = ISNULL(ph.ph_lastname + ', ', '') + ISNULL(ph.ph_firstname + ' ', ''), p.[PECOS status] = CASE WHEN ppes.ppes_desc LIKE 'VERIFIED%' THEN 'Y' ELSE 'N' END
	FROM #period AS p 
	JOIN HCHB.dbo.CLIENT_EPISODE_PHYSICIANS AS cep ON cep.cep_epiid = p.cefs_epiid
	JOIN HCHB.dbo.PHYSICIAN_OFFICES AS po ON po.po_id = cep.cep_poid
	JOIN HCHB.dbo.PHYSICIANS AS ph ON ph.ph_id = po.po_phid
	JOIN HCHB.[dbo].PHYSICIANS_PECOS_ENROLLMENT_STATUSES AS ppes ON ph.ph_PECOSEnrollmentStatus = ppes.ppes_id
	WHERE cep.cep_sortorder = 0

	INSERT INTO #LastSubmissonDate (pp_id, LastSubmissonDate)
	SELECT pp.pp_id, MAX(p.DateCreated) AS LastSubmissonDate	
	FROM #period AS pp 	
	JOIN HCHB.Billing.INVOICES AS i ON pp.pp_id = i.i_ppid
	JOIN HCHB.dbo.pps_resubmitEBhistory AS p ON i.i_id = p.InvNum
	GROUP BY pp.pp_id

	--select * from #LastSubmissonDate

	UPDATE pp
	SET LastSubmissonDate = lsd.LastSubmissonDate
	FROM #period AS pp
	JOIN #LastSubmissonDate AS lsd ON lsd.pp_id = pp.pp_id;	
 
 	INSERT INTO #ClaimHeld
	(
	    pp_id,
		chr_id,
	    ClaimHeldReason,
	    ClaimHeldComment,
		CHPriority
	)
	SELECT 
		pp.pp_id,
		chr.chr_id,
		chr.chr_description AS ClaimHeldReason, 
		cpc.cpc_chcomment AS ClaimHeldComment,
		ROW_NUMBER() OVER(PARTITION BY pp.pp_id ORDER BY cpc.cpc_priority, LTRIM(chr.chr_description)) AS CHPriority
	FROM #period AS pp 
	JOIN HCHB.PDGM.CLIENT_PERIOD_CLAIMSAUDIT AS cpc ON cpc.cpc_ppid = pp.pp_id
	JOIN HCHB.dbo.CLAIM_HELD_REASONS AS chr ON chr.chr_id = cpc.cpc_chrid
	--AND (EXISTS (SELECT 1 FROM #ReasonHeld rh WHERE rh.chr_id = chr.chr_id OR (rh.chr_id = -99999995 AND chr.chr_id IS NULL)))

--select * from #ClaimHeld

	INSERT INTO #cltbill (pp_id, Branchid, cltid, primid, PatientName, MRNumber, SOP, EOP, SOE, ps_desc, pp_msp,cefs_ps,cefs_psid,  
						  epi_id, epi_RecertFlag, InsuredID, Revenue, pp_periodEnded, pp_drCode, LastSubmissonDate, 
						  BillingStatus,ceoh_TransType,cefs_ManualHold,PaperWorkAcceptedFlag,PrimaryPhysician,[PECOS status])				
	SELECT
		p.pp_id,
		ce.epi_branchcode AS BranchID,
		ce.epi_paid AS CltID,
		p.pp_cefsId AS PrimID,
		ISNULL(ce.epi_LastName,'') + ', ' + ISNULL(ce.epi_FirstName, '') + 
											CASE
												WHEN ISNULL(ce.epi_mi,'') = '' THEN ''
												ELSE ' ' + ce.epi_mi
											END AS PatientName,
		ce.epi_mrnum AS MRNumber,			
		p.pp_startDate,
		p.pp_enddate,
		ce.epi_StartofEpisode AS SOEDate,
		p.ps_desc,
		p.pp_msp,
		p.cefs_ps,
		p.cefs_psid, 
		ce.epi_id AS Epiid,
		ce.epi_RecertFlag,
		p.InsuredID AS InsuredID, 
		p.Revenue,
		p.pp_periodEnded,
		p.pp_drCode,
		p.LastSubmissonDate,
		p.BillingStatus,
		p.ceoh_TransType,
		p.cefs_ManualHold,
		ce.epi_paperworkreceived,
		p.PrimaryPhysician,
		p.[PECOS status]
	 FROM #period AS p
	JOIN HCHB.dbo.CLIENT_EPISODES AS ce ON ce.epi_id = p.cefs_epiid
	--JOIN @BillingStatus AS bs ON bs.BillingStatus = p.BillingStatus
	LEFT JOIN HCHB.dbo.AR_GROUPS_PAYOR_SOURCES AS argps ON p.cefs_psid = argps.argps_psid AND argps.argps_active = 'Y'
	WHERE ce.epi_NonAdmitDate IS NULL                    --Exlcude Non-Admits
	AND ce.epi_status NOT IN ('NON-ADMIT') 
	AND (
		ce.epi_status <> 'PENDING'
		OR (ce.epi_status = 'PENDING' AND ce.epi_startofepisode <= getdate())
	)  --Exclude Pending Episodes
	--AND EXISTS(SELECT * FROM @Patients WHERE ID IN ('-99999999', ce.epi_paid))
	--AND (@ARGroup = 0 OR (@ARGroup = 1 AND argps.argps_argid IS NOT NULL) OR argps.argps_argid = @ARGroup)
	
	--select * from #cltbill

	UPDATE cb SET ClaimHeldReasons = STUFF((SELECT CHAR(10) + ch2.ClaimHeldReason
								  FROM #ClaimHeld AS ch2
								  WHERE ch2.pp_id = cb.pp_id 
								  --AND (ch2.CHPriority <= @HeldReasonstoDisplay)	
								 ORDER BY ch2.CHPriority				  
						   FOR XML PATH('')),1,1,''),
		ClaimHeldComments = STUFF((SELECT CHAR(10) + REPLACE(ch2.ClaimHeldComment,CHAR(13),'')
								   FROM #ClaimHeld AS ch2
								   WHERE ch2.pp_id = cb.pp_id 
								   --AND (ch2.CHPriority <= @HeldReasonstoDisplay)
								   ORDER BY ch2.CHPriority
							FOR XML PATH('')),1,1,''),
		chr_id = ch.chr_id,
		CHPriority = ch.CHPriority
		FROM #cltbill AS cb JOIN #ClaimHeld AS ch ON ch.pp_id = cb.pp_id

	INSERT INTO #PeriodWithMSPPayor(pp_id,cefs_id)
	SELECT DISTINCT cb.pp_id,cefsPrimary.cefs_id FROM #cltbill cb 
	JOIN HCHB.dbo.CLIENT_EPISODE_FS cefsSecondary ON cb.primid = cefsSecondary.cefs_id 
	JOIN HCHB.dbo.CLIENT_EPISODE_FS AS cefsPrimary ON cefsSecondary.cefs_epiid = cefsPrimary.cefs_epiid AND cefsPrimary.cefs_ps = 'P' AND cefsPrimary.cefs_active = 'Y'
	WHERE cb.pp_msp =  1

	/*DETERMINE FIRST BILLABLE VISIT HAS BEEN VERIFIED OR NOT*/
  	INSERT INTO #SCHEDSubset
    (
        PeriodId,
		ShiftDate
    )
	SELECT cb.pp_id, s.ShiftDate 
       FROM HCHB.dbo.SCHED s 
            JOIN #cltbill cb
                ON s.PrimID = cb.primid
        WHERE s.Confirmed = 'V'
              AND s.WkrID > 0
              AND s.ExcludeFromBilling = 0 AND cb.pp_msp = 0

	INSERT INTO #SCHEDSubset
    (
        PeriodId,
		ShiftDate
    )
	SELECT msp.pp_id, s.ShiftDate 
       FROM HCHB.dbo.SCHED s 
	JOIN #PeriodWithMSPPayor AS msp ON s.PrimID = msp.cefs_id 
        WHERE s.Confirmed = 'V'
              AND s.WkrID > 0
              AND s.ExcludeFromBilling = 0
			  
  INSERT INTO #PeriodsWIthVerifiedFirstVisit
    (
        periodid
    )
	SELECT  cb.pp_id
        FROM #cltbill cb
            JOIN #SCHEDSubset s
                ON s.PeriodId = cb.pp_id
        WHERE 
                  s.ShiftDate = cb.SOP
                  OR
                  ( s.ShiftDate BETWEEN cb.SOP AND cb.eop 
				  AND 
                      (
                          ISNULL(cb.epi_recertflag, '') IN ( 'F', 'R' )
                          OR cb.SOP > cb.SOE
                      ) 
                  )
              
        GROUP BY cb.pp_id
			
	UPDATE cb
	SET cb.FirstVisitBillable = 1
	FROM 
	#cltbill AS cb 
	JOIN #PeriodsWIthVerifiedFirstVisit AS pwivfv ON cb.pp_id=pwivfv.periodid

	/* Temp table to house the SOC workflow stages for each period */
	/* Only pulls back certain SOC workflow stages */
	/* These stages are - Review Evaluation Documentation, Obtain Additional Authorization, Review/Edit/Approve 485 Order */
	/* Review 485 for Appropriate Coding, Review/Edit/Approve Held 485 Order for Additional Clinical Review, Review Proposed 485 Edits */
	/* Edit/Lock OASIS */
	/* If Edit/Lock OASIS is closed then show 'SOC Workflow Complete' */
	INSERT INTO #SOCworkflow (rownum, EpisodeID, pp_id, EpisodePayorId, WorkflowStage,cees_enddate)
	SELECT
		ROW_NUMBER() OVER (PARTITION BY cee.cee_epiid, cb.pp_id ORDER BY s.st_stageDesc, cees.cees_enddate DESC) AS rownum,
		cee.cee_epiid AS EpisodeID,
		cb.pp_id,
		ISNULL(cees.cees_cefsid, cb.primid) AS EpisodePayorId, 
		s.st_stageDesc AS Workflowstage,
		cees.cees_enddate
	FROM 
	#cltbill AS cb
	JOIN HCHB.dbo.CLIENT_EPISODE_EVENTS AS cee ON cee.cee_epiid = cb.epi_id
	JOIN HCHB.dbo.CLIENT_EPISODE_EVENT_STAGES AS cees ON cees.cees_ceeid = cee.cee_id
	JOIN HCHB.dbo.STAGES AS s ON s.st_id = cees.cees_stid
	WHERE
	cees.cees_active = 'Y'
	AND cee.cee_evid = 20 --SOC/RECERT
	AND ((cees.cees_stid IN (9, 220, 92, 529, 260, 532) AND cees.cees_enddate IS NULL) OR  (cees.cees_stid = 30))
	AND (cees.cees_cefsid IS NULL OR cees.cees_cefsid = cb.primid)
	
	/*Temp table to house EOP Workflowand when Workflow is closed show CLAIM RELEASED*/
	INSERT INTO #EOPWorkflow(rownum, pp_id, stageid,cees_startdate,cees_enddate)
	SELECT 
	 ROW_NUMBER() OVER (PARTITION BY cb.pp_id ORDER BY ISNULL(cees.cees_enddate, '9999-12-31') desc) AS rownum,
	cb.pp_id,
	s.st_id,
	cees.cees_startdate,
	cees.cees_enddate	
	FROM #cltbill AS cb
	JOIN HCHB.dbo.CLIENT_EPISODE_EVENTS AS cee ON cee.cee_epiid = cb.epi_id
	JOIN HCHB.dbo.CLIENT_EPISODE_EVENT_STAGES AS cees ON cees.cees_ceeid = cee.cee_id
	JOIN HCHB.dbo.CLIENT_EPISODE_EVENT_STAGE_PARAMETERS ceesp ON cees.cees_id = ceesp.ceesp_ceesid
	JOIN HCHB.dbo.STAGES AS s ON s.st_id = cees.cees_stid
	WHERE 
	cees.cees_active = 'Y'
	AND cee.cee_evid = 70 ----EOP Workflow
	AND cees.cees_stid IN (104,105,106)
	AND (cees.cees_cefsid IS NULL OR cees.cees_cefsid = cb.primid) 
	AND ceesp.ceesp_tableid = cb.pp_id
	AND ceesp.ceesp_type = 602  --Parameter type 602 means pdgm period
	
	ORDER BY cb.pp_id,s.st_id DESC	


	UPDATE cb
	SET cb.EOPWorkflowStage = CASE WHEN eopwf.cees_enddate IS NULL THEN st.st_StageDesc ELSE 'CLAIM RELEASED'END , cb.EOPWorkflowStartDate = eopwf.cees_startdate
	FROM #cltbill cb JOIN #EOPWorkflow AS eopwf ON eopwf.pp_id = cb.pp_id 
	JOIN HCHB.dbo.STAGES AS st ON eopwf.stageid = st.st_id
	WHERE eopwf.rownum = 1

	
	/*Updating F2F allow Billing Flag*/
	UPDATE cb SET cb.[F2FAllowBilling] = ISNULL(f2f.f2frs_allowBilling,'N')
	FROM #cltbill AS cb
	JOIN #FaceToFace_Details AS f2f ON cb.epi_id = f2f.epiid
	 WHERE f2f.faceTofaceDayBeforeSetup IS NOT NULL
	   AND f2f.faceTofaceDayAfterSetup IS NOT NULL
	   AND ISNULL(f2f.ceftf_required, 1) = 1 --the episode requires F2F

	/* Return results. */
	TRUNCATE TABLE VNSNY_BI.dbo.FACT_PDGM_UNBILLED

	INSERT INTO VNSNY_BI.dbo.FACT_PDGM_UNBILLED
	(
		[pp_id], 
		[epi_id], 
		[PatientName], 
		[MRNumber], 
		[InsuredID], 
		[Branch], 
		ps_id, 
		[PayorSource], 
		[SOPDate], 
		[EOPDate], 
		[SOEDate], 
		[AnticipatedRevenue], 
		[BillingStatus], 
		[LastSubmissonDate], 
		[PeriodEnded], 
		[DischargeStatusCode], 
		[PayorOrder], 
		[SOCWorkflowStatus], 
		[ClaimHeldReasons], 
		[ClaimHeldComments],  
		[chr_id],
		[CHPriority],
		[cefs_ManualHold], 
		[ceoh_TransType], 
		[F2FAllowBilling], 
		[PaperWorkAcceptedFlag], 
		[PrimaryPhysician], 
		[PECOS Verified], 
		[FirstVisitBillable], 
		[EOPWorkflowStage], 
		[EOPWorkflowStartDate]
	)
	SELECT DISTINCT
		cb.pp_id,
		cb.epi_id,
		cb.PatientName AS PatientName,
		cb.MRNumber AS MRNumber,
		cb.InsuredID AS InsuredID,		
		cb.Branchid AS Branch,
		cb.cefs_psid as ps_id,
		cb.ps_desc AS PayorSource,
		cb.SOP AS SOPDate,
		cb.EOP AS EOPDate,
		cb.SOE AS SOEDate,
		cb.Revenue AS AnticipatedRevenue,		
		cb.BillingStatus AS BillingStatus,
		cb.LastSubmissonDate AS LastSubmissonDate,
		CASE WHEN cb.pp_periodEnded = 1 THEN 'Y' ELSE 'N' END AS PeriodEnded,
		cb.pp_drCode AS DischargeStatusCode,		
		cb.cefs_ps AS PayorOrder,
		CASE WHEN wf.WorkflowStage = 'EDIT/LOCK OASIS' AND wf.cees_enddate IS NOT NULL THEN 'SOC WORKFLOW COMPLETE' 
			 ELSE wf.WorkflowStage 
		END AS SOCWorkflowStatus,
		cb.ClaimHeldReasons AS ClaimHeldReasons,
		cb.ClaimHeldComments AS ClaimHeldComments
		,[chr_id]
		,[CHPriority]
		,CASE WHEN cb.cefs_ManualHold = 1 THEN 'Y' ELSE 'N' END AS cefs_ManualHold
		,cb.ceoh_TransType
		,cb.F2FAllowBilling
		,cb.PaperWorkAcceptedFlag
		,cb.PrimaryPhysician
		,cb.[PECOS status] AS [PECOS Verified]
		,CASE WHEN cb.FirstVisitBillable = 1 THEN 'Verified' ELSE 'Unverified' END AS FirstVisitBillable
		,cb.EOPWorkflowStage
		,cb.EOPWorkflowStartDate
--into  FACT_PDGM_UNBILLED 
	FROM #cltbill AS cb
	LEFT JOIN #SOCworkflow AS wf ON wf.EpisodeID = cb.epi_id
		 AND wf.pp_id = cb.pp_id
		 AND wf.EpisodePayorId = cb.PrimId	 
		 AND wf.rownum = 1
WHERE cb.EOP < getdate()
--WHERE cb.epi_id = 390269
END
GO
/****** Object:  StoredProcedure [dbo].[rpt_PPSUnbilledReport]    Script Date: 10/13/2021 10:15:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






CREATE PROCEDURE [dbo].[rpt_PPSUnbilledReport]


AS

	
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	
	
	--  Testing
	--DECLARE 
	--	@BranchID as varchar(5000) = null,
	--	@CltID as int = 0,
	--	@EOEDate as datetime = null,
	--	@ARGroup as int = 0,
	--	@rptreqrpt_id INT = 13307804,
	--	@parmMSPOnly INT = 0,
	--	@lpid INT = 0
	IF OBJECT_ID('tempdb..#auth') IS NOT NULL DROP TABLE #auth
	IF OBJECT_ID('tempdb..#cltbill') IS NOT NULL DROP TABLE #cltbill
	IF OBJECT_ID('tempdb..#workflow') IS NOT NULL DROP TABLE #workflow

	CREATE TABLE #auth  (auth_id int , auth_cefsid int, auth_finalclaim bit, auth_startDate datetime, auth_active char(1), cefs_ps char(1), cefs_psid int, cefs_ptid int, ec_epiid int, cefs_MedicareNo varchar(25), freq INT, 
						TotPaid money, RAPBilledDate datetime);
	CREATE TABLE #cltbill    (cltbillid int, Branchid char(3), cltid int, primid int, freq int, finalclaim bit, PeriodBeg datetime, transferstatus int, epi_id int, ssid varchar(25), TotPaid money, RAPBilledDate datetime);
	CREATE TABLE #workflow (EpisodeID int, EventID int, EOEEventID int, WorkFlowDate Datetime, WorkflowStage varchar(1000), ClaimHeld varchar(50), EpisodePayorId INT);
	--CREATE TABLE #branches    (branchid char(5) PRIMARY KEY CLUSTERED);
	

	--INSERT #branches
	--SELECT branch_code FROM branches with(nolock)
	--WHERE ((PATINDEX('%' + ',' + branch_code + ',' + '%', @BranchID) > 0) OR (@branchid IS NULL) OR (@branchid =',(ALL),'))

		INSERT #auth
		SELECT 			
			a.auth_id,
			a.auth_cefsid,
			a.auth_finalclaim,
			a.auth_startDate,
			a.auth_active,
			c.cefs_ps,
			c.cefs_psid,
			c.cefs_ptid,
			c.cefs_epiid,
			c.cefs_MedicareNo,
			ps.ps_freq,
			0,
			NULL
		FROM hchb.dbo.AUTHORIZATIONS as a 
			JOIN  hchb.dbo.CLIENT_EPISODE_FS c
			ON a.auth_cefsid = c.cefs_id
			AND cefs_active = 'Y'
			AND c.cefs_ps IN ('P','A','S')
		JOIN  hchb.dbo.PAYOR_SOURCES ps on c.cefs_psid = ps.ps_id and ps.ps_freq IN (5,8)
		WHERE a.auth_finalclaim = 0
			AND a.auth_active = 'Y'
			--AND EXISTS (SELECT 1 FROM dbo.CLIENT_EPISODES as ce 
			--	                   INNER JOIN #branches as b ON ce.epi_branchcode = b.branchid
				          --WHERE c.cefs_epiid = ce.epi_id)
						  ;

		UPDATE A
		SET TotPaid = isnull(( SELECT TOP 1 cast(SUM(h.p_balance) as money) AS TotPaid
						FROM hchb.Billing.EPISODIC_LINE_ITEMS_REVENUE lir
							JOIN hchb.accounting.PAYMENTS h
							ON h.p_invoiceid = lir.elir_invoiceid
						WHERE lir.elir_authorizationid = a.auth_id 
						GROUP BY lir.elir_authorizationid
						),0)
		FROM #auth AS A;
			
		UPDATE A
		SET RAPBilledDate = (SELECT MIN(SubmitDate) AS RAPBilledDate
						FROM hchb.dbo.pps_resubmitEBhistory p
						WHERE p.Rap = 1
						and p.Cltbillid = a.auth_id 
						GROUP BY p.CltBillID)
		FROM #auth AS A;
	
	INSERT #cltbill
	SELECT DISTINCT
		CltBillID = a.auth_id,
		BranchID = ce.epi_branchcode,
		CltID = ce.epi_paid,
		PrimID = a.auth_cefsid,
		Freq = a.freq, 
		FinalClaim = a.auth_finalclaim,
		PeriodBeg = a.auth_startDate,
		TransferStatus = (CASE
			WHEN a.auth_active = 'Y' THEN 1
			ELSE 3
		END),
		Epiid = ce.epi_id,
		SSID = a.cefs_MedicareNo, 
		a.TotPaid,
		a.RAPBilledDate 
	FROM #auth a
		JOIN  hchb.dbo.CLIENT_EPISODES ce
			ON ce.epi_id = a.ec_epiid
		LEFT JOIN hchb.dbo.AR_GROUPS_PAYOR_SOURCES argps
		ON a.cefs_psid = argps.argps_psid   
		AND argps.argps_active = 'Y'
	WHERE
		ce.epi_NonAdmitDate IS NULL                    --Exlcude Non-Admits
		AND ce.epi_status NOT IN ('NON-ADMIT')
		AND (
			ce.epi_status <> 'PENDING'
			OR (ce.epi_status = 'PENDING' AND ce.epi_startofepisode <= getdate())
		)  --Exclude Pending Episodes
		--AND (ce.epi_EndofEpisode <= @EOEDate OR ce.epi_DischargeDate <= @EOEDate OR ce.epi_EndofEpisode = NULL)
		AND (
			(auth_startdate = epi_startofepisode AND cefs_ps <> 'A')
			OR (auth_startdate >= epi_startofepisode AND auth_startdate <= epi_endofepisode AND cefs_ps = 'A')
		)
		--AND (@Cltid = 0 OR @CltID = -99999999 OR ce.epi_paid = @Cltid)
		--AND (@ARGroup = 0 OR (@ARGroup = 1 AND argps.argps_argid IS NOT NULL) OR argps.argps_argid = @ARGroup)
	;

	--Temp table to house the EOE workflow stage for each episode
	--Only pulls back certain workflow stages for EOE Events
	--These stages are - Bill EOE, Bill EOE-Held, Perform Claims Audit, Review/Clear Held Claim
	INSERT #workflow
	SELECT
		cee.cee_epiid AS EpisodeID,
		cee.cee_id AS EventID,
		cee.cee_evid As EOEEventID,
		cees.cees_startdate AS WorkFlowDate,
		s.st_stageDesc as Workflowstage,
		ClaimHeld = convert(varchar(50), null),
		EpisodePayorId = ISNULL(cees.cees_cefsid, cb.primid)
	FROM (
		SELECT DISTINCT epi_id FROM #cltbill
	) ce
		JOIN #cltbill cb
		ON cb.epi_id = ce.epi_id
		JOIN hchb.dbo.CLIENT_EPISODE_EVENTS cee
		ON cee.cee_epiid = ce.epi_id
		JOIN hchb.dbo.CLIENT_EPISODE_EVENT_STAGES cees
		ON cees.cees_ceeid = cee.cee_id
		JOIN hchb.dbo.STAGES s
		ON s.st_id = cees.cees_stid
   WHERE
		cee_evid IN (74,105)
		AND cees_enddate IS NULL
		AND cees_endby IS NULL
		AND s.st_id IN (117,230,48,525,535)
		AND (cees.cees_cefsid IS NULL OR cees.cees_cefsid = cb.primid)
	GROUP BY cee.cee_epiid, cee.cee_id,cee.cee_evid, cee.cee_evdate, s.st_stageDesc, cees.cees_startdate, ISNULL(cees.cees_cefsid, cb.primid)

	UPDATE #workflow
	SET ClaimHeld = chr.chr_description
	FROM #workflow t
		JOIN hchb.dbo.CLIENT_EPISODE_CLAIMSAUDIT a
		ON t.episodeid = a.cec_epiid
		JOIN (
			SELECT cec_epiid, max(cec_lastupdate) AS cec_lastupdate
			FROM #workflow t1
				JOIN hchb.dbo.CLIENT_EPISODE_CLAIMSAUDIT
				ON t1.episodeid = cec_epiid
			GROUP BY cec_epiid
		) b
		ON a.cec_epiid = b.cec_epiid
		AND a.cec_lastupdate = b.cec_lastupdate
		JOIN hchb.dbo.CLAIM_HELD_REASONS chr
		ON chr.chr_id = a.cec_chrid



	--If list of EPS payors are included, retrieve their revenue.
	/*
	if exists(select 1 from #cltbill c where c.freq = 8)
	begin
	
		INSERT INTO @CEFSIDS_TBL 
		SELECT distinct
			primid
		from #cltbill c
		where c.freq = 8
			
		
		exec usp_EPS_Revenue @CEFSIDS_TBL	
	
	end
	*/

	--truncate table PPS_UNBILLED 
	IF OBJECT_ID('tempdb..#PPS_UNBILLED') IS NOT NULL DROP TABLE #PPS_UNBILLED
	create table #PPS_UNBILLED(
	[epi_id] [int] NULL,
	[auth_id] [int] NULL,
	[HICNum] [varchar](25) NULL,
	[BranchID] [varchar](3) NULL,
	[fallname] [varchar](60) NULL,
	[SOE] [date] NULL,
	[EOE] [date] NULL,
	[DISCHARGE_DATE] [date] NULL,
	[RAP_BILLED_DTE] [date] NULL,
	[CALC_PAYMENT] [float] NULL,
	[Cash_Collected] [float] NULL,
	[UNBILELD_CASH] [float] NULL,
	[REASON_UNBILLED] [varchar](50) NULL,
	[WORK_FLOW_STAGE] [varchar](70) NULL,
	[WORK_FLOW_DATE] [date] NULL,
	[EPISODE_TIMING] [varchar](2) NULL,
	[CEFS_ID] [int] NULL,
	[EPI_PSID] [int] NULL,
	[PS_ID] [int] NULL,
	[ADMISSION_COORDINATOR] [varchar](50) NULL,
	[PRIMARY_PHYSICIAN] [varchar](50) NULL,
	[MSP] [varchar](2) NULL,
	[HOMECARE_COORDINATER] [varchar](50) NULL)

	/* Return results. */
	INSERT  #PPS_UNBILLED
	SELECT DISTINCT
	　　cb.epi_id,
	    AUTHI_ID = cb.cltbillid,
		HICNum = cb.ssid,
		cb.Branchid AS BranchID,
		FullName = isnull(ce.epi_LastName,'') + ', ' + isnull(ce.epi_FirstName, '') + CASE
			WHEN isnull(ce.epi_mi,'') = '' THEN ''
			ELSE ' ' + ce.epi_mi
		END
	,	ce.epi_StartofEpisode as SOEDate,
		ce.epi_EndofEpisode as EOEDate,
		ce.epi_DischargeDate AS DischargeDate,
		cb.RAPBilledDate AS RAPBilledDate,
		CalcPayment = case when cb.freq = 8 then ISNULL(er.estimatedpay,0.00) --EPS payors
						   else ISNULL(ssh.ssh_revenue,0.00) 
					  end,
		ISNULL(cb.TotPaid,0.00) as CashCollected,
		UnbilledCash = 
			case when ps.ps_freq = 8
					then 
						CASE
							WHEN ER.estimatedpay IS NULL AND cb.Totpaid IS NULL THEN 0.00
							WHEN ER.estimatedpay IS NULL THEN 0.00 - cb.Totpaid
							WHEN cb.Totpaid IS NULL THEN ER.estimatedpay
							ELSE (ER.estimatedpay - cb.Totpaid)
						END 
				 else 
						CASE
							WHEN ssh.ssh_revenue IS NULL AND cb.Totpaid IS NULL THEN 0.00
							WHEN ssh.ssh_revenue IS NULL THEN 0.00 - cb.Totpaid
							WHEN cb.Totpaid IS NULL THEN ssh.ssh_revenue
							ELSE (ssh.ssh_revenue - cb.Totpaid)
						END 
			end,
		CASE
			WHEN (Temp2.Workflowstage = 'PERFORM CLAIMS AUDIT') THEN 'PERFORM CLAIMS AUDIT NOT COMPLETED'
			WHEN (Temp2.Workflowstage = 'BILL EOE') THEN 'BILL EOE - NOT SUBMITTED'
			WHEN (Temp2.Workflowstage = 'REVIEW/CLEAR HELD CLAIM') THEN UPPER(Temp2.ClaimHeld)
			WHEN (Temp2.Workflowstage = 'PREVIEW FINAL BILL') THEN UPPER(Temp2.ClaimHeld)
			WHEN (Temp2.Workflowstage = 'BILL HELD EOE') THEN UPPER(Temp2.ClaimHeld)
			ELSE 'UNKNOWN'
		END as ReasonUnbilled,
		Temp2.Workflowstage as WorkFlowStage,
		Temp2.WorkFlowDate AS WorkFlowDate,
		et.et_code as EpisodeTiming,
		CEFS_ID = cefs_ptid,
		epi_slid,
		PS_ID = cefs_psid
	,	AdmissionCoordinator = isnull(ac_lastname, '') + isnull(', ' + ac_firstname, '')
	,	PrimaryPhysician = isnull(ph_lastname + ', ', '') + isnull (ph_firstname, '') + CASE
			WHEN isnull(ph_mi, '') = '' THEN ''
			ELSE ' ' + ph_mi
		END
	,	msp = coalesce(fn.msp, 'N')
	,   HomecareCoordinator = hcc.hcc_name
	FROM #cltbill cb
		LEFT JOIN hchb.dbo.SUMMARY_STATISTIC_HEADERS ssh
		ON ssh.ssh_authid = cb.cltbillid
		JOIN hchb.dbo.CLIENT_EPISODES ce
			JOIN hchb.dbo.EPISODE_TIMING et
			ON et.et_id = ce.epi_timing
			JOIN hchb.dbo.ADMISSION_COORDINATORS
			ON isnull(ce.epi_acid, 0) = ac_id
			LEFT JOIN hchb.dbo.PHYSICIANS ph1
			ON ce.epi_phid1 = ph1.ph_id
		ON cb.epi_id = ce.epi_id
		JOIN #workflow Temp2
		ON Temp2.EpisodeID = cb.epi_id
		AND Temp2.EpisodePayorId = cb.PrimId
		JOIN hchb.dbo.AUTHORIZATIONS auth1
		ON cb.cltbillid = auth1.auth_id
		JOIN hchb.dbo.CLIENT_EPISODE_FS cesf
		ON cefs_id = auth_cefsid 
		join hchb.dbo.PAYOR_SOURCES ps on ps.ps_id = cefs_psid 
		left join vnsny_bi.dbo.EPS_REVENUE er on ER.cefs_id = cb.primid   
		left join hchb.dbo.homecare_coordinators hcc on hcc.hcc_id = ce.epi_hccid		    
		outer apply ( select msp = case when cefs_ps = 'S' and cefs.cefs_active = 'Y' and pt.pt_desc = 'MEDICARE' then 'Y'
									else 'N'
								end	
					  from hchb.dbo.client_episode_fs cefs
					  join hchb.dbo.payor_types pt on pt.pt_id = cefs_ptid
				      where 
						cefs_epiid = ce.epi_id 
						and cefs.cefs_id = cesf.cefs_id
					) fn
	WHERE  	
	--((isnull(fn.msp,'N') = case when  @parmMSPOnly = 1 then 'Y' end) or @parmMSPOnly = 0)
    -- and 
    (((ssh.ssh_revenue <> 0) or (cb.freq = 8 and er.estimatedpay <> 0)) 
			and (EXISTS(SELECT 1 FROM hchb.dbo.authorizations auth2 JOIN hchb.dbo.programs ON auth2.auth_programid = pg_id WHERE pg_useforpps IN (0,1) AND auth2.auth_id = auth1.auth_id)))	
	

	truncate table billing_line_items_revenue

	
	insert into  billing_line_items_revenue 	 
	select * 
	from hchb.Billing.[LINE_ITEMS_REVENUE]
	          left join #PPS_UNBILLED on lir_authorizationid = auth_id and lir_episodeid = epi_id 
	where lir_lineitemid not in (select lir_lineitemid 
	                             from hchb.Billing.[LINE_ITEMS_REVENUE] 
								 where lir_lineitemtypeid = 4 and lir_revenueadjustedamount = 0)  


END
GO
/****** Object:  StoredProcedure [dbo].[usp_EPS_Revenue]    Script Date: 10/13/2021 10:15:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_EPS_Revenue]
(@CEFSIDS_TBL CEFSIDS READONLY , @RecalculateForZone3 bit = 0, @StandardPayOnly bit = 0)
AS
BEGIN

SET NOCOUNT ON 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

DECLARE @CEFSIDS_ZONE2_TBL CEFSIDS
DECLARE @CEFSIDS_ZONE2_NOTLUPA CEFSIDS

CREATE TABLE #CEFSIDS_ER (cefs_id int, zone int)

BEGIN TRY
	
	IF (SELECT COUNT(*) FROM @CEFSIDS_TBL) > 0
		INSERT INTO #CEFSIDS_ER (cefs_id, zone)
		SELECT t.cefs_id, v.vez_zone 
		FROM @CEFSIDS_TBL t
		JOIN HCHB.DBO.V_EPS_ZONES v ON v.vez_cefsid = t.cefs_id
			
		
	SELECT * INTO #EPS_REVENUE_GROUPER FROM HCHB.dbo.TVPDef_EPS_REVENUE_GROUPER()
	SELECT * INTO #EPS_WAGEINFO FROM  HCHB.dbo.TVPDef_EPS_WAGEINFO()
	SELECT * INTO #EPS_VISITS FROM  HCHB.dbo.TVPDef_EPS_Visits()
	SELECT * INTO #EPS_STANDARDPAY FROM  HCHB.dbo.TVPDEF_EPS_REVENUE_STANDARD_PAYMENT()
	SELECT * INTO #EPS_LUPAPAY FROM  HCHB.[dbo].[TVPDEF_EPS_REVENUE_LUPA_PAYMENT]()
	SELECT * INTO #EPS_OUTLIERPAY FROM  HCHB.dbo.TVPDEF_EPS_REVENUE_OUTLIER_PAYMENT()
	SELECT * INTO #EPS_REVENUE_OUTPUT FROM  HCHB.[dbo].[fn_CreateEPSRevenueTable]()
	
	
	--step 1. Get Wage Info
		exec  HCHB.DBO.usp_EPS_Revenue_WageInfo @CEFSIDS_TBL		

	-- step 2. Calculate visits -- need some kind ov #EPS_VISITS temp table that rev calc (step 44  below) can use ?

	exec HCHB.DBO.usp_EPS_Revenue_Visits @CEFSIDS_TBL


	-- STEP 3. Find oasis
	exec HCHB.DBO.usp_EPS_Revenue_FindOasis @CEFSIDS_TBL

	-- Step 4. Calculate resource utilization group (the grouper)

	exec HCHB.DBO.usp_EPS_Revenue_Group

	-- step 5. Calculate revenue	
	insert into vnsny_bi.dbo.EPS_REVENUE_GROUPER select * from #EPS_REVENUE_GROUPER
		--step a. Update rate code/. This HIPPS/RateCode is used in outlier calculation.
		UPDATE g SET INTERIM_HIPPS = hhi.hc_hipps, HIPPS = hh.hc_hipps
		FROM #EPS_REVENUE_GROUPER g
		JOIN #EPS_WAGEINFO w ON g.cefs_id = w.cefs_id
		LEFT JOIN HCHB.DBO.HIPPS_HHRG_CONVERSION hhi ON hhi.hc_rpid =  w.rp_id AND hhi.hc_hhrg = g.interim_HHRG AND hhi.hc_active = 'Y'
		LEFT JOIN HCHB.DBO.HIPPS_HHRG_CONVERSION hh ON hh.hc_rpid =  w.rp_id AND hh.hc_hhrg = g.HHRG AND hh.hc_active = 'Y'
			
	 
		--step c. Revenue Calculation
		
		--only use zone 2 cefs_ids for revenue calculation
		INSERT INTO @CEFSIDS_ZONE2_TBL(cefs_id)
		SELECT cefs_id
		FROM #CEFSIDS_ER t
		WHERE (zone = 2 OR @RecalculateForZone3 = 1)

		--Standard Payment
		exec HCHB.DBO.usp_EPS_Revenue_GetStandardPayment @CEFSIDS_ZONE2_TBL
		
		IF(@StandardPayOnly = 0) --if the caller needs only standard pay then don't run this lupa and outlier calculations
		BEGIN

			--LUPA
			exec HCHB.DBO.usp_EPS_Revenue_GetLUPAPayment @CEFSIDS_ZONE2_TBL

			--OUTLIER 
			-- run this outlier on non-lupa episodes
			INSERT INTO @CEFSIDS_ZONE2_NOTLUPA (cefs_id)
			SELECT t.cefs_id
			FROM @CEFSIDS_ZONE2_TBL t
			LEFT JOIN #EPS_LUPAPAY lp ON lp.cefs_id = t.cefs_id
			WHERE  lp.cefs_id IS NULL

			exec HCHB.DBO.usp_EPS_Revenue_GetOutlierPayment @CEFSIDS_ZONE2_NOTLUPA
		
		END
		
   insert into vnsny_bi.dbo.EPS_REVENUE_GROUPER select * from #EPS_REVENUE_GROUPER

	-- step 5. Output data

	INSERT INTO #EPS_REVENUE_OUTPUT (CEFS_ID, 
					CEO_ID, 
					INTERIM_HHRG, 
					HHRG,
					INTERIM_HIPPS,
					HIPPS,               
					ESTIMATEDPAY,   --This payment is based on INTERIM HHRG is used in INTERIM/Estimated payment
					PAYMENT,            
					CALCTYPE)
	                
	--This is for episodes not ended
	SELECT CEFS_ID = t.cefs_id, 
		CEO_ID = g.ceo_id, 
		INTERIM_HHRG = g.INTERIM_HHRG, 
		HHRG = g.HHRG,
		INTERIM_HIPPS = g.INTERIM_HIPPS, 
		HIPPS = g.HIPPS, 	
		ESTIMATEDPAY = sp.estimated_pay, --This is payment calculated on OASIS HHRG. Only standard pay. No LUPA and Outlier calculations are included in this calculation.
		PAYMENT = CASE WHEN ISNULL(lp.calc_type,'S') = 'L' THEN lp.lupa_pay
							WHEN ISNULL(op.calc_type,'S') = 'O' THEN op.outlier_pay
							ELSE sp.standard_pay END,
		CALCTYPE = CASE WHEN ISNULL(lp.calc_type,'S') = 'L' THEN 'L'
							WHEN ISNULL(op.calc_type,'S') = 'O' THEN 'O'
							ELSE sp.calc_type END
		
	FROM #CEFSIDS_ER t
	INNER JOIN #EPS_REVENUE_GROUPER g ON g.cefs_id = t.cefs_id
	INNER JOIN #EPS_STANDARDPAY sp ON sp.cefs_id = t.cefs_id
	LEFT JOIN #EPS_LUPAPAY lp ON lp.cefs_id = t.cefs_id
	LEFT JOIN #EPS_OUTLIERPAY op ON op.cefs_id = t.cefs_id
	WHERE (t.zone = 2 OR @RecalculateForZone3 = 1) --episodes not ended only unless use revenue calculator for zone 3 is on

	UNION ALL

	--This is for episodes ended. No need to run revenue calculator on ended episoded. Just get payment from PPSFS. 
	SELECT CEFS_ID = t.cefs_id, 
		CEO_ID = g.ceo_id, INTERIM_HHRG = g.INTERIM_HHRG, 
		HHRG = g.HHRG,
		INTERIM_HIPPS = g.INTERIM_HIPPS, 
		HIPPS = g.HIPPS, 	 
		ESTIMATEDPAY = p.EstimatedPayment,
		PAYMENT = p.Payment,
		CALCTYPE = (CASE WHEN a.auth_LUPA = 1 THEN 'L'
			WHEN a.auth_Outlier = 1 THEN 'O'
			ELSE 'S' END)	
		
	FROM #CEFSIDS_ER t
	INNER JOIN #EPS_REVENUE_GROUPER g ON g.cefs_id = t.cefs_id
	INNER JOIN HCHB.DBO.AUTHORIZATIONS a ON a.auth_cefsid = t.cefs_id AND a.auth_active = 'Y'
	INNER JOIN HCHB.DBO.PPSFS p ON p.CltBillID = a.auth_id									
	WHERE (t.zone = 3 AND @RecalculateForZone3 = 0) --ended episodes when the use revenue calculator for zone 3 is off
	AND p.ppstype <> 'E' and p.transferstatus <> 3

	--If calling object has this table then dump the output into it
	IF OBJECT_ID('tempdb..#EPS_REVENUE') IS NOT NULL
		BEGIN
		
			INSERT INTO VNSNY_BI.DBO.EPS_REVENUE
				(CEFS_ID, 
					CEO_ID, 
					INTERIM_HHRG,
					HHRG,
					INTERIM_HIPPS,
					HIPPS,                
					ESTIMATEDPAY,  
					PAYMENT,             
					CALCTYPE)	
			SELECT CEFS_ID, 
					CEO_ID, 
					INTERIM_HHRG, 
					HHRG,
					INTERIM_HIPPS,
					HIPPS,                
					ESTIMATEDPAY,
					PAYMENT,                
					CALCTYPE
			FROM #EPS_REVENUE_OUTPUT
		END
	ELSE              
		SELECT * FROM #EPS_REVENUE_OUTPUT
	
	END TRY

	BEGIN CATCH	
		IF @@TRANCOUNT > 0 ROLLBACK TRAN	

		exec HCHB.DBO.usp_RethrowError
	END CATCH

end
GO
