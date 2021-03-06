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
/****** Object:  UserDefinedFunction [dbo].[f_Get_Physician_Id]    Script Date: 10/13/2021 10:17:09 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[f_Get_Physician_Id]
(
	@ph_SSN varchar(50),
	@ph_EIN varchar(50),
	@ph_NPI varchar(50),
	@npitt_NPIRequired bit,
	@npitt_NPIOptional bit,
	@npitt_ProviderIDRequired bit,
	@npitt_ProviderIDOptional bit,
	@phid int,
	@ProfileID int,
	@Optional bit = 0
)
RETURNS @retPhysicianInformation TABLE 
(
	phid int, 
	PPlace1Type varchar(2),
	PPlace1 varchar(max),
	PPlace2Type varchar(2),
	PPlace2 varchar(max)
)
AS 
BEGIN
	
	DECLARE @PSSN varchar(40),@PEIN varchar(40),@PNPI varchar(40)
	DECLARE @PNPIExists bit, @PSSNExists bit, @PEINExists bit 
	DECLARE @a bit, @b bit, @c bit, @d bit
	DECLARE @PPlace1Type varchar(2), @PPlace1 varchar(25), @PPlace2Type varchar(2), @PPlace2 varchar(25), @SSNEINOutputted bit
	SET @PNPIExists = 0
	SET @PSSNExists = 0 
	SET @PEINExists = 0
	SET @SSNEINOutputted = 0


	-- Preprocessing / Setup
	select @PSSN = @ph_SSN, @PEIN = @ph_EIN, @PNPI = @ph_NPI 
	if(@PNPI is not null and @PNPI <> '')
	BEGIN
		SET @PNPIExists = 1
	END
	if(@PEIN is not null and @PEIN <> '')
	BEGIN
		SET @PEINExists = 1
	END
	if(@PSSN is not null and @PSSN <> '')
	BEGIN
		SET @PSSNExists = 1
	END

	-- Get Provider Output Settings from Profile
	SELECT @a = @npitt_NPIRequired,@b = @npitt_NPIOptional,@c = @npitt_ProviderIDRequired,@d = @npitt_ProviderIDOptional 

	-- Get output settings
	-- Find out what goes in the first place

	if(@a = 1) 
	begin
		if @PNPIExists = 1
		begin
			SET @PPlace1Type = 'XX'
			SET @PPlace1 = @PNPI
		end
		else
		begin
			if(@Optional = 1)
				return			
		end
	end
	else if (@b = 1) and (@PNPIExists = 1)
	begin
		SET @PPlace1Type = 'XX'
		SET @PPlace1 = @PNPI
	end
	else if (@c = 1)
	begin
		If(@PEINExists = 1)
		begin
			SET @PPlace1Type = '24'
			SET @PPlace1 = @PEIN
			SET @SSNEINOutputted = 1
		end
		else if (@PSSNExists = 1)
		begin
			SET @PPlace1Type = '34'
			SET @PPlace1 = @PSSN
			SET @SSNEINOutputted = 1
		end
		else
		begin
			if(@Optional = 1)
				return		
		end
	end
	else if (@d = 1 and (@PEINExists = 1 or @PSSNExists = 1))
	begin
		If(@PEINExists = 1)
		begin
			SET @PPlace1Type = '24'
			SET @PPlace1 = @PEIN
			SET @SSNEINOutputted = 1
		end
		else if (@PSSNExists = 1)
		begin
			SET @PPlace1Type = '34'
			SET @PPlace1 = @PSSN
			SET @SSNEINOutputted = 1
		end
	end

	-- Find out for place 2 (NPI will never go here)
	if(@c = 1)
	begin
		if(@PEINExists = 1 and @SSNEINOutputted = 0)
		BEGIN
			SET @PPlace2Type = 'EI'
			SET @PPlace2 = @PEIN
			SET @SSNEINOutputted = 1
		END
		else if (@PSSNExists = 1 and @SSNEINOutputted = 0)
		begin
			SET @PPlace2Type = 'SY'
			SET @PPlace2 = @PSSN
			SET @SSNEINOutputted = 1
		end

		IF @SSNEINOutputted = 0
		BEGIN
			if(@Optional = 1)
				return		
		END
	end
	else if (@d = 1)
	begin
		if(@PEINExists = 1 and @SSNEINOutputted = 0)
		BEGIN
			SET @PPlace2Type = 'EI'
			SET @PPlace2 = @PEIN
			SET @SSNEINOutputted = 1
		END
		else if (@PSSNExists = 1 and @SSNEINOutputted = 0)
		begin
			SET @PPlace2Type = 'SY'
			SET @PPlace2 = @PSSN
			SET @SSNEINOutputted = 1
		end
	end
	
	INSERT INTO @retPhysicianInformation
	SELECT @phid,IsNull(@PPlace1Type,''), IsNull(@PPlace1,''), IsNull(@PPlace2Type,''), IsNull(@PPlace2,'')

	RETURN 
END




--select * from VNSNY_BI.dbo.UNBILLED_REASON
GO
