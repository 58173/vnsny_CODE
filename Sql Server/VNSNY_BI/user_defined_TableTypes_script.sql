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
/****** Object:  UserDefinedTableType [dbo].[CEFSIDS]    Script Date: 10/13/2021 10:18:21 AM ******/
CREATE TYPE [dbo].[CEFSIDS] AS TABLE(
	[cefs_id] [int] NOT NULL,
	PRIMARY KEY CLUSTERED 
(
	[cefs_id] ASC
)WITH (IGNORE_DUP_KEY = OFF)
)
GO
