
INSERT INTO [VNSNY_BI].[dbo].[YTD_MONTH]
     (
       [MONTH_ID]
      ,[MONTH_DATE]
      ,[YTD_MONTH_ID]
      ,[PY_YTD_MONTH_ID]
      ,[MONTH_END_DATE]
	  )
  SELECT [MONTH_ID]
      ,[MONTH_DATE]
      ,[YTD_MONTH_ID]
      ,[PY_YTD_MONTH_ID]
      ,[MONTH_END_DATE]
  FROM [dbo].['tmp_ytd_month']
  EXCEPT
  SELECT  [MONTH_ID]
      ,[MONTH_DATE]
      ,[YTD_MONTH_ID]
      ,[PY_YTD_MONTH_ID]
      ,[MONTH_END_DATE]
  FROM [VNSNY_BI].[dbo].[YTD_MONTH];









SELECT * FROM  [dbo].[YTD_MONTH]
order by month_id;