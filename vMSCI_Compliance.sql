create VIEW `ccep`.`vMSCI_Compliance` AS
with temp as 
(
SELECT  `Company_ID`
      ,`Entity_name`
      ,`country`
      ,`ticker`
      ,`cusip`
      ,`sedol`
      ,`isin`
      ,`IssueColumnName`
      ,TRANSLATE( `IssueValue`, ',', '') as IssueValue
	  ,'DM' AS `Issue_Source`
	  ,reportingperiod
FROM `ccep`.`msci_compliance_4592_Parametric_Compliance_DM` 
UNION ALL 
SELECT `Company_ID`
      ,`Entity_name`
      ,`country`
      ,`ticker`
      ,`cusip`
      ,`sedol`
      ,`isin`
      ,`IssueColumnName`
      ,TRANSLATE( `IssueValue`, ',', '') as IssueValue
	  ,'Diversity' AS `Issue_Source`
	  ,reportingperiod
  FROM `ccep`.`msci_compliance_4592_Parametric_Compliance_DM_Diversity`
UNION ALL
SELECT `Company_ID`
      ,`Entity_name`
      ,`country`
      ,`ticker`
      ,`cusip`
      ,`sedol`
      ,`isin`
      ,`IssueColumnName`
      ,TRANSLATE( `IssueValue`, ',', '') as IssueValue
	  ,'Islamic' AS `Issue_Source`
	  ,reportingperiod
  FROM `ccep`.`msci_compliance_4592_Parametric_CustomIslamic2`
)
, MSCI AS
( SELECT `Company_ID`
      ,`Entity_name`
      ,`country`
	  ,`ticker`
      ,`cusip`
      ,`sedol`
      ,`isin`
      ,`IssueColumnName`
	  ,`IssueValue`
	  ,CASE WHEN `IssueValue` like '%`0-9,.`%' AND `IssueValue` NOT LIKE '%`A-z`%' AND `IssueValue` NOT LIKE '%`!-,:-~`%' THEN CAST(IssueValue AS float) ELSE NULL END AS IssueValueFloat
	  ,`Issue_Source`
	  ,reportingperiod
FROM temp
)
SELECT DISTINCT s.security_tkn_id
	  ,`Company_ID`
      ,`Entity_name`
      ,`IssueColumnName`
	  ,`IssueValue` 
	  ,IssueValueFloat
	  ,`Issue_Source`
  	  ,20171201 AS DateKey
	  ,msci.ReportingPeriod
	  ,periodenddate as QuarterEndDate
	  FROM MSCI
	  INNER JOIN ccep.`union_shadow_SECURITY` s ON 1 = 1
	  inner join ccep.quarterdate as q on msci.reportingperiod = q.reportingperiod
	  where s.ticker = msci.`ticker` and (s.cusip = MSCI.cusip OR s.sedol = MSCI.sedol OR s.isin = MSCI.isin)
