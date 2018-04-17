CREATE VIEW `ccep`.`vRI_QuarterlyAccountTopicScore`
AS
WITH AccountSecurity AS
(
	SELECT qas.`account_tkn_id`
		,qas.security_tkn_id
		,qas.`ReportingPeriod`
		,qas.`MarketWeight`
		,1.0 / qas.`NumberOfHoldings` AS HoldingWeight
		,'Your Portfolio' AS BenchmarkOrAccount
		,`BenchmarkShortName`
		,`Topic`
		,`TopicFrom`
		,`Group`
		,`Data_Source`
		,`RestrictionGroupDesc`
		,`Metric`
		,`Condition`
		,`Aggregate`
		,`MultiplyBy`
	FROM `ccep`.`vQuarterlyAccountSecurity` qas
	INNER JOIN `ccep`.`vRI_AccountTopic` ria
	ON ria.`account_tkn_id` = qas.`account_tkn_id` AND qas.`ReportingPeriod` = ria.`ReportingPeriod`
	-- check for security holdings
	where `MarketWeightNetCash` > 0
)
,Const AS
(
	SELECT qbs.`benchmark_tkn_id`
		,qbs.`ReportingPeriod`
		,count(*) AS NumberOfConst 
	FROM `ccep`.`vQuarterlyBenchmarkSecurity`  qbs
	INNER JOIN  
		(SELECT DISTINCT `ReportingPeriod`, `BenchmarkShortName` FROM `ccep`.`vRI_AccountTopic`) qas  --  this contains only the benchmark being used.
		ON qbs.`BenchmarkShortName` = qas.`BenchmarkShortName` AND qbs.`ReportingPeriod` = qas.`ReportingPeriod`
	GROUP BY  qbs.`benchmark_tkn_id`, qbs.`ReportingPeriod`
)
, BenchmarkSecurity AS
(
	SELECT  ria.`account_tkn_id`
			,qbs.security_tkn_id
			,qbs.`ReportingPeriod`
			,qbs.`weight` AS `MarketWeight`
			, 1.0 / Const.NumberOfConst AS HoldingWeight
			,'Benchmark' AS BenchmarkOrAccount
			,ria.`BenchmarkShortName`
			,`Topic`
			,`TopicFrom`
			,`Group`
			,`Data_Source`
			,`RestrictionGroupDesc`
			,`Metric`
			,`Condition`
			,`Aggregate`
			,`MultiplyBy`
	FROM `ccep`.`vQuarterlyBenchmarkSecurity` qbs
	INNER JOIN Const
		ON qbs.`ReportingPeriod` = Const.`ReportingPeriod`
		AND qbs.`benchmark_tkn_id` = Const.`benchmark_tkn_id`
	INNER JOIN `ccep`.`vRI_AccountTopic` ria
		ON ria.`BenchmarkShortName` = qbs.`BenchmarkShortName`
		AND ria.`ReportingPeriod` = qbs.`ReportingPeriod`
)
,qas AS
(
	SELECT qas.*
		,s.ticker
		,s.cusip
		,s.sedol
		,s.isin			
		,s.`security_name` 
	FROM AccountSecurity qas
		INNER JOIN ccep.`union_shadow_SECURITY` AS s
		ON qas.`security_tkn_id` = s.`security_tkn_id` 
	UNION ALL
	SELECT qbs.*
		,s.ticker
		,s.cusip
		,s.sedol
		,s.isin			
		,s.`security_name` 
	FROM BenchmarkSecurity qbs
		INNER JOIN ccep.`union_shadow_SECURITY` AS s
		ON qbs.`security_tkn_id` = s.`security_tkn_id`
)
,AccountSecurityTopicScoreFromUnion AS 
(
	SELECT qas.`account_tkn_id`
		, qas.security_tkn_id
		, qas.`security_name`
		, `BenchmarkOrAccount`
		, `MarketWeight`
		, CAST(`value` AS varchar(50)) AS `IssueValue`
		,CASE WHEN `Condition` = 'exists' THEN
				CASE WHEN `Aggregate` = 'Sum of market cap' AND sr.security_tkn_id IS NOT NULL THEN NVL(`MarketWeight`, 0)
					 WHEN `Aggregate` = '% count' AND sr.security_tkn_id IS NOT NULL THEN NVL(HoldingWeight, 0)
					ELSE 0
				END
			  WHEN `condition` = '>=0' AND `Aggregate` = 'weighted avg' AND CAST(TRANSLATE(`value`, ',', '') AS FLOAT) >= 0 THEN `MarketWeight` * CAST(TRANSLATE(`value`, ',', '') AS FLOAT)  --this line got changed
			  WHEN `Condition` = '>=0' AND `Aggregate` = 'Value' THEN -- currently, no topic has this but it might happen
				CASE WHEN sr.security_tkn_id IS NOT NULL THEN NVL(CAST(TRANSLATE(`value`, ',', '') AS FLOAT), 1) * NVL(`MarketWeight`, 0)
					ELSE 0
				END
			  ELSE 0 
			END AS score
      ,`BenchmarkShortName`
      ,qas.`ReportingPeriod`
      ,`Topic`
      ,`TopicFrom`
      ,`Group`
      ,`Data_Source`
      ,`RestrictionGroupDesc`
      ,`Metric`
      ,`Condition`
      ,`Aggregate`
      ,`MultiplyBy`
	FROM qas
	LEFT JOIN 
	(SELECT security_tkn_id, restriction_group_desc, sr.restriction_group_tkn_id, value, `ReportingPeriod`
		FROM `ccep`.`union_shadow_SECURITY_RESTRICTION_GROUP` sr
		INNER JOIN `ccep`.`union_shadow_RESTRICTION_GROUP` g  -- getting the description and use it to match what's in the restriction group from the spreadsheet
		ON sr.restriction_group_tkn_id = g.restriction_group_tkn_id) sr 
	ON qas.`ReportingPeriod` = sr.`ReportingPeriod`
	AND qas.security_tkn_id = sr.security_tkn_id
	AND sr.restriction_group_desc = qas.`RestrictionGroupDesc`
	WHERE `Data_Source` = 'Union Database'
)
,AccountSecurityTopicScoreFromMSCI
AS
(
	SELECT qas.`account_tkn_id`
		, qas.security_tkn_id
		, qas.`security_name`
		,`BenchmarkOrAccount`
		, `MarketWeight`
		,`IssueValue`
		, CASE 
			WHEN `Condition` = '>=0'  THEN
				CASE WHEN `Aggregate` = 'weighted avg' AND `IssueValueFloat` >= 0 THEN `MarketWeight` * `IssueValueFloat`
					 WHEN `Aggregate` = '% count' AND `IssueValueFloat` >= 0 THEN NVL(HoldingWeight, 0) -- currently, no topic has this
					 ELSE 0 END
			WHEN `Condition` = '>0'  THEN
				CASE WHEN `Aggregate` = 'weighted avg' AND `IssueValueFloat` > 0 THEN `MarketWeight` * `IssueValueFloat`  -- currently, no topic has this
					 WHEN `Aggregate` = '% count' AND `IssueValueFloat` > 0 THEN NVL(HoldingWeight, 0) 
					 ELSE 0 END
			ELSE 0
			END AS score
      ,`BenchmarkShortName`
      ,qas.`ReportingPeriod`
      ,`Topic`
      ,`TopicFrom`
      ,`Group`
      ,`Data_Source`
      ,`RestrictionGroupDesc`
      ,`Metric`
      ,`Condition`
      ,`Aggregate`
      ,`MultiplyBy`
	FROM qas
	LEFT JOIN `ccep`.`M_vMSCI_Compliance` MSCI
	ON qas.`Metric` = MSCI.`IssueColumnName`
	AND qas.security_tkn_id = MSCI.security_tkn_id
	AND qas.ReportingPeriod = MSCI.`ReportingPeriod`
	WHERE ( (`Data_Source` = 'MSCI Compliance DM' AND MSCI.`Issue_Source` = 'DM') OR
	(`Data_Source` = 'MSCI Compliance DM Diversity' AND MSCI.`Issue_Source` = 'Diversity') OR
	(`Data_Source` = 'MSCI Parametric_CustomIslamic2' AND MSCI.`Issue_Source` = 'Islamic'))
)
SELECT account_tkn_id
	,`ReportingPeriod`
	,`BenchmarkOrAccount`
	,`Group`
	,Topic
	,SUM(NVL(Score, 0)) * MAX(`MultiplyBy`) AS RI_SCore
FROM 
(
SELECT * FROM AccountSecurityTopicScoreFromUnion
UNION ALL 
SELECT * FROM AccountSecurityTopicScoreFromMSCI
) qas
GROUP BY `account_tkn_id`
	,`ReportingPeriod`
	,`BenchmarkOrAccount`
	,`Group`
	,Topic
