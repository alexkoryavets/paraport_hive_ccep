CREATE VIEW `ccep`.`vRI_AccountTopic`
AS
WITH temp AS (SELECT ar.`account_tkn_id`,
                ar.`restriction_group_tkn_id`,
                `management_benchmark_tkn_id`,
                `reporting_benchmark_tkn_id`,
                `management_benchmark_short_name`,
                `reporting_benchmark_short_name`,
                g.`restriction_group_desc` AS `ScreenName`,
                ar.ReportingPeriod
         FROM   `ccep`.`vQuarterlyAccountRestrictionGroup` ar
                INNER JOIN `ccep`.`vQuarterlyAccountBenchmark` ab
                        ON ar.account_tkn_id = ab.account_tkn_id
                INNER JOIN `ccep`.`union_shadow_restriction_group` g
                        ON ar.`restriction_group_tkn_id` = g.restriction_group_tkn_id
						   )
,TopicsFromScreenPkg 
     AS (
	SELECT NVL(a.account_tkn_id, b.account_tkn_id) AS account_tkn_id
	,NVL(a.BenchmarkShortName, b.BenchmarkShortName) AS BenchmarkShortName
	,NVL(a.ReportingPeriod, b.ReportingPeriod) AS ReportingPeriod
	,NVL(a.`Topic`, b.`Topic`) AS Topic
	,CONCAT(NVL(a.TopicFrom, ''), NVL(b.TopicFrom, '')) AS TopicFrom
	FROM
	(SELECT ar.`account_tkn_id`
		,ar.ReportingPeriod
                ,ar.`reporting_benchmark_short_name` AS BenchmarkShortName
                ,ri.`Topic`
                ,'screen' AS TopicFrom 
         FROM   temp ar
	 INNER JOIN `ccep`.`sri_reporting_screens_wirehouses` ri
		ON ar.ScreenName = ri.screen ) a
         FULL JOIN
         (SELECT ar.`account_tkn_id`, 
		ar.ReportingPeriod,
                ar.`reporting_benchmark_short_name` AS BenchmarkShortName, 
                sp.Topic                 AS Topic, 
                'package'                AS TopicFrom 
                --ar.`restriction_group_tkn_id`, 
         FROM   temp ar 
                INNER JOIN `ccep`.`sri_reporting_screen_packages` sp 
                        ON ar.`screenname` = sp.`screen`) b 
		ON a.`account_tkn_id` = b.`account_tkn_id` AND a.ReportingPeriod = b.ReportingPeriod and a.`Topic` = b.`Topic`
						)
,TopicsFromBenchmark
	AS ( 
	select qab.`account_tkn_id`
	,`capweighted_benchmark_shortname` AS BenchmarkShortName
	,i.`topics` AS `Topic`
	,'Benchmark' as TopicFrom
	,qab.ReportingPeriod
 --, `Metric_name_or_Restriction_group` as `Metrics`
 from `ccep`.`vQuarterlyAccountBenchmark` qab
	inner join 
	(SELECT * FROM `ccep`.`sri_reporting_indexes`) i
	on qab.`management_benchmark_short_name` = i.`RI_Index_shortname`
	AND  qab.`ReportingPeriod` = i.`ReportingPeriod`
)
-- clean up RI information from the Excel spreadsheet
,RI_Definition AS
(
SELECT screen AS `ScreenName`
      ,`Group`
      ,`Topic`
      ,`Icons`
      ,`Method`
      ,`Data_Source`
      ,CASE WHEN `Data_Source` = 'Union Database' THEN translate(`restriction_group`, 'Use constituent list under restriction group: ', '') ELSE NULL END AS RestrictionGroupDesc
	  ,CASE WHEN `Data_Source` = 'Union Database' THEN NULL ELSE `restriction_group` END AS Metric
      ,`Condition`
      ,`Aggregate`
      ,`Mutliply_factor`
  FROM `ccep`.`sri_reporting_screens_wirehouses`
)
SELECT DISTINCT NVL(ar.account_tkn_id, bt.account_tkn_id) AS account_tkn_id
       ,NVL(bo.BenchmarkShortName, ar.BenchmarkShortName) AS BenchmarkShortName
	   , ar.BenchmarkShortName AS `reporting_benchmark_short_name`
       --restriction_group_tkn_id, 
       ,NVL(ar.ReportingPeriod, bt.ReportingPeriod) AS ReportingPeriod
	   ,NVL(ar.`Topic`, bt.`Topic`) AS Topic
	   ,CONCAT(NVL(ar.TopicFrom, ''), NVL(bt.TopicFrom, '')) AS TopicFrom
       ,`Group`
       ,`Data_Source`
	   ,`RestrictionGroupDesc`
	   ,`Metric`
       ,`Condition`
       ,`Aggregate`
       ,NVL(mutliply_factor, 1)       AS MultiplyBy 
FROM   TopicsFromScreenPkg ar 
		FULL OUTER JOIN TopicsFromBenchmark bt
		ON ar.account_tkn_id = bt.account_tkn_id
		AND ar.ReportingPeriod = bt.ReportingPeriod
		AND ar.Topic = bt.Topic
		LEFT JOIN 
		(SELECT DISTINCT account_tkn_id
				, ReportingPeriod
				, BenchmarkShortName
				FROM TopicsFromBenchmark) bo -- benchmark override
		ON (bo.account_tkn_id = coalesce(ar.account_tkn_id, bt.account_tkn_id)
		AND bo.ReportingPeriod = coalesce(ar.ReportingPeriod, bt.ReportingPeriod))
       INNER JOIN RI_Definition ri 
               ON NVL(ar.`Topic`, bt.`Topic`) = ri.Topic