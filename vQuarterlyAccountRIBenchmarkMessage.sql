CREATE VIEW `ccep`.`vQuarterlyAccountRIBenchmarkMessage`
AS
SELECT DISTINCT NVL(qab.`account_tkn_id`, qarg.`account_tkn_id`) AS `AccountTknId`
 ,NVL(qab.ReportingPeriod, qarg.ReportingPeriod) AS `ReportingPeriodName`
 ,'Responsible Investing' AS `MetricGroup`
 ,'' AS `MetricTopic`
 ,'' AS `MetricName`
 ,'RI Benchmark Message' AS `MetricItem`
 , 1 AS `ItemDisplayOrder`
 ,NVL('Benchmark calculations based on ' + `capweighted_benchmark_longname`, '') AS `ValueText`
 ,0 AS `Value`
 ,'' AS `Unit`
FROM `ccep`.`vQuarterlyAccountRestrictionGroup` qarg
FULL JOIN
(SELECT  account_tkn_id, `capweighted_benchmark_longname`, qab.ReportingPeriod
FROM `ccep`.`vQuarterlyAccountBenchmark` qab
INNER JOIN
 (SELECT `RI_Index_shortname`
 , `capweighted_benchmark_longname`
 , `ReportingPeriod`
 FROM `ccep`.`sri_reporting_Indexes`) i
 on qab.`management_benchmark_short_name` = i.`RI_Index_shortname`
 AND  qab.`ReportingPeriod` = i.`ReportingPeriod`
) qab
ON qarg.account_tkn_id = qab.account_tkn_id AND qarg.ReportingPeriod = qab.ReportingPeriod
