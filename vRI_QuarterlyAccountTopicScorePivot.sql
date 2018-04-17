create view `ccep`.`vRI_QuarterlyAccountTopicScorePivot` AS
select	ri.`account_tkn_id` as `AccountTknID`
		,ri.`ReportingPeriod` as `ReportingPeriodName`
		,`Responsible Investing` as MetricGroup
		,ri.`Group` as `MetricTopic` 
		,ri.`BenchmarkOrAccount` as MetricName 
		,ri.`Topic` as MetricItem
		,1 as ItemDisplayOrder
		,CAST(ri.`RI_SCore` AS VARCHAR(50)) AS ValueText
		,ri.`RI_SCore` as Value
		,SUBSTR(`Data_Presentation`, -(LEN(`Data_Presentation`) - (CHARINDEX(' Y',`Data_Presentation`)+1))) as Unit
FROM `ccep`.`vRI_QuarterlyAccountTopicScore` ri
	left join (select distinct `group`, topic , `Data_Presentation` from `ccep`.`sRI_reporting_Screens_Wirehouses`) ris
		on ri.`Group` = ris.`group` and ri.`Topic` = ris.topic
UNION ALL
select	`AccountTknId`
		,`ReportingPeriodName`
		,`MetricGroup`
		,`MetricTopic` 
		,`MetricName` 
		,`MetricItem` 
		,`ItemDisplayOrder`
		,`ValueText`
		,`Value`
		,`Unit`
FROM `ccep`.`vQuarterlyAccountRIBenchmarkMessage`