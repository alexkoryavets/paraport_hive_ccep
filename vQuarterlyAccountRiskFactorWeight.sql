CREATE VIEW ccep.vQuarterlyAccountRiskFactorWeight
AS
WITH QtrlyAccBenchmarkRFVersion AS
(SELECT qab.account_tkn_id
    ,qab.reporting_benchmark_tkn_id
    ,`reporting_benchmark_short_name`
    ,`reporting_benchmark_presentation_name`
    ,p.`ReportingPeriod`
    ,p.`rf_provider_tkn_id`
    ,v.`rf_version_id`
    ,rf.`rf_sector_id`
    ,rf.`rf_tkn_id`
    ,rf.`display_name`
    ,pt.`rf_provider_type_tkn_id`
    ,t.`name` AS TypeName
    ,t.`rf_type_tkn_id`
FROM `ccep`.`M_vQuarterlyAccountBenchmark` qab
INNER JOIN
(SELECT * FROM `ccep`.`union_quarterly_RF_PROVIDER`) p
ON qab.`rf_provider_tkn_id` = p.`rf_provider_tkn_id` AND  qab.ReportingPeriod = p.ReportingPeriod
INNER JOIN
(SELECT * FROM `ccep`.`union_quarterly_RF_VERSION`) v
ON p.`rf_provider_tkn_id` = v.`rf_provider_tkn_id` AND p.`ReportingPeriod` = v.`ReportingPeriod`
INNER JOIN `ccep`.`RF_PROVIDER_TYPE` pt
ON p.`rf_provider_tkn_id` = pt.`rf_provider_tkn_id`
INNER JOIN `ccep`.`RF_TYPE` t
ON pt.`rf_type_tkn_id` =  t.`rf_type_tkn_id`
INNER JOIN (SELECT * FROM `ccep`.`union_quarterly_RISK_FACTOR`) rf
ON pt.`rf_provider_type_tkn_id` = rf.`rf_provider_type_tkn_id`
)
, AccountSecProfileWeightedAvg AS
(
    SELECT p.`account_tkn_id`
        ,p.`reporting_benchmark_tkn_id`
        ,p.`rf_provider_tkn_id`
        ,p.`TypeName`
        ,p.rf_sector_id
        ,p.display_name AS MetricItem
        ,`rf_type_tkn_id`
        ,p.`rf_tkn_id`
        ,qas.`security_tkn_id`
        ,qas.`MarketWeight`
        ,`value`
        ,qas.`MarketWeight` * `value` AS Weight
        ,p.`ReportingPeriod`
        ,'Portfolio' AS DataSource
    FROM QtrlyAccBenchmarkRFVersion p
    INNER JOIN `ccep`.`M_vQuarterlyAccountSecurity` qas
        ON p.`ReportingPeriod` = qas.`ReportingPeriod` AND p.`account_tkn_id` = qas.`account_tkn_id`
    INNER JOIN (SELECT '2017 Q4' AS ReportingPeriod, * FROM `ccep`.`union_quarterly_RF_VALUE`) rv
        ON p.`rf_version_id` = rv.`rf_version_id` AND p.`rf_tkn_id` = rv.rf_tkn_id AND rv.`security_tkn_id` = qas.`security_tkn_id` AND p.ReportingPeriod = rv.ReportingPeriod
    WHERE (`TypeName` = 'industry' OR `TypeName` = 'country')
    --AND p.account_tkn_id = 113294
)
,BenchmarkSecProfileWeightedAvg AS
(
    SELECT p.`account_tkn_id`
          ,p.`reporting_benchmark_tkn_id`
          ,p.`rf_provider_tkn_id`
          ,p.`TypeName`
          --,s.sector_name
          ,p.rf_sector_id
          ,p.display_name AS MetricItem
          ,`rf_type_tkn_id`
          ,p.`rf_tkn_id`
          ,bs.`security_tkn_id`
          ,bs.`weight` AS `MarketWeight`
          ,`value`
          ,bs.`weight` * `value` AS Weight
          ,p.`ReportingPeriod`
          ,NVL( p.`reporting_benchmark_presentation_name`, p.`reporting_benchmark_short_name`) As DataSource
    FROM QtrlyAccBenchmarkRFVersion p
    INNER JOIN `ccep`.`vQuarterlyBenchmarkSecurity` bs
        ON p.`reporting_benchmark_tkn_id` = bs.benchmark_tkn_id AND p.`ReportingPeriod` = bs.`ReportingPeriod`
    INNER JOIN (SELECT '2017 Q4' AS ReportingPeriod, * FROM `ccep`.`union_quarterly_RF_VALUE`) rv
        ON p.`rf_version_id` = rv.`rf_version_id` AND p.`rf_tkn_id` = rv.rf_tkn_id AND rv.`security_tkn_id` = bs.`security_tkn_id` AND p.ReportingPeriod = rv.ReportingPeriod
    WHERE (`TypeName` = 'industry' OR `TypeName` = 'country') AND p.account_tkn_id in (SELECT account_tkn_id FROM AccountSecProfileWeightedAvg)
    --and p.account_tkn_id = 113294
)
, AccountProfileWeight AS
(
    SELECT `account_tkn_id`
          ,`ReportingPeriod`
          ,`rf_provider_tkn_id`
          ,`TypeName`
          ,MetricItem
          ,rf_sector_id
          ,DataSource
          ,SUM(`Weight`) AS ValueFloat
    FROM
    (SELECT * FROM AccountSecProfileWeightedAvg
    UNION ALL SELECT * FROM BenchmarkSecProfileWeightedAvg
    ) a
    --WHERE account_tkn_id = 113294
    GROUP BY `account_tkn_id`, `ReportingPeriod`, `rf_provider_tkn_id`, `TypeName`, MetricItem, rf_sector_id, DataSource
)
,SectorWeight as (
    SELECT  w.`account_tkn_id`
    ,ac_sec.`ReportingPeriod`
    ,'Sector Weights' AS `TypeName`
    ,ac_sec.sector_name AS MetricItem
    ,ac_sec.`rf_provider_tkn_id`
    ,DataSource
    ,SUM(NVL(ValueFloat, 0)) AS ValueFloat
    FROM (
        SELECT `ReportingPeriod`,`rf_sector_id` , s.sector_name, rf_provider_tkn_id
        FROM `ccep`.`union_quarterly_RF_SECTOR` s
    ) ac_sec
    LEFT JOIN AccountProfileWeight w
    ON  w.`rf_sector_id` = ac_sec.`rf_sector_id` AND ac_sec.`ReportingPeriod` = w.`ReportingPeriod` and ac_sec.`rf_provider_tkn_id` = w.`rf_provider_tkn_id`
    where `account_tkn_id` IS NOT NULL AND `TypeName` = 'industry'
    GROUP BY `account_tkn_id`, ac_sec.`ReportingPeriod`, ac_sec.sector_name, ac_sec.`rf_provider_tkn_id`, DataSource
)
SELECT a.account_tkn_id
    , a.ReportingPeriod
    , TypeName
    , CASE WHEN TypeName = 'Country' THEN REPLACE(MetricItem, ' Mkt', '') ELSE MetricItem END AS MetricItem
    , Bias
    , NVL(AccountWeight, 0) AS AccountWeight
    , NVL(BenchmarkWeight, 0) AS BenchmarkWeight
    , NVL(AccountSourceName,'Portfolio') AS AccountSourceName
    , BenchmarkName
--INTO ccep.M_vQuarterlyAccountSectorWeights
FROM
(SELECT NVL(a.account_tkn_id, b.account_tkn_id) AS account_tkn_id
    , NVL(a.rf_provider_tkn_id, b.rf_provider_tkn_id) AS rf_provider_tkn_id
    , NVL(a.ReportingPeriod, b.ReportingPeriod) AS ReportingPeriod
    , NVL(a.TypeName, b.TypeName) AS TypeName
    , NVL(a.MetricItem, b.MetricItem) AS MetricItem
    , NVL(a.ValueFloat, 0) - NVL(b.ValueFloat, 0) AS Bias
    , a.ValueFloat AS AccountWeight
    , b.ValueFloat AS BenchmarkWeight
    , a.DataSource AS AccountSourceName
    , b.DataSource AS BenchmarkName
    --INTO ccep.M_vQuarterlyAccountSectorWeights
    FROM
    (SELECT `account_tkn_id`, `rf_provider_tkn_id`, `ReportingPeriod`, DataSource, `TypeName`, MetricItem, ValueFloat
        FROM SectorWeight
        WHERE DataSource = 'Portfolio') a
    FULL JOIN
    (SELECT `account_tkn_id`, `rf_provider_tkn_id`, `ReportingPeriod`, DataSource, `TypeName`, MetricItem, ValueFloat
        FROM SectorWeight
        WHERE DataSource <> 'Portfolio') b
    ON a.`account_tkn_id` = b.`account_tkn_id` AND a.ReportingPeriod = b.ReportingPeriod AND a.TypeName = b.TypeName AND a.MetricItem = b.MetricItem AND a.`rf_provider_tkn_id` = b.`rf_provider_tkn_id`   
UNION ALL
    -- industry and Country
    SELECT NVL(a.account_tkn_id, b.account_tkn_id) AS account_tkn_id
        , NVL(a.rf_provider_tkn_id, b.rf_provider_tkn_id) AS rf_provider_tkn_id
        , NVL(a.ReportingPeriod, b.ReportingPeriod) AS ReportingPeriod
        , NVL(a.TypeName, b.TypeName) AS TypeName
        , NVL(a.MetricItem, b.MetricItem) AS MetricItem
        , NVL(a.ValueFloat, 0) - NVL(b.ValueFloat, 0) AS Bias
        , a.ValueFloat AS AccountWeight
        , b.ValueFloat AS BenchmarkWeight
        , a.DataSource AS AccountSourceName
        , b.DataSource AS BenchmarkName
    FROM
    (SELECT `account_tkn_id`, `rf_provider_tkn_id`, `ReportingPeriod`, DataSource, `TypeName`, MetricItem, ValueFloat
        FROM AccountProfileWeight WHERE DataSource = 'Portfolio') a
    FULL JOIN
    (SELECT `account_tkn_id`, `rf_provider_tkn_id`, `ReportingPeriod`, DataSource, `TypeName`, MetricItem, ValueFloat
        FROM AccountProfileWeight WHERE DataSource <> 'Portfolio') b
    ON a.`account_tkn_id` = b.`account_tkn_id`
        AND a.ReportingPeriod = b.ReportingPeriod
        AND a.TypeName  = b.TypeName
        AND a.MetricItem = b.MetricItem
        AND a.`rf_provider_tkn_id` = b.`rf_provider_tkn_id`
) a