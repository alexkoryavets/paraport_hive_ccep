CREATE VIEW `ccep`.`vQuarterlyAccountBenchmarkFundamental`
AS
WITH absec AS
(
SELECT vas.`ReportingPeriod`
      ,vas.`account_tkn_id`
      ,-1 AS Benchmark_tkn_id
      ,vas.`AccountShortName` AS Short_name
      ,vas.`security_tkn_id`
      ,vas.`MARKET_VALUE` AS `Security_Value`
FROM `ccep`.`M_vQuarterlyAccountSecurity` vas
WHERE `MARKET_VALUE` > 0 AND `security_tkn_id` > 0
UNION ALL
SELECT `ReportingPeriod`
      ,-1 AS account_tkn_id
      ,`benchmark_tkn_id`
      ,`BenchmarkShortName`
      ,qbs.`security_tkn_id`
      ,weight * 10000 AS `Security_Value`
FROM `ccep`.`M_vQuarterlyBenchmarkSecurity` qbs
)
SELECT absec.`ReportingPeriod`
    ,`account_tkn_id`
    ,`benchmark_tkn_id`
    ,absec.`security_tkn_id`
    ,`Security_Value`
    ,`pe_ratio`
    ,`pe_ratio_value`
    ,`px_to_book_ratio`
    ,`pb_ratio_value`
    ,`cur_mkt_cap`
    ,`cur_mkt_cap_value`
    ,`FundamentalCurrency`+'USD' AS fx_ticker
    ,`quote_factor` AS fx_quote_factor
    ,`px_last` AS fx_rate
    ,CASE WHEN `px_last` IS NULL THEN `cur_mkt_cap_value` ELSE `cur_mkt_cap_value` * `px_last` / `quote_factor` END AS converted_market_cap
FROM absec
LEFT JOIN ccep.M_vQuarterlySecurityFundamentalMapping m
    ON absec.`security_tkn_id` = m.`security_tkn_id` AND absec.ReportingPeriod = m.ReportingPeriod
LEFT JOIN QuarterDate qd
  ON m.ReportingPeriod = qd.ReportingPeriod
LEFT JOIN `ccep`.`T_FX_RATES` fx
  ON fx.ymd = qd.PeriodEndDateKey AND fx.ticker=`FundamentalCurrency`+'USD'