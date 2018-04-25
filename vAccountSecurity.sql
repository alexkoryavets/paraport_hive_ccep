ALTER view `ccep`.`vAccountSecurity` AS
WITH agg AS
(SELECT `account_tkn_id`
       ,AccountShortName
       ,`REPORT_DATE`
      ,`ReportingPeriod`
      ,`PeriodEndDate`
      ,`inception_date`
      ,`security_tkn_id`
      ,`SEC_TYPE_CODE`
      ,`SECURITY`
      ,`TICKER`
      ,NVL(`SEDOL`, '') AS SEDOL
      ,NVL(`ISIN`, '') AS ISIN
      ,NVL(`CUSIP`, '') AS CUSIP
      ,sum(`QUANTITY`) as `QUANTITY`
      ,sum(`TOTAL_COST`) as `TOTAL_COST`
      ,sum(`MARKET_VALUE`) as `MARKET_VALUE`
      ,sum(`UNREALIZED_GAIN_LOSS`) as `UNREALIZED_GAIN_LOSS`
      ,SUM(CASE WHEN h.SEC_TYPE_CODE like 'ca%' THEN 0 ELSE `MARKET_VALUE` END) AS Security_Value
      ,SUM(CASE WHEN h.SEC_TYPE_CODE LIKE 'ca%' THEN `MARKET_VALUE` ELSE 0 END) AS Cash_Value
  FROM `ccep`.`m_apx_quarterly_holdings` h
  GROUP BY `account_tkn_id`
       ,AccountShortName
      ,`ReportingPeriod`
      ,`PeriodEndDate`
      ,`inception_date`
      ,`REPORT_DATE`
      ,`security_tkn_id`
      ,`SEC_TYPE_CODE`
      ,`SECURITY`
      ,`TICKER`
      ,NVL(`SEDOL`, '')
      ,NVL(`ISIN`, '')
      ,NVL(`CUSIP`, '')
  
)
,account_total AS
(
    SELECT SUM(`MARKET_VALUE`) AS AccountMarketValue
        , SUM(Security_Value) AS AccountMarketValueNetCash
        , SUM(Cash_Value) AS AccountCashValue
        , SUM(CASE WHEN Security_Value = 0 THEN 0 ELSE 1 END) AS NumberOfHoldings
        , `account_tkn_id`
        , `REPORT_DATE`
    FROM agg
        GROUP BY account_tkn_id, `REPORT_DATE`
)
SELECT agg.`account_tkn_id`
      ,AccountShortName
      ,agg.`ReportingPeriod`
      ,agg.`PeriodEndDate`
      ,agg.`REPORT_DATE`
      ,agg.`inception_date`
      ,security_tkn_id
      ,`SEC_TYPE_CODE`
      ,`SECURITY`
      ,agg.SEDOL
      ,agg.CUSIP
      ,agg.ISIN
      ,agg.TICKER
      ,`QUANTITY`
      ,`TOTAL_COST`
      ,`MARKET_VALUE`
      ,`UNREALIZED_GAIN_LOSS`
      ,`Security_Value`
      ,CASE WHEN AccountMarketValue IS NULL OR AccountMarketValue = 0 THEN 0 ELSE `MARKET_VALUE` / AccountMarketValue END AS MarketWeight
      ,CASE WHEN AccountMarketValueNetCash IS NULL OR AccountMarketValueNetCash = 0 THEN 0 ELSE Security_Value / AccountMarketValueNetCash END AS MarketWeightNetCash
      ,NumberOfHoldings
      ,AccountMarketValue
      ,AccountMarketValueNetCash
FROM agg
    INNER JOIN account_total t
    ON agg.account_tkn_id = t.account_tkn_id
    AND agg.`REPORT_DATE` = t.`REPORT_DATE`