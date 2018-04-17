CREATE VIEW `ccep`.`vQuarterlyAccountSecurity` AS
SELECT `account_tkn_id`
      ,AccountShortName
      ,`ReportingPeriod`
      ,security_tkn_id
      ,`SEC_TYPE_CODE`
      ,`SECURITY`
      ,SEDOL
      ,CUSIP
      ,ISIN
      ,TICKER
      ,`QUANTITY`
      ,`TOTAL_COST`
      ,`MARKET_VALUE`
      ,`UNREALIZED_GAIN_LOSS`
      ,MarketWeight
      ,MarketWeightNetCash
      ,NumberOfHoldings
      ,Security_Value
      ,AccountMarketValue
      ,AccountMarketValueNetCash
FROM `ccep`.`m_vAccountSecurity` h
WHERE `PeriodEndDate` = `REPORT_DATE`