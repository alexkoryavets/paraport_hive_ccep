ALTER VIEW `ccep`.`apx_quarterly_holdings`
AS
WITH m_qtr_date AS
(
  SELECT
    ReportingPeriod,
    periodstartdate as first_date_of_quarter,
    periodenddate as last_date_of_quarter
  FROM
    `ccep`.`quarterdate`
),
HoldingS AS
(
SELECT `REPORT_DATE`
        ,`account_tkn_id`
        ,D.ReportingPeriod
        ,`inception_date`
        ,`short_name` as AccountShortName
      ,`PORTFOLIO_CODE`
      ,`SEC_TYPE_CODE`
      ,`SECURITY_SYMBOL`
      ,SEDOL
      ,CASE WHEN SEDOL IS NULL OR LENGTH(SEDOL) = 0 THEN NULL ELSE SUBSTRING(`SEDOL`, 0, LENGTH(SEDOL)-1) END AS SEDOL_SHORT  --strip off the trailing digit.
      ,`ISIN`
      ,`TICKER`
      ,`CUSIP`
      ,`SECURITY`
      ,`LOT`
      ,`QUANTITY`
      ,`TOTAL_COST`
      ,`TOTAL_COST_LOCAL`
      ,`MARKET_VALUE`
      ,`MARKET_VALUE_LOCAL`
      ,`PRICE_LOCAL`
      ,`LOCAL_ISO`
      ,`UNREALIZED_GAIN_LOSS`
      ,last_date_of_quarter AS PeriodEndDate
FROM `ccep`.`union_shadow_ADVISOR_CENTER_ACCOUNT` a
  inner join `ccep`.`apx_holdings` h
  on a.short_name = h.`PORTFOLIO_CODE`
  CROSS JOIN `m_Qtr_date` D
WHERE
  `REPORT_DATE` >= D.first_date_of_quarter AND `REPORT_DATE` <= D.last_date_of_quarter
),
Holdings_Security AS (
SELECT  `account_tkn_id`
        ,`REPORT_DATE`
        ,MAX(CASE
         WHEN NVL(s.`security_tkn_id`, 0) > NVL((CASE WHEN NVL(c.`security_tkn_id`, 0) > NVL(i.`security_tkn_id`, 0) THEN c.`security_tkn_id` ELSE i.`security_tkn_id` END), 0) THEN s.`security_tkn_id`
         ELSE (CASE WHEN NVL(c.`security_tkn_id`, 0) > NVL(i.`security_tkn_id`, 0) THEN c.`security_tkn_id` ELSE i.`security_tkn_id` END)
         END) AS security_tkn_id
        ,ReportingPeriod
        ,`inception_date`
        ,AccountShortName
        ,`SEC_TYPE_CODE`
        ,`SECURITY_SYMBOL`
        ,h.SEDOL_SHORT AS SEDOL
        ,h.`ISIN`
        ,h.`TICKER`
        ,h.CUSIP
        ,`SECURITY`
        ,`LOT`
        ,`QUANTITY`
        ,`TOTAL_COST`
        ,`TOTAL_COST_LOCAL`
        ,`MARKET_VALUE`
        ,`MARKET_VALUE_LOCAL`
        ,`PRICE_LOCAL`
        ,`LOCAL_ISO`
        ,`UNREALIZED_GAIN_LOSS`
        ,`PeriodEndDate`
--        ,NVL(s.security_name, `SECURITY`) AS security_name
FROM HoldingS H
    LEFT JOIN (SELECT `SEDOL`, `TICKER`, MAX(security_tkn_id) AS security_tkn_id FROM (SELECT `SEDOL`, `TICKER`, RANK() over (partition by `SEDOL`, `TICKER` order by del_ind) as rn, security_tkn_id FROM `ccep`.`Union_Shadow_SECURITY` where length(SEDOL) > 0) as sq where rn = 1 GROUP BY `SEDOL`, `TICKER`) s ON (h.SEDOL_SHORT = s.`SEDOL`)
    LEFT JOIN (SELECT `CUSIP`, `TICKER`, MAX(security_tkn_id) AS security_tkn_id FROM (SELECT `CUSIP`, `TICKER`, RANK() over (partition by `CUSIP`, `TICKER` order by del_ind) as rn, security_tkn_id FROM `ccep`.`Union_Shadow_SECURITY` where length(CUSIP) > 0) as sq where rn = 1 GROUP BY `CUSIP`, `TICKER`) c ON (h.CUSIP = c.`CUSIP`)
    LEFT JOIN (SELECT `ISIN`,  `TICKER`, MAX(security_tkn_id) AS security_tkn_id FROM (SELECT `ISIN`,  `TICKER`, RANK() over (partition by `ISIN`,  `TICKER` order by del_ind) as rn, security_tkn_id FROM `ccep`.`Union_Shadow_SECURITY` where length(ISIN)  > 0) as sq where rn = 1 GROUP BY `ISIN` , `TICKER`) i ON (h.ISIN = i.`ISIN`)
where
	(
	    h.TICKER IS NULL OR
	    h.ticker = '' OR   
	    h.ticker = s.`TICKER` or
	    h.ticker = c.`TICKER` or
	    h.ticker = i.`TICKER`
    )
GROUP BY
    `account_tkn_id`
    ,`REPORT_DATE`
    ,ReportingPeriod
    ,`inception_date`
    ,AccountShortName
    ,`SEC_TYPE_CODE`
    ,`SECURITY_SYMBOL`
    ,h.SEDOL_SHORT
    ,h.`ISIN`
    ,h.`TICKER`
    ,h.CUSIP
    ,`SECURITY`
    ,`LOT`
    ,`QUANTITY`
    ,`TOTAL_COST`
    ,`TOTAL_COST_LOCAL`
    ,`MARKET_VALUE`
    ,`MARKET_VALUE_LOCAL`
    ,`PRICE_LOCAL`
    ,`LOCAL_ISO`
    ,`UNREALIZED_GAIN_LOSS`
    ,`PeriodEndDate`
)
SELECT
    hs.`account_tkn_id`
    ,hs.`REPORT_DATE`
    ,hs.security_tkn_id
    ,hs.ReportingPeriod
    ,hs.`inception_date`
    ,hs.AccountShortName
    ,hs.`SEC_TYPE_CODE`
    ,hs.`SECURITY_SYMBOL`
    ,hs.SEDOL
    ,hs.`ISIN`
    ,hs.`TICKER`
    ,hs.CUSIP
    ,hs.`SECURITY`
    ,hs.`LOT`
    ,hs.`QUANTITY`
    ,hs.`TOTAL_COST`
    ,hs.`TOTAL_COST_LOCAL`
    ,hs.`MARKET_VALUE`
    ,hs.`MARKET_VALUE_LOCAL`
    ,hs.`PRICE_LOCAL`
    ,hs.`LOCAL_ISO`
    ,hs.`UNREALIZED_GAIN_LOSS`
    ,hs.`PeriodEndDate`
    ,NVL(s.security_name, `SECURITY`) AS security_name
FROM
    Holdings_Security AS hs
        LEFT JOIN
        `ccep`.`Union_Shadow_SECURITY` AS s ON hs.`security_tkn_id` = s.`security_tkn_id`