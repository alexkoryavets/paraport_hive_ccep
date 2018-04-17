CREATE VIEW [dbo].[vQuarterlySecurityFundamentalMapping] AS
WITH sec AS
(SELECT ReportingPeriod, [security_tkn_id] FROM [dbo].[M_vQuarterlyAccountSecurity]
    WHERE [MARKET_VALUE] > 0 AND [security_tkn_id] > 0
    UNION
    SELECT ReportingPeriod, [security_tkn_id] FROM [dbo].[M_vQuarterlyBenchmarkSecurity]
)
,Fund AS
(   -- faking the fundamental files for Q4 and Q1.  In Data Lake, we should have use the quarterenddatekey to pick the correct dates for quarters
    SELECT ReportingPeriod AS ReportingPeriodF,
        a.*
        FROM [dbo].[QuarterDate] q
        INNER JOIN
        (SELECT [id_bb_global], [exch_code],[id_exch_symbol],[ticker_and_exch_code],[name],[id_bb_company],[id_bb_security],[id_bb_unique],[id_sedol1],[id_isin],[id_cusip],[pe_ratio],[px_to_book_ratio],[cur_mkt_cap],[crncy]
                , Replace(ticker, '.', '/') AS ticker
                ,LEFT([id_sedol1], 6) AS [id_sedol1_cleaned]
                ,'20171229' AS ymd -- hard code it for now, use just ymd in data lake
            FROM [dbo].[T_FUNDAMENTALS]
            WHERE ymd=20180309 -- hard code it for now, use just ymd in data lake
            UNION ALL
            SELECT [id_bb_global], [exch_code],[id_exch_symbol],[ticker_and_exch_code],[name],[id_bb_company],[id_bb_security],[id_bb_unique],[id_sedol1],[id_isin],[id_cusip],[pe_ratio],[px_to_book_ratio],[cur_mkt_cap],[crncy]
                , Replace(ticker, '.', '/') AS ticker
                ,LEFT([id_sedol1], 6) AS [id_sedol1_cleaned]
                ,'20180329' AS ymd
            FROM [dbo].[T_FUNDAMENTALS]
            WHERE ymd=20180313) a
        ON q.PeriodEndDateKey = a.ymd
)
,Axiom_Quarterly_Security AS    -- faking the reporting period.  In Data Lake, we will have the ReportingPeriod from the partition
(
    SELECT '2017 Q4' AS ReportingPeriodAX, * FROM [dbo].[AXIOM_CURRENT_SECURITY_2017Q4]
)
,Axiom_Match1_bloomberg_global AS
(
    SELECT m.*, sm.[underlying_ticker], sm.security_type, sm.[bloomberg_security_id], sm.[bloomberg_global], sm.bloomberg_company
        , s.security_name, s.[bloomberg_ticker_and_exchange], s.[bloomberg_ticker], s.cusip, s.sedol, s.isin, Replace(s.ticker, '.', '/') AS mticker, s.[bloomberg_company_id], s.security_type_tkn_id, s.[instrument_type_id], s.[del_ind]
        , f.*
    FROM sec m
    LEFT join [dbo].[SECURITY] s
        ON m.security_tkn_id = s.security_tkn_id
    LEFT JOIN Axiom_Quarterly_Security sm  -- snapshot load, with ReportingPeriod added from partition
        ON s.ppa_security_id = sm.[ppa_security_id]
        AND m.ReportingPeriod = sm.ReportingPeriodAX
    LEFT JOIN Fund f
        ON sm.bloomberg_global = f.id_bb_global
        AND sm.ReportingPeriodAX = f.ReportingPeriodF
)
,Axiom_ADR AS
(  
    SELECT m.ReportingPeriod
        ,m.security_tkn_id
        ,m.[underlying_ticker], m.security_type
        ,ISNULL(us.[bloomberg_security_id], m.[bloomberg_security_id]) AS [bloomberg_security_id]
        ,ISNULL(us.[bloomberg_global], m.[bloomberg_global]) AS [bloomberg_global]
        ,ISNULL(us.bloomberg_company, m.bloomberg_company) AS bloomberg_company
        ,us.is_stale
        ,m.security_name, m.[bloomberg_ticker_and_exchange], m.[bloomberg_ticker], m.cusip, m.sedol, m.isin, m.mticker, m.[bloomberg_company_id], m.security_type_tkn_id, m.[instrument_type_id], m.[del_ind]
        ,f.*
    from Axiom_Match1_bloomberg_global  m
    LEFT JOIN Axiom_Quarterly_Security us
    ON m.security_type = 'ADR' AND m.[underlying_ticker] = us.[bloomberg_ticker] AND m.bloomberg_company = us.bloomberg_company AND m.ReportingPeriod = us.ReportingPeriodAX
    LEFT JOIN Fund f
    ON us.bloomberg_global = f.id_bb_global AND us.ReportingPeriodAX = f.ReportingPeriodF
    WHERE m.[id_bb_global] is null -- and m.[underlying_ticker] = 'ADVANC/F TB'
)
,Axiom_Match2_ADR AS
(
    SELECT * FROM
    (SELECT *
    , rank() OVER (PARTITION BY ReportingPeriod, security_tkn_id ORDER BY is_stale ASC) AS seq
    FROM Axiom_ADR) a
    WHERE seq = 1
    --WHERE ymd IS NOT NULL
)
,Match1_cusip_crncy AS
(
    SELECT m.ReportingPeriod, m.security_tkn_id, security_type, s.[bloomberg_ticker_and_exchange], s.cusip, s.sedol, s.isin, mticker, c.iso_code, s.[bloomberg_company_id], s.security_type_tkn_id, s.[instrument_type_id], s.[del_ind]
        , f.[id_bb_global]
    FROM Axiom_Match2_ADR m
    INNER JOIN [dbo].[SECURITY] s
        on m.security_tkn_id = s.security_tkn_id
    INNER JOIN CURRENCY c
        on s.currency_tkn_id = c.currency_tkn_id
    LEFT JOIN Fund f
        ON s.cusip= f.id_cusip AND c.iso_code = f.[crncy] AND m.ReportingPeriod = f.ReportingPeriodF
    WHERE m.[id_bb_global] IS NULL
)
,Match2_ticker_and_exch_code AS
(
    SELECT m.ReportingPeriod, m.security_tkn_id, security_type, m.[bloomberg_ticker_and_exchange], m.cusip, m.sedol, m.isin, mticker, m.iso_code, m.[bloomberg_company_id], m.security_type_tkn_id, m.[instrument_type_id], m.[del_ind], f.*
    FROM Match1_cusip_crncy m
    LEFT JOIN Fund f
        ON m.[bloomberg_ticker_and_exchange] = f.ticker_and_exch_code AND m.ReportingPeriod = f.ReportingPeriodF
    WHERE m.id_bb_global IS NULL
)
,ADR_and_Internation AS (
    SELECT m.ReportingPeriod, m.security_tkn_id, [ADR_Name], security_type, [Local_SEDOL], [Local_Name]
        , ISNULL(s2.cusip, m.cusip) as cusip
        , ISNULL(s2.sedol, m.sedol) AS sedol
        , ISNULL(s2.sedol, m.isin) AS isin
        , ISNULL(Replace(s2.ticker, '.', '/'), m.mticker) AS mticker
        , ISNULL(c.iso_code, m.iso_code) AS iso_code
        , ISNULL(s2.[bloomberg_ticker_and_exchange], m.[bloomberg_ticker_and_exchange]) AS [bloomberg_ticker_and_exchange]
        , ISNULL(s2.[bloomberg_company_id], m.[bloomberg_company_id]) AS [bloomberg_company_id]
        , ISNULL(s2.security_type_tkn_id, m.security_type_tkn_id) AS security_type_tkn_id
        , ISNULL(s2.[instrument_type_id], m.[instrument_type_id]) AS [instrument_type_id]
        , ISNULL(s2.[del_ind], m.[del_ind]) AS [del_ind]
    FROM Match2_ticker_and_exch_code m
        LEFT JOIN [dbo].[ADR_TO_UNDERLYING_MAPPING] adr_m
        on m.cusip = adr_m.ADR_CUSIP AND m.[instrument_type_id] = 3
        LEFT JOIN dbo.[SECURITY] s2
        on adr_m.Local_SEDOL = s2.sedol
        LEFT JOIN CURRENCY c
            on s2.currency_tkn_id = c.currency_tkn_id
        WHERE m.id_bb_global IS NULL
)
,Match3_sedol_currency AS
(
    SELECT m.ReportingPeriod, m.security_tkn_id, security_type, m.cusip, m.sedol, m.isin, m.mticker, m.iso_code, m.[bloomberg_ticker_and_exchange], m.[bloomberg_company_id], m.security_type_tkn_id, m.[instrument_type_id], m.[del_ind]
    ,f.*
    --INTO Match3_sedol_currency
    FROM ADR_and_Internation m
        LEFT JOIN Fund f
        ON m.sedol= f.[id_sedol1_cleaned] AND m.iso_code = f.[crncy] AND m.ReportingPeriod = f.ReportingPeriodF
)
,Match4_ticker_currency AS (
    SELECT m.ReportingPeriod, m.security_tkn_id, security_type, m.cusip, m.sedol, m.isin, m.mticker, m.iso_code, m.[bloomberg_ticker_and_exchange], m.[bloomberg_company_id], m.security_type_tkn_id, m.[instrument_type_id], m.[del_ind]
    , f.*
    FROM Match3_sedol_currency m
    LEFT JOIN Fund f
    ON m.mticker = f.ticker AND m.iso_code = f.crncy AND m.ReportingPeriod = f.ReportingPeriodF
    WHERE m.id_bb_global IS NULL
)
,Match5_company_id_currency AS (   
SELECT m.ReportingPeriod, m.security_tkn_id, security_type, m.cusip, m.sedol, m.isin, m.mticker, m.iso_code, m.[bloomberg_ticker_and_exchange], m.[bloomberg_company_id], m.security_type_tkn_id, m.[instrument_type_id], m.[del_ind]
    , f.*
--  INTO Match6_company_code_currency
    FROM Match4_ticker_currency m
    LEFT JOIN Fund f
    ON m.[bloomberg_company_id] = f.id_bb_company AND m.iso_code = f.[crncy] AND m.ReportingPeriod = f.ReportingPeriodF
    WHERE m.id_bb_global IS NULL
)
,Match6_sedol AS ( 
SELECT m.ReportingPeriod, m.security_tkn_id, security_type, m.cusip, m.sedol, m.isin, m.mticker, m.iso_code, m.[bloomberg_ticker_and_exchange], m.[bloomberg_company_id], m.security_type_tkn_id, m.[instrument_type_id], m.[del_ind]
    , f.*
    FROM Match5_company_id_currency m
    LEFT JOIN Fund f
    ON m.sedol = f.id_sedol1_cleaned AND m.ReportingPeriod = f.ReportingPeriodF --AND m.iso_code = f.[crncy]
    WHERE m.id_bb_global IS NULL
)
,Match7_cusip AS ( 
SELECT m.ReportingPeriod, m.security_tkn_id, security_type, m.cusip, m.sedol, m.isin, m.mticker, m.iso_code, m.[bloomberg_ticker_and_exchange], m.[bloomberg_company_id], m.security_type_tkn_id, m.[instrument_type_id], m.[del_ind]
    , f.*
    FROM Match6_sedol m
    LEFT JOIN Fund f
    ON m.cusip = f.id_cusip --AND m.iso_code = f.[crncy]
    WHERE m.id_bb_global IS NULL AND m.ReportingPeriod = f.ReportingPeriodF
)
,combined AS
(
SELECT ReportingPeriod, security_tkn_id, id_bb_global, security_type, '01_Axiom_bloomberg_global' AS match_type, cusip, sedol, isin, mticker, '' iso_code, [bloomberg_ticker_and_exchange], [bloomberg_company_id], security_type_tkn_id, [instrument_type_id], [del_ind]
    FROM Axiom_Match1_bloomberg_global
UNION ALL
SELECT ReportingPeriod, security_tkn_id, id_bb_global, security_type, '02_Axiom_adr' AS match_type, cusip, sedol, isin, mticker, '' iso_code, [bloomberg_ticker_and_exchange], [bloomberg_company_id], security_type_tkn_id, [instrument_type_id], [del_ind]
    FROM Axiom_Match2_ADR
UNION ALL
SELECT ReportingPeriod, security_tkn_id, id_bb_global, security_type, '1_cusip_crncy' AS match_type, cusip, sedol, isin, mticker, iso_code, [bloomberg_ticker_and_exchange], [bloomberg_company_id], security_type_tkn_id, [instrument_type_id], [del_ind]
    FROM Match1_cusip_crncy
UNION ALL
SELECT ReportingPeriod, security_tkn_id, id_bb_global, security_type, '2_ticker_and_exch_code' AS match_type, cusip, sedol, isin, mticker, iso_code, [bloomberg_ticker_and_exchange], [bloomberg_company_id], security_type_tkn_id, [instrument_type_id], [del_ind]
    FROM Match2_ticker_and_exch_code
UNION ALL
SELECT ReportingPeriod, security_tkn_id, id_bb_global, security_type, '3_sedol_currency' AS match_type, cusip, sedol, isin, mticker, iso_code, [bloomberg_ticker_and_exchange], [bloomberg_company_id], security_type_tkn_id, [instrument_type_id], [del_ind]
    FROM Match3_sedol_currency
UNION ALL
SELECT ReportingPeriod, security_tkn_id, id_bb_global, security_type, '4_ticker_currency' AS match_type, cusip, sedol, isin, mticker, iso_code, [bloomberg_ticker_and_exchange], [bloomberg_company_id], security_type_tkn_id, [instrument_type_id], [del_ind]
  FROM Match4_ticker_currency
UNION ALL
SELECT ReportingPeriod, security_tkn_id, id_bb_global, security_type, '5_company_id_currency' AS match_type, cusip, sedol, isin, mticker, iso_code, [bloomberg_ticker_and_exchange], [bloomberg_company_id], security_type_tkn_id, [instrument_type_id], [del_ind]
  FROM Match5_company_id_currency
UNION ALL
SELECT ReportingPeriod, security_tkn_id, id_bb_global, security_type, '6_sedol' AS match_type, cusip, sedol, isin, mticker, iso_code, [bloomberg_ticker_and_exchange], [bloomberg_company_id], security_type_tkn_id, [instrument_type_id], [del_ind]
  FROM Match6_sedol
UNION ALL
SELECT ReportingPeriod, security_tkn_id, id_bb_global, security_type, '7_cusip' AS match_type, cusip, sedol, isin, mticker, iso_code, [bloomberg_ticker_and_exchange], [bloomberg_company_id], security_type_tkn_id, [instrument_type_id], [del_ind]
  FROM Match7_cusip
)
select m.*, f.name AS FundamentalName, f.crncy AS FundamentalCurrency, s.security_name AS union_name
    ,[pe_ratio]
    ,CASE WHEN [pe_ratio] like '%[0-9,.]%' AND [pe_ratio] NOT LIKE '%[A-z]%' AND [pe_ratio] NOT LIKE '%[!-,:-~]%' THEN CAST([pe_ratio] AS float) ELSE NULL END AS [pe_ratio_value]
    ,[px_to_book_ratio]
    ,CASE WHEN [px_to_book_ratio] like '%[0-9,.]%' AND [px_to_book_ratio] NOT LIKE '%[A-z]%' AND [px_to_book_ratio] NOT LIKE '%[!-,:-~]%' THEN CAST([px_to_book_ratio] AS float) ELSE NULL END AS [pb_ratio_value]
    ,[cur_mkt_cap]
    ,CASE WHEN [cur_mkt_cap] like '%[0-9,.]%' AND [cur_mkt_cap] NOT LIKE '%[A-z]%' AND [cur_mkt_cap] NOT LIKE '%[!-,:-~]%' THEN CAST([cur_mkt_cap] AS float) ELSE NULL END AS [cur_mkt_cap_value]
--INTO dbo.M_vQuarterlySecurityFundamental
FROM combined m
INNER JOIN [dbo].[SECURITY] s
    on m.security_tkn_id = s.security_tkn_id
LEFT JOIN fund f
    on m.id_bb_global = f.id_bb_global AND m.ReportingPeriod = f.ReportingPeriodF
WHERE m.id_bb_global IS NOT NULL
 
CREATE VIEW [dbo].[vQuarterlyAccountBenchmarkFundamental]
AS
WITH absec AS
(
SELECT vas.[ReportingPeriod]
      ,vas.[account_tkn_id]
      ,-1 AS Benchmark_tkn_id
      ,vas.[AccountShortName] AS Short_name
      ,vas.[security_tkn_id]
      ,vas.[MARKET_VALUE] AS [Security_Value]
FROM [dbo].[M_vQuarterlyAccountSecurity] vas
WHERE [MARKET_VALUE] > 0 AND [security_tkn_id] > 0
UNION ALL
SELECT [ReportingPeriod]
      ,-1 AS account_tkn_id
      ,[benchmark_tkn_id]
      ,[BenchmarkShortName]
      ,qbs.[security_tkn_id]
      ,weight * 10000 AS [Security_Value]
FROM [dbo].[M_vQuarterlyBenchmarkSecurity] qbs
)
SELECT absec.[ReportingPeriod]
    ,[account_tkn_id]
    ,[benchmark_tkn_id]
    ,absec.[security_tkn_id]
    ,[Security_Value]
    ,[pe_ratio]
    ,[pe_ratio_value]
    ,[px_to_book_ratio]
    ,[pb_ratio_value]
    ,[cur_mkt_cap]
    ,[cur_mkt_cap_value]
    ,[FundamentalCurrency]+'USD' AS fx_ticker
    ,[quote_factor] AS fx_quote_factor
    ,[px_last] AS fx_rate
    ,CASE WHEN [px_last] IS NULL THEN [cur_mkt_cap_value] ELSE [cur_mkt_cap_value] * [px_last] / [quote_factor] END AS converted_market_cap
FROM absec
LEFT JOIN dbo.M_vQuarterlySecurityFundamentalMapping m
    ON absec.[security_tkn_id] = m.[security_tkn_id] AND absec.ReportingPeriod = m.ReportingPeriod
LEFT JOIN QuarterDate qd
  ON m.ReportingPeriod = qd.ReportingPeriod
LEFT JOIN [dbo].[T_FX_RATES] fx
  ON fx.ymd = qd.PeriodEndDateKey AND fx.ticker=[FundamentalCurrency]+'USD'