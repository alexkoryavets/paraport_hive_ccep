ALTER VIEW `ccep`.`vQuarterlyBenchmarkSecurity`
AS
WITH QtrlyBenchmarkHistory AS
(
  SELECT
    benchmark_tkn_id ,
    ReportingPeriod , --this should ve the partion quarter column
    benchmark_version_tkn_id AS useVersion
  FROM ccep.union_quarterly_benchmark_version h
) ,
baseBenchmarkSecurity AS
(
  SELECT
    qbh.ReportingPeriod ,
    b.benchmark_tkn_id ,
    bs.benchmark_version_tkn_id ,
    b.rf_provider_tkn_id ,
    CAST(bs.weight AS FLOAT) AS Weight ,
    CAST(bs.shares AS FLOAT) AS shares ,
    CAST(bs.market_value AS FLOAT) AS market_value ,
    bs.security_tkn_id
  FROM ccep.union_shadow_benchmark b
  LEFT JOIN QtrlyBenchmarkHistory qbh
    ON b.benchmark_tkn_id = qbh.benchmark_tkn_id
  LEFT JOIN ccep.union_quarterly_benchmark_security bs
    ON qbh.useVersion = bs.benchmark_version_tkn_id
) ,
CTE AS
(
  SELECT 0 AS Lev ,
    qbh.ReportingPeriod ,
    qbh.benchmark_tkn_id ,
    qbh.security_tkn_id ,
    qbh.rf_provider_tkn_id ,
    NULL AS composition_benchmark_tkn_id ,
    CAST(1.0 * Weight AS FLOAT) AS weight ,
    CAST(shares AS FLOAT) AS shares ,
    CAST(market_value AS FLOAT) AS market_value
  FROM baseBenchmarkSecurity qbh
  WHERE security_tkn_id IS NOT NULL
  UNION ALL
  SELECT 1 AS Lev ,
    qbh.ReportingPeriod ,
    c.benchmark_tkn_id ,
    qbh.security_tkn_id ,
    qbh.rf_provider_tkn_id ,
    c.composition_benchmark_tkn_id ,
    CAST(qbh.weight * c.weight AS FLOAT) AS weight ,
    CAST(qbh.shares * c.weight AS FLOAT) AS shares ,
    CAST(qbh.market_value * c.weight AS FLOAT) AS market_value
  FROM baseBenchmarkSecurity qbh
  INNER JOIN ccep.union_shadow_benchmark_combo_composition c
    ON c.composition_benchmark_tkn_id = qbh.benchmark_tkn_id
  WHERE qbh.security_tkn_id IS NOT NULL
    AND c.benchmark_tkn_id IN (SELECT benchmark_tkn_id FROM baseBenchmarkSecurity WHERE security_tkn_id IS NULL)
) ,
  Combined AS
  (
    SELECT CTE.ReportingPeriod ,
      CTE.benchmark_tkn_id ,
      b.short_name AS BenchmarkShortName ,
      b.long_name AS BenchmarkLongName ,
      b.presentation_name AS BenchmarkPresentationName ,
      b.rf_provider_tkn_id ,
      CTE.Weight ,
      CTE.shares ,
      CTE.market_value ,
      CTE.security_tkn_id
    FROM CTE
    INNER JOIN ccep.union_shadow_benchmark b
        ON CTE.benchmark_tkn_id = b.benchmark_tkn_id
  )
  SELECT bs.* FROM combined bs
  INNER JOIN (
    SELECT reporting_benchmark_tkn_id AS benchmark_tkn_id,
        ReportingPeriod
    FROM ccep.vquarterlyaccountbenchmark
    UNION
    SELECT management_benchmark_tkn_id AS benchmark_tkn_id,
        ReportingPeriod
    FROM ccep.vquarterlyaccountbenchmark) qab
        on qab.ReportingPeriod = bs.ReportingPeriod and bs.benchmark_tkn_id = qab.benchmark_tkn_id