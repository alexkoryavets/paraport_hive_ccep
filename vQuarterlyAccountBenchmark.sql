ALTER VIEW `ccep`.`vQuarterlyAccountBenchmark`
AS
SELECT qa.`account_tkn_id`
    , qa.`aggregate_benchmark_tkn_id` AS `reporting_benchmark_tkn_id`
    , rb.short_name AS `reporting_benchmark_short_name`
    , rb.long_name AS `reporting_benchmark_long_name`
    , rb.presentation_name AS `reporting_benchmark_presentation_name`
    , qa.benchmark_tkn_id AS `management_benchmark_tkn_id`
    , mb.short_name AS `management_benchmark_short_name`
    , mb.long_name AS `management_benchmark_long_name`
    , mb.presentation_name AS `management_benchmark_presentation_name`
    , rb.`rf_provider_tkn_id`
    ,qa.`ReportingPeriod`
FROM `ccep`.`UNION_QUARTERLY_ACCOUNT` qa
INNER JOIN `ccep`.`Union_Shadow_ADVISOR_CENTER_ACCOUNT` a
    ON qa.`account_tkn_id` = a.`account_tkn_id`
    -- adding qa.`ReportingPeriod` = a.`snapshotQuarter`
INNER JOIN ccep.Union_Shadow_BENCHMARK mb
    ON qa.benchmark_tkn_id = mb.benchmark_tkn_id
INNER JOIN ccep.Union_Shadow_BENCHMARK rb
    ON qa.`aggregate_benchmark_tkn_id` = rb.benchmark_tkn_id
WHERE a.close_date IS NULL