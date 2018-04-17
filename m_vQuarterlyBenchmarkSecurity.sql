drop table if exists `ccep`.`m_vQuarterlyBenchmarkSecurity`;

CREATE TABLE `ccep`.`m_vQuarterlyBenchmarkSecurity` (
	`benchmark_tkn_id` int,
	`benchmarkshortname` string,
	`benchmarklongname` string,
	`benchmarkpresentationname` string,
	`rf_provider_tkn_id` int,
	`weight` float,
	`shares` float,
	`market_value` float,
	`security_tkn_id` int)
PARTITIONED BY (`reportingperiod` string);

insert into `ccep`.`m_vQuarterlyBenchmarkSecurity` partition (reportingperiod)
SELECT
	`benchmark_tkn_id`,
	`benchmarkshortname`,
	`benchmarklongname`,
	`benchmarkpresentationname`,
	`rf_provider_tkn_id`,
	`weight`,
	`shares`,
	`market_value`,
	`security_tkn_id`,
	`reportingperiod`
FROM `ccep`.`vQuarterlyBenchmarkSecurity`;