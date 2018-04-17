drop table if exists `ccep`.`m_vQuarterlyAccountSecurity`;

CREATE TABLE `ccep`.`m_vQuarterlyAccountSecurity` (
	`account_tkn_id` int,
	`accountshortname` string,
	`security_tkn_id` int,
	`sec_type_code` string,
	`security` string,
	`sedol` string,
	`cusip` string,
	`isin` string,
	`ticker` string,
	`quantity` double,
	`total_cost` double,
	`market_value` double,
	`unrealized_gain_loss` double,
	`marketweight` double,
	`marketweightnetcash` double,
	`numberofholdings` bigint,
	`security_value` double,
	`accountmarketvalue` double,
	`accountmarketvaluenetcash` double)
PARTITIONED BY (`reportingperiod` string);

insert into `ccep`.`m_vQuarterlyAccountSecurity` partition (reportingperiod)
SELECT
	`account_tkn_id`,
	`accountshortname`,
	`security_tkn_id`,
	`sec_type_code`,
	`security`,
	`sedol`,
	`cusip`,
	`isin`,
	`ticker`,
	`quantity`,
	`total_cost`,
	`market_value`,
	`unrealized_gain_loss`,
	`marketweight`,
	`marketweightnetcash`,
	`numberofholdings`,
	`security_value`,
	`accountmarketvalue`,
	`accountmarketvaluenetcash`,
	`reportingperiod`
FROM `ccep`.`vQuarterlyAccountSecurity`;