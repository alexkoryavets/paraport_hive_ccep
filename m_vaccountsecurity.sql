
drop table if exists `ccep`.`m_vaccountsecurity`;

CREATE TABLE `ccep`.`m_vaccountsecurity` (
	account_tkn_id INT,
	accountshortname STRING,
	reportingperiod STRING,
	periodenddate STRING,
	inception_date DATE,
	security_tkn_id INT,
	sec_type_code STRING,
	`security` STRING,
	sedol STRING,
	cusip STRING,
	isin STRING,
	ticker STRING,
	quantity DOUBLE,
	total_cost DOUBLE,
	market_value DOUBLE,
	unrealized_gain_loss DOUBLE,
	security_value DOUBLE,
	marketweight DOUBLE,
	marketweightnetcash DOUBLE,
	numberofholdings BIGINT,
	accountmarketvalue DOUBLE,
	accountmarketvaluenetcash DOUBLE
)
partitioned by (report_date STRING);

insert into `ccep`.`m_vaccountsecurity` partition (report_date)
select
	account_tkn_id,
	accountshortname,
	reportingperiod,
	periodenddate,
	inception_date,
	security_tkn_id,
	sec_type_code,
	`security`,
	sedol,
	cusip,
	isin,
	ticker,
	quantity,
	total_cost,
	market_value,
	unrealized_gain_loss,
	security_value,
	marketweight,
	marketweightnetcash,
	numberofholdings,
	accountmarketvalue,
	accountmarketvaluenetcash,
	report_date
from
	`ccep`.`vAccountSecurity`;
