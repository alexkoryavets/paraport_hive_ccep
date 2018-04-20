--	Query 1: No duplicates
select
	account_tkn_id,
	count(*)
from
	ccep.vQuarterlyAccountBenchmark
group by
	account_tkn_id
having
	count(*) > 1;

--	Query 2: All accounts have data
select
	*
from
	ccep.union_shadow_advisor_center_account as src
		inner join
		ccep.quarterdate as qd on src.reportingperiod = qd.reportingperiod and src.inception_date <= qd.periodenddate
		left join
		ccep.vQuarterlyAccountBenchmark as tgt on src.account_tkn_id = tgt.account_tkn_id
where
	tgt.account_tkn_id is null
	;



----	Query 3: Sum equals 100% (unrelevant)
--select
--	account_tkn_id,
--	reportingperiod,
--	report_date,
--	security_tkn_id,
--	count(*)
--from
--	ccep.union_shadow_advisor_center_account as src
--		left join
--		ccep.m_vaccountsecurity as tgt on src.account_tkn_id = tgt.account_tkn_id
--where
--	tgt.account_tkn_id is null;
--
