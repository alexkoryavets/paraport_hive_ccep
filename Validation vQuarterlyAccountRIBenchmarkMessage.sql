--	Query 1: No duplicates
select
	accounttknid,
	reportingperiodname,
	metricgroup,
	count(*)
from
	ccep.m_vQuarterlyAccountRIBenchmarkMessage
group by
	accounttknid,
	reportingperiodname,
	metricgroup
having
	count(*) > 1;


--	Query 2: All accounts have data
select
	*
from
	ccep.union_shadow_advisor_center_account as src
		left join
		ccep.m_vQuarterlyAccountRIBenchmarkMessage as tgt on src.account_tkn_id = tgt.accounttknid
where
	tgt.accounttknid is null;
