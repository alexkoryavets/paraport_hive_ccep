DROP TABLE IF EXISTS ccep.qtr_date;
 
CREATE TABLE ccep.qtr_date AS
WITH NonMarketDays AS (
SELECT
    cast(translate(cast(t.`date` as string), '-', '') AS BIGINT) AS `date`
FROM
    ccep.axiom_shadow_calendar AS t
WHERE
    t.code = '#A'
)
SELECT
    *,
    CONCAT(CAST(`year` AS string), 'Q', CAST(`quarter_number` AS string)) AS ReportingPeriod
FROM
    ccep.ccep_uat_t_ppa_ccep_dim_date AS d
        LEFT JOIN
        NonMarketDays AS nmd ON nmd.`date` = d.`date_key`
WHERE
    nmd.`date` IS NULL;

drop table if exists ccep.QuarterDate;
 
create table ccep.QuarterDate AS
with qtr as
(select reportingperiod, max(date_key) as QuarterEndDate, min(date_key) as QuarterBeginDate from ccep.qtr_date c
where c.year>=2017
group by reportingperiod
)
,qtr2 as
(
select reportingperiod, QuarterBeginDate, QuarterEndDate, rank() over (order by reportingperiod) as Seq
from qtr)
select
    a.reportingperiod AS ReportingPeriod,
    to_date(from_unixtime(UNIX_TIMESTAMP(CAST(a.QuarterBeginDate AS string),'yyyyMMdd'))) AS PeriodStartDate,
    a.QuarterBeginDate AS PeriodStartDateKey,
    to_date(from_unixtime(UNIX_TIMESTAMP(CAST(a.QuarterEndDate AS string),'yyyyMMdd'))) AS PeriodEndDate,
    a.QuarterEndDate AS PeriodEndDateKey,
    to_date(from_unixtime(UNIX_TIMESTAMP(CAST(b.QuarterBeginDate AS string),'yyyyMMdd'))) as NextPeriodStartDate,
    b.QuarterBeginDate as NextPeriodStartDateKey
from
    qtr2 AS a
        inner join
        qtr2 as b on a.seq+1 = b.seq
order by
    ReportingPeriod;