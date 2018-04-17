CREATE VIEW `ccep`.`vQuarterlyAccountRestrictionGroup`
AS
WITH ar1 as
(
    SELECT `account_tkn_id`
        ,`restriction_group_tkn_id`
        ,MAX(`begin_date`) as useDate
        ,q.`ReportingPeriod`
        ,q.`PeriodEndDate`
    FROM `ccep`.`union_shadow_ACCOUNT_RESTRICTION` h
    CROSS JOIN `ccep`.`QuarterDate` q
    WHERE CAST(`begin_date` as date) <= q.`PeriodEndDate`
        AND UNIX_TIMESTAMP(periodenddate, 'yyyy-MM-dd') >= UNIX_TIMESTAMP('2017-11-01', 'yyyy-MM-dd')
        AND UNIX_TIMESTAMP(periodenddate, 'yyyy-MM-dd') < current_timestamp
        AND restriction_group_tkn_id IS NOT NULL
    GROUP BY `account_tkn_id`, `restriction_group_tkn_id`, q.`ReportingPeriod`, q.`PeriodEndDate`
 )
 SELECT ar1.`account_tkn_id`, ar1.`restriction_group_tkn_id`, ar1.useDate, ar1.`ReportingPeriod`, ar1.`PeriodEndDate`
 FROM ar1
 INNER JOIN `ccep`.`union_shadow_ADVISOR_CENTER_ACCOUNT` a
   ON ar1.account_tkn_id = a.account_tkn_id
   