ALTER VIEW `ccep`.`vAccountMetrics`
AS
SELECT aa.`account_tkn_id`
, a.`annual_gains_budget`
, `value` AS TaxManagementMandate
FROM  `ccep`.`union_shadow_ADVISOR_CENTER_ACCOUNT` aa
INNER JOIN `ccep`.`union_shadow_ACCOUNT` a ON a.account_tkn_id = aa.account_tkn_id
LEFT JOIN (select ac.* from ccep.union_shadow_ACCOUNT_CHARACTERISTIC ac 
INNER JOIN ccep.union_shadow_CHARACTERISTIC AS c ON c.characteristic_tkn_id = ac.characteristic_tkn_id and c.name = 'LossHarvesting') as ac ON ac.account_tkn_id = aa.account_tkn_id
