ALTER VIEW `ccep`.`vQuarterlyAccountPredictedTrackingError`
AS
SELECT p.`account_tkn_id`
      ,p.ReportingPeriod
      ,`initial_TE` AS PredictedTrackingError
  FROM `ccep`.`union_quarterly_port` p
  INNER JOIN `ccep`.`union_shadow_advisor_center_account` a
  ON p.`account_tkn_id` = a.`account_tkn_id`