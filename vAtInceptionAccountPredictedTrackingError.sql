CREATE VIEW `ccep`.`vAtInceptionAccountPredictedTrackingError`
AS
WITH TE
AS
(
  SELECT aa.account_tkn_id
    , s.last_update_date
    , aa.short_name
    , aa.inception_date
    , s.benchmark_version_tkn_id
    , s.initTE
    , s.finalTE
    , RANK() OVER (PARTITION BY aa.account_tkn_id  ORDER BY s.last_update_date desc) AS seq
    FROM `ccep`.`union_shadow_ADVISOR_CENTER_ACCOUNT` aa
    INNER JOIN `ccep`.`union_shadow_ACCOUNT` a
    ON aa.account_tkn_id = a.account_tkn_id
    INNER JOIN `ccep`.`union_shadow_scenario` s on s.account_tkn_id = a.account_tkn_id
    WHERE CAST(s.create_date AS DATE) = CAST(a.inception_date AS DATE)
    --and a.inception_date >= '3/2/2018'
    AND a.product_type_id != 2
    AND a.close_date IS NULL
    AND s.confirmed_ind = 'true'
    -- live accounts
    AND a.account_type_id = 1
)
SELECT account_tkn_id
    , TE.short_name AS AccountShortName
    , inception_date
    , TE.last_update_date AS ScenarioDate
    , bv.benchmark_tkn_id
    , b.short_name AS BenchmarkShortName
    , NVL(b.presentation_name, b.long_name) AS BenchmarkPresentationName
    , initTE
    , finalTE
FROM TE
INNER JOIN `ccep`.`union_shadow_BENCHMARK_VERSION` bv
ON TE.benchmark_version_tkn_id = bv.benchmark_version_tkn_id
INNER JOIN `ccep`.`union_shadow_BENCHMARK` b
ON bv.benchmark_tkn_id = b.benchmark_tkn_id
WHERE seq = 1