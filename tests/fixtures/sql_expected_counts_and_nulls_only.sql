DECLARE VARIABLE validation_begin_ts TIMESTAMP DEFAULT current_timestamp();

CREATE OR REPLACE TEMP VIEW final_metrics_view AS

WITH
count_metrics AS (
  SELECT
    (SELECT COUNT(1) FROM `c`.`s`.`a`) AS source_count,
    (SELECT COUNT(1) FROM `c`.`s`.`b`) AS target_count
)

,
null_metrics_v1 AS (
  SELECT
    (SELECT COUNT(1) FROM `c`.`s`.`a` WHERE `v1` IS NULL) as source_nulls_v1,
    (SELECT COUNT(1) FROM `c`.`s`.`b` WHERE `v1` IS NULL) as target_nulls_v1
),
null_metrics_v2 AS (
  SELECT
    (SELECT COUNT(1) FROM `c`.`s`.`a` WHERE `v2` IS NULL) as source_nulls_v2,
    (SELECT COUNT(1) FROM `c`.`s`.`b` WHERE `v2` IS NULL) as target_nulls_v2
)

SELECT
  validation_begin_ts AS validation_begin_ts,
  current_timestamp() AS validation_complete_ts,
  'c' AS source_catalog,
  's' AS source_schema,
  'a' AS source_table,
  'c' AS target_catalog,
  's' AS target_schema,
  'b' AS target_table,
  NULL AS business_domain,
  NULL AS business_owner,
  NULL AS business_priority,
  NULL AS expected_sla_hours,
  NULL AS estimated_impact_usd,
  parse_json(to_json(struct(
    NULL AS applied_filter
    ,
    NULL AS configured_primary_keys
,
    struct(
      FORMAT_NUMBER(source_count, '#,##0') AS source_count,
      FORMAT_NUMBER(target_count, '#,##0') AS target_count,
      FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,
      FORMAT_STRING('%.2f%%', CAST(0.02 * 100 AS DOUBLE)) AS tolerance_percent,
      CASE WHEN COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.02 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS count_validation,
    struct(
      FORMAT_NUMBER(source_nulls_v1, '#,##0') AS source_nulls,
      FORMAT_NUMBER(target_nulls_v1, '#,##0') AS target_nulls,FORMAT_STRING('%.2f%%', CAST(CASE WHEN source_nulls_v1 = 0 AND target_nulls_v1 > 0 THEN 100.0 WHEN target_nulls_v1 = 0 AND source_nulls_v1 > 0 THEN 100.0 ELSE COALESCE(ABS(target_nulls_v1 - source_nulls_v1) / NULLIF(CAST(source_nulls_v1 AS DOUBLE), 0), 0) * 100 END AS DOUBLE)) as relative_diff_percent,FORMAT_STRING('%.2f%%', CAST(0.05 * 100 AS DOUBLE)) AS tolerance_percent,
  CASE WHEN CASE WHEN source_nulls_v1 = 0 THEN target_nulls_v1 = 0 ELSE COALESCE(ABS(target_nulls_v1 - source_nulls_v1) / NULLIF(CAST(source_nulls_v1 AS DOUBLE), 0), 0) <= 0.05 END THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS null_validation_v1,
    struct(
      FORMAT_NUMBER(source_nulls_v2, '#,##0') AS source_nulls,
      FORMAT_NUMBER(target_nulls_v2, '#,##0') AS target_nulls,FORMAT_STRING('%.2f%%', CAST(CASE WHEN source_nulls_v2 = 0 AND target_nulls_v2 > 0 THEN 100.0 WHEN target_nulls_v2 = 0 AND source_nulls_v2 > 0 THEN 100.0 ELSE COALESCE(ABS(target_nulls_v2 - source_nulls_v2) / NULLIF(CAST(source_nulls_v2 AS DOUBLE), 0), 0) * 100 END AS DOUBLE)) as relative_diff_percent,FORMAT_STRING('%.2f%%', CAST(0.05 * 100 AS DOUBLE)) AS tolerance_percent,
  CASE WHEN CASE WHEN source_nulls_v2 = 0 THEN target_nulls_v2 = 0 ELSE COALESCE(ABS(target_nulls_v2 - source_nulls_v2) / NULLIF(CAST(source_nulls_v2 AS DOUBLE), 0), 0) <= 0.05 END THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS null_validation_v2))) as result_payload,
  ( COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.02 AND CASE WHEN source_nulls_v1 = 0 THEN target_nulls_v1 = 0 ELSE COALESCE(ABS(target_nulls_v1 - source_nulls_v1) / NULLIF(CAST(source_nulls_v1 AS DOUBLE), 0), 0) <= 0.05 END AND CASE WHEN source_nulls_v2 = 0 THEN target_nulls_v2 = 0 ELSE COALESCE(ABS(target_nulls_v2 - source_nulls_v2) / NULLIF(CAST(source_nulls_v2 AS DOUBLE), 0), 0) <= 0.05 END) AS overall_validation_passed
FROM
count_metrics CROSS JOIN null_metrics_v1 CROSS JOIN null_metrics_v2
;

INSERT INTO `datapact`.`results`.`run_history` (task_key, status, run_id, job_id, job_name, job_start_ts, validation_begin_ts, validation_complete_ts, source_catalog, source_schema, source_table, target_catalog, target_schema, target_table, business_domain, business_owner, business_priority, expected_sla_hours, estimated_impact_usd, result_payload)
SELECT 't_counts_nulls', CASE WHEN overall_validation_passed THEN 'SUCCESS' ELSE 'FAILURE' END,
:run_id, :job_id, 'counts_nulls_job', :job_start_ts, validation_begin_ts, validation_complete_ts, source_catalog, source_schema, source_table, target_catalog, target_schema, target_table, business_domain, business_owner, business_priority, expected_sla_hours, estimated_impact_usd, result_payload FROM final_metrics_view;

SELECT RAISE_ERROR(CONCAT('DataPact validation failed for task: t_counts_nulls. Payload: \n', to_json(result_payload, map('pretty', 'true')))) FROM final_metrics_view WHERE overall_validation_passed = false;

SELECT to_json(result_payload, map('pretty', 'true')) AS result FROM final_metrics_view WHERE overall_validation_passed = true;
