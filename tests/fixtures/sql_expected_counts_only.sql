DECLARE VARIABLE validation_begin_ts TIMESTAMP DEFAULT current_timestamp();

CREATE OR REPLACE TEMP VIEW final_metrics_view AS

WITH
source_stats AS (
  SELECT
    COUNT(1) AS source_count
  FROM `c`.`s`.`a`
)
,
target_stats AS (
  SELECT
    COUNT(1) AS target_count
  FROM `c`.`s`.`b`
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
      FORMAT_NUMBER(source_count, 0) AS source_count,
      FORMAT_NUMBER(target_count, 0) AS target_count,
      FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,
      FORMAT_STRING('%.2f%%', CAST(0.01 * 100 AS DOUBLE)) AS tolerance_percent,
      CASE WHEN COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.01 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS count_validation))) as result_payload,
  ( COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.01) AS overall_validation_passed
FROM
source_stats CROSS JOIN target_stats
;

INSERT INTO `datapact`.`results`.`run_history` (task_key, status, run_id, job_id, job_name, job_start_ts, validation_begin_ts, validation_complete_ts, source_catalog, source_schema, source_table, target_catalog, target_schema, target_table, business_domain, business_owner, business_priority, expected_sla_hours, estimated_impact_usd, result_payload)
SELECT 't_counts', CASE WHEN overall_validation_passed THEN 'SUCCESS' ELSE 'FAILURE' END,
:run_id, :job_id, 'counts_job', :job_start_ts, validation_begin_ts, validation_complete_ts, source_catalog, source_schema, source_table, target_catalog, target_schema, target_table, business_domain, business_owner, business_priority, expected_sla_hours, estimated_impact_usd, result_payload FROM final_metrics_view;

SELECT RAISE_ERROR(CONCAT('DataPact validation failed for task: t_counts. Payload: \n', to_json(result_payload, map('pretty', 'true')))) FROM final_metrics_view WHERE overall_validation_passed = false;

SELECT to_json(result_payload, map('pretty', 'true')) AS result FROM final_metrics_view WHERE overall_validation_passed = true;
