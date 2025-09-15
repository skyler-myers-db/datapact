
CREATE OR REPLACE TEMP VIEW final_metrics_view AS
WITH
count_metrics AS (
  SELECT
    (SELECT COUNT(1) FROM `c`.`s`.`a`) AS source_count,
    (SELECT COUNT(1) FROM `c`.`s`.`b`) AS target_count
)



SELECT
  current_timestamp() AS started_at,
  'c' AS source_catalog,
  's' AS source_schema,
  'a' AS source_table,
  'c' AS target_catalog,
  's' AS target_schema,
  'b' AS target_table,
  parse_json(to_json(struct(
    struct(
      FORMAT_NUMBER(source_count, '#,##0') AS source_count,
      FORMAT_NUMBER(target_count, '#,##0') AS target_count,
      FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,
      FORMAT_STRING('%.2f%%', CAST(0.01 * 100 AS DOUBLE)) AS tolerance_percent,
      CASE WHEN COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.01 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS count_validation))) as result_payload,
  ( COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.01) AS overall_validation_passed,
  current_timestamp() AS completed_at
FROM
count_metrics
;

INSERT INTO `datapact`.`results`.`run_history` (task_key, status, run_id, job_id, job_name, timestamp, started_at, completed_at, source_catalog, source_schema, source_table, target_catalog, target_schema, target_table, result_payload)
SELECT 't_counts', CASE WHEN overall_validation_passed THEN 'SUCCESS' ELSE 'FAILURE' END,
:run_id, :job_id, 'counts_job', current_timestamp(), started_at, completed_at, source_catalog, source_schema, source_table, target_catalog, target_schema, target_table, result_payload FROM final_metrics_view;

SELECT RAISE_ERROR(CONCAT('DataPact validation failed for task: t_counts. Payload: \n', to_json(result_payload, map('pretty', 'true')))) FROM final_metrics_view WHERE overall_validation_passed = false;

SELECT to_json(result_payload, map('pretty', 'true')) AS result FROM final_metrics_view WHERE overall_validation_passed = true;
