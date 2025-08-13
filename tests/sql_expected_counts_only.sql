CREATE OR REPLACE TEMP VIEW final_metrics_view AS
WITH
count_metrics AS (
  SELECT
    (SELECT COUNT(1) FROM `c`.`s`.`src`) AS source_count,
    (SELECT COUNT(1) FROM `cat`.`sch`.`tgt`) AS target_count
)



SELECT
  parse_json(to_json(struct(    
    'c' AS source_catalog,
    's' AS source_schema,
    'src' AS source_table,
    'cat' AS target_catalog,
    'sch' AS target_schema,
    'tgt' AS target_table,
    current_timestamp() AS started_at
,
    struct(
      FORMAT_NUMBER(source_count, '#,##0') AS source_count,
      FORMAT_NUMBER(target_count, '#,##0') AS target_count,
      FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,
      FORMAT_STRING('%.2f%%', CAST(0.05 * 100 AS DOUBLE)) AS tolerance_percent,
      CASE WHEN COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.05 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS count_validation,
    current_timestamp() AS completed_at
  ))) as result_payload,
  ( COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.05) AS overall_validation_passed
FROM
count_metrics
;

INSERT INTO `cat`.`res`.`history` (task_key, status, run_id, job_id, job_name, timestamp, result_payload)
SELECT 't2_counts', CASE WHEN overall_validation_passed THEN 'SUCCESS' ELSE 'FAILURE' END,
:run_id, :job_id, 'counts_job', current_timestamp(), result_payload FROM final_metrics_view;

SELECT RAISE_ERROR(CONCAT('DataPact validation failed for task: t2_counts. Payload: \n', to_json(result_payload, map('pretty', 'true')))) FROM final_metrics_view WHERE overall_validation_passed = false;

SELECT to_json(result_payload, map('pretty', 'true')) AS result FROM final_metrics_view WHERE overall_validation_passed = true;