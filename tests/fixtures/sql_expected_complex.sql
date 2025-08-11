CREATE OR REPLACE TEMP VIEW final_metrics_view AS
WITH
count_metrics AS (
  SELECT
    (SELECT COUNT(1) FROM `cat`.`sch`.`src`) AS source_count,
    (SELECT COUNT(1) FROM `cat`.`sch`.`tgt`) AS target_count
)
,
row_hash_metrics AS (
  SELECT COUNT(1) AS total_compared_rows,
         COALESCE(SUM(CASE WHEN s.row_hash <> t.row_hash THEN 1 ELSE 0 END), 0) AS mismatch_count
  FROM (
    SELECT 
`id1`, `id2`,
      md5(to_json(struct(*))) AS row_hash
    FROM `cat`.`sch`.`src`
  ) s
  INNER JOIN (
    SELECT 
`id1`, `id2`,
      md5(to_json(struct(*))) AS row_hash
    FROM `cat`.`sch`.`tgt`
  ) t
  ON s.`id1` = t.`id1` AND s.`id2` = t.`id2`)
,
null_metrics_v1 AS (
  SELECT
    SUM(CASE WHEN s.`v1` IS NULL THEN 1 ELSE 0 END) as source_nulls_v1,
    SUM(CASE WHEN t.`v1` IS NULL THEN 1 ELSE 0 END) as target_nulls_v1,
    COUNT(1) as total_compared_v1
  FROM `cat`.`sch`.`src` s JOIN `cat`.`sch`.`tgt` t
    ON s.`id1` = t.`id1` AND s.`id2` = t.`id2`),
null_metrics_v2 AS (
  SELECT
    SUM(CASE WHEN s.`v2` IS NULL THEN 1 ELSE 0 END) as source_nulls_v2,
    SUM(CASE WHEN t.`v2` IS NULL THEN 1 ELSE 0 END) as target_nulls_v2,
    COUNT(1) as total_compared_v2
  FROM `cat`.`sch`.`src` s JOIN `cat`.`sch`.`tgt` t
    ON s.`id1` = t.`id1` AND s.`id2` = t.`id2`)
,
agg_metrics_v1_SUM AS (
  SELECT 
    TRY_CAST((SELECT SUM(`v1`) FROM `cat`.`sch`.`src`) AS DECIMAL(38, 6)) AS source_value_v1_SUM,
    TRY_CAST((SELECT SUM(`v1`) FROM `cat`.`sch`.`tgt`) AS DECIMAL(38, 6)) AS target_value_v1_SUM
),
agg_metrics_v1_AVG AS (
  SELECT 
    TRY_CAST((SELECT AVG(`v1`) FROM `cat`.`sch`.`src`) AS DECIMAL(38, 6)) AS source_value_v1_AVG,
    TRY_CAST((SELECT AVG(`v1`) FROM `cat`.`sch`.`tgt`) AS DECIMAL(38, 6)) AS target_value_v1_AVG
),
agg_metrics_v2_SUM AS (
  SELECT 
    TRY_CAST((SELECT SUM(`v2`) FROM `cat`.`sch`.`src`) AS DECIMAL(38, 6)) AS source_value_v2_SUM,
    TRY_CAST((SELECT SUM(`v2`) FROM `cat`.`sch`.`tgt`) AS DECIMAL(38, 6)) AS target_value_v2_SUM
)
SELECT 
  parse_json(to_json(struct(
    struct(
      FORMAT_NUMBER(source_count, '#,##0') AS source_count,
      FORMAT_NUMBER(target_count, '#,##0') AS target_count,
      FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,
      FORMAT_STRING('%.2f%%', CAST(0.05 * 100 AS DOUBLE)) AS tolerance_percent,
      CASE WHEN COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.05 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS count_validation, 
    struct(
      FORMAT_NUMBER(total_compared_rows, '#,##0') AS compared_rows,
      FORMAT_NUMBER(mismatch_count, '#,##0') AS mismatch_count,
      FORMAT_STRING('%.2f%%', CAST(COALESCE((mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)), 0) * 100 AS DOUBLE)) as mismatch_percent,
      FORMAT_STRING('%.2f%%', CAST(1e-06 * 100 AS DOUBLE)) AS threshold_percent,
      CASE WHEN COALESCE((mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)), 0) <= 1e-06 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS row_hash_validation, 
    struct(
      FORMAT_NUMBER(source_nulls_v1, '#,##0') AS source_nulls,
      FORMAT_NUMBER(target_nulls_v1, '#,##0') AS target_nulls,FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_nulls_v1 - target_nulls_v1) / NULLIF(CAST(total_compared_v1 AS DOUBLE), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,FORMAT_STRING('%.2f%%', CAST(0.0 * 100 AS DOUBLE)) AS threshold_percent,
  CASE WHEN COALESCE(ABS(source_nulls_v1 - target_nulls_v1) / NULLIF(CAST(total_compared_v1 AS DOUBLE), 0), 0) <= 0.0 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS null_validation_v1, 
    struct(
      FORMAT_NUMBER(source_nulls_v2, '#,##0') AS source_nulls,
      FORMAT_NUMBER(target_nulls_v2, '#,##0') AS target_nulls,FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_nulls_v2 - target_nulls_v2) / NULLIF(CAST(total_compared_v2 AS DOUBLE), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,FORMAT_STRING('%.2f%%', CAST(0.0 * 100 AS DOUBLE)) AS threshold_percent,
  CASE WHEN COALESCE(ABS(source_nulls_v2 - target_nulls_v2) / NULLIF(CAST(total_compared_v2 AS DOUBLE), 0), 0) <= 0.0 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS null_validation_v2, 
    struct(
      FORMAT_NUMBER(source_value_v1_SUM, '#,##0.00') as source_value,
      FORMAT_NUMBER(target_value_v1_SUM, '#,##0.00') as target_value,
      FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_value_v1_SUM - target_value_v1_SUM) / NULLIF(ABS(CAST(source_value_v1_SUM AS DOUBLE)), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,
      FORMAT_STRING('%.2f%%', CAST(0.0 * 100 AS DOUBLE)) AS tolerance_percent,
      CASE WHEN COALESCE(ABS(source_value_v1_SUM - target_value_v1_SUM) / NULLIF(ABS(CAST(source_value_v1_SUM AS DOUBLE)), 0), 0) <= 0.0 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS agg_validation_v1_SUM, 
    struct(
      FORMAT_NUMBER(source_value_v1_AVG, '#,##0.00') as source_value,
      FORMAT_NUMBER(target_value_v1_AVG, '#,##0.00') as target_value,
      FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_value_v1_AVG - target_value_v1_AVG) / NULLIF(ABS(CAST(source_value_v1_AVG AS DOUBLE)), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,
      FORMAT_STRING('%.2f%%', CAST(1e-06 * 100 AS DOUBLE)) AS tolerance_percent,
      CASE WHEN COALESCE(ABS(source_value_v1_AVG - target_value_v1_AVG) / NULLIF(ABS(CAST(source_value_v1_AVG AS DOUBLE)), 0), 0) <= 1e-06 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS agg_validation_v1_AVG, 
    struct(
      FORMAT_NUMBER(source_value_v2_SUM, '#,##0.00') as source_value,
      FORMAT_NUMBER(target_value_v2_SUM, '#,##0.00') as target_value,
      FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_value_v2_SUM - target_value_v2_SUM) / NULLIF(ABS(CAST(source_value_v2_SUM AS DOUBLE)), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,
      FORMAT_STRING('%.2f%%', CAST(0.1 * 100 AS DOUBLE)) AS tolerance_percent,
      CASE WHEN COALESCE(ABS(source_value_v2_SUM - target_value_v2_SUM) / NULLIF(ABS(CAST(source_value_v2_SUM AS DOUBLE)), 0), 0) <= 0.1 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS agg_validation_v2_SUM))) as result_payload,
  ( COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.05 AND  COALESCE((mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)), 0) <= 1e-06 AND COALESCE(ABS(source_nulls_v1 - target_nulls_v1) / NULLIF(CAST(total_compared_v1 AS DOUBLE), 0), 0) <= 0.0 AND COALESCE(ABS(source_nulls_v2 - target_nulls_v2) / NULLIF(CAST(total_compared_v2 AS DOUBLE), 0), 0) <= 0.0 AND  COALESCE(ABS(source_value_v1_SUM - target_value_v1_SUM) / NULLIF(ABS(CAST(source_value_v1_SUM AS DOUBLE)), 0), 0) <= 0.0 AND  COALESCE(ABS(source_value_v1_AVG - target_value_v1_AVG) / NULLIF(ABS(CAST(source_value_v1_AVG AS DOUBLE)), 0), 0) <= 1e-06 AND  COALESCE(ABS(source_value_v2_SUM - target_value_v2_SUM) / NULLIF(ABS(CAST(source_value_v2_SUM AS DOUBLE)), 0), 0) <= 0.1) AS overall_validation_passed
FROM
count_metrics CROSS JOIN row_hash_metrics CROSS JOIN null_metrics_v1 CROSS JOIN null_metrics_v2 CROSS JOIN agg_metrics_v1_SUM CROSS JOIN agg_metrics_v1_AVG CROSS JOIN agg_metrics_v2_SUM
;

INSERT INTO `cat`.`res`.`history` (task_key, status, run_id, job_id, job_name, timestamp, result_payload)
SELECT 't_complex', CASE WHEN overall_validation_passed THEN 'SUCCESS' ELSE 'FAILURE' END,
:run_id, :job_id, 'complex_job', current_timestamp(), result_payload FROM final_metrics_view;

SELECT RAISE_ERROR(CONCAT('DataPact validation failed for task: t_complex. Payload: \n', to_json(result_payload, map('pretty', 'true')))) FROM final_metrics_view WHERE overall_validation_passed = false;

SELECT to_json(result_payload, map('pretty', 'true')) AS result FROM final_metrics_view WHERE overall_validation_passed = true;