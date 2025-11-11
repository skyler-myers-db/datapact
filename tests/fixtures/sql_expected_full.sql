DECLARE VARIABLE validation_begin_ts TIMESTAMP DEFAULT current_timestamp();

CREATE OR REPLACE TEMP VIEW final_metrics_view AS
WITH
count_metrics AS (
  SELECT
    (SELECT COUNT(1) FROM `c`.`s`.`a`) AS source_count,
    (SELECT COUNT(1) FROM `c`.`s`.`b`) AS target_count
)
,
row_hash_metrics AS (
  SELECT COUNT(1) AS total_compared_rows,
         COALESCE(SUM(CASE WHEN s.row_hash <> t.row_hash THEN 1 ELSE 0 END), 0) AS mismatch_count
  FROM (
    SELECT
`id`,
      md5(to_json(struct(`id`, `v`))) AS row_hash
    FROM `c`.`s`.`a`
  ) s
  INNER JOIN (
    SELECT
`id`,
      md5(to_json(struct(`id`, `v`))) AS row_hash
    FROM `c`.`s`.`b`
  ) t
  ON s.`id` = t.`id`)
,
null_metrics_v AS (
  SELECT
    SUM(CASE WHEN s.`v` IS NULL THEN 1 ELSE 0 END) as source_nulls_v,
    SUM(CASE WHEN t.`v` IS NULL THEN 1 ELSE 0 END) as target_nulls_v,
    COUNT(1) as total_compared_v
  FROM `c`.`s`.`a` s JOIN `c`.`s`.`b` t
    ON s.`id` = t.`id`)
,
agg_metrics_v_SUM AS (
  SELECT
    TRY_CAST((SELECT SUM(`v`) FROM `c`.`s`.`a`) AS DECIMAL(38, 6)) AS source_value_v_SUM,
    TRY_CAST((SELECT SUM(`v`) FROM `c`.`s`.`b`) AS DECIMAL(38, 6)) AS target_value_v_SUM
)
SELECT
  validation_begin_ts AS validation_begin_ts,
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
    'id' AS configured_primary_keys
, 
    struct(
      FORMAT_NUMBER(source_count, '#,##0') AS source_count,
      FORMAT_NUMBER(target_count, '#,##0') AS target_count,
      FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,
      FORMAT_STRING('%.2f%%', CAST(0.01 * 100 AS DOUBLE)) AS tolerance_percent,
      CASE WHEN COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.01 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS count_validation,
    struct(
      FORMAT_NUMBER(total_compared_rows, '#,##0') AS compared_rows,
      FORMAT_NUMBER(mismatch_count, '#,##0') AS mismatch_count,
      FORMAT_STRING('%.2f%%', CAST(COALESCE((mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)), 0) * 100 AS DOUBLE)) as mismatch_percent,
      FORMAT_STRING('%.2f%%', CAST(0.0 * 100 AS DOUBLE)) AS tolerance_percent,
      CASE WHEN COALESCE((mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)), 0) <= 0.0 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS row_hash_validation,
    struct(
      FORMAT_NUMBER(source_nulls_v, '#,##0') AS source_nulls,
      FORMAT_NUMBER(target_nulls_v, '#,##0') AS target_nulls,FORMAT_STRING('%.2f%%', CAST(
        CASE
          WHEN source_nulls_v = 0 AND target_nulls_v > 0 THEN 100.0
          WHEN source_nulls_v = 0 THEN 0.0
          WHEN target_nulls_v = 0 AND source_nulls_v > 0 THEN 100.0
          ELSE COALESCE(ABS(source_nulls_v - target_nulls_v) / NULLIF(CAST(source_nulls_v AS DOUBLE), 0), 0) * 100
        END AS DOUBLE
      )) as relative_diff_percent,FORMAT_STRING('%.2f%%', CAST(0.02 * 100 AS DOUBLE)) AS tolerance_percent,
  CASE WHEN     CASE
      WHEN source_nulls_v = 0 THEN target_nulls_v = 0
      ELSE COALESCE(ABS(source_nulls_v - target_nulls_v) / NULLIF(CAST(source_nulls_v AS DOUBLE), 0), 0) <= 0.02
    END
 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS null_validation_v    ,
    struct(
      FORMAT_NUMBER(source_value_v_SUM, '#,##0.00') as source_value,
      FORMAT_NUMBER(target_value_v_SUM, '#,##0.00') as target_value,
      FORMAT_STRING('%.2f%%', CAST((CASE
  WHEN source_value_v_SUM IS NULL OR target_value_v_SUM IS NULL THEN NULL
  WHEN source_value_v_SUM = target_value_v_SUM THEN CAST(0 AS DECIMAL(38, 12))
  WHEN GREATEST(ABS(source_value_v_SUM), ABS(target_value_v_SUM)) = 0 THEN CAST(0 AS DECIMAL(38, 12))
  ELSE
    CAST(ABS(source_value_v_SUM - target_value_v_SUM) AS DECIMAL(38, 12)) /
    CAST(GREATEST(ABS(source_value_v_SUM), ABS(target_value_v_SUM)) AS DECIMAL(38, 12))
END) * 100 AS DOUBLE)) as relative_diff_percent,
      FORMAT_STRING('%.2f%%', CAST(0.05 * 100 AS DOUBLE)) AS tolerance_percent,
      CASE WHEN COALESCE(CASE
  WHEN source_value_v_SUM IS NULL OR target_value_v_SUM IS NULL THEN NULL
  WHEN source_value_v_SUM = target_value_v_SUM THEN CAST(0 AS DECIMAL(38, 12))
  WHEN GREATEST(ABS(source_value_v_SUM), ABS(target_value_v_SUM)) = 0 THEN CAST(0 AS DECIMAL(38, 12))
  ELSE
    CAST(ABS(source_value_v_SUM - target_value_v_SUM) AS DECIMAL(38, 12)) /
    CAST(GREATEST(ABS(source_value_v_SUM), ABS(target_value_v_SUM)) AS DECIMAL(38, 12))
END, CAST(0 AS DECIMAL(38, 12))) <= 0.05 THEN 'PASS' ELSE 'FAIL' END AS status
    ) AS agg_validation_v_SUM))) as result_payload,
  ( COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.01 AND  COALESCE((mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)), 0) <= 0.0 AND CASE
        WHEN source_nulls_v = 0 THEN target_nulls_v = 0
        ELSE COALESCE(ABS(source_nulls_v - target_nulls_v) / NULLIF(CAST(source_nulls_v AS DOUBLE), 0), 0) <= 0.02
      END AND  COALESCE(CASE
  WHEN source_value_v_SUM IS NULL OR target_value_v_SUM IS NULL THEN NULL
  WHEN source_value_v_SUM = target_value_v_SUM THEN CAST(0 AS DECIMAL(38, 12))
  WHEN GREATEST(ABS(source_value_v_SUM), ABS(target_value_v_SUM)) = 0 THEN CAST(0 AS DECIMAL(38, 12))
  ELSE
    CAST(ABS(source_value_v_SUM - target_value_v_SUM) AS DECIMAL(38, 12)) /
    CAST(GREATEST(ABS(source_value_v_SUM), ABS(target_value_v_SUM)) AS DECIMAL(38, 12))
END, CAST(0 AS DECIMAL(38, 12))) <= 0.05) AS overall_validation_passed
FROM
count_metrics CROSS JOIN row_hash_metrics CROSS JOIN null_metrics_v CROSS JOIN agg_metrics_v_SUM
;

INSERT INTO `datapact`.`results`.`run_history` (task_key, status, run_id, job_id, job_name, job_start_ts, validation_begin_ts, validation_complete_ts, source_catalog, source_schema, source_table, target_catalog, target_schema, target_table, business_domain, business_owner, business_priority, expected_sla_hours, estimated_impact_usd, result_payload)
SELECT 't1', CASE WHEN overall_validation_passed THEN 'SUCCESS' ELSE 'FAILURE' END,
:run_id, :job_id, 'job_name_here', :job_start_ts, validation_begin_ts, current_timestamp(), source_catalog, source_schema, source_table, target_catalog, target_schema, target_table, business_domain, business_owner, business_priority, expected_sla_hours, estimated_impact_usd, result_payload FROM final_metrics_view;

SELECT RAISE_ERROR(CONCAT('DataPact validation failed for task: t1. Payload: \n', to_json(result_payload, map('pretty', 'true')))) FROM final_metrics_view WHERE overall_validation_passed = false;

SELECT to_json(result_payload, map('pretty', 'true')) AS result FROM final_metrics_view WHERE overall_validation_passed = true;
