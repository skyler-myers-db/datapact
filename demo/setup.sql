-- DataPact Demo Environment Setup Script
-- This script is idempotent and uses fully-qualified, deterministic statements
-- to comply with the Databricks Statement Execution API.

-- Step 1: Create the catalog.
CREATE CATALOG IF NOT EXISTS datapact_demo_catalog;

-- Step 2: Create the schemas.
CREATE SCHEMA IF NOT EXISTS datapact_demo_catalog.source_data;
CREATE SCHEMA IF NOT EXISTS datapact_demo_catalog.target_data;

-- Step 3: Create and populate source tables.
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.users AS
SELECT
  id AS user_id,
  md5(CAST(id AS STRING)) || '@example.com' AS email,
  CASE
    WHEN rand() < 0.6 THEN 'USA'
    WHEN rand() < 0.8 THEN 'CAN'
    WHEN rand() < 0.9 THEN 'MEX'
    ELSE 'GBR'
  END AS country,
  date_add('2022-01-01', CAST(rand() * 365 AS INT)) AS signup_date
FROM (SELECT explode(sequence(1, 10000)) AS id);

CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.transactions AS
SELECT
  uuid() AS transaction_id,
  (abs(rand()) * 9999 + 1)::INT AS user_id,
  (rand() * 500 + 5)::DECIMAL(10, 2) AS amount
FROM (SELECT explode(sequence(1, 50000)) AS id);

-- Step 4: Create the target users table as a copy of the source.
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.users AS
  TABLE datapact_demo_catalog.source_data.users;

-- Step 5: Introduce intentional, DETERMINISTIC discrepancies into the target table.
-- NOTE: We use the modulo operator (%) instead of `ORDER BY rand()` because
-- non-deterministic functions are not allowed in UPDATE/DELETE subqueries.

-- 1. Update 5% of emails (1 in every 20 users).
UPDATE datapact_demo_catalog.target_data.users
SET email = md5(email) || '@changed.com'
WHERE user_id % 20 = 0;

-- 2. Delete 2% of users (1 in every 50 users).
DELETE FROM datapact_demo_catalog.target_data.users
WHERE user_id % 50 = 0;

-- 3. Add 300 new users that don't exist in the source.
INSERT INTO datapact_demo_catalog.target_data.users
SELECT
  id AS user_id,
  'newuser_' || md5(CAST(id AS STRING)) || '@new.com' AS email,
  'N/A' as country,
  current_date() AS signup_date
FROM (SELECT explode(sequence(10001, 10300)) AS id);

-- 4. Set 10% of signup_dates to NULL (1 in every 10 users).
UPDATE datapact_demo_catalog.target_data.users
SET signup_date = NULL
WHERE user_id % 10 = 0;

-- Step 6: Create the target transactions table as an identical copy.
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.transactions AS
  TABLE datapact_demo_catalog.source_data.transactions;
