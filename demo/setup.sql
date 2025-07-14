-- DataPact Demo Environment Setup Script
-- This script is idempotent and can be run multiple times safely.

-- Step 1: Create the catalog first, if it doesn't already exist.
CREATE CATALOG IF NOT EXISTS datapact_demo_catalog;

-- Step 2: Now that the catalog is guaranteed to exist, select it.
USE CATALOG datapact_demo_catalog;

-- Step 3: Create the schemas, if they don't already exist.
CREATE SCHEMA IF NOT EXISTS source_data;
CREATE SCHEMA IF NOT EXISTS target_data;

-- Step 4: Create or replace the tables. This is already idempotent.
-- Generate a realistic source users table with 10,000 rows.
CREATE OR REPLACE TABLE source_data.users AS
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

-- Generate a realistic source transactions table with 50,000 rows.
CREATE OR REPLACE TABLE source_data.transactions AS
SELECT
  uuid() AS transaction_id,
  (abs(rand()) * 9999 + 1)::INT AS user_id,
  (rand() * 500 + 5)::DECIMAL(10, 2) AS amount
FROM (SELECT explode(sequence(1, 50000)) AS id);

-- Create the target users table based on the source.
CREATE OR REPLACE TABLE target_data.users AS TABLE source_data.users;

-- Introduce intentional discrepancies into the target users table.
-- 1. Update 5% of emails (500 users).
UPDATE target_data.users
SET email = md5(email) || '@changed.com'
WHERE user_id IN (SELECT user_id FROM source_data.users ORDER BY rand() LIMIT 500);

-- 2. Delete 2% of users (200 users).
DELETE FROM target_data.users
WHERE user_id IN (SELECT user_id FROM source_data.users ORDER BY rand() LIMIT 200);

-- 3. Add 300 new users that don't exist in the source.
INSERT INTO target_data.users
SELECT
  id AS user_id,
  'newuser_' || md5(CAST(id AS STRING)) || '@new.com' AS email,
  'N/A' as country,
  current_date() AS signup_date
FROM (SELECT explode(sequence(10001, 10300)) AS id);

-- 4. Set 10% of signup_dates to NULL (1000 users).
UPDATE target_data.users
SET signup_date = NULL
WHERE user_id IN (SELECT user_id FROM source_data.users ORDER BY rand() LIMIT 1000);

-- Create the target transactions table as an identical copy.
CREATE OR REPLACE TABLE target_data.transactions AS TABLE source_data.transactions;
