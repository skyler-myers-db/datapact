-- DataPact: Comprehensive Demo Environment Setup Script
-- This script creates a rich, realistic dataset to showcase all of DataPact's features.

-- Step 1: Create the catalog and schemas.
CREATE CATALOG IF NOT EXISTS datapact_demo_catalog;
CREATE SCHEMA IF NOT EXISTS datapact_demo_catalog.source_data;
CREATE SCHEMA IF NOT EXISTS datapact_demo_catalog.target_data;

CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.users AS
SELECT
  id AS user_id,
  md5(CAST(id AS STRING)) || '@example.com' AS email,
  CASE WHEN rand() < 0.8 THEN 'USA' WHEN rand() < 0.95 THEN 'CAN' ELSE 'MEX' END AS country,
  date_add('2022-01-01', CAST(rand() * 365 AS INT)) AS signup_date,
  CASE WHEN id % 100 = 0 THEN NULL ELSE 'active' END AS status,
  (rand() * 1000)::INT AS total_logins
FROM (SELECT explode(sequence(1, 10000)) AS id);

CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.transactions AS
SELECT
  uuid() AS transaction_id,
  (abs(rand()) * 9999 + 1)::INT AS user_id,
  (rand() * 500 + 5)::DECIMAL(10, 2) AS amount,
  CASE WHEN rand() < 0.6 THEN 'electronics' WHEN rand() < 0.9 THEN 'groceries' ELSE 'apparel' END as category
FROM (SELECT explode(sequence(1, 50000)) AS id);

-- Step 2: Create target tables as copies
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.users TABLE datapact_demo_catalog.source_data.users;
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.transactions TABLE datapact_demo_catalog.source_data.transactions;

-- Step 3: Introduce intentional, DETERMINISTIC discrepancies for the demo
-- USERS table discrepancies (designed to FAIL specific tests)
-- 1. Update 5% of emails (for hash check failure)
UPDATE datapact_demo_catalog.target_data.users SET email = md5(email) || '@changed.com' WHERE user_id % 20 = 0;
-- 2. Delete 2% of users (for count check failure)
DELETE FROM datapact_demo_catalog.target_data.users WHERE user_id % 50 = 0;
-- 3. Add 300 new users (for count check failure)
INSERT INTO datapact_demo_catalog.target_data.users (user_id, email, country, signup_date, status, total_logins)
SELECT id, 'newuser_' || md5(CAST(id AS STRING)) || '@new.com', 'N/A', current_date(), 'new', 0
FROM (SELECT explode(sequence(10001, 10300)) AS id);
-- 4. Set 10% of signup_dates to NULL (for null check failure)
UPDATE datapact_demo_catalog.target_data.users SET signup_date = NULL WHERE user_id % 10 = 0;
-- 5. Change some statuses to NULL (for another null check failure)
UPDATE datapact_demo_catalog.target_data.users SET status = NULL WHERE user_id % 33 = 0;
-- 6. Inflate total_logins (for SUM aggregate check failure)
UPDATE datapact_demo_catalog.target_data.users SET total_logins = total_logins + 50 WHERE country = 'CAN';

-- TRANSACTIONS table discrepancies (designed to PASS all tests)

CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.transactions AS TABLE datapact_demo_catalog.source_data.transactions;

-- Step 4: Showcase advanced features: SCD-style updates and selective hashing.
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.products AS SELECT uuid() as product_id, 'Product ' || id as product_name, (rand() * 100)::DECIMAL(10, 2) as price FROM (SELECT explode(sequence(1, 5000)) as id);
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.products AS TABLE datapact_demo_catalog.source_data.products;
-- Update 2% of product prices, simulating a real SCD change.
UPDATE datapact_demo_catalog.target_data.products SET price = price * 1.1 WHERE abs(hash(product_id)) % 50 = 0;

CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.page_views AS SELECT (abs(rand()) * 999999 + 1)::INT AS user_id, uuid() as session_id, '/page/' || (rand()*100)::INT as page_path, current_timestamp() as view_ts FROM (SELECT explode(sequence(1, 2000000)) as id);
-- Target table has identical core data but different view_ts for all rows.
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.page_views AS SELECT user_id, session_id, page_path, date_add(view_ts, 1) as view_ts FROM datapact_demo_catalog.source_data.page_views;

-- Step 5: Showcase edge cases: No primary key and empty tables.
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.log_messages (ts TIMESTAMP, level STRING, message STRING);
INSERT INTO datapact_demo_catalog.source_data.log_messages VALUES (current_timestamp(), 'INFO', 'System started');
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.log_messages AS TABLE datapact_demo_catalog.source_data.log_messages;
INSERT INTO datapact_demo_catalog.target_data.log_messages VALUES (current_timestamp(), 'INFO', 'Target system also started');

CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.empty_audits (id INT, name STRING);
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.empty_audits (id INT, name STRING);
