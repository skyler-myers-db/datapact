-- DataPact: Comprehensive Demo Environment Setup Script
-- This script creates a rich, realistic dataset to showcase all of DataPact's features.

-- Step 1: Create the catalog and schemas.
CREATE CATALOG IF NOT EXISTS datapact_demo_catalog;
CREATE SCHEMA IF NOT EXISTS datapact_demo_catalog.source_data;
CREATE SCHEMA IF NOT EXISTS datapact_demo_catalog.target_data;

-- Step 2: Create large-scale Users and Transactions tables.
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.users AS
SELECT
  id AS user_id,
  md5(CAST(id AS STRING)) || '@example.com' AS email,
  CASE WHEN rand() < 0.6 THEN 'USA' WHEN rand() < 0.8 THEN 'CAN' ELSE 'GBR' END AS country,
  date_add('2022-01-01', CAST(rand() * 730 AS INT)) AS signup_date
FROM (SELECT explode(sequence(1, 1000000)) AS id);

CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.transactions AS
SELECT
  uuid() AS transaction_id,
  (abs(rand()) * 999999 + 1)::INT AS user_id,
  (rand() * 500 + 5)::DECIMAL(10, 2) AS amount,
  uuid() as product_id,
  current_timestamp() - (rand() * 365 * 2) * INTERVAL '1 day' as transaction_ts
FROM (SELECT explode(sequence(1, 5000000)) AS id);

-- Step 3: Create target tables with intentional, deterministic discrepancies.
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.users AS TABLE datapact_demo_catalog.source_data.users;
UPDATE datapact_demo_catalog.target_data.users SET email = md5(email) || '@changed.com' WHERE user_id % 20 = 0;
DELETE FROM datapact_demo_catalog.target_data.users WHERE user_id % 50 = 0;
INSERT INTO datapact_demo_catalog.target_data.users SELECT id AS user_id, 'newuser_' || md5(CAST(id AS STRING)) || '@new.com' AS email, 'N/A' as country, current_date() AS signup_date FROM (SELECT explode(sequence(1000001, 1003000)) AS id);
UPDATE datapact_demo_catalog.target_data.users SET signup_date = NULL WHERE user_id % 10 = 0;

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
