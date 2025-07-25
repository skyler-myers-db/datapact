-- DataPact: Comprehensive "Enterprise-in-a-Box" Demo Setup Script
-- This script creates a rich, realistic dataset across 12 tables and multiple business domains.

-- Step 1: Foundational Setup
CREATE CATALOG IF NOT EXISTS datapact_demo_catalog;
CREATE SCHEMA IF NOT EXISTS datapact_demo_catalog.source_data;
CREATE SCHEMA IF NOT EXISTS datapact_demo_catalog.target_data;

-- =========================================================================================
-- Domain: SALES
-- Tables: users, transactions, products
-- =========================================================================================

-- Table 1: users (Large table with multiple discrepancy types)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.users AS
SELECT
  id AS user_id, md5(CAST(id AS STRING)) || '@example.com' AS email,
  CASE WHEN rand() < 0.6 THEN 'USA' WHEN rand() < 0.8 THEN 'CAN' ELSE 'GBR' END AS country,
  date_add('2022-01-01', CAST(rand() * 730 AS INT)) AS signup_date,
  CASE WHEN id % 100 < 5 THEN NULL ELSE 'active' END AS status,
  (rand() * 1000)::INT as total_logins
FROM (SELECT explode(sequence(1, 1000000)) AS id);

CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.users AS TABLE datapact_demo_catalog.source_data.users;
UPDATE datapact_demo_catalog.target_data.users SET email = md5(email) || '@changed.com' WHERE user_id % 20 = 0; -- 5% hash mismatch
DELETE FROM datapact_demo_catalog.target_data.users WHERE user_id % 50 = 0; -- 2% deleted
INSERT INTO datapact_demo_catalog.target_data.users SELECT id AS user_id, 'newuser_' || md5(CAST(id AS STRING)) || '@new.com' AS email, 'N/A' as country, current_date() AS signup_date, 'pending' as status, 0 as total_logins FROM (SELECT explode(sequence(1000001, 1003000)) AS id); -- ~0.3% added
UPDATE datapact_demo_catalog.target_data.users SET signup_date = NULL WHERE user_id % 10 = 0; -- 10% null mismatch
UPDATE datapact_demo_catalog.target_data.users SET status = NULL WHERE user_id % 25 = 0; -- 4% null mismatch
UPDATE datapact_demo_catalog.target_data.users SET total_logins = total_logins * 2 WHERE country = 'CAN'; -- Aggregate mismatch

-- Table 2: transactions (Large, identical table to show a clean pass)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.transactions AS
SELECT
  uuid() AS transaction_id, (abs(rand()) * 999999 + 1)::INT AS user_id,
  (rand() * 500 + 5)::DECIMAL(10, 2) AS amount, uuid() as product_id,
  current_timestamp() - (rand() * 365 * 2) * INTERVAL '1 day' as transaction_ts
FROM (SELECT explode(sequence(1, 5000000)) AS id);
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.transactions AS TABLE datapact_demo_catalog.source_data.transactions;

-- Table 3: products (SCD pattern with accepted threshold)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.products AS SELECT uuid() as product_id, 'Product ' || id as product_name, (rand() * 100)::DECIMAL(10, 2) as price FROM (SELECT explode(sequence(1, 5000)) as id);
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.products AS TABLE datapact_demo_catalog.source_data.products;
UPDATE datapact_demo_catalog.target_data.products SET price = price * 1.1 WHERE abs(hash(product_id)) % 50 = 0; -- 2% price change

-- =========================================================================================
-- Domain: MARKETING
-- Tables: campaigns, ad_spend
-- =========================================================================================

-- Table 4: campaigns (Subtle data change)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.campaigns AS
SELECT id as campaign_id, 'Campaign ' || id as campaign_name, 'active' as status FROM (SELECT explode(sequence(1, 100)) as id);
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.campaigns AS TABLE datapact_demo_catalog.source_data.campaigns;
UPDATE datapact_demo_catalog.target_data.campaigns SET status = 'finished' WHERE campaign_id IN (10, 20, 30); -- Small hash mismatch

-- Table 5: ad_spend (Subtle data type/rounding causing agg failure)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.ad_spend AS
SELECT id as ad_id, (rand() * 100 + 10)::DECIMAL(10, 2) as daily_spend FROM (SELECT explode(sequence(1, 1000)) as id);
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.ad_spend AS
SELECT ad_id, daily_spend * 1.001 as daily_spend FROM datapact_demo_catalog.source_data.ad_spend; -- Tiny aggregate difference

-- =========================================================================================
-- Domain: HR
-- Tables: employees, salary_bands
-- =========================================================================================

-- Table 6: employees (New joiners and leavers scenario)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.employees AS
SELECT id as employee_id, 'emp-' || id || '@corp.com' as email, date_add('2020-01-01', id * 10) as hire_date FROM (SELECT explode(sequence(1, 500)) as id);
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.employees AS SELECT * FROM datapact_demo_catalog.source_data.employees WHERE employee_id < 490; -- Leavers
INSERT INTO datapact_demo_catalog.target_data.employees VALUES (501, 'new.hire@corp.com', current_date()), (502, 'another.new@corp.com', current_date()); -- New joiners

-- Table 7: salary_bands (Passing test with different structure)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.salary_bands (band_id STRING, min_salary INT, max_salary INT);
INSERT INTO datapact_demo_catalog.source_data.salary_bands VALUES ('A', 50000, 70000), ('B', 70001, 90000);
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.salary_bands AS TABLE datapact_demo_catalog.source_data.salary_bands;

-- =========================================================================================
-- Domain: FINANCE
-- Tables: gl_postings
-- =========================================================================================

-- Table 8: gl_postings (Missing batch of records)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.gl_postings AS
SELECT id as posting_id, 'JE-' || id as journal_entry, (rand() * 10000)::DECIMAL(12,2) as amount, '2024-03-15' as posting_date FROM (SELECT explode(sequence(1, 20000)) as id);
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.gl_postings AS SELECT * FROM datapact_demo_catalog.source_data.gl_postings WHERE posting_id <= 19500; -- A batch of 500 postings is missing

-- =========================================================================================
-- Domain: LOGGING & OPS
-- Tables: page_views, log_messages, empty_audits, status_codes
-- =========================================================================================

-- Table 9: page_views (Selective hashing test)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.page_views AS SELECT (abs(rand()) * 999999 + 1)::INT AS user_id, uuid() as session_id, '/page/' || (rand()*100)::INT as page_path, current_timestamp() as view_ts FROM (SELECT explode(sequence(1, 2000000)) as id);
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.page_views AS SELECT user_id, session_id, page_path, date_add(view_ts, 1) as view_ts FROM datapact_demo_catalog.source_data.page_views; -- view_ts is different

-- Table 10: log_messages (No PK, count fails)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.log_messages (ts TIMESTAMP, level STRING, message STRING);
INSERT INTO datapact_demo_catalog.source_data.log_messages VALUES (current_timestamp(), 'INFO', 'System started');
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.log_messages AS TABLE datapact_demo_catalog.source_data.log_messages;
INSERT INTO datapact_demo_catalog.target_data.log_messages VALUES (current_timestamp(), 'INFO', 'Target system also started');

-- Table 11: empty_audits (Edge case: empty tables, should pass)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.empty_audits (id INT, name STRING);
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.empty_audits (id INT, name STRING);

-- Table 12: status_codes (Null validation test)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.status_codes AS
SELECT id as code, 'Description for ' || id as description, CASE WHEN id % 10 = 0 THEN NULL ELSE 'general' END as category
FROM (SELECT explode(sequence(100, 150)) as id);
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.status_codes AS TABLE datapact_demo_catalog.source_data.status_codes;
UPDATE datapact_demo_catalog.target_data.status_codes SET category = NULL WHERE code IN (101, 102, 103); -- Introduce extra nulls
