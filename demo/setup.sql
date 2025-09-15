-- DataPact: Enterprise Data Quality Demonstration Environment
-- Executive-Ready Demo: Simulating Real-World Data Quality Challenges at Scale
-- This setup creates a comprehensive data ecosystem mirroring Fortune 500 enterprise scenarios
-- Step 1: Enterprise Data Foundation
CREATE CATALOG IF NOT EXISTS datapact_demo_catalog COMMENT 'Enterprise Data Quality Validation Catalog';
CREATE SCHEMA IF NOT EXISTS datapact_demo_catalog.source_data COMMENT 'Production Source Systems';
CREATE SCHEMA IF NOT EXISTS datapact_demo_catalog.target_data COMMENT 'Analytics & BI Target Systems';
CREATE SCHEMA IF NOT EXISTS datapact_demo_catalog.reference_data COMMENT 'Master Data & Reference Tables';
-- =========================================================================================
-- Domain: CUSTOMER EXPERIENCE & CRM
-- Business Context: Customer 360 data powering $2B annual revenue
-- Critical for: Personalization, Marketing Campaigns, Customer Retention Analytics
-- =========================================================================================
-- Table 1: Customer Master (1M+ customers, enterprise CRM backbone)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.users AS
SELECT id AS user_id,
  md5(CAST(id AS STRING)) || '@' || CASE
    WHEN rand() < 0.3 THEN 'gmail.com'
    WHEN rand() < 0.5 THEN 'outlook.com'
    WHEN rand() < 0.7 THEN 'yahoo.com'
    ELSE 'corporate.com'
  END AS email,
  CASE
    WHEN rand() < 0.45 THEN 'USA'
    WHEN rand() < 0.65 THEN 'CAN'
    WHEN rand() < 0.80 THEN 'GBR'
    WHEN rand() < 0.90 THEN 'AUS'
    ELSE 'DEU'
  END AS country,
  date_add('2020-01-01', CAST(rand() * 1460 AS INT)) AS signup_date,
  CASE
    WHEN id % 100 < 5 THEN NULL
    WHEN id % 50 = 0 THEN 'churned'
    WHEN id % 30 = 0 THEN 'at_risk'
    WHEN id % 20 = 0 THEN 'premium'
    ELSE 'active'
  END AS status,
  (rand() * 1000)::INT as total_logins,
  ROUND(rand() * 100000, 2) as lifetime_value,
  CASE
    WHEN rand() < 0.2 THEN 'Enterprise'
    WHEN rand() < 0.5 THEN 'SMB'
    ELSE 'Consumer'
  END as segment,
  ROUND(rand() * 10, 1) as satisfaction_score
FROM (
    SELECT explode(sequence(1, 1000000)) AS id
  );
-- Simulating real-world ETL issues and data quality problems
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.users AS TABLE datapact_demo_catalog.source_data.users;
-- Data Quality Issue #1: PII Masking gone wrong (5% records affected)
UPDATE datapact_demo_catalog.target_data.users
SET email = md5(email) || '@masked.com'
WHERE user_id % 20 = 0;
-- Data Quality Issue #2: Failed CDC batch (2% records missing)
DELETE FROM datapact_demo_catalog.target_data.users
WHERE user_id % 50 = 0
  AND status != 'premium';
-- Premium customers protected
-- Data Quality Issue #3: Duplicate prevention failure (0.3% duplicates)
INSERT INTO datapact_demo_catalog.target_data.users
SELECT id AS user_id,
  'dup_' || md5(CAST(id AS STRING)) || '@duplicate.com' AS email,
  'UNKNOWN' as country,
  current_date() AS signup_date,
  'unverified' as status,
  0 as total_logins,
  0 as lifetime_value,
  'Unknown' as segment,
  NULL as satisfaction_score
FROM (
    SELECT explode(sequence(1000001, 1003000)) AS id
  );
-- Data Quality Issue #4: Timezone conversion error (10% date corruption)
UPDATE datapact_demo_catalog.target_data.users
SET signup_date = NULL
WHERE user_id % 10 = 0;
-- Data Quality Issue #5: Status mapping failure (4% status corruption)
UPDATE datapact_demo_catalog.target_data.users
SET status = NULL
WHERE user_id % 25 = 0;
-- Data Quality Issue #6: Metric calculation error (Regional aggregation issue)
UPDATE datapact_demo_catalog.target_data.users
SET total_logins = total_logins * 2
WHERE country = 'CAN';
-- Canadian metrics doubled due to dual-system reporting
-- Table 2: Financial Transactions (5M+ records, $500M+ transaction volume)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.transactions AS
SELECT uuid() AS transaction_id,
  (abs(rand()) * 999999 + 1)::INT AS user_id,
  ROUND(
    (rand() * 2000 + 10) * CASE
      WHEN rand() < 0.1 THEN 5 -- High-value transactions
      WHEN rand() < 0.3 THEN 2 -- Medium-value
      ELSE 1
    END,
    2
  ) AS amount,
  uuid() as product_id,
  CASE
    WHEN rand() < 0.4 THEN 'CREDIT_CARD'
    WHEN rand() < 0.6 THEN 'DEBIT_CARD'
    WHEN rand() < 0.8 THEN 'PAYPAL'
    WHEN rand() < 0.9 THEN 'APPLE_PAY'
    ELSE 'BANK_TRANSFER'
  END as payment_method,
  CASE
    WHEN rand() < 0.02 THEN 'FAILED'
    WHEN rand() < 0.05 THEN 'PENDING'
    WHEN rand() < 0.08 THEN 'REFUNDED'
    ELSE 'COMPLETED'
  END as transaction_status,
  current_timestamp() - (rand() * 365 * 2) * INTERVAL '1 day' as transaction_ts,
  ROUND(rand() * 100, 4) as tax_amount,
  ROUND(rand() * 50, 2) as shipping_cost,
  CASE
    WHEN rand() < 0.15 THEN 'PROMO2024'
    ELSE NULL
  END as discount_code
FROM (
    SELECT explode(sequence(1, 5000000)) AS id
  );
-- Clean replication to demonstrate successful validation
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.transactions AS TABLE datapact_demo_catalog.source_data.transactions;
-- This table passes all validations - demonstrating system works correctly when data is clean
-- Table 3: Product Catalog (5K SKUs, supporting $2B revenue)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.products AS
SELECT uuid() as product_id,
  CONCAT(
    CASE
      WHEN rand() < 0.2 THEN 'Premium '
      WHEN rand() < 0.4 THEN 'Standard '
      ELSE ''
    END,
    CASE
      WHEN id % 5 = 0 THEN 'Electronics '
      WHEN id % 5 = 1 THEN 'Apparel '
      WHEN id % 5 = 2 THEN 'Home '
      WHEN id % 5 = 3 THEN 'Sports '
      ELSE 'Beauty '
    END,
    'Item ',
    id
  ) as product_name,
  ROUND(
    (rand() * 500 + 10) * CASE
      WHEN rand() < 0.2 THEN 3
      ELSE 1
    END,
    2
  ) as price,
  ROUND(rand() * 0.3, 2) as margin_rate,
  (rand() * 1000)::INT as inventory_count,
  CASE
    WHEN rand() < 0.1 THEN 'DISCONTINUED'
    WHEN rand() < 0.2 THEN 'LOW_STOCK'
    ELSE 'ACTIVE'
  END as status,
  date_add('2020-01-01', CAST(rand() * 1000 AS INT)) as launch_date,
  ROUND(rand() * 5, 1) as avg_rating,
  (rand() * 10000)::INT as review_count
FROM (
    SELECT explode(sequence(1, 5000)) as id
  );
-- Simulating approved price updates (within tolerance)
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.products AS TABLE datapact_demo_catalog.source_data.products;
-- Business-approved price adjustments (2% of products, within 2.5% tolerance)
UPDATE datapact_demo_catalog.target_data.products
SET price = ROUND(price * 1.08, 2),
  -- 8% increase for inflation
  margin_rate = ROUND(margin_rate * 0.95, 2) -- Margin compression
WHERE abs(hash(product_id)) % 50 = 0;
-- =========================================================================================
-- Domain: MARKETING & CUSTOMER ACQUISITION
-- Business Context: $50M annual marketing budget, 200+ campaigns
-- Critical for: ROI Analysis, Attribution, Budget Optimization
-- =========================================================================================
-- Table 4: Marketing Campaigns (Driving 40% of new customer acquisition)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.campaigns AS
SELECT id as campaign_id,
  CONCAT(
    CASE
      WHEN id % 4 = 0 THEN 'Q1_2024_'
      WHEN id % 4 = 1 THEN 'Q2_2024_'
      WHEN id % 4 = 2 THEN 'Q3_2024_'
      ELSE 'Q4_2024_'
    END,
    CASE
      WHEN rand() < 0.3 THEN 'Brand_Awareness_'
      WHEN rand() < 0.6 THEN 'Lead_Gen_'
      WHEN rand() < 0.8 THEN 'Retention_'
      ELSE 'Product_Launch_'
    END,
    id
  ) as campaign_name,
  CASE
    WHEN id < 20 THEN 'completed'
    WHEN id < 80 THEN 'active'
    ELSE 'planned'
  END as status,
  ROUND(rand() * 500000 + 10000, 2) as budget,
  ROUND(rand() * 400000 + 5000, 2) as spend_to_date,
  (rand() * 10000)::INT as leads_generated,
  (rand() * 1000)::INT as conversions,
  ROUND(rand() * 0.05, 4) as conversion_rate,
  CASE
    WHEN rand() < 0.3 THEN 'DIGITAL'
    WHEN rand() < 0.6 THEN 'SOCIAL'
    WHEN rand() < 0.8 THEN 'EMAIL'
    ELSE 'TRADITIONAL'
  END as channel
FROM (
    SELECT explode(sequence(1, 200)) as id
  );
-- Campaign status synchronization issues
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.campaigns AS TABLE datapact_demo_catalog.source_data.campaigns;
-- Status update lag from marketing automation platform
UPDATE datapact_demo_catalog.target_data.campaigns
SET status = 'completed',
  spend_to_date = budget * 0.98 -- Near-full spend
WHERE campaign_id IN (10, 20, 30, 40, 50);
-- Table 5: Digital Ad Spend (Daily granularity, $50M annual)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.ad_spend AS
SELECT id as ad_id,
  (id % 200 + 1) as campaign_id,
  date_add('2024-01-01', id % 365) as spend_date,
  ROUND(
    (rand() * 5000 + 100) * CASE
      WHEN id % 7 IN (5, 6) THEN 1.5 -- Weekend boost
      WHEN id % 30 = 0 THEN 3 -- Month-end push
      ELSE 1
    END,
    2
  ) as daily_spend,
  (rand() * 100000)::INT as impressions,
  (rand() * 5000)::INT as clicks,
  ROUND(rand() * 0.05, 4) as ctr,
  ROUND(rand() * 10 + 0.5, 2) as cpc,
  CASE
    WHEN rand() < 0.4 THEN 'GOOGLE'
    WHEN rand() < 0.7 THEN 'META'
    WHEN rand() < 0.85 THEN 'LINKEDIN'
    ELSE 'TIKTOK'
  END as platform
FROM (
    SELECT explode(sequence(1, 10000)) as id
  );
-- Currency conversion rounding issues
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.ad_spend AS
SELECT ad_id,
  campaign_id,
  spend_date,
  ROUND(daily_spend * 1.001, 2) as daily_spend,
  -- 0.1% FX conversion drift
  impressions,
  clicks,
  ctr,
  ROUND(cpc * 1.001, 2) as cpc,
  -- Matching FX drift
  platform
FROM datapact_demo_catalog.source_data.ad_spend;
-- =========================================================================================
-- Domain: HUMAN CAPITAL MANAGEMENT
-- Business Context: 10,000+ employees, $1B+ annual payroll
-- Critical for: Compliance, Talent Analytics, Succession Planning
-- =========================================================================================
-- Table 6: Employee Master (Supporting HR compliance and analytics)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.employees AS
SELECT id as employee_id,
  CONCAT(
    'emp-',
    LPAD(CAST(id AS STRING), 6, '0'),
    '@enterprise.com'
  ) as email,
  CONCAT(
    CASE
      WHEN rand() < 0.3 THEN 'John'
      WHEN rand() < 0.6 THEN 'Sarah'
      WHEN rand() < 0.8 THEN 'Michael'
      ELSE 'Emily'
    END,
    ' ',
    CASE
      WHEN rand() < 0.3 THEN 'Smith'
      WHEN rand() < 0.6 THEN 'Johnson'
      WHEN rand() < 0.8 THEN 'Williams'
      ELSE 'Brown'
    END
  ) as full_name,
  date_add('2015-01-01', CAST(rand() * 3000 AS INT)) as hire_date,
  CASE
    WHEN id % 100 = 0 THEN date_add(current_date(), - CAST(rand() * 30 AS INT))
    ELSE NULL
  END as termination_date,
  CASE
    WHEN rand() < 0.1 THEN 'EXECUTIVE'
    WHEN rand() < 0.25 THEN 'SENIOR_MANAGER'
    WHEN rand() < 0.45 THEN 'MANAGER'
    WHEN rand() < 0.70 THEN 'SENIOR_IC'
    ELSE 'IC'
  END as level,
  CASE
    WHEN rand() < 0.2 THEN 'ENGINEERING'
    WHEN rand() < 0.35 THEN 'SALES'
    WHEN rand() < 0.50 THEN 'MARKETING'
    WHEN rand() < 0.65 THEN 'OPERATIONS'
    WHEN rand() < 0.75 THEN 'FINANCE'
    WHEN rand() < 0.85 THEN 'HR'
    ELSE 'LEGAL'
  END as department,
  ROUND(50000 + rand() * 200000, -3) as base_salary,
  ROUND(rand() * 0.3, 2) as bonus_percentage,
  ROUND(rand() * 10, 1) as performance_rating
FROM (
    SELECT explode(sequence(1, 10000)) as id
  );
-- HR system sync issues - typical month-end scenario
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.employees AS
SELECT *
FROM datapact_demo_catalog.source_data.employees
WHERE employee_id <= 9950 -- Recent terminations not yet synced
  OR employee_id > 10000;
-- Keep any overflow
-- New hires added but not in source system yet
INSERT INTO datapact_demo_catalog.target_data.employees
SELECT 10000 + id as employee_id,
  CONCAT('newhire-', id, '@enterprise.com') as email,
  CONCAT('New Hire ', id) as full_name,
  current_date() as hire_date,
  NULL as termination_date,
  'IC' as level,
  'ENGINEERING' as department,
  75000 as base_salary,
  0.15 as bonus_percentage,
  NULL as performance_rating -- No rating yet
FROM (
    SELECT explode(sequence(1, 25)) as id
  );
-- Table 7: Compensation Bands (Global pay equity framework)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.salary_bands (
    band_id STRING,
    band_name STRING,
    level STRING,
    min_salary DECIMAL(10, 2),
    max_salary DECIMAL(10, 2),
    midpoint DECIMAL(10, 2),
    geographic_modifier DECIMAL(3, 2)
  );
INSERT INTO datapact_demo_catalog.source_data.salary_bands
VALUES (
    'IC1',
    'Individual Contributor I',
    'IC',
    50000,
    75000,
    62500,
    1.0
  ),
  (
    'IC2',
    'Individual Contributor II',
    'IC',
    70000,
    100000,
    85000,
    1.0
  ),
  (
    'SIC1',
    'Senior Individual Contributor',
    'SENIOR_IC',
    95000,
    140000,
    117500,
    1.0
  ),
  (
    'M1',
    'Manager I',
    'MANAGER',
    110000,
    160000,
    135000,
    1.0
  ),
  (
    'M2',
    'Manager II',
    'MANAGER',
    140000,
    200000,
    170000,
    1.0
  ),
  (
    'SM1',
    'Senior Manager',
    'SENIOR_MANAGER',
    180000,
    250000,
    215000,
    1.0
  ),
  (
    'DIR',
    'Director',
    'EXECUTIVE',
    220000,
    320000,
    270000,
    1.0
  ),
  (
    'VP',
    'Vice President',
    'EXECUTIVE',
    300000,
    450000,
    375000,
    1.0
  ),
  (
    'SVP',
    'Senior Vice President',
    'EXECUTIVE',
    400000,
    600000,
    500000,
    1.0
  ),
  (
    'C',
    'C-Suite',
    'EXECUTIVE',
    500000,
    1000000,
    750000,
    1.0
  );
-- Reference data - should be identical
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.salary_bands AS TABLE datapact_demo_catalog.source_data.salary_bands;
-- This is master data that should never change without approval
-- =========================================================================================
-- Domain: FINANCIAL REPORTING & COMPLIANCE
-- Business Context: SOX Compliance, SEC Reporting, $10B+ in managed assets
-- Critical for: Financial Close, Audit, Regulatory Reporting
-- =========================================================================================
-- Table 8: General Ledger (Core financial records, audit-critical)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.gl_postings AS
SELECT id as posting_id,
  CONCAT('JE-2024-', LPAD(CAST(id AS STRING), 8, '0')) as journal_entry,
  CASE
    WHEN rand() < 0.3 THEN '1000-CASH'
    WHEN rand() < 0.5 THEN '1200-AR'
    WHEN rand() < 0.7 THEN '2000-AP'
    WHEN rand() < 0.85 THEN '3000-REVENUE'
    ELSE '4000-EXPENSE'
  END as account_code,
  ROUND(
    (rand() * 100000 - 50000) * CASE
      WHEN id % 100 = 0 THEN 10 -- Large adjustments
      ELSE 1
    END,
    2
  ) as amount,
  CASE
    WHEN amount >= 0 THEN 'DEBIT'
    ELSE 'CREDIT'
  END as entry_type,
  date_add('2024-01-01', id % 365) as posting_date,
  CASE
    WHEN id % 30 = 0 THEN 'ADJUSTMENT'
    WHEN id % 50 = 0 THEN 'ACCRUAL'
    WHEN id % 100 = 0 THEN 'RECLASS'
    ELSE 'STANDARD'
  END as posting_type,
  CASE
    WHEN rand() < 0.95 THEN 'POSTED'
    WHEN rand() < 0.98 THEN 'PENDING'
    ELSE 'REVERSED'
  END as status,
  CONCAT('FY2024-Q', CEILING((id % 365) / 91.25)) as fiscal_period
FROM (
    SELECT explode(sequence(1, 100000)) as id
  );
-- Critical financial data with reconciliation issues
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.gl_postings AS
SELECT *
FROM datapact_demo_catalog.source_data.gl_postings
WHERE posting_id <= 97500;
-- 2.5% of entries missing (failed batch load)
-- This represents a critical issue that would trigger SOX compliance alerts
-- =========================================================================================
-- Domain: DIGITAL OPERATIONS & INFRASTRUCTURE
-- Business Context: 99.95% SLA, 100M+ daily events, $50M infrastructure
-- Critical for: Site Reliability, Performance, Security Monitoring
-- =========================================================================================
-- Table 9: Web Analytics (10M daily page views - enterprise traffic volume)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.page_views AS
SELECT (abs(rand()) * 999999 + 1)::INT AS user_id,
  uuid() as session_id,
  CASE
    WHEN rand() < 0.2 THEN '/home'
    WHEN rand() < 0.35 THEN '/products/' || (rand() * 100)::INT
    WHEN rand() < 0.5 THEN '/checkout'
    WHEN rand() < 0.65 THEN '/search'
    WHEN rand() < 0.75 THEN '/account'
    WHEN rand() < 0.85 THEN '/support'
    ELSE '/blog/' || (rand() * 50)::INT
  END as page_path,
  current_timestamp() - (rand() * 24) * INTERVAL '1 hour' as view_ts,
  ROUND(rand() * 30, 2) as time_on_page_seconds,
  CASE
    WHEN rand() < 0.7 THEN 'DIRECT'
    WHEN rand() < 0.85 THEN 'ORGANIC'
    WHEN rand() < 0.95 THEN 'PAID'
    ELSE 'SOCIAL'
  END as traffic_source,
  CASE
    WHEN rand() < 0.6 THEN 'DESKTOP'
    WHEN rand() < 0.9 THEN 'MOBILE'
    ELSE 'TABLET'
  END as device_type
FROM (
    SELECT explode(sequence(1, 1000000)) as id
  );
-- Timestamp drift issue (common in distributed logging)
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.page_views AS
SELECT user_id,
  session_id,
  page_path,
  date_add(view_ts, 1) as view_ts,
  -- 1 day timestamp drift
  time_on_page_seconds,
  traffic_source,
  device_type
FROM datapact_demo_catalog.source_data.page_views;
-- Table 10: Application Logs (High-volume, no PK, critical for debugging)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.log_messages (
    ts TIMESTAMP,
    level STRING,
    service STRING,
    message STRING,
    trace_id STRING,
    user_id INT,
    latency_ms DOUBLE
  );
INSERT INTO datapact_demo_catalog.source_data.log_messages
SELECT current_timestamp() - (rand() * 24) * INTERVAL '1 hour' as ts,
  CASE
    WHEN rand() < 0.7 THEN 'INFO'
    WHEN rand() < 0.85 THEN 'WARN'
    WHEN rand() < 0.95 THEN 'ERROR'
    ELSE 'DEBUG'
  END as level,
  CASE
    WHEN rand() < 0.3 THEN 'api-gateway'
    WHEN rand() < 0.5 THEN 'auth-service'
    WHEN rand() < 0.7 THEN 'payment-service'
    WHEN rand() < 0.85 THEN 'inventory-service'
    ELSE 'notification-service'
  END as service,
  CASE
    level
    WHEN 'ERROR' THEN 'Failed to process request: ' || uuid()
    WHEN 'WARN' THEN 'High latency detected for endpoint: /api/v2/' || (rand() * 10)::INT
    ELSE 'Request processed successfully'
  END as message,
  uuid() as trace_id,
  CASE
    WHEN rand() < 0.8 THEN (rand() * 1000000)::INT
    ELSE NULL
  END as user_id,
  CASE
    WHEN level = 'ERROR' THEN rand() * 5000 + 1000
    ELSE rand() * 500
  END as latency_ms
FROM (
    SELECT explode(sequence(1, 10000)) as id
  );
-- Log shipping delay causing count mismatch
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.log_messages AS TABLE datapact_demo_catalog.source_data.log_messages;
-- Additional logs in target (from different service)
INSERT INTO datapact_demo_catalog.target_data.log_messages
SELECT current_timestamp() as ts,
  'INFO' as level,
  'data-pipeline' as service,
  'ETL job completed: ' || id as message,
  uuid() as trace_id,
  NULL as user_id,
  rand() * 1000 as latency_ms
FROM (
    SELECT explode(sequence(1, 50)) as id
  );
-- Table 11: Audit Trail (Empty - representing fresh compliance period)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.empty_audits (
    audit_id STRING,
    action_type STRING,
    user_id INT,
    timestamp TIMESTAMP,
    details STRING
  ) COMMENT 'Audit trail for SOX compliance - cleared quarterly';
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.empty_audits (
    audit_id STRING,
    action_type STRING,
    user_id INT,
    timestamp TIMESTAMP,
    details STRING
  ) COMMENT 'Audit trail replica - should match source exactly';
-- Table 12: API Response Codes (Reference data with categorization issues)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.status_codes AS
SELECT id as code,
  CASE
    id
    WHEN 200 THEN 'OK - Request successful'
    WHEN 201 THEN 'Created - Resource created'
    WHEN 204 THEN 'No Content - Request successful, no content'
    WHEN 400 THEN 'Bad Request - Invalid request'
    WHEN 401 THEN 'Unauthorized - Authentication required'
    WHEN 403 THEN 'Forbidden - Access denied'
    WHEN 404 THEN 'Not Found - Resource not found'
    WHEN 429 THEN 'Too Many Requests - Rate limit exceeded'
    WHEN 500 THEN 'Internal Server Error'
    WHEN 502 THEN 'Bad Gateway - Upstream server error'
    WHEN 503 THEN 'Service Unavailable - Temporary outage'
    ELSE 'HTTP Status ' || id
  END as description,
  CASE
    WHEN id BETWEEN 200 AND 299 THEN 'SUCCESS'
    WHEN id BETWEEN 300 AND 399 THEN 'REDIRECT'
    WHEN id BETWEEN 400 AND 499 THEN 'CLIENT_ERROR'
    WHEN id BETWEEN 500 AND 599 THEN 'SERVER_ERROR'
    WHEN id % 10 = 0 THEN NULL -- Some uncategorized
    ELSE 'UNKNOWN'
  END as category,
  CASE
    WHEN id BETWEEN 500 AND 599 THEN 'HIGH'
    WHEN id IN (401, 403, 429) THEN 'MEDIUM'
    ELSE 'LOW'
  END as severity
FROM (
    SELECT explode(sequence(100, 599)) as id
  )
WHERE id IN (
    200,
    201,
    204,
    400,
    401,
    403,
    404,
    429,
    500,
    502,
    503,
    301,
    302,
    304
  );
-- Data quality issue: Category mapping failures
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.status_codes AS TABLE datapact_demo_catalog.source_data.status_codes;
-- Introduce null category for common codes (mapping failure)
UPDATE datapact_demo_catalog.target_data.status_codes
SET category = NULL
WHERE code IN (200, 404, 500);
-- Critical codes losing categorization
-- =========================================================================================
-- Domain: REGULATORY COMPLIANCE & RISK MANAGEMENT
-- Business Context: GDPR, CCPA, SOX compliance, $100M+ potential exposure
-- =========================================================================================
-- Table 13: Data Privacy Requests (GDPR/CCPA compliance tracking)
CREATE OR REPLACE TABLE datapact_demo_catalog.source_data.privacy_requests AS
SELECT uuid() as request_id,
  (rand() * 1000000)::INT as user_id,
  CASE
    WHEN rand() < 0.4 THEN 'DATA_ACCESS'
    WHEN rand() < 0.7 THEN 'DATA_DELETION'
    WHEN rand() < 0.9 THEN 'DATA_PORTABILITY'
    ELSE 'CONSENT_WITHDRAWAL'
  END as request_type,
  CASE
    WHEN rand() < 0.7 THEN 'COMPLETED'
    WHEN rand() < 0.85 THEN 'IN_PROGRESS'
    WHEN rand() < 0.95 THEN 'PENDING'
    ELSE 'REJECTED'
  END as status,
  date_add('2024-01-01', CAST(rand() * 365 AS INT)) as request_date,
  CASE
    WHEN status = 'COMPLETED' THEN date_add(request_date, CAST(rand() * 30 AS INT))
    ELSE NULL
  END as completion_date,
  CASE
    WHEN rand() < 0.5 THEN 'GDPR'
    WHEN rand() < 0.8 THEN 'CCPA'
    ELSE 'OTHER'
  END as regulation,
  ROUND(rand() * 10, 1) as processing_days
FROM (
    SELECT explode(sequence(1, 5000)) as id
  );
CREATE OR REPLACE TABLE datapact_demo_catalog.target_data.privacy_requests AS TABLE datapact_demo_catalog.source_data.privacy_requests;
-- Compliance reporting delay
DELETE FROM datapact_demo_catalog.target_data.privacy_requests
WHERE request_date > date_add(current_date(), -7)
  AND status = 'PENDING';
-- Recent pending requests not yet synced
-- =========================================================================================
-- Additional Enterprise Reference Data
-- =========================================================================================
-- Currency Exchange Rates (for multi-national operations)
CREATE OR REPLACE TABLE datapact_demo_catalog.reference_data.exchange_rates AS
SELECT currency_pair,
  rate,
  date_add('2024-01-01', id % 365) as rate_date,
  'OFFICIAL' as rate_type,
  current_timestamp() as last_updated
FROM (
    SELECT CASE
        id % 5
        WHEN 0 THEN 'USD/EUR'
        WHEN 1 THEN 'USD/GBP'
        WHEN 2 THEN 'USD/CAD'
        WHEN 3 THEN 'USD/AUD'
        ELSE 'USD/JPY'
      END as currency_pair,
      CASE
        id % 5
        WHEN 0 THEN ROUND(0.85 + rand() * 0.1, 4)
        WHEN 1 THEN ROUND(0.79 + rand() * 0.05, 4)
        WHEN 2 THEN ROUND(1.35 + rand() * 0.1, 4)
        WHEN 3 THEN ROUND(1.52 + rand() * 0.1, 4)
        ELSE ROUND(149 + rand() * 5, 2)
      END as rate,
      id
    FROM (
        SELECT explode(sequence(1, 1825)) as id
      )
  );
-- Business Metrics Summary (Executive KPIs)
CREATE OR REPLACE TABLE datapact_demo_catalog.reference_data.business_metrics AS
SELECT date_add('2024-01-01', id) as metric_date,
  ROUND(2000000 + rand() * 500000, 2) as daily_revenue,
  (80000 + (rand() * 20000))::INT as daily_orders,
  (50000 + (rand() * 15000))::INT as daily_active_users,
  ROUND(0.02 + rand() * 0.03, 4) as conversion_rate,
  ROUND(25 + rand() * 75, 2) as average_order_value,
  ROUND(0.95 + rand() * 0.049, 4) as site_availability,
  ROUND(200 + rand() * 100, 0) as avg_response_time_ms,
  ROUND(0.05 + rand() * 0.03, 4) as cart_abandonment_rate,
  ROUND(7 + rand() * 2, 1) as customer_satisfaction_score
FROM (
    SELECT explode(sequence(0, 364)) as id
  );
-- Final setup: Create indexes for performance (optional)
-- Note: These would be created in production for query optimization
-- Display summary message
SELECT 'DataPact Enterprise Demo Environment Setup Complete!' as message,
  COUNT(DISTINCT table_name) as tables_created,
  'Ready for validation testing' as status
FROM information_schema.tables
WHERE table_schema IN ('source_data', 'target_data', 'reference_data')
  AND table_catalog = 'datapact_demo_catalog';
