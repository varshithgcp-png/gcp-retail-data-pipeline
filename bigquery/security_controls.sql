-- Fit Analytics BigQuery security examples.
-- Replace PROJECT_ID, group names and regions before deployment.

-- Row-level access policies apply to tables.
-- Example 1: UK analysts can only see UK customer-dimension rows.
CREATE OR REPLACE ROW ACCESS POLICY uk_customer_access
ON `PROJECT_ID.fitanalytics_curated.dim_customer`
GRANT TO ("group:uk-fit-analysts@example.com")
FILTER USING (region = 'UK');

-- Example 2: DE analysts can only see DE customer-dimension rows.
CREATE OR REPLACE ROW ACCESS POLICY de_customer_access
ON `PROJECT_ID.fitanalytics_curated.dim_customer`
GRANT TO ("group:de-fit-analysts@example.com")
FILTER USING (region = 'DE');

-- Reporting users should query approved reporting views rather than receive
-- broad read access to the full curated dataset.

-- Column-level security:
-- Create a policy-tag taxonomy and attach appropriate policy tags to
-- sensitive customer-profile columns using BigQuery schema configuration /
-- Data Catalog. Policy tags are not attached with plain CREATE TABLE DDL.

-- Authorized views:
-- Create the reporting view first, then authorize that view against the
-- source dataset using BigQuery dataset access configuration.
