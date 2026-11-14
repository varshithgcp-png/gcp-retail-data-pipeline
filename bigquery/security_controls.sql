-- Fit Analytics BigQuery security examples.
-- Replace PROJECT_ID, group names and regions before deployment.

-- Example 1: UK analysts can only see UK rows.
CREATE OR REPLACE ROW ACCESS POLICY uk_customer_access
ON `PROJECT_ID.fitanalytics_reporting.vw_customer_fit_profile`
GRANT TO ("group:uk-fit-analysts@example.com")
FILTER USING (region = 'UK');

-- Example 2: DE analysts can only see DE rows.
CREATE OR REPLACE ROW ACCESS POLICY de_customer_access
ON `PROJECT_ID.fitanalytics_reporting.vw_customer_fit_profile`
GRANT TO ("group:de-fit-analysts@example.com")
FILTER USING (region = 'DE');

-- For column-level security, create a policy-tag taxonomy and attach
-- appropriate policy tags to sensitive table columns using BigQuery
-- schema configuration / Data Catalog. Policy tags cannot be attached
-- with a simple CREATE TABLE DDL statement in this project.

-- Authorized views should be granted access at the dataset level after
-- the reporting view is created. Do not give broad curated-dataset read
-- access to general reporting users when an authorized view is enough.
