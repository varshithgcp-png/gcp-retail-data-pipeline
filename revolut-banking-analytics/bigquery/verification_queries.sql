-- Replace PROJECT_ID. Run after Day 0 -> Day 1 -> Day 2.
SELECT business_date, source_file, file_hash, source_row_count, bronze_row_count, processing_status, pipeline_status
FROM `PROJECT_ID.revolut_bronze.CONTROL_TABLE_bronze`
WHERE entity_name='__FILE__'
ORDER BY business_date;
SELECT customer_id, COUNT(*) cnt FROM `PROJECT_ID.revolut_silver.slv_customers` GROUP BY customer_id HAVING cnt>1;
SELECT account_id, COUNT(*) cnt FROM `PROJECT_ID.revolut_silver.slv_accounts` GROUP BY account_id HAVING cnt>1;
SELECT customer_id FROM `PROJECT_ID.revolut_gold.dim_customer` WHERE is_current AND customer_id IN (369,370,371,372) ORDER BY customer_id;
SELECT account_id FROM `PROJECT_ID.revolut_gold.dim_account` WHERE is_current AND account_id IN (481,482,483,484) ORDER BY account_id;
SELECT customer_id, city, risk_rating, effective_from, effective_to, is_current FROM `PROJECT_ID.revolut_gold.dim_customer` WHERE customer_id IN (102,369) ORDER BY customer_id,effective_from;
SELECT account_id, balance, status, effective_from, effective_to, is_current FROM `PROJECT_ID.revolut_gold.dim_account` WHERE account_id IN (12,481) ORDER BY account_id,effective_from;
SELECT rule_name, severity, COUNT(*) issue_count FROM `PROJECT_ID.revolut_silver.dq_quarantine` GROUP BY rule_name,severity ORDER BY issue_count DESC;
SELECT * FROM `PROJECT_ID.revolut_gold.kpi_top5_customers_by_transaction_volume`;
SELECT * FROM `PROJECT_ID.revolut_gold.kpi_product_revenue_contribution`;
SELECT * FROM `PROJECT_ID.revolut_gold.kpi_inactive_accounts` LIMIT 100;
SELECT * FROM `PROJECT_ID.revolut_gold.kpi_suspicious_transactions` LIMIT 100;
SELECT * FROM `PROJECT_ID.revolut_gold.kpi_customer_account_history` WHERE customer_id IN (102,369) ORDER BY customer_id,customer_effective_from;
SELECT * FROM `PROJECT_ID.revolut_gold.kpi_loan_repayment_health` ORDER BY repayment_health DESC;
SELECT * FROM `PROJECT_ID.revolut_gold.kpi_card_product_adoption` LIMIT 100;
