select account_id from {{ ref('dim_account') }} where is_current group by account_id having count(*) != 1
