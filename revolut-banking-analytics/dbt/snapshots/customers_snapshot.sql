{% snapshot customers_snapshot %}
{{ config(target_schema='revolut_snapshots', unique_key='customer_id', strategy='timestamp', updated_at='updated_at', invalidate_hard_deletes=false) }}
select * from {{ ref('slv_customers') }}
{% endsnapshot %}
