{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        partition_by='purchased_at'
    )
}}

select *,
{{ extract_year('purchased_at') }} as order_year,
{{ extract_month('purchased_at') }} as order_month,
payment_value/total_item_value as revenue_per_item
from {{ ref('stg_orders_enriched') }} 


{% if is_incremental() %}
    where purchased_at > (
        select max(purchased_at) - interval '45 days' from {{ this }}
    )
{% endif %}