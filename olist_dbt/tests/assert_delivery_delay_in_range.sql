select * from {{ ref('int_orders_enriched') }}
where order_status = 'delivered' 
and (coalesce(delivery_delay_days,-1) < 0
or coalesce(delivery_delay_days,-1) > 365)