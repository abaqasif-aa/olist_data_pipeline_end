select 
seller_id,
count(order_id) as total_orders,
sum(is_late)*100/count(order_id) as delayed_order_percentage,
sum(total_item_value) as total_revenue,
avg(delivery_delay_days) as avg_delivery_delay
from {{ref('int_orders_enriched')}}
group by 1