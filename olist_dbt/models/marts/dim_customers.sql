select 
customer_id,
customer_city,
customer_state,
count(order_id) as total_orders,
sum(total_item_value) as total_revenue,
min(purchased_at) as first_order_date,  
max(purchased_at) as last_order_date
from {{ref('int_orders_enriched')}}
group by 1,2,3