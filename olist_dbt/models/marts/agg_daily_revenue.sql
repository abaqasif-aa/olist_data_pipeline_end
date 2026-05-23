select 
order_year,
order_month,
product_category_name_english,
sum(payment_value) as total_revenue,
count(order_id) as total_orders,
avg(payment_value) as avg_revenue_per_order
from {{ ref('int_orders_enriched') }}
group by 1,2,3