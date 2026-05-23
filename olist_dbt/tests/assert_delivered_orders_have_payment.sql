-- This test FAILS if any rows are returned
select order_id
from {{ ref('int_orders_enriched') }}
where order_status = 'delivered'
and payment_value = 0