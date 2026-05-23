select *
from {{ ref('dim_customers') }}
where coalesce(total_revenue,0) = 0 
or coalesce(total_orders,0) =0