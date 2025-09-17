select 
    order_id,
    sum(payment_amount) as total_amount
from {{ ref('stg_stripe__payments') }}
group by 1
having sum(payment_amount) < 0