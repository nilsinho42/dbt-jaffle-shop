with 
payments as (
    select * 
    from {{ ref('stg_stripe__payments') }}
    where payment_status = 'success'
),
pivoted as 
(
    select order_id
        {%- set method_types=['bank_transfer', 'coupon', 'credit_card', 'gift_card'] -%}
        {%- for method in method_types -%}
            , sum(case when payment_method = '{{method}}' then payment_amount else 0 end) as {{method}}_amount
        {% endfor %}
--        sum(case when payment_method = 'bank_transfer' then payment_amount else 0 end) as bank_transfer_amount,
--        sum(case when payment_method = 'coupon' then payment_amount else 0 end) as coupon_amount,
--        sum(case when payment_method = 'credit_card' then payment_amount else 0 end) as credit_card_amount,
--        sum(case when payment_method = 'gift_card' then payment_amount else 0 end) as gift_card_amount
    from payments
    group by 1
)
select * 
from pivoted