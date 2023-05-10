{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

{{ config(materialized='table') }}

with orders as (

     select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from PC_FIVETRAN_DB.STAGING.customer_orders_stg
),

payments as (

    select
        id as payment_id,
        order_id,
        payment_method,

        -- `amount` is currently stored in cents, so we convert it to dollars
        amount / 100 as amount

    from PC_FIVETRAN_DB.STAGING.payments_stg

),

order_payments as (

    select
        order_id,

        {% for payment_method in payment_methods -%}
        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
        {% endfor -%}

        sum(amount) as total_amount

    from payments

    group by order_id

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,

        {% for payment_method in payment_methods -%}

        order_payments.{{ payment_method }}_amount,

        {% endfor -%}

        order_payments.total_amount as amount

    from orders


    left join order_payments
        on orders.order_id = order_payments.order_id

)

select * from final