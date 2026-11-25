-- Daily order metrics per currency (UTC order date). Amounts are never summed across
-- currencies. Orders without an order_created event have no date/currency and are
-- excluded here (they are reported in ops_order_anomalies).
--   gross_order_value: all created orders
--   paid_value:        orders whose latest payment succeeded
--   net_revenue:       paid orders that were not cancelled
with orders as (
    select * from {{ ref('fct_orders') }} where has_order_created
),

daily as (
    select
        order_date,
        currency,
        count(*) as orders_created,
        count_if(payment_status = 'succeeded') as orders_paid,
        count_if(payment_status = 'failed') as orders_payment_failed,
        count_if(cancelled_at_utc is not null) as orders_cancelled,
        count_if(shipped_at_utc is not null) as orders_shipped,
        count_if(current_status = 'awaiting_payment') as orders_awaiting_payment,
        sum(order_total) as gross_order_value,
        sum(iff(payment_status = 'succeeded', order_total, 0)) as paid_value,
        sum(iff(payment_status = 'succeeded' and cancelled_at_utc is null, order_total, 0))
            as net_revenue
    from orders
    group by order_date, currency
)

select
    *,
    round(div0(orders_paid, orders_paid + orders_payment_failed), 4) as payment_success_rate,
    round(div0(orders_cancelled, orders_created), 4) as cancellation_rate
from daily
