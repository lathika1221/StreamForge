-- One row per order line, with the order's currency, date and outcome. Product
-- attributes are only those present in the events (sku, product_name, unit_price).
select
    i.order_id,
    i.line_number,
    i.sku,
    i.product_name,
    i.quantity,
    i.unit_price,
    i.line_total,
    o.currency,
    o.created_at_utc,
    o.order_date,
    o.current_status,
    o.payment_status,
    (o.payment_status = 'succeeded' and o.cancelled_at_utc is null) as is_net_revenue
from {{ ref('stg_order_items') }} as i
inner join {{ ref('fct_orders') }} as o on i.order_id = o.order_id
