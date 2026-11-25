-- Daily sales per product (sku) and currency. net_value counts only paid, non-cancelled
-- orders (same definition as net_revenue in agg_daily_orders).
select
    order_date,
    sku,
    currency,
    max_by(product_name, created_at_utc) as product_name,
    sum(quantity) as units,
    count(distinct order_id) as orders,
    sum(line_total) as gross_value,
    sum(iff(is_net_revenue, line_total, 0)) as net_value
from {{ ref('fct_order_items') }}
group by order_date, sku, currency
