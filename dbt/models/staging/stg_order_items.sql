-- One row per order line, flattened from order_created.payload:items. Product attributes
-- are only those in the event: sku, product_name, quantity, unit_price.
select
    o.order_id,
    o.event_id as order_event_id,
    item.index + 1 as line_number,
    item.value:sku::varchar as sku,
    item.value:product_name::varchar as product_name,
    item.value:quantity::number(10, 0) as quantity,
    {{ to_money('item.value:unit_price') }} as unit_price,
    (item.value:quantity::number(10, 0) * {{ to_money('item.value:unit_price') }})::number(14, 2)
        as line_total
from {{ ref('stg_order_created') }} as o,
    lateral flatten(input => o.items) as item
