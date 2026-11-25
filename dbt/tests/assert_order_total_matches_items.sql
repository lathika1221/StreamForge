-- Error: an order's total must equal the sum of its item lines (the event contract
-- guarantees it; a failure means a transformation bug).
select o.order_id, o.order_total, sum(i.line_total) as items_total
from {{ ref('stg_order_created') }} as o
inner join {{ ref('stg_order_items') }} as i on o.order_id = i.order_id
group by o.order_id, o.order_total
having o.order_total <> sum(i.line_total)
