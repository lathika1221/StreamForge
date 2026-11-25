-- One row per order_created event (deduplicated by event_id). Only attributes that exist
-- in the payload: customer_id (no customer details), shipping_country, currency, total.
with deduplicated as (
    {{ dedupe_raw_events(source('raw', 'order_created')) }}
)

select
    {{ event_envelope_columns() }},
    payload:customer_id::varchar as customer_id,
    payload:currency::varchar as currency,
    {{ to_money('payload:order_total') }} as order_total,
    payload:shipping_country::varchar as shipping_country,
    array_size(payload:items) as item_count,
    payload:items as items
from deduplicated
