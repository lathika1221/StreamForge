-- One row per shipment_dispatched event (deduplicated by event_id).
with deduplicated as (
    {{ dedupe_raw_events(source('raw', 'shipment_dispatched')) }}
)

select
    {{ event_envelope_columns() }},
    payload:shipment_id::varchar as shipment_id,
    payload:carrier::varchar as carrier,
    payload:tracking_number::varchar as tracking_number,
    try_to_date(payload:estimated_delivery_date::varchar) as estimated_delivery_date
from deduplicated
