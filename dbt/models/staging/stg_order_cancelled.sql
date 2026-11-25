-- One row per order_cancelled event (deduplicated by event_id).
with deduplicated as (
    {{ dedupe_raw_events(source('raw', 'order_cancelled')) }}
)

select
    {{ event_envelope_columns() }},
    payload:reason::varchar as cancellation_reason,
    payload:cancelled_by::varchar as cancelled_by
from deduplicated
