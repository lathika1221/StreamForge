-- One row per payment_processed event (deduplicated by event_id).
with deduplicated as (
    {{ dedupe_raw_events(source('raw', 'payment_processed')) }}
)

select
    {{ event_envelope_columns() }},
    payload:payment_id::varchar as payment_id,
    {{ to_money('payload:amount') }} as amount,
    payload:currency::varchar as currency,
    payload:method::varchar as payment_method,
    payload:status::varchar as payment_status,
    payload:failure_reason::varchar as failure_reason
from deduplicated
