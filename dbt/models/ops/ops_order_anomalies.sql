-- One row per (order, anomaly). Data findings, not pipeline failures: the build reports
-- them as warnings (tests/assert_no_*.sql).
with orders as (
    select * from {{ ref('fct_orders') }}
),

raw_events as (
    {% for table in ['order_created', 'payment_processed', 'order_cancelled', 'shipment_dispatched'] %}
    select event_id, order_id, sha2(raw_value, 256) as raw_value_sha256
    from {{ source('raw', table) }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

conflicting_duplicates as (
    -- same event_id stored with different content (staging keeps the earliest copy)
    select event_id, any_value(order_id) as order_id, count(distinct raw_value_sha256) as versions
    from raw_events
    group by event_id
    having count(distinct raw_value_sha256) > 1
)

select order_id, 'missing_order_created' as anomaly,
    'events seen: ' || event_count as detail
from orders where not has_order_created
union all
select order_id, 'sequence_gap',
    'latest sequence ' || latest_sequence_number || ', events ' || event_count
from orders where has_sequence_gap
union all
select order_id, 'shipped_without_successful_payment',
    'payment_status: ' || coalesce(payment_status, 'none')
from orders where shipped_at_utc is not null and coalesce(payment_status, '') <> 'succeeded'
union all
select order_id, 'cancelled_and_shipped', null
from orders where cancelled_at_utc is not null and shipped_at_utc is not null
union all
select order_id, 'payment_amount_mismatch',
    'order ' || order_total || ' ' || currency || ' vs payment ' || payment_amount || ' '
        || payment_currency
from orders where payment_amount_matches = false and currency = payment_currency
union all
select order_id, 'currency_mismatch', currency || ' vs ' || payment_currency
from orders where currency <> payment_currency
union all
select order_id, 'conflicting_duplicate_event',
    'event ' || event_id || ' has ' || versions || ' versions'
from conflicting_duplicates
