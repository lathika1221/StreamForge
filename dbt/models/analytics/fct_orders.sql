-- One row per order, derived from ALL of its events.
--
-- Ordering: current_status comes from the event with the highest sequence_number (ties:
-- event time, then Kafka position), never from arrival order, so out-of-order delivery is
-- handled. Missing events: every order_id seen in ANY event is kept (has_order_created
-- flags a missing order_created; has_sequence_gap flags gaps). Invalid records never
-- reach RAW (they are in the Kafka DLQ) and are counted in ops_ingestion_reconciliation.
--
-- Incremental: only orders touched since the last run (events ingested after the
-- watermark minus the look-back) are recomputed, each from its full event history.
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        on_schema_change='fail'
    )
}}

with touched_orders as (
    select distinct order_id
    from {{ ref('fct_order_events') }}
    {% if is_incremental() %}
    where ingested_at_utc > (
        select dateadd(hour, -{{ var('incremental_lookback_hours') }}, max(last_ingested_at_utc))
        from {{ this }}
    )
    {% endif %}
),

events as (
    select e.*
    from {{ ref('fct_order_events') }} as e
    inner join touched_orders as t on e.order_id = t.order_id
),

event_stats as (
    select
        order_id,
        count(*) as event_count,
        min(sequence_number) as min_sequence_number,
        max(sequence_number) as latest_sequence_number,
        count(distinct sequence_number) as distinct_sequence_numbers,
        count_if(event_type = 'order_created') > 0 as has_order_created,
        max(event_timestamp_utc) as last_event_at_utc,
        max(ingested_at_utc) as last_ingested_at_utc
    from events
    group by order_id
),

latest_event as (
    select order_id, event_type as latest_event_type
    from events
    qualify row_number() over (
        partition by order_id
        order by sequence_number desc, event_timestamp_utc desc, kafka_partition desc,
            kafka_offset desc
    ) = 1
),

created as (
    select c.*
    from {{ ref('stg_order_created') }} as c
    inner join touched_orders as t on c.order_id = t.order_id
    qualify row_number() over (
        partition by c.order_id
        order by c.sequence_number, c.event_timestamp_utc, c.kafka_partition, c.kafka_offset
    ) = 1
),

payments as (
    select p.*
    from {{ ref('stg_payment_processed') }} as p
    inner join touched_orders as t on p.order_id = t.order_id
),

latest_payment as (
    select *
    from payments
    qualify row_number() over (
        partition by order_id
        order by sequence_number desc, event_timestamp_utc desc, kafka_partition desc,
            kafka_offset desc
    ) = 1
),

first_successful_payment as (
    select order_id, event_timestamp_utc as paid_at_utc
    from payments
    where payment_status = 'succeeded'
    qualify row_number() over (
        partition by order_id order by sequence_number, event_timestamp_utc
    ) = 1
),

cancellation as (
    select x.*
    from {{ ref('stg_order_cancelled') }} as x
    inner join touched_orders as t on x.order_id = t.order_id
    qualify row_number() over (
        partition by x.order_id order by x.sequence_number, x.event_timestamp_utc
    ) = 1
),

shipment as (
    select s.*
    from {{ ref('stg_shipment_dispatched') }} as s
    inner join touched_orders as t on s.order_id = t.order_id
    qualify row_number() over (
        partition by s.order_id order by s.sequence_number, s.event_timestamp_utc
    ) = 1
)

select
    s.order_id,

    -- from order_created (null when that event is missing)
    c.customer_id,
    c.currency,
    c.order_total,
    c.item_count,
    c.shipping_country,
    c.event_timestamp_utc as created_at_utc,
    to_date(c.event_timestamp_utc) as order_date,

    -- payment (latest by sequence) and first successful payment time
    p.payment_status,
    p.payment_method,
    p.amount as payment_amount,
    p.currency as payment_currency,
    p.failure_reason,
    sp.paid_at_utc,

    -- cancellation / shipment
    x.cancellation_reason,
    x.cancelled_by,
    x.event_timestamp_utc as cancelled_at_utc,
    sh.carrier,
    sh.event_timestamp_utc as shipped_at_utc,
    sh.estimated_delivery_date,

    -- lifecycle state from the highest sequence_number
    case le.latest_event_type
        when 'order_created' then 'awaiting_payment'
        when 'payment_processed' then iff(p.payment_status = 'succeeded', 'paid', 'payment_failed')
        when 'order_cancelled' then 'cancelled'
        when 'shipment_dispatched' then 'shipped'
    end as current_status,
    le.latest_event_type in ('order_cancelled', 'shipment_dispatched') as is_terminal,

    -- data-quality flags
    s.has_order_created,
    (s.min_sequence_number <> 1
        or s.latest_sequence_number <> s.distinct_sequence_numbers) as has_sequence_gap,
    iff(
        c.order_total is null or p.amount is null,
        null,
        c.order_total = p.amount and c.currency = p.currency
    ) as payment_amount_matches,
    s.event_count,
    s.latest_sequence_number,
    s.last_event_at_utc,
    s.last_ingested_at_utc
from event_stats as s
inner join latest_event as le on s.order_id = le.order_id
left join created as c on s.order_id = c.order_id
left join latest_payment as p on s.order_id = p.order_id
left join first_successful_payment as sp on s.order_id = sp.order_id
left join cancellation as x on s.order_id = x.order_id
left join shipment as sh on s.order_id = sh.order_id
