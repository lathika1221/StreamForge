-- One row per deduplicated event, any type. Incremental: merges events ingested after the
-- current watermark minus a look-back window (late arrivals); MERGE makes re-reading the
-- window harmless.
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        on_schema_change='fail'
    )
}}

select
    event_id,
    order_id,
    event_type,
    sequence_number,
    event_timestamp_utc,
    ingested_at_utc,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    batch_id
from {{ ref('stg_order_events') }}
{% if is_incremental() %}
where ingested_at_utc > (
    select dateadd(hour, -{{ var('incremental_lookback_hours') }}, max(ingested_at_utc))
    from {{ this }}
)
{% endif %}
