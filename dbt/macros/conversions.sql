{#- Explicit conversions used by every model.

    to_utc: RAW timestamps carry mixed offsets (EVENT_TIMESTAMP +00:00, INGESTED_AT in the
    account's default America/Los_Angeles offset). All outputs are normalised to UTC.

    to_money: amounts are JSON strings with 2 decimals (e.g. "23596.00"). -#}

{% macro to_utc(column) -%}
    convert_timezone('UTC', {{ column }})::timestamp_tz
{%- endmacro %}

{% macro to_money(expression) -%}
    try_to_number({{ expression }}::varchar, 12, 2)
{%- endmacro %}

{#- Deduplicate a RAW event table by event_id: the earliest ingested copy wins, with the
    Kafka position as a deterministic tie-break. -#}
{% macro dedupe_raw_events(relation) -%}
    select *
    from {{ relation }}
    qualify row_number() over (
        partition by event_id
        order by ingested_at, kafka_partition, kafka_offset
    ) = 1
{%- endmacro %}

{#- Envelope + lineage columns shared by every staging event model. -#}
{% macro event_envelope_columns() -%}
    event_id,
    order_id,
    event_type,
    schema_version,
    sequence_number,
    source,
    {{ to_utc('event_timestamp') }} as event_timestamp_utc,
    {{ to_utc('ingested_at') }} as ingested_at_utc,
    {{ to_utc('kafka_timestamp') }} as kafka_timestamp_utc,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_key,
    batch_id,
    sha2(raw_value, 256) as raw_value_sha256
{%- endmacro %}
