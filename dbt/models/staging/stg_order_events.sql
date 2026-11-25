-- All deduplicated events in one lean shape: the basis for order lifecycle logic.
{% set event_models = [
    'stg_order_created', 'stg_payment_processed', 'stg_order_cancelled', 'stg_shipment_dispatched'
] %}

{% for model in event_models %}
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
from {{ ref(model) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
