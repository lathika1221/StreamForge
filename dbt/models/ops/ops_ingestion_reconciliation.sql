-- Per consumer batch: the audit counts vs. the RAW rows actually stored. Invalid records
-- never reach Snowflake (they go to the Kafka DLQ); dbt sees them as dlq_records here.
--   rows_match:          RAW rows for the batch = rows_inserted
--   accounting_ok:       records_consumed = valid_records + dlq_records
--   load_accounting_ok:  valid_records = rows_inserted + duplicates_skipped
with raw_rows as (
    select batch_id, count(*) as raw_rows
    from (
        {% for table in ['order_created', 'payment_processed', 'order_cancelled', 'shipment_dispatched'] %}
        select batch_id from {{ source('raw', table) }}
        {% if not loop.last %}union all{% endif %}
        {% endfor %}
    )
    group by batch_id
)

select
    a.batch_id,
    a.consumer_group,
    a.topic,
    a.offset_ranges,
    a.records_consumed,
    a.valid_records,
    a.rows_inserted,
    a.duplicates_skipped,
    a.dlq_records,
    coalesce(r.raw_rows, 0) as raw_rows,
    coalesce(r.raw_rows, 0) = a.rows_inserted as rows_match,
    a.records_consumed = a.valid_records + a.dlq_records as accounting_ok,
    a.valid_records = a.rows_inserted + a.duplicates_skipped as load_accounting_ok,
    {{ to_utc('a.started_at') }} as started_at_utc,
    {{ to_utc('a.loaded_at') }} as loaded_at_utc
from {{ source('ops', 'ingestion_batches') }} as a
left join raw_rows as r on a.batch_id = r.batch_id
