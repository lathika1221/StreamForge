-- Reconciliation (error): every consumer batch's audit counts must match the RAW rows,
-- and every RAW row must belong to an audited batch.
select batch_id, 'audit/raw mismatch' as problem
from {{ ref('ops_ingestion_reconciliation') }}
where not (rows_match and accounting_ok and load_accounting_ok)

union all

select r.batch_id, 'raw rows without an audit row'
from (
    {% for table in ['order_created', 'payment_processed', 'order_cancelled', 'shipment_dispatched'] %}
    select distinct batch_id from {{ source('raw', table) }}
    {% if not loop.last %}union{% endif %}
    {% endfor %}
) as r
left join {{ source('ops', 'ingestion_batches') }} as a on r.batch_id = a.batch_id
where a.batch_id is null
