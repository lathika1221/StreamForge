-- Business rule (warn): payment amount or currency differs from the order, or the same
-- event_id was stored with conflicting content.
{{ config(severity='warn') }}
select * from {{ ref('ops_order_anomalies') }}
where anomaly in ('payment_amount_mismatch', 'currency_mismatch', 'conflicting_duplicate_event')
