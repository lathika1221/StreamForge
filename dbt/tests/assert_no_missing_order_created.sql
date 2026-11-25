-- Business rule (warn): orders with events but no order_created event.
{{ config(severity='warn') }}
select * from {{ ref('ops_order_anomalies') }} where anomaly = 'missing_order_created'
