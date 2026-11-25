-- Business rule (warn): an order has both a cancellation and a shipment.
{{ config(severity='warn') }}
select * from {{ ref('ops_order_anomalies') }} where anomaly = 'cancelled_and_shipped'
