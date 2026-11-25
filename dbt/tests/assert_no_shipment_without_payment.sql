-- Business rule (warn): an order was shipped without a successful payment.
{{ config(severity='warn') }}
select * from {{ ref('ops_order_anomalies') }} where anomaly = 'shipped_without_successful_payment'
