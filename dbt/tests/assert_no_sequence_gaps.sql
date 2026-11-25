-- Business rule (warn): orders whose sequence numbers are not contiguous from 1
-- (an event is missing or not yet arrived). A data finding, not a pipeline failure.
{{ config(severity='warn') }}
select * from {{ ref('ops_order_anomalies') }} where anomaly = 'sequence_gap'
