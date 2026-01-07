{{
  config(
    materialized='table'
  )
}}

select
    customer_id,
    customer_name,
    industry,
    country,
    region,
    batch_date,
    ingestion_ts
from {{ ref('customer_demographics') }}
qualify row_number() over (partition by customer_id order by batch_date desc) = 1

