{{
  config(
    materialized='incremental',
    unique_key='customer_id'
  )
}}

select
    customer_id,
    customer_name,
    industry,
    country,
    state as region,
    '{{ var("batch_date") }}'::date as batch_date,
    current_timestamp() as ingestion_ts

from {{ source('raw_s3', 'customers') }}

{% if is_incremental() %}
    where customer_id not in (select customer_id from {{ this }})
{% endif %}

