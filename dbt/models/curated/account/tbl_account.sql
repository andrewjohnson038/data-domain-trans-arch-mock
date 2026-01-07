{{
  config(
    materialized='table'
  )
}}

select distinct
    account_id,
    customer_id,
    max(batch_date) as batch_date,
    current_timestamp() as ingestion_ts
from {{ ref('tbl_loan') }}
group by account_id, customer_id

