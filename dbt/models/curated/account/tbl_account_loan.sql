{{
  config(
    materialized='table'
  )
}}

select
    account_id,
    loan_id,
    origination_date,
    loan_product,
    exposure_amount,
    batch_date,
    ingestion_ts
from {{ ref('tbl_loan') }}
order by account_id, origination_date

