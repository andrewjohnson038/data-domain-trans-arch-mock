-- pulls from raw trans system a daily feed file, stores data as snapshot (daily trans history) in structured zone

{{
  config(
    materialized='incremental',   -- Incrementally add data
    unique_key='loan_id'          -- Merge key for dbt to avoid duplicates
  )
}}

select
    loan_id,                       -- Unique loan identifier
    account_id,                    -- Associated account
    customer_id,                   -- Customer associated with loan
    origination_date,              -- Loan origination date
    loan_product,                  -- Product type
    loan_amount,                   -- Original loan amount
    interest_rate,                 -- Loan interest rate
    term_months,                   -- Loan term in months
    '{{ var("batch_date") }}'::date as batch_date,  -- Snapshot batch date
    current_timestamp() as ingestion_ts             -- When ingested into structured table
from {{ source('raw_s3', 'loan_sys_a_transactions') }}

-- Incremental only adds new rows based on batch_date
{% if is_incremental() %}
    where '{{ var("batch_date") }}'::date > (select max(batch_date) from {{ this }})
{% endif %}
