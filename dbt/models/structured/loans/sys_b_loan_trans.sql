-- pulls from raw trans system b daily feed file, stores data as snapshot (daily trans history) in structured zone

{{
  config(
    materialized='incremental',
    unique_key='loan_id'
  )
}}

select
    facility_id as loan_id,           -- Unique identifier for the loan/facility
    acct_id as account_id,            -- Associated account ID for the loan
    customer_id,                      -- Customer ID who owns the loan
    open_date as origination_date,    -- Date the loan was opened/originated
    product_type as loan_product,     -- Type/category of the loan product
    credit_limit,                     -- Maximum credit available for this loan
    utilized_amount,                  -- Amount of credit currently used
    null as term_months,              -- Loan term in months (not provided in source)
    null as interest_rate,            -- Interest rate for the loan (not provided in source)
    '{{ var("batch_date") }}'::date as batch_date,  -- daily snapshot date
    current_timestamp() as ingestion_ts              -- when loaded into structured table
from {{ source('raw_s3', 'loan_sys_b_facilities') }}

-- Incremental only adds new rows based on batch_date
{% if is_incremental() %}
    where '{{ var("batch_date") }}'::date > (select max(batch_date) from {{ this }})
{% endif %}
