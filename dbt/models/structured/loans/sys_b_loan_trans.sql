{{
  config(
    materialized='incremental',
    unique_key='loan_id'
  )
}}

select
    facility_id as loan_id,
    acct_id as account_id,
    customer_id,
    open_date as origination_date,
    product_type as loan_product,
    credit_limit,
    utilized_amount,
    null as term_months,
    null as interest_rate,
    '{{ var("batch_date") }}'::date as batch_date,
    current_timestamp() as ingestion_ts

from {{ source('raw_s3', 'loan_sys_b_facilities') }}

{% if is_incremental() %}
    where open_date >= (select max(origination_date) from {{ this }})
{% endif %}

