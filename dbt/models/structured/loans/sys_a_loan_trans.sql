{{
  config(
    materialized='incremental',
    unique_key='loan_id'
  )
}}

select
    loan_id,
    account_id,
    customer_id,
    origination_date,
    loan_product,
    loan_amount,
    interest_rate,
    term_months,
    '{{ var("batch_date") }}'::date as batch_date,
    current_timestamp() as ingestion_ts

from {{ source('raw_s3', 'loan_sys_a_transactions') }}

{% if is_incremental() %}
    where origination_date >= (select max(origination_date) from {{ this }})
{% endif %}

