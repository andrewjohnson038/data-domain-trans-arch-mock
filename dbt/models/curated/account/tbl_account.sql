{{
  config(
    materialized='table'
  )
}}

with accounts as (
    select distinct
        account_id,
        customer_id,
        batch_date,
        ingestion_ts
    from {{ ref('sys_a_loan_trans') }}
    union all
    select distinct
        account_id,
        customer_id,
        batch_date,
        ingestion_ts
    from {{ ref('sys_b_loan_trans') }}
)

select
    account_id,
    customer_id,
    max(batch_date) as batch_date,
    current_timestamp() as ingestion_ts
from accounts
group by account_id, customer_id

