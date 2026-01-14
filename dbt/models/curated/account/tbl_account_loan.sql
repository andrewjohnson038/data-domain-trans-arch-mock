{{
  config(
    materialized='table'
  )
}}

with loans as (
    select
        loan_id,
        account_id,
        origination_date,
        batch_date,
        ingestion_ts
    from {{ ref('sys_a_loan_trans') }}
    union all
    select
        loan_id,
        account_id,
        origination_date,
        batch_date,
        ingestion_ts
    from {{ ref('sys_b_loan_trans') }}
)

select
    account_id,
    loan_id,
    origination_date,
    batch_date,
    ingestion_ts
from loans
order by account_id, origination_date

