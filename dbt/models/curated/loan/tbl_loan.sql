{{
  config(
    materialized='table'
  )
}}

with sys_a_loans as (
    select
        loan_id,
        origination_date,
        loan_product,
        loan_amount as exposure_amount,
        interest_rate,
        term_months,
        batch_date,
        ingestion_ts
    from {{ ref('sys_a_loan_trans') }}
),

sys_b_loans as (
    select
        loan_id,
        origination_date,
        loan_product,
        utilized_amount as exposure_amount,
        null as interest_rate,
        null as term_months,
        batch_date,
        ingestion_ts
    from {{ ref('sys_b_loan_trans') }}
)

select * from sys_a_loans
union all
select * from sys_b_loans

