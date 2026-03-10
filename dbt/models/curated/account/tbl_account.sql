-- Account Master Table (Curated Zone)
-- Consolidates latest account state from snapshot transaction sources
-- TEST: uses DELETE+INSERT (dbt incremental (DuckDB supported)) -> doesn't support merge macro, so deletes matching rows first then inserts
-- PROD: uses merge_upsert macro (Snowflake)

{{ config(
    materialized='incremental',
    unique_key='account_id'
) }}

{% if target.name == 'test' %}

with accounts_source as (
    select
        account_id,
        customer_id,
        batch_date,
        ingestion_ts
    from {{ ref('sys_a_loan_trans') }}

    union all

    select
        account_id,
        customer_id,
        batch_date,
        ingestion_ts
    from {{ ref('sys_b_loan_trans') }}
),

latest_records as (
    select *
    from (
        select *,
            row_number() over (
                partition by account_id
                order by batch_date desc, ingestion_ts desc
            ) as rn
        from accounts_source
    ) ranked
    where rn = 1
)

select
    account_id,
    customer_id,
    batch_date,
    ingestion_ts
from latest_records

{% if is_incremental() %}
where account_id not in (select account_id from {{ this }})
{% endif %}

{% else %}

{% set source_query %}
with accounts_source as (
    select
        account_id,
        customer_id,
        batch_date,
        ingestion_ts
    from {{ ref('sys_a_loan_trans') }}

    union all

    select
        account_id,
        customer_id,
        batch_date,
        ingestion_ts
    from {{ ref('sys_b_loan_trans') }}
),

latest_records as (
    select *
    from (
        select *,
            row_number() over (
                partition by account_id
                order by batch_date desc, ingestion_ts desc
            ) as rn
        from accounts_source
    ) ranked
    where rn = 1
)

select
    account_id,
    customer_id,
    batch_date,
    ingestion_ts
from latest_records
{% endset %}

{% set update_columns = [
    'customer_id',
    'batch_date',
    'ingestion_ts'
] %}

{{ merge_upsert(
    target_table=this,
    source_query=source_query,
    unique_key='account_id',
    update_columns=update_columns
) }}

{% endif %}
