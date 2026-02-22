-- Account Master Table - tbl_account (Curated Zone)
-- Consolidates latest account state from snapshot transaction sources

{{ config(
    materialized='incremental',
    unique_key='account_id'
) }}

{% set source_query %}

WITH accounts_source AS (

    SELECT
        account_id,
        customer_id,
        batch_date,
        ingestion_ts

    FROM (

        SELECT
            account_id,
            customer_id,
            batch_date,
            ingestion_ts
        FROM {{ ref('sys_a_loan_trans') }}

        UNION ALL

        SELECT
            account_id,
            customer_id,
            batch_date,
            ingestion_ts
        FROM {{ ref('sys_b_loan_trans') }}

    ) snapshot_union

),

latest_records AS (

    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY account_id
                ORDER BY account_last_updated_dt DESC, ingestion_ts DESC
            ) as rn
        FROM accounts_source
    )
    WHERE rn = 1

)

SELECT
    account_id,
    customer_id,
    batch_date,
    ingestion_ts

FROM latest_records

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
