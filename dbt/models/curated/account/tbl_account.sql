-- Account Master Table (Curated Zone)
-- Incrementally consolidates accounts from snapshot sources
-- Updates account_last_updated_dt only if account data has changed
-- ingestion_ts tracks when the row was ingested from the snapshot (source)

{{ config(
    materialized='incremental',        -- Incremental: only new/changed accounts merged
    unique_key='account_id'             -- Merge key
) }}

{% set source_query %}
with accounts as (
    -- Pull accounts from trans system snapshot tables
    select distinct
        account_id,                       -- Unique account identifier
        customer_id,                      -- Customer associated with account
        batch_date as snapshot_date,      -- Snapshot batch date
        ingestion_ts,                     -- When the row was ingested from source
        md5(concat_ws('|', customer_id)) as account_hash  -- Hash of relevant fields to detect changes
    from {{ ref('sys_a_loan_trans') }}

    union all

    select distinct
        account_id,
        customer_id,
        batch_date as snapshot_date,
        ingestion_ts,
        md5(concat_ws('|', customer_id)) as account_hash
    from {{ ref('sys_b_loan_trans') }}
),

latest_change as (
    -- Only include accounts that are new or have changed compared to master
    select
        a.account_id,
        a.customer_id,
        a.snapshot_date,
        a.ingestion_ts,
        a.account_hash
    from accounts a
    left join {{ this }} m  -- Join to current master table
        on a.account_id = m.account_id
    where m.account_id is null  -- New account
       or a.account_hash != md5(concat_ws('|', m.customer_id))  -- Account changed
)

select
    account_id,
    customer_id,
    case
        when account_hash != md5(concat_ws('|', (select customer_id from {{ this }} where account_id = latest_change.account_id)))
        then snapshot_date                -- Update last updated only if account changed
        else (select account_last_updated_dt from {{ this }} where account_id = latest_change.account_id)
    end as account_last_updated_dt,
    ingestion_ts                         -- Always the source ingestion timestamp
from latest_change
{% endset %}

{% set update_columns = [
    'customer_id',
    'account_last_updated_dt',
    'ingestion_ts'
] %}  -- Columns to update/insert (account_id is merge key)

-- Execute merge/upsert macro
{{ merge_upsert(
    target_table=this,
    source_query=source_query,
    unique_key='account_id',
    update_columns=update_columns
) }}
