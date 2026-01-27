-- Customer Master Table (Curated Zone)
-- Pulls standardized customer demographics from structured zone
-- Enriches data with full country and state names from dimension tables
-- Updates existing customers if anything changed and inserts new customers

{{ config(
    materialized='incremental',        -- dbt: incremental updates instead of full reload
    unique_key='customer_id'           -- dbt: merge/upsert key
) }}

{% set source_query %}   -- Define source query for merge_upsert macro
select
    c.customer_id,
    c.customer_name,
    c.customer_industry,
    c.customer_state_cd,
    s.customer_state_name,
    c.customer_country_cd,
    d.customer_country_name,
    current_timestamp() as ingestion_ts         -- Metadata: when this row is inserted/updated
from {{ ref('customer_demographics') }} c
left join {{ ref('country_state_ref') }} s on c.customer_state_cd = s.state_code and c.customer_country_cd = s.country_code
left join {{ ref('country_ref') }} d on c.customer_country_cd = d.country_code
{% endset %}  -- end set

{% set update_columns = [
    'customer_name',
    'customer_industry',
    'customer_state_cd',
    'customer_state_name',
    'customer_country_cd',
    'customer_country_name',
    'ingestion_ts'
] %}  -- Columns to update or insert (customer_id handled by unique_key)

-- Execute merge/upsert macro
{{ merge_upsert(
    target_table=this,                 -- Target is this master table
    source_query=source_query,         -- SQL snippet defined above
    unique_key='customer_id',          -- Merge on customer_id
    update_columns=update_columns      -- Columns to update/insert
) }}
