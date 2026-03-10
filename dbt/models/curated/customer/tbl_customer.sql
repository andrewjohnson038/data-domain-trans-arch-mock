-- Customer Master Table (Curated Zone)
-- TEST: uses DELETE+INSERT (dbt incremental (DuckDB supported)) -> doesn't support merge macro, so deletes matching rows first then inserts
-- PROD: uses merge_upsert macro (Snowflake)

{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

{% if target.name == 'test' %}

select
    c.customer_id,
    c.customer_name,
    c.customer_industry,
    c.customer_state_cd,
    csr.state_name as customer_state_name,
    c.customer_country_cd,
    cr.country_name as customer_country_name,
    current_localtimestamp() as ingestion_ts
from {{ ref('customer_demographics') }} c
left join {{ ref('country_state_ref') }} csr
    on c.customer_state_cd = csr.state_cd
    and c.customer_country_cd = csr.country_cd
left join {{ ref('country_ref') }} cr
    on c.customer_country_cd = cr.country_cd

{% if is_incremental() %}
where c.customer_id not in (select customer_id from {{ this }})
{% endif %}

{% else %}

{% set source_query %}
select
    c.customer_id,
    c.customer_name,
    c.customer_industry,
    c.customer_state_cd,
    csr.state_name as customer_state_name,
    c.customer_country_cd,
    cr.country_name as customer_country_name,
    current_timestamp() as ingestion_ts
from {{ ref('customer_demographics') }} c
left join {{ ref('country_state_ref') }} csr
    on c.customer_state_cd = csr.state_cd
    and c.customer_country_cd = csr.country_cd
left join {{ ref('country_ref') }} cr
    on c.customer_country_cd = cr.country_cd
{% endset %}

{% set update_columns = [
    'customer_name',
    'customer_industry',
    'customer_state_cd',
    'customer_state_name',
    'customer_country_cd',
    'customer_country_name',
    'ingestion_ts'
] %}

{{ merge_upsert(
    target_table=this,
    source_query=source_query,
    unique_key='customer_id',
    update_columns=update_columns
) }}

{% endif %}
