-- Customer Master Table (Curated Zone)
-- Upserts customer master records using merge_upsert macro

{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

{% set source_query = """
select
    c.customer_id,
    c.customer_name,
    c.customer_industry,
    c.customer_state_cd,
    s.state_name as 'customer_state_name'
    c.customer_country_cd,
    d.customer_country_name,
    current_timestamp() as ingestion_ts

from """ ~ ref('customer_demographics') ~ """ c

left join """ ~ ref('country_state_ref') ~ """ csr
    on c.customer_state_cd = csr.state_cd
    and c.customer_country_cd = csr.country_cd

left join """ ~ ref('country_ref') ~ """ cr
    on c.customer_country_cd = cr.country_cd
""" %}

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
