-- System A: Pulls from raw trans system A daily feed file
-- Stores data as snapshot (daily trans history) in structured zone
-- Standardizes column names to canonical format

{{ config(materialized='incremental', unique_key='loan_id') }}

WITH source_data AS (

{% if target.name == 'test' %}

-- TEST MODE: Read from seed CSV - change target name if using prod
select
    loan_id,
    account_id,
    customer_id,
    origination_date,
    loan_product,
    loan_amount,
    interest_rate,
    term_months,
    cast('{{ var("batch_date") }}' as date) as batch_date,
    current_timestamp() as ingestion_ts
from {{ ref('credit_loans_2025_01') }}

{% else %}

-- PROD MODE: Read from S3 via Snowflake external stage
select
    loan_id,
    account_id,
    customer_id,
    origination_date,
    loan_product,
    loan_amount,
    interest_rate,
    term_months,
    cast('{{ var("batch_date") }}' as date) as batch_date,
    current_timestamp() as ingestion_ts
from {{ source('raw_s3', 'loan_sys_a_transactions') }}

{% endif %}

)

select *
from source_data

{% if is_incremental() %}
where batch_date > (select max(batch_date) from {{ this }})
{% endif %}
