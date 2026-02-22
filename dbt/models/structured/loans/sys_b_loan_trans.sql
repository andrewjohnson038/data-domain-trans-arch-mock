-- System B: Pulls from raw trans system B daily feed file
-- Stores data as snapshot (daily trans history) in structured zone
-- Standardizes column names to canonical format

{{ config(materialized='incremental', unique_key='loan_id') }}

WITH source_data AS (

{% if target.name == 'test' %}

-- TEST MODE: Read from seed CSV - change target name if using prod
select
    facility_id as loan_id,
    borrower_account_id as account_id,
    borrower_id as customer_id,
    start_date as origination_date,
    product_type as loan_product,
    facility_amount as loan_amount,
    null as interest_rate,
    null as term_months,
    cast('{{ var("batch_date") }}' as date) as batch_date,
    current_timestamp() as ingestion_ts
from {{ ref('lender_facilities_2025_01') }}

{% else %}

-- PROD MODE: Read from S3 via Snowflake external stage
select
    facility_id as loan_id,
    acct_id as account_id,
    customer_id,
    open_date as origination_date,
    product_type as loan_product,
    credit_limit as loan_amount,
    null as interest_rate,
    null as term_months,
    cast('{{ var("batch_date") }}' as date) as batch_date,
    current_timestamp() as ingestion_ts
from {{ source('raw_s3', 'loan_sys_b_facilities') }}

{% endif %}

)

select *
from source_data

{% if is_incremental() %}
where batch_date > (select max(batch_date) from {{ this }})
{% endif %}
