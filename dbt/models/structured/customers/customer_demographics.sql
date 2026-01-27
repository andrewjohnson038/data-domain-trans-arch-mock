-- Structured Zone: Customer Staging Table
-- Pulls from daily raw file with messy column names
-- Standardizes to canonical column names: customer_id, customer_name, industry, country, state
-- Stage table is fully rewritten daily with only the latest file

{{ config(
    materialized='table'              -- Fully materialized table: overwritten daily
) }}

select
    cust_code as customer_id,
    company_name as customer_name,
    sector as customer_industry,
    nation as customer_country_name,
    st as customer_state_cd,
    current_timestamp() as ingestion_ts -- Timestamp for when the stage table was populated
from {{ source('raw_s3', 'customer_demographics_daily') }}  -- Raw S3 file with original column names
