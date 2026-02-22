-- Structured Zone: Customer Staging Table
-- Pulls from daily raw file with messy column names
-- Standardizes to canonical column names: customer_id, customer_name, industry, country, state
-- Stage table is fully rewritten daily with only the latest file

{{ config(
    materialized='table'
) }}

{% if target.name == 'test' %}
    -- TEST MODE: Read from seed CSV
    select
        cust_code as customer_id,
        company_name as customer_name,
        sector as customer_industry,
        nation as customer_country_name,
        st as customer_state_cd,
        current_timestamp() as ingestion_ts  -- DuckDB syntax (no parentheses)
    from {{ ref('customers') }}  -- References dbt/seeds/customers.csv

{% else %}
    -- PROD MODE: Read from S3 via Snowflake external stage
    select
        cust_code as customer_id,
        company_name as customer_name,
        sector as customer_industry,
        nation as customer_country_name,
        st as customer_state_cd,
        current_timestamp() as ingestion_ts  -- Snowflake syntax
    from {{ source('raw_s3', 'customer_demographics_daily') }}

{% endif %}
