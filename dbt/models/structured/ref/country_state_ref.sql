-- Structured Zone: country ref data

{{ config(
    materialized='table'
) }}

{% if target.name == 'test' %}
    -- TEST MODE: Read from CSV
    select
        country_cd,
        country_name,
        state_cd,
        state_name
    from {{ ref('country_state_ref') }}  -- References dbt/seeds/customers.csv

{% else %}
    -- PROD MODE: Read from S3 via Snowflake external stage
    select
        country_cd,
        country_name,
        state_cd,
        state_name
    from {{ source('raw_s3', 'country_state_ref') }}

{% endif %}
