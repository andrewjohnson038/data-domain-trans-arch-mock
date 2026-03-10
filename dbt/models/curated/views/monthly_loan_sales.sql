-- view: monthly_loan_sales.sql -> monthly sales aggregated view

{{
  config(
    materialized='view'
  )
}}

select
    date_trunc('month', l.origination_date) as origination_month,
    c.customer_state_cd as us_region,
    c.customer_industry as industry,
    l.loan_product,
    count(distinct l.loan_id) as loan_count,
    sum(l.exposure_amount) as total_exposure,
    avg(l.exposure_amount) as avg_exposure,
    count(distinct c.customer_id) as unique_customers,
    max(l.batch_date) as batch_date,
    {% if target.name == 'test' %}
    current_localtimestamp() as ingestion_ts  -- duckdb
    {% else %}
    current_timestamp() as ingestion_ts  -- snowflake
    {% endif %}

from {{ ref('tbl_loan') }} l
left join {{ ref('tbl_account_loan') }} al on l.loan_id = al.loan_id
left join {{ ref('tbl_account') }} a on al.account_id = a.account_id
left join {{ ref('tbl_customer') }} c on a.customer_id = c.customer_id

group by
    date_trunc('month', l.origination_date),
    c.customer_state_cd,
    c.customer_industry,
    l.loan_product

order by
    origination_month desc,
    total_exposure desc
