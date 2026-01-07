{{
  config(
    materialized='view'
  )
}}

select
    date_trunc('month', l.origination_date) as origination_month,
    c.region as us_region,
    c.industry,
    l.loan_product,
    count(distinct l.loan_id) as loan_count,
    sum(l.exposure_amount) as total_exposure,
    avg(l.exposure_amount) as avg_exposure,
    count(distinct l.customer_id) as unique_customers,
    max(l.batch_date) as batch_date,
    current_timestamp() as ingestion_ts
    
from {{ ref('tbl_loan') }} l
inner join {{ ref('tbl_customer') }} c
    on l.customer_id = c.customer_id

where c.country = 'US'

group by 
    date_trunc('month', l.origination_date),
    c.region,
    c.industry,
    l.loan_product

order by 
    origination_month desc,
    total_exposure desc

