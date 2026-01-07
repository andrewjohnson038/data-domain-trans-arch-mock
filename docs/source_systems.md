# Source Systems Documentation

## Overview
This document describes the source systems that feed data into the Lending Analytics Platform.

## System A: Term Loans (Core Lending System)
- **Product Type**: Term loans (fixed amount, fixed term)
- **Key Fields**: loan_id, account_id, customer_id, origination_date, loan_product, loan_amount, interest_rate, term_months
- **Data Format**: CSV files in S3
- **Update Frequency**: Daily

## System B: Revolving Credit Facilities
- **Product Type**: Revolving credit facilities (credit limits, variable utilization)
- **Key Fields**: facility_id, acct_id, customer_id, open_date, product_type, credit_limit, utilized_amount
- **Data Format**: CSV files in S3
- **Update Frequency**: Daily
- **Note**: facility_id is mapped to loan_id in the structured layer

## Customer Master
- **Source**: Customer master data system
- **Key Fields**: customer_id, customer_name, industry, country, state
- **Data Format**: CSV files in S3
- **Update Frequency**: Daily

## Data Mapping
- System A fields map directly to structured layer
- System B fields require mapping (facility_id → loan_id, acct_id → account_id, etc.)
- Customer data standardized with state → region mapping

