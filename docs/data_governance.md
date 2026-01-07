# Data Governance

## Overview
This document outlines the data governance policies and practices for the Lending Analytics Platform, including source system semantics, data quality rules, and testing strategies.

---

## Source System Semantics

### Loan System A - Core Lending System

#### Overview
- **Purpose**: Term loans for business customers
- **Vendor**: Internal core banking system
- **Update Frequency**: Daily batch at 10 PM EST
- **File Location**: `s3://lender-data/raw/loan_sys_a/`

#### File Format
`credit_loans_YYYY_MM.csv`

#### Schema
| Field | Type | Description | Nullable | Business Rules |
|-------|------|-------------|----------|----------------|
| loan_id | VARCHAR(20) | Unique loan identifier, format L-A-#### | No | Primary key |
| account_id | VARCHAR(20) | Account identifier, format ACC-### | No | FK to accounts |
| customer_id | VARCHAR(20) | Customer identifier, format CUST-### | No | FK to customers |
| origination_date | DATE | Date loan was originated | No | <= current_date |
| loan_product | VARCHAR(50) | Product type (TERM_LOAN) | No | Fixed value |
| loan_amount | DECIMAL(15,2) | Total loan amount in USD | No | > 0 |
| interest_rate | DECIMAL(5,4) | Annual interest rate (decimal) | No | Between 0.05 and 0.25 |
| term_months | INTEGER | Loan term in months | No | 12, 24, 36, 48, or 60 |

#### Business Semantics
- **One loan per account**: Each account has exactly one term loan
- **Fixed rate**: Interest rate set at origination
- **Fixed term**: Loan has predetermined maturity date
- **Amortizing**: Principal and interest paid monthly

---

### Loan System B - Credit Platform

#### Overview
- **Purpose**: Revolving credit facilities for business customers
- **Vendor**: Third-party cloud platform
- **Update Frequency**: Daily batch at 11 PM EST
- **File Location**: `s3://lender-data/raw/loan_sys_b/`

#### File Format
`lender_facilities_YYYY_MM.csv`

#### Schema
| Field | Type | Description | Nullable | Business Rules |
|-------|------|-------------|----------|----------------|
| facility_id | VARCHAR(20) | Unique facility identifier, format F-B-#### | No | Primary key |
| acct_id | VARCHAR(20) | Account identifier, format ACC-### | No | FK to accounts |
| customer_id | VARCHAR(20) | Customer identifier, format CUST-### | No | FK to customers |
| open_date | DATE | Date facility was opened | No | <= current_date |
| product_type | VARCHAR(50) | Product type (REVOLVER) | No | Fixed value |
| credit_limit | DECIMAL(15,2) | Maximum credit available | No | > 0 |
| utilized_amount | DECIMAL(15,2) | Currently drawn amount | No | >= 0, <= credit_limit |

#### Business Semantics
- **Multiple facilities per account**: An account can have multiple revolving facilities
- **Variable rate**: Interest rate fluctuates monthly (tracked in separate system)
- **No fixed term**: Facility remains open until closed
- **Non-amortizing**: Interest-only payments, principal repayment flexible

#### Key Differences from System A
- Uses `facility_id` instead of `loan_id`
- Uses `acct_id` instead of `account_id`
- No `term_months` (not applicable)
- No `interest_rate` field (variable, tracked elsewhere)
- Has `credit_limit` and `utilized_amount` instead of `loan_amount`

---

### Customer Master Data

#### Overview
- **Purpose**: Customer demographic and classification data
- **Vendor**: Internal CRM system
- **Update Frequency**: Daily batch at 10 PM EST
- **File Location**: `s3://lender-data/raw/customers/`

#### File Format
`customers.csv`

#### Schema
| Field | Type | Description | Nullable | Business Rules |
|-------|------|-------------|----------|----------------|
| customer_id | VARCHAR(20) | Unique customer identifier | No | Primary key |
| customer_name | VARCHAR(200) | Legal customer name | No | - |
| industry | VARCHAR(100) | Industry classification | Yes | NAICS-based |
| country | VARCHAR(2) | ISO country code | No | ISO 3166-1 alpha-2 |
| state | VARCHAR(2) | US state code (if applicable) | Yes | US state abbreviation |

#### Business Semantics
- **Shared across systems**: Same customer_id used in both loan systems
- **Slowly changing**: Customer info updates infrequently
- **Regional analysis**: State field enables US regional reporting

---

## Data Integration Challenges

### Field Name Mismatches
| System A | System B | Curated |
|----------|----------|---------|
| loan_id | facility_id | loan_id |
| account_id | acct_id | account_id |
| origination_date | open_date | origination_date |
| loan_product | product_type | loan_product |

### Semantic Differences
- **Exposure metric**: 
  - System A: `loan_amount` = total exposure
  - System B: `utilized_amount` = current exposure (not `credit_limit`)
- **Term concept**:
  - System A: Has fixed `term_months`
  - System B: No term (perpetual facility)

### Solution Strategy
1. **Structured Layer**: Standardize field names, preserve semantic differences
2. **Curated Layer**: Unify into `exposure_amount`, keep system-specific nulls

---

## Data Quality Rules

Data quality testing is implemented at each layer with different focus areas:

| Layer           | DQ Focus            | Examples                                                                         |
| --------------- | ------------------- | -------------------------------------------------------------------------------- |
| Structured Zone | Technical / schema  | Not null, unique keys, data types, formats                                       |
| Curated Zone    | Business / semantic | Referential integrity, aggregation checks, SCD2 consistency, multi-source merges |

### Structured Layer DQ Rules

**Focus**: Technical validation and schema compliance

#### sys_a_loan_trans
- **Primary Key**: `loan_id` must be unique and not null
- **Format Validation**: `loan_id` must match pattern `L-A-####`
- **Foreign Keys**: `account_id` and `customer_id` must not be null
- **Date Validation**: `origination_date` must be <= current_date
- **Value Constraints**: 
  - `loan_product` must equal 'TERM_LOAN'
  - `loan_amount` must be > 0
  - `interest_rate` must be between 0.05 and 0.25
  - `term_months` must be in (12, 24, 36, 48, 60)
- **Metadata**: `batch_date` and `ingestion_ts` must not be null

#### sys_b_loan_trans
- **Primary Key**: `loan_id` must be unique and not null
- **Format Validation**: `loan_id` must match pattern `F-B-####`
- **Foreign Keys**: `account_id` and `customer_id` must not be null
- **Date Validation**: `origination_date` must be <= current_date
- **Value Constraints**:
  - `loan_product` must equal 'REVOLVER'
  - `credit_limit` must be > 0
  - `utilized_amount` must be >= 0 and <= credit_limit
- **Nullable Fields**: `term_months` and `interest_rate` must be NULL (not applicable)
- **Metadata**: `batch_date` and `ingestion_ts` must not be null

#### customer_demographics
- **Primary Key**: `customer_id` must be unique and not null
- **Required Fields**: `customer_name`, `country` must not be null
- **Format Validation**: 
  - `country` must be valid ISO 3166-1 alpha-2 code
  - `state` must be valid US state abbreviation if country = 'US'
- **Metadata**: `batch_date` and `ingestion_ts` must not be null

### Curated Layer DQ Rules

**Focus**: Business logic validation and referential integrity

#### tbl_loan
- **Primary Key**: `loan_id` must be unique across all source systems
- **Referential Integrity**: 
  - All `account_id` values must exist in `tbl_account`
  - All `customer_id` values must exist in `tbl_customer`
- **Business Rules**:
  - `exposure_amount` must be > 0
  - For TERM_LOAN: `interest_rate` must not be null, `term_months` must not be null
  - For REVOLVER: `interest_rate` must be null, `term_months` must be null
- **Multi-Source Merge**: No duplicate `loan_id` values across System A and System B
- **Data Completeness**: All loans from structured layer must be present

#### tbl_account
- **Primary Key**: `account_id` must be unique
- **Referential Integrity**: All `customer_id` values must exist in `tbl_customer`
- **Business Rule**: Each account must have at least one loan in `tbl_loan`
- **Derivation Check**: Account list must match distinct accounts from `tbl_loan`

#### tbl_customer
- **Primary Key**: `customer_id` must be unique
- **SCD Consistency**: Latest record per customer_id must be selected (no duplicates)
- **Business Rules**:
  - `customer_name` must not be null
  - `country` must not be null
  - If `country = 'US'`, `region` should not be null
- **Data Completeness**: All customers referenced in loans must exist

#### tbl_account_loan
- **Composite Key**: `(account_id, loan_id)` must be unique
- **Referential Integrity**:
  - All `account_id` values must exist in `tbl_account`
  - All `loan_id` values must exist in `tbl_loan`
- **Business Rule**: `exposure_amount` must match corresponding value in `tbl_loan`
- **Relationship Completeness**: All account-loan relationships from `tbl_loan` must be present

---

## Metadata Tracking

- `batch_date`: ETL batch processing date
- `ingestion_ts`: Timestamp of data ingestion

## Historical Tracking

- SCD Type 2 ready for historical tracking
- Audit trail maintained through metadata fields
- Data lineage documented in schema definitions

## Security

- Credentials managed through environment variables
- Role-based access control in Snowflake
- Secure connections to source systems
