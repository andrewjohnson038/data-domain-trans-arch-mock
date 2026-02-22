# Lending Analytics Platform

A production-ready data pipeline demonstrating enterprise data engineering patterns for a fintech lending platform. This project showcases how to build a scalable, maintainable data architecture using modern ETL tools and domain-driven design principles.

---

## Table of Contents

1. [Overview](#overview)
2. [Repository Structure](#repository-structure)
3. [Data Flow: From Raw to Analytics](#data-flow-from-raw-to-analytics)
4. [Why Domain-Driven Design?](#why-domain-driven-design)
5. [Technology Stack Explained](#technology-stack-explained)
6. [Getting Started](#getting-started)
7. [Documentation](#documentation)

---

## Overview

### What is This Project?

Imagine a fintech company that offers loans through two different systems:
- **System A**: Traditional term loans (like a mortgage - fixed amount, fixed repayment period)
- **System B**: Revolving credit facilities (like a credit card - you have a limit, you use what you need)

Both systems store data differently, use different field names, and have different structures. This project demonstrates how to:
1. **Ingest** data from both systems
2. **Standardize** it into a common format
3. **Organize** it by business domains (loans, customers, accounts)
4. **Transform** it into analytics-ready views

The end result: Business analysts can query a single, unified view of all loans regardless of which system they came from.

**There will be two flows** 
- one for **testing:** (uses duckdb to write tables temporarely than to csvs in the repo). This is to showcase the workflow without needing a SnowFlake Instance.
- one for **prod:** this is to simulate a true workflow. This would need additional customized set up if this repo is forked by the user. Would require AWS S3 & Snowflake set-up.

from {{ ref('sys_a_loan_trans') }}
```

```
TEST MODE:
┌─────────────────┐
│ seeds/          │
│ - sys_a.csv     │               ┌──────────────┐
│ - sys_b.csv     │──dbt seed──>  │  DuckDB      │
└─────────────────┘               │  (temp)      │
                                  └──────────────┘
                      
┌─────────────────────┼─────────────────────┐
│                     │                     │
▼                     ▼                     ▼
┌──────────────┐      ┌──────────────┐    ┌──────────────┐
│ Structured   │      │  Curated     │    │   Views      │
│ (DuckDB)     │─────>│  (DuckDB)    │───>│  (DuckDB)    │
└──────────────┘      └──────────────┘    └──────────────┘
      │
      ▼
┌──────────────┐
│ Export CSVs  │
│ test_results/│
└──────────────┘

PROD MODE:
┌─────────────────┐
│ S3 Bucket       │
│ - sys_a.csv     │ ──Snowflake──>  ┌──────────────┐
│ - sys_b.csv     │    Stage        │  Snowflake   │
└─────────────────┘                 │              │
                                    └──────────────┘
                       │
┌──────────────────────┼──────────────────────┐
│                      │                      │
▼                      ▼                      ▼
┌──────────────┐       ┌──────────────┐     ┌──────────────┐
│ Structured   │       │  Curated     │     │   Views      │
│ (Snowflake)  │──────>│  (Snowflake) │────>│  (Snowflake) │
└──────────────┘       └──────────────┘     └──────────────┘
│
▼
(Data stays in Snowflake)

### The Challenge

When you have multiple source systems, you face several problems:
- **Different field names**
- **Different data structures**
- **No unified view**: Analysts need to know which system to query for different types of loans
- **Maintenance Issues**: Changes to source systems break downstream reports
- **Consumer Difficulties**: Consumers struggle to understand the data they are using, what data to use, etc
- **Governance**: With multiple systems means complexities between definitions, dq rules, applicapility and clarity

### The Solution

Utilize a **layered data architecture** that progressively transforms raw data into business-friendly domain models that works like a consumable data catalog:

```
Raw Data → Structured Layer → Curated Domain Models → Analytics Views
```

Each layer has a specific purpose and builds upon the previous one, making the system maintainable and scalable.

---

## Repository Structure

```
lending-analytics-platform/
│
├── README.md                    # This file
├── requirements.txt             # Python dependencies
│
├── airflow/                     # Orchestration layer
│   └── dags/
│       └── loan_data_pipeline_dag.py  # Defines the ETL workflow
│
├── dbt/                         # Transformation layer
│   ├── dbt_project.yml          # Project configuration
│   ├── profiles.yml             # Database connection settings
│   ├── models/
│   │   ├── structured/          # Standardizes source system data
│   │   │   ├── loans/
│   │   │   │   ├── sys_a_loan_trans.sql
│   │   │   │   └── sys_b_loan_trans.sql
│   │   │   ├── customers/
│   │   │   │   └── customer_demographics.sql
│   │   │   └── dq_tests.yml     # Data quality tests
│   │   └── curated/             # Business domain models
│   │       ├── account/         # Account domain
│   │       │   ├── tbl_account.sql
│   │       │   └── tbl_account_loan.sql
│   │       ├── loan/            # Loan domain
│   │       │   └── tbl_loan.sql
│   │       ├── customer/        # Customer domain
│   │       │   └── tbl_customer.sql
│   │       ├── views/           # Analytics views
│   │       │   └── monthly_loan_sales.sql
│   │       └── dq_tests.yml     # Data quality tests
│   └── macros/                  # Reusable SQL functions
│       ├── add_metadata.sql
│       └── merge_upsert.sql
│
├── example_data/                # Sample data at each layer
│   ├── raw_layer/               # Original source data
│   ├── structured_layer/        # After standardization
│   ├── curated_layer/           # After domain modeling
│   └── view_layer/              # Final analytics output
│
├── logging/                     # Pipeline execution logs
│   ├── success.log
│   └── failure.log
│
└── docs/                        # Detailed documentation
    ├── architecture.md          # Architecture deep dive
    ├── data_governance.md       # Data quality and governance
    └── source_systems.md        # Source system details
```

---

## Data Flow: From Raw to Analytics

Let's walk through what happens to data as it moves through each layer:

### Step 1: Raw Layer (Source Systems)

**Location**: `example_data/raw_layer/`

**What it is**: Data exactly as it comes from the source systems, stored in S3. Typcally stored as parquet by file date.

**Example - System A** (`credit_loans_2025_01.csv`):
```csv
loan_id,account_id,customer_id,origination_date,loan_product,loan_amount,interest_rate,term_months
L-A-1001,ACC-001,CUST-001,2025-01-05,TERM_LOAN,50000,0.11,24
```

**Example - System B** (`lender_facilities_2025_01.csv`):
```csv
facility_id,acct_id,customer_id,open_date,product_type,credit_limit,utilized_amount
F-B-9001,ACC-003,CUST-003,2025-01-08,REVOLVER,100000,25000
```

**Key Point**: Notice how System A uses `loan_id` while System B uses `facility_id`. They're the same concept, but different ids. These will need to be mastered into one loan/product domain.

### Step 2: Structured Layer (Standardization)

**Location**: `dbt/models/structured/`

**What happens**: We standardize field names and add metadata. This is where we solve the "different field names" problem.

**Transformation**:
- System A: `loan_id` → `loan_id` (stays the same)
- System B: `facility_id` → `loan_id` (renamed to match)
- System B: `acct_id` → `account_id` (standardized)
- System B: `open_date` → `origination_date` (standardized)
- Both: Add `batch_date` and `ingestion_ts` (metadata tracking)

**Result**: Both systems now have the same field names, making them comparable.

**See**: `example_data/structured_layer/` for examples

### Step 3: Curated Layer (Domain Models)

**Location**: `dbt/models/curated/`

**What happens**: We organize data by business domains. This is where domain-driven design comes in.

#### Why Organize by Domains?

Think of domains as **business concepts** that make sense to your users. It allows a data function to build around standardized data products and governance recognizable to the organization:
- **Loan Domain**: "Show me all loans" - regardless of which system they came from
- **Customer Domain**: "Show me all customers" - single source of truth
- **Account Domain**: "Show me all accounts" - a customer can have multiple accounts

#### Domain Structure

**`curated/loan/tbl_loan.sql`**: 
- Combines loans from both System A and System B
- Creates unified fields like `exposure_amount` (loan_amount for System A, utilized_amount for System B)
- Business users can now query "all loans" without knowing about source systems

**`curated/customer/tbl_customer.sql`**:
- Single source of truth for customer information
- Ensures we have the latest customer data

**`curated/account/tbl_account.sql`**:
- Distinct accounts across all systems
- Links accounts to customers

**`curated/account/tbl_account_loan.sql`**:
- Relationship table showing which loans belong to which accounts
- Important because one account can have multiple loans (especially in System B)

**See**: `example_data/curated_layer/` for examples

### Step 4: View Layer (Analytics)

**Location**: `dbt/models/curated/views/`

**What happens**: Pre-computed aggregations for common business questions.

**Example**: `monthly_loan_sales.sql`
- Aggregates loans by month, region, industry, and product type
- Ready for dashboards and reports
- Business users don't need to write complex SQL

**See**: `example_data/view_layer/` for examples

---

## Why Domain-Driven Design?

### The Problem with System-Centric Thinking

Traditional data warehouses often organize data by source system:
```
database/
  ├── system_a_tables/
  └── system_b_tables/
```

This creates problems:
- Analysts need to know which system to query
- Combining data from multiple systems requires complex joins
- When a new system is added, all reports need updating
- Business logic is scattered across system-specific code

### The Domain-Driven Solution

We organize by **business domains** instead:
```
curated/
  ├── loan/        # All loans, regardless of source
  ├── customer/    # All customers, single source of truth
  └── account/     # All accounts, unified view
```

**Benefits**:
1. **Business-Friendly**: Analysts think in terms of "loans" and "customers", not "System A" and "System B"
2. **Maintainable**: When System C is added, we update the domain models, not every report
3. **Scalable**: New domains can be added without affecting existing ones
4. **Clear Ownership**: Each domain has clear boundaries and responsibilities

### Domain Organization in This Project

We separate domains into their own folders:
- `curated/loan/` - Everything related to loans
- `curated/customer/` - Everything related to customers  
- `curated/account/` - Everything related to accounts (including relationships like `tbl_account_loan.sql`)

This makes it easy to:
- Find all code related to a domain
- Understand relationships between domains
- Add new domain tables without cluttering other domains

---

## Technology Stack Explained

### Apache Airflow (Orchestration)

**What it does**: Schedules and coordinates the entire ETL pipeline.

**Why we use it**: 
- Ensures tasks run in the correct order
- Handles failures and retries automatically
- Provides visibility into pipeline execution
- Can run on a schedule (e.g., daily at 2 AM)

**In this project**: 
- `airflow/dags/loan_data_pipeline_dag.py` defines the workflow
- Runs structured layer → tests → curated layer → tests → views
- Logs success/failure to `logging/` directory

**Think of it as**: The conductor of an orchestra - it doesn't play the music, but ensures everyone plays at the right time.

### dbt (Data Build Tool) - Transformation

**What it does**: Manages SQL transformations and data quality tests.

**Why we use it**:
- Version control for SQL (like Git for code)
- Reusable macros and functions
- Built-in testing framework
- Documentation generation
- Dependency management (knows what to run first)

**In this project**:
- All SQL transformations are in `dbt/models/`
- `dbt/models/structured/` standardizes source data
- `dbt/models/curated/` creates domain models
- `dbt/models/structured/dq_tests.yml` defines data quality tests

### Snowflake (Data Warehouse)

**What it does**: Stores and queries the transformed data.

**Why we use it**:
- Handles large volumes of data efficiently
- Separates storage from compute (cost-effective)
- Built-in support for structured and semi-structured data
- Scales automatically

**In this project**:
- Raw data is loaded from S3 into Snowflake
- Structured and curated layers are tables in Snowflake
- Views are materialized for fast querying
- Note: Example data added to the repo to see what the data would look like at each transformed layer.

### AWS S3 (Storage)

**What it does**: Stores raw data files from source systems.

**Why we use it**:
- Cost-effective storage for large files
- Integrates well with Snowflake
- Acts as a "data lake" - store everything, process later
- Provides audit trail of source data

**In this project**:
- Source systems drop CSV files into S3
- Snowflake reads from S3 via external stages
- Raw data is preserved for reprocessing if needed

---

## Getting Started

### Prerequisites

- Python 3.8+
- Access to Snowflake account
- AWS account with S3 access
- Basic understanding of SQL

### Installation

```bash
# Install Python dependencies
pip install -r requirements.txt
```

### Configuration

1. **Set Snowflake credentials** as environment variables:
   ```bash
   export SNOWFLAKE_ACCOUNT="your_account"
   export SNOWFLAKE_USER="your_user"
   export SNOWFLAKE_PASSWORD="your_password"
   ```

2. **Update `dbt/profiles.yml`** with your Snowflake connection details

3. **Configure Airflow** to point to your Snowflake connection

### Running the Pipeline

```bash
# Run structured layer transformations
dbt run --select structured

# Run curated layer transformations
dbt run --select curated

# Run analytics views
dbt run --select views

# Run data quality tests
dbt test
```

### Setting Up Airflow

####  to initialize airflow:
```bash
     Step 1: Initialize the database
     bash airflow db init
     Step 2: Create an admin user (needed to log into the UI)
     bash airflow users create \
                       --username admin \
                                  --password admin \
                                             --firstname Admin \
                                                         --lastname User \
                                                                    --role Admin \
                                                                           --email admin@example.com
     Step 3: Start the scheduler (in one terminal tab)
     bash airflow scheduler
     Step 4: Start the webserver (in a new terminal tab)
     bash airflow api-server --port 8080
     Step 5: Open the UI
     Go to http://localhost:8080 in your browser and log in with admin / admin.
```
- Note: If you see a 500 error in Airflow, You will need to copy the dags in the repo to your global dag folder in order to run. All dags run from a global airflow folder at set up (/Users/xxxx/airflow/dags)
- Note: Also make sure in your airflow.cfg file, the dags_folder = is set to the right path. You may have a global files folder set up already. It will not orchestrate in airflow correctly if you fork this repo and don't set to the dbt folder in the repo path.

### Example Data

Explore the example data files in `example_data/` to see how data transforms at each layer:
- `raw_layer/` - Original source data
- `structured_layer/` - After standardization
- `curated_layer/` - After domain modeling
- `view_layer/` - Final analytics output

---

## Documentation

For more detailed information, see:

- **[Architecture Documentation](docs/architecture.md)**: Deep dive into the architecture and design decisions
- **[Source Systems Documentation](docs/source_systems.md)**: Detailed information about System A, System B, and data mappings
- **[Data Governance Documentation](docs/data_governance.md)**: Data quality, testing, and governance practices

---

## Key Takeaways

1. **Layered Architecture**: Raw → Structured → Curated → Views provides clear separation of concerns
2. **Domain-Driven Design**: Organizing by business domains makes data accessible to business users
3. **Standardization First**: The structured layer solves the "different field names" problem early
4. **Maintainability**: Clear structure makes it easy to add new systems or domains

---

## Questions?

This project demonstrates production-ready patterns for building scalable data pipelines. The structure, tooling, and domain organization can be adapted to any industry or use case.

For questions or contributions, please refer to the documentation in the `docs/` directory.
