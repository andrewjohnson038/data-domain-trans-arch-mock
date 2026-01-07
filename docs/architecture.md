# Architecture Documentation

## Overview
This document describes the architecture of the Lending Analytics Platform.

## Data Flow
```
Raw Data (S3) → Structured Layer (Snowflake) → Curated Domain Mart → Analytics Views
```

## Layers

### 1. Raw Layer
- Source system data preserved as-is in S3
- No transformations applied
- Maintains original schema and format

### 2. Structured Layer
- Standardized schemas with metadata enrichment
- Field name standardization across source systems
- Metadata tracking (batch_date, source_system, ingestion_ts)

### 3. Curated Layer
- Business domain models (loans, customers, accounts)
- Unified representation across source systems
- Domain-driven design principles

### 4. View Layer
- Analytics-ready aggregations
- Pre-computed metrics for reporting
- Optimized for query performance

## Technology Stack
- **Orchestration**: Apache Airflow (AWS MWAA)
- **Transformation**: dbt (data build tool)
- **Data Warehouse**: Snowflake
- **Storage**: AWS S3
- **Language**: Python, SQL
- **Logging**: Python - set alerts on batch processing throughout process

