# 🏛️ SQL Data Warehouse Project

A production-style **Data Warehouse** built entirely in SQL (PostgreSQL), implementing the **Medallion Architecture** (Bronze → Silver → Gold) to consolidate, cleanse, and transform raw sales data from two source systems — **CRM** and **ERP** — into a clean Star Schema ready for business intelligence and analytics.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Flow & Layers](#data-flow--layers)
- [Gold Layer Data Model](#gold-layer-data-model)
- [Getting Started](#getting-started)
- [Data Quality Checks](#data-quality-checks)
- [Naming Conventions](#naming-conventions)

---

## Overview

This project demonstrates a complete data warehousing solution built as a portfolio project, covering the full pipeline from raw CSV ingestion to a business-ready Star Schema:

- **Two source systems**: CRM (customer & product data) and ERP (location, demographics, product categories)
- **Medallion Architecture**: Three-layer pipeline — Bronze (raw), Silver (cleansed), Gold (business-ready)
- **Star Schema**: One fact table and two dimension tables optimized for analytical queries
- **Data cleansing**: Deduplication, type normalization, text standardization, sales recalculation, and date validation
- **Stored procedures**: Automated, reusable `load_bronze()` and `load_silver()` procedures with timing logs
- **Quality checks**: Dedicated SQL test scripts validating integrity at both Silver and Gold layers

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                            │
│                                                                  │
│   ┌──────────────────────┐   ┌──────────────────────────────┐  │
│   │        CRM           │   │             ERP              │  │
│   │  cust_info.csv       │   │  CUST_AZ12.csv (demographics)│  │
│   │  prd_info.csv        │   │  LOC_A101.csv  (locations)   │  │
│   │  sales_details.csv   │   │  PX_CAT_G1V2.csv (categories)│  │
│   └──────────┬───────────┘   └──────────────┬───────────────┘  │
└──────────────┼──────────────────────────────┼───────────────────┘
               │                              │
               ▼                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER  (schema: bronze)                               │
│  Raw ingestion via COPY command — no transformations             │
│  Procedure: bronze.load_bronze()                                 │
│                                                                  │
│  crm_cust_info │ crm_prd_info │ crm_sales_details               │
│  erp_cust_az12 │ erp_loc_a101 │ erp_px_cat_g1v2                │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER  (schema: silver)                               │
│  Cleansed, deduplicated, standardized, type-corrected            │
│  Procedure: silver.load_silver()                                 │
│  + dwh_create_date metadata column on all tables                 │
│                                                                  │
│  crm_cust_info │ crm_prd_info │ crm_sales_details               │
│  erp_cust_az12 │ erp_loc_a101 │ erp_px_cat_q1v2                │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER  (schema: gold)  — VIEWS                         │
│  Star Schema for BI and analytical reporting                     │
│                                                                  │
│        dim_customers ◄──── fact_sales ────► dim_products        │
└──────────────────────────────────────────────────────────────────┘
```

---

## Data Sources

### CRM System (`datasets/source_crm/`)

| File | Rows | Description |
|---|---|---|
| `cust_info.csv` | ~18,000 | Customer profiles: ID, name, marital status, gender, creation date |
| `prd_info.csv` | ~500 | Product catalog: ID, key, name, cost, product line, start/end dates |
| `sales_details.csv` | ~60,000+ | Transactional sales: order number, product, customer, dates, amount, quantity, price |

### ERP System (`datasets/source_erp/`)

| File | Rows | Description |
|---|---|---|
| `CUST_AZ12.csv` | ~18,000 | Customer demographics: birthdate, gender (alternate source) |
| `LOC_A101.csv` | ~18,000 | Customer geographic data: country of residence |
| `PX_CAT_G1V2.csv` | 37 | Product category & subcategory reference: 4 categories, 37 subcategories |

**Product Categories in ERP:**
- **Bikes** — Mountain Bikes, Road Bikes, Touring Bikes
- **Accessories** — Helmets, Lights, Tires, Locks, etc.
- **Clothing** — Jerseys, Gloves, Shorts, Socks, etc.
- **Components** — Frames, Brakes, Chains, Wheels, etc.

---

## Tech Stack

| Component | Technology |
|---|---|
| Database | PostgreSQL 15+ |
| Language | SQL (PL/pgSQL for stored procedures) |
| Architecture | Medallion Architecture (Bronze / Silver / Gold) |
| Data Model | Star Schema (Fact + Dimensions) |
| ETL Method | Stored Procedures (`CALL layer.load_()`) |
| Raw Ingestion | PostgreSQL `COPY` command |

---

## Project Structure

```
sql-data-warehouse-project/
├── datasets/
│   ├── source_crm/
│   │   ├── cust_info.csv          # CRM customer data
│   │   ├── prd_info.csv           # CRM product catalog
│   │   └── sales_details.csv      # CRM sales transactions
│   └── source_erp/
│       ├── CUST_AZ12.csv          # ERP customer demographics
│       ├── LOC_A101.csv           # ERP customer locations
│       └── PX_CAT_G1V2.csv       # ERP product categories
│
├── docs/
│   ├── data_catalog               # Gold layer table & column definitions
│   └── naming_convention          # Schema, table, column naming standards
│
├── sctipts/                       # (note: folder typo preserved from source)
│   ├── init_database.sql          # Create bronze/silver/gold schemas
│   ├── bronze/
│   │   └── bronze_layer.sql       # Bronze DDL + load_bronze() procedure
│   ├── silver/
│   │   └── silver_layer.sql       # Silver DDL + load_silver() procedure
│   └── gold/
│       └── gold_layer.sql         # Gold layer VIEW definitions (Star Schema)
│
└── tests/
    ├── quality_checks_silver.sql  # Data quality checks for Silver layer
    └── quality_checks_gold.sql    # Integrity checks for Gold layer
```

---

## Data Flow & Layers

### 🥉 Bronze Layer — Raw Ingestion

**Script**: `sctipts/bronze/bronze_layer.sql`  
**Procedure**: `CALL bronze.load_bronze();`

Creates 6 tables mirroring the exact structure of the source CSV files. Data is loaded using PostgreSQL's `COPY` command with no transformations. Each table load is timed and logged via `RAISE NOTICE`.

| Table | Source |
|---|---|
| `bronze.crm_cust_info` | `source_crm/cust_info.csv` |
| `bronze.crm_prd_info` | `source_crm/prd_info.csv` |
| `bronze.crm_sales_details` | `source_crm/sales_details.csv` |
| `bronze.erp_cust_az12` | `source_erp/CUST_AZ12.csv` |
| `bronze.erp_loc_a101` | `source_erp/LOC_A101.csv` |
| `bronze.erp_px_cat_g1v2` | `source_erp/PX_CAT_G1V2.csv` |

---

### 🥈 Silver Layer — Cleansing & Transformation

**Script**: `sctipts/silver/silver_layer.sql`  
**Procedure**: `CALL silver.load_silver();`

All 6 tables are cleansed and loaded from Bronze into Silver. A `dwh_create_date TIMESTAMP` metadata column is added to every table for auditing.

**Transformations applied per table:**

**`silver.crm_cust_info`** — Customer master data
- Deduplication using `ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC)` — keeps the most recent record per customer
- Removes NULL `cst_id` records
- `TRIM()` applied to first name and last name to remove leading/trailing spaces
- Gender normalized: `'M'` → `'Male'`, `'F'` → `'Female'`, everything else → `'n/a'`
- Marital status normalized: `'M'` → `'Married'`, `'S'` → `'Single'`, everything else → `'n/a'`

**`silver.crm_prd_info`** — Product catalog
- Product key split into `cat_id` (first 5 chars, dashes replaced with underscores) and `prd_key` (remaining chars)
- `NULL` cost replaced with `0` using `COALESCE`
- Product line codes expanded: `'M'` → `'Mountain'`, `'R'` → `'Road'`, `'S'` → `'Other Sales'`, `'T'` → `'Touring'`
- `prd_start_dt` cast from `TIMESTAMP` to `DATE`
- `prd_end_dt` calculated using `LEAD()` window function — derived as one day before the next product version's start date

**`silver.crm_sales_details`** — Sales transactions
- Date columns stored as integers (e.g., `20101229`) converted to proper `DATE` type; invalid values (0 or not 8 digits) set to `NULL`
- Sales amount recalculated: if `sls_sales` is NULL, negative, or inconsistent with `quantity × price`, it is recalculated as `sls_quantity × ABS(sls_price)`
- Price corrected: if NULL or negative, derived as `sls_sales / sls_quantity`

**`silver.erp_loc_a101`** — Customer locations
- Hyphens stripped from customer ID (`AW-00011000` → `AW00011000`)
- Country codes normalized: `'DE'` → `'Germany'`, `'US'`/`'USA'` → `'United States'`, empty strings → `'n/a'`

**`silver.erp_cust_az12`** — Customer demographics
- `'NAS'` prefix stripped from customer IDs (e.g., `NASAW00011000` → `AW00011000`) for key matching
- Future birthdates set to `NULL` (data quality fix)
- Gender normalized: `'F'`/`'FEMALE'` → `'Female'`, `'M'`/`'MALE'` → `'Male'`, else `'n/a'`

**`silver.erp_px_cat_q1v2`** — Product categories
- Loaded as-is from Bronze (already clean reference data)

---

### 🥇 Gold Layer — Business Star Schema

**Script**: `sctipts/gold/gold_layer.sql`

The Gold layer consists of **three SQL Views** implementing a Star Schema. Surrogate keys are generated using `ROW_NUMBER() OVER (...)`.

#### `gold.dim_customers`

Joins `silver.crm_cust_info` (primary) with `silver.erp_cust_az12` (demographics) and `silver.erp_loc_a101` (geography) using the normalized customer key as the join key.

- Gender resolution: CRM value is used when available (not `'n/a'`); otherwise falls back to ERP gender via `COALESCE`
- Surrogate key: `customer_key` (auto-generated via `ROW_NUMBER()`)

#### `gold.dim_products`

Joins `silver.crm_prd_info` with `silver.erp_px_cat_q1v2` on `cat_id`.

- Filters to **current products only** (`WHERE prd_end_dt IS NULL`) — historical versions excluded
- Surrogate key: `product_key` (ordered by `prd_start_dt`, then `prd_key`)

#### `gold.fact_sales`

Joins `silver.crm_sales_details` with `gold.dim_products` and `gold.dim_customers` to resolve surrogate keys.

- References dimension views directly via surrogate keys (`product_key`, `customer_key`)
- Contains business metrics: `sales_amount`, `quantity`, `price`

---

## Gold Layer Data Model

```
                    ┌─────────────────────┐
                    │   gold.dim_customers │
                    │─────────────────────│
                    │ customer_key (PK)    │
                    │ customer_id          │
                    │ customer_number      │
                    │ first_name           │
                    │ last_name            │
                    │ country              │
                    │ marital_status       │
                    │ gender               │
                    │ birthdate            │
                    │ create_date          │
                    └──────────┬──────────┘
                               │ (Many-to-One)
                               │
┌──────────────────────────────▼──────────────────────────────┐
│                        gold.fact_sales                        │
│──────────────────────────────────────────────────────────────│
│ order_number                                                  │
│ product_key  (FK → dim_products.product_key)                 │
│ customer_key (FK → dim_customers.customer_key)               │
│ order_date                                                    │
│ shipping_date                                                 │
│ due_date                                                      │
│ sales_amount                                                  │
│ quantity                                                      │
│ price                                                         │
└──────────────────────────────┬──────────────────────────────┘
                               │ (Many-to-One)
                               │
                    ┌──────────▼──────────┐
                    │   gold.dim_products  │
                    │─────────────────────│
                    │ product_key (PK)     │
                    │ product_id           │
                    │ product_number       │
                    │ product_name         │
                    │ category_id          │
                    │ category             │
                    │ subcategory          │
                    │ maintenance          │
                    │ cost                 │
                    │ product_line         │
                    │ start_date           │
                    └─────────────────────┘
```

---

## Getting Started

### Prerequisites

- PostgreSQL 15+ installed and running
- A database client (psql, DBeaver, pgAdmin, etc.)
- This repository cloned locally

### 1. Create the Database

```sql
-- In psql or your database client
CREATE DATABASE data_warehouse;
\c data_warehouse
```

### 2. Initialize Schemas

```sql
-- Run the initialization script
\i sctipts/init_database.sql
```

This creates three schemas: `bronze`, `silver`, and `gold`.

### 3. Set Up and Load the Bronze Layer

```sql
\i sctipts/bronze/bronze_layer.sql
```

> **Important**: Before running, update the file paths in `bronze.load_bronze()` to match your local directory. Locate the `COPY` statements and replace the hardcoded Windows path (`D:/1ProjectPorto/...`) with your absolute path to the `datasets/` folder.

```sql
-- Example path update inside the procedure:
-- Change: 'D:/1ProjectPorto/.../cust_info.csv'
-- To:     '/your/local/path/datasets/source_crm/cust_info.csv'
```

Then call the procedure:

```sql
CALL bronze.load_bronze();
```

### 4. Load and Transform the Silver Layer

```sql
\i sctipts/silver/silver_layer.sql
CALL silver.load_silver();
```

### 5. Create the Gold Layer Views

```sql
\i sctipts/gold/gold_layer.sql
```

### 6. Verify the Full Pipeline

```sql
-- Sanity check: total sales by country and category
SELECT
    c.country,
    p.category,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers c ON c.customer_key = f.customer_key
JOIN gold.dim_products p  ON p.product_key  = f.product_key
GROUP BY c.country, p.category
ORDER BY total_sales DESC
LIMIT 10;
```

---

## Data Quality Checks

Run after each layer to validate the pipeline output.

### Silver Layer Checks (`tests/quality_checks_silver.sql`)

| Check | Table | Expected Result |
|---|---|---|
| Duplicate / NULL primary keys | `crm_cust_info` | Empty set |
| Unwanted whitespace in `cst_key` | `crm_cust_info` | Empty set |
| Valid marital status values | `crm_cust_info` | Only: `Married`, `Single`, `n/a` |
| Duplicate / NULL product IDs | `crm_prd_info` | Empty set |
| Negative or NULL product cost | `crm_prd_info` | Empty set |
| Invalid date order (start > end) | `crm_prd_info` | Empty set |
| Order date after ship/due date | `crm_sales_details` | Empty set |
| Sales ≠ Quantity × Price | `crm_sales_details` | Empty set |
| Out-of-range birthdates | `erp_cust_az12` | Dates within 1924–today |
| Whitespace in category fields | `erp_px_cat_q1v2` | Empty set |
| Country value consistency | `erp_loc_a101` | Full country names only |

### Gold Layer Checks (`tests/quality_checks_gold.sql`)

| Check | Expected Result |
|---|---|
| Duplicate `customer_key` in `dim_customers` | Empty set |
| Duplicate `product_key` in `dim_products` | Empty set |
| Orphaned rows in `fact_sales` (no matching customer or product) | Empty set |
| Sanity check: total sales by country and category | Returns data |

---

## Naming Conventions

The project follows strict naming standards documented in `docs/naming_convention`.

### Schema / Layer Naming

| Layer | Schema | Purpose |
|---|---|---|
| Bronze | `bronze` | Raw, unmodified source data |
| Silver | `silver` | Cleansed and standardized data |
| Gold | `gold` | Business-ready Star Schema views |

### Table Naming

| Layer | Format | Example |
|---|---|---|
| Bronze & Silver | `<sourcesystem>_<entity>` | `bronze.crm_cust_info`, `silver.erp_loc_a101` |
| Gold Dimensions | `dim_<entity>` | `gold.dim_customers` |
| Gold Facts | `fact_<entity>` | `gold.fact_sales` |

### Column Naming

| Convention | Usage | Example |
|---|---|---|
| `_key` suffix | Surrogate keys in Gold layer | `customer_key`, `product_key` |
| `_id` suffix | Business/source system keys | `cst_id`, `prd_id` |
| `dwh_` prefix | DWH metadata/audit columns | `dwh_create_date` |

### Stored Procedures

All data loading procedures use the `load_<layer>` naming pattern:

```sql
CALL bronze.load_bronze();   -- Ingest raw CSV data
CALL silver.load_silver();   -- Clean and transform data
```

---

## License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

---

## About

Built by **Rio Al Fandi** as a data engineering portfolio project, exploring core concepts and best practices in building a modern SQL Data Warehouse. Inspired by the *Data With Baraa* YouTube channel.
