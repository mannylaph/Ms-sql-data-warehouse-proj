# Data Warehouse ETL Scripts  

## Overview  
This repository contains SQL scripts for managing the **ETL pipeline** and **data quality checks** across Bronze → Silver → Gold layers in a Data Warehouse.  

The repo follows a layered approach:  
- **Bronze Layer** → Raw ingestion and initial loading (stored procedures).  
- **Silver Layer** → Cleansed, standardized staging data (upstream).  
- **Gold Layer** → Curated business-ready dimensions and fact tables (views).  
- **Quality Checks** → Validation queries to ensure referential integrity and attribute consistency.  

---

## Folder & Script Structure  

### 1. Bronze Layer
- **Script:** `bronze_load_procedures.sql`  
- **Type:** Stored Procedures  
- **Description:**  
  - Loads raw data into the Bronze schema from external systems.  
  - Implements logging (start time, end time, batch duration).  
  - Error handling is built-in with `TRY…CATCH`.  
- **Inputs:** None (executed directly; sources configured within procedures).  
- **Execution:**  
  ```sql
  EXEC bronze.load_bronze;
  ```

**Bronze Script Example:**

```sql
/*****************************************************
Author: Emmanuel Iheukwumere  
Date: 2025-08-17  
Procedure: bronze.load_bronze  
Description: Loads raw source data into Bronze layer,  
             applies logging, and error handling.  
Inputs: None  
Execution: EXEC bronze.load_bronze;  

WARNING: This script should only be run in controlled 
ETL environments. Ensure source connectors are properly 
configured before execution.  
*****************************************************/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    -- Declare time-tracking variables
    DECLARE @start_time DATETIME, @end_time DATETIME,
            @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '========================================';
        PRINT 'Loading the Bronze Layer...';
        PRINT '========================================';

        -- ETL logic to insert into bronze tables would go here

        SET @batch_end_time = GETDATE();

        PRINT 'Bronze Load Successful.';
        PRINT 'Batch Start Time: ' + CONVERT(VARCHAR, @batch_start_time);
        PRINT 'Batch End Time: ' + CONVERT(VARCHAR, @batch_end_time);
    END TRY
    BEGIN CATCH
        PRINT 'Error occurred while loading Bronze Layer: ' + ERROR_MESSAGE();
    END CATCH
END;
GO
```

---

### 2. Gold Layer  
- **Script:** `gold_views.sql`  
- **Type:** Views  
- **Description:**  
  - Defines curated business tables:  
    - `gold.dim_customers` → Customer dimension (CRM + ERP merge).  
    - `gold.dim_products` → Product dimension (active products only).  
    - `gold.fact_sales` → Fact table joining sales transactions to dimensions.  
  - Uses `IF OBJECT_ID…DROP VIEW` logic to safely recreate views.  
- **Inputs:** Underlying `silver.*` staging tables.  
- **Execution:** Run the script directly in SQL Server to (re)create views.

**Gold Script Example:**

```sql
/*****************************************************
Author: Emmanuel Iheukwumere  
Date: 2025-08-17  
Script: gold_views.sql  
Description: Creates Gold Layer views for curated business  
             reporting (Dimensions + Facts).  
Inputs: Silver tables (crm_customers, crm_sales_details, etc.)  
Execution: Run directly in SQL Server (idempotent).  

WARNING: Ensure Silver layer is fully refreshed before 
rebuilding Gold views. Running against stale data may 
produce inconsistent reporting outputs.  
*****************************************************/

-- Drop and recreate Customer Dimension
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
SELECT customer_id, first_name, last_name, gender, birth_date
FROM silver.crm_customers;
GO

-- Drop and recreate Product Dimension
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT product_id, product_name, category, price
FROM silver.crm_products
WHERE product_end_date IS NULL;
GO

-- Drop and recreate Sales Fact
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT s.sales_id, s.customer_number, s.product_number, s.sales_date, s.amount
FROM silver.crm_sales_details s;
GO
```

---

### 3. Quality Checks  
- **Script:** `quality_checks.sql`  
- **Type:** Ad-hoc Queries  
- **Description:**  
  - Validates the Gold schema:  
    - **Data Preview:** Spot-check sample rows.  
    - **Attribute Checks:** Distinct values for categorical fields (e.g., gender).  
    - **Integrity Checks:** Ensures every Fact row maps to valid Dimension rows.  
    - **Row Count Reconciliation:** Compares row counts between Fact and source Silver table to confirm no missing transactions.  
- **Inputs:** None. Queries reference Gold views directly.  
- **Execution:** Run queries individually; they do not modify data.  
- **Output:**  
  - `SELECT` result sets for manual inspection or pipeline validation.  

**Quality Checks Script Example:**

```sql
/*****************************************************
Author: Emmanuel Iheukwumere  
Date: 2025-08-17  
Script: quality_checks.sql  
Description: Validation queries for Gold Layer (data preview,  
             distinct checks, referential integrity,  
             and row count reconciliation).  
Inputs: Gold views (dim_customers, dim_products, fact_sales).  
Execution: Run queries individually (read-only).  

WARNING: This script is read-only. It should not be used 
to modify production data. Integrate with CI/CD pipelines 
for automated data validation.  
*****************************************************/

-- Preview Customers
SELECT TOP 10 * FROM gold.dim_customers;

-- Distinct Gender Values
SELECT DISTINCT gender FROM gold.dim_customers;

-- Preview Sales Facts
SELECT TOP 10 * FROM gold.fact_sales;

-- Null Check (Fact to Dimension Mapping)
SELECT *
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c ON s.customer_number = c.customer_id
LEFT JOIN gold.dim_products p ON s.product_number = p.product_id
WHERE c.customer_id IS NULL OR p.product_id IS NULL;

-- Row Count Reconciliation
SELECT 
    (SELECT COUNT(*) FROM gold.fact_sales) AS fact_sales_count,
    (SELECT COUNT(*) FROM silver.crm_sales_details) AS source_sales_count;
```

---

## Example Workflow  

1. **Run Bronze Procedures**  
   ```sql
   EXEC bronze.load_bronze;
   ```

2. **Refresh Gold Views**  
   ```sql
   :r gold_views.sql
   ```

3. **Run Quality Checks**  
   ```sql
   :r quality_checks.sql
   ```

---

## Caveats & Notes  
- All scripts are **idempotent** (safe to re-run).  
- `gold.dim_products` filters out expired products (`WHERE product_end_date IS NULL`).  
- QC script is **read-only** and intended for validation pipelines.  
- **Row Count Reconciliation** helps ensure completeness of Fact data. A mismatch may indicate dropped records during transformation.  
- For automated CI/CD, integrate QC queries into test suites with row-count and null-check validations.  
