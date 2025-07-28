/*****************************************************************************************
    Script Name :   Create_Bronze_Tables.sql
    Description :   This script creates source (bronze layer) staging tables for 
                    raw data ingestion in the 'DataWarehouse' database. It drops 
                    existing tables if they already exist and recreates them to 
                    ensure a clean slate for development/testing.

    Author      :   Emmanuel Iheukwumere
    Created On  :   2025-07-25
    Environment :   Microsoft SQL Server

    WARNING     :   ⚠️ This script will DROP and RECREATE all bronze-layer tables.
                    Any existing data in these tables will be permanently lost.
                    Use only in development or controlled environments.

    Note        :   These tables serve as raw staging structures for CRM and ERP systems.
******************************************************************************************/

-- ============================================
-- CRM Customer Info (bronze.crm_cust_info)
-- ============================================
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firtname        NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
GO

-- ============================================
-- CRM Product Info (bronze.crm_product_info)
-- ============================================
IF OBJECT_ID('bronze.crm_product_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_product_info;
GO

CREATE TABLE bronze.crm_product_info (
    product_id          INT,
    product_key         NVARCHAR(50),
    product_name        NVARCHAR(50),
    product_cost        INT,
    product_line        NVARCHAR(50),
    product_start_date  DATETIME,
    product_end_date    DATETIME
);
GO

-- ============================================
-- CRM Sales Details (bronze.crm_sales_details)
-- ============================================
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sales_order_num     NVARCHAR(50),
    sales_product_key   NVARCHAR(50),
    sales_customer_id   INT,
    sales_order_dt      INT,
    sales_ship_dt       INT,
    sales_due_date      INT,
    sales_sales         INT,
    sales_quantity      INT,
    sales_price         INT
);
GO

-- ============================================
-- ERP Customers (bronze.erp_Customers)
-- ============================================
IF OBJECT_ID('bronze.erp_Customers', 'U') IS NOT NULL
    DROP TABLE bronze.erp_Customers;
GO

CREATE TABLE bronze.erp_Customers (
    Customer_id         NVARCHAR(50),
    Birth_date          DATE,
    Gender              NVARCHAR(50)
);
GO

-- ============================================
-- ERP Location (bronze.erp_location)
-- ============================================
IF OBJECT_ID('bronze.erp_location', 'U') IS NOT NULL
    DROP TABLE bronze.erp_location;
GO

CREATE TABLE bronze.erp_location (
    Customer_id         NVARCHAR(50),
    Country             NVARCHAR(50)
);
GO

-- ============================================
-- ERP Category (bronze.erp_category)
-- ============================================
IF OBJECT_ID('bronze.erp_category', 'U') IS NOT NULL
    DROP TABLE bronze.erp_category;
GO

CREATE TABLE bronze.erp_category (
    id                  NVARCHAR(50),
    category            NVARCHAR(50),
    subcategory         NVARCHAR(50),
    maintenance         NVARCHAR(50)
);
GO
