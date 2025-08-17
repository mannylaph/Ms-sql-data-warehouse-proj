/*****************************************************************************************
    Stored Procedure :   silver.load_silver
    Description     :   Transforms and loads cleaned data from the Bronze Layer into the
                        Silver Layer (conformed, deduplicated, and business-ready tables). 
                        The procedure truncates existing Silver tables before reloading.

    Author          :   Emmanuel Iheukwumere
    Created On      :   2025-08-17
    Environment     :   Microsoft SQL Server

    Input Params    :   None

    Output          :   None (writes progress and duration to console via PRINT)

    How to Run      :   EXEC silver.load_silver;

    WARNING         :   ⚠️ This procedure will TRUNCATE all existing data in the Silver 
                        Layer tables before inserting transformed records. Ensure that 
                        Bronze layer is fully loaded and validated before execution.
******************************************************************************************/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    -- Declare time-tracking variables
    DECLARE 
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '==================================================';
        PRINT '🚀 Starting Silver Layer Data Load';
        PRINT '==================================================';

        /*********************************************************
         * CRM Tables Section
         *********************************************************/
        PRINT '--------------------------------------------------';
        PRINT '📁 Loading CRM Tables';
        PRINT '--------------------------------------------------';

        -- silver.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data into Table: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firtname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firtname),
            TRIM(cst_lastname),
            CASE UPPER(TRIM(cst_marital_status))
                WHEN 'S' THEN 'Single'
                WHEN 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            CASE UPPER(TRIM(cst_gndr))
                WHEN 'M' THEN 'Male'
                WHEN 'F' THEN 'Female'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
        ) c
        WHERE flag_last = 1;
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '............';

        -- silver.crm_product_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_product_info';
        TRUNCATE TABLE silver.crm_product_info;

        PRINT '>> Inserting Data into Table: silver.crm_product_info';
        INSERT INTO silver.crm_product_info (
            product_id, category_id, product_key, product_name,
            product_cost, product_line, product_start_date, product_end_date
        )
        SELECT 
            product_id,
            REPLACE(SUBSTRING(product_key,1,5),'-','_') AS category_id,
            SUBSTRING(product_key,7,LEN(product_key)) AS product_key,
            product_name,
            COALESCE(product_cost, 0),
            CASE UPPER(TRIM(product_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            CAST(product_start_date AS DATE),
            CAST(LEAD(product_start_date) OVER (PARTITION BY SUBSTRING(product_key,7,LEN(product_key)) ORDER BY product_start_date) - 1 AS DATE)
        FROM bronze.crm_product_info;
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '............';

        -- silver.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data into Table: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sales_order_num, sales_product_key, sales_customer_id,
            sales_order_dt, sales_ship_dt, sales_due_date,
            sales_sales, sales_quantity, sales_price
        )
        SELECT 
            sales_order_num,
            sales_product_key,
            sales_customer_id,
            CASE WHEN sales_order_dt = 0 OR LEN(sales_order_dt) != 8 THEN NULL ELSE CAST(CAST(sales_order_dt AS NVARCHAR) AS DATE) END,
            CASE WHEN sales_ship_dt = 0 OR LEN(sales_ship_dt) != 8 THEN NULL ELSE CAST(CAST(sales_ship_dt AS NVARCHAR) AS DATE) END,
            CASE WHEN sales_due_date = 0 OR LEN(sales_due_date) != 8 THEN NULL ELSE CAST(CAST(sales_due_date AS NVARCHAR) AS DATE) END,
            CASE WHEN sales_sales IS NULL OR sales_sales <= 0 OR sales_sales != sales_quantity * ABS(sales_price)
                 THEN sales_quantity * ABS(sales_price)
                 ELSE sales_sales
            END,
            sales_quantity,
            CASE WHEN sales_price IS NULL OR sales_price <= 0
                 THEN sales_sales / NULLIF(sales_quantity, 0)
                 ELSE sales_price
            END
        FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '............';


        /*********************************************************
         * ERP Tables Section
         *********************************************************/
        PRINT '--------------------------------------------------';
        PRINT '📁 Loading ERP Tables';
        PRINT '--------------------------------------------------';

        -- silver.erp_customers
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_customers';
        TRUNCATE TABLE silver.erp_customers;

        PRINT '>> Inserting Data into Table: silver.erp_customers';
        INSERT INTO silver.erp_customers (
            Customer_id, Birth_date, Gender
        )
        SELECT
            CASE WHEN Customer_id LIKE 'NAS%' THEN SUBSTRING(Customer_id, 4, LEN(Customer_id)) ELSE Customer_id END,
            CASE WHEN Birth_date > GETDATE() THEN NULL ELSE Birth_date END,
            CASE 
                WHEN UPPER(TRIM(Gender)) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(Gender)) IN ('M','MALE') THEN 'Male'
                ELSE 'N/A'
            END
        FROM bronze.erp_customers;
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '............';

        -- silver.erp_location
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_location';
        TRUNCATE TABLE silver.erp_location;

        PRINT '>> Inserting Data into Table: silver.erp_location';
        INSERT INTO silver.erp_location (
            Customer_id, Country
        )
        SELECT
            REPLACE(Customer_id, '-', ''),
            CASE 
                WHEN TRIM(Country) = 'DE' THEN 'Germany'
                WHEN TRIM(Country) IN ('US','USA') THEN 'United States'
                WHEN TRIM(Country) IS NULL OR TRIM(Country) = '' THEN 'N/A'
                ELSE TRIM(Country)
            END
        FROM bronze.erp_location;
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '............';

        -- silver.erp_category
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_category';
        TRUNCATE TABLE silver.erp_category;

        PRINT '>> Inserting Data into Table: silver.erp_category';
        INSERT INTO silver.erp_category (
            id, category, subcategory, maintenance
        )
        SELECT 
            id, category, subcategory, maintenance
        FROM bronze.erp_category;
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        /*********************************************************
         * Batch Summary
         *********************************************************/
        SET @batch_end_time = GETDATE();

        PRINT '==================================================';
        PRINT '✅ Silver Layer Load Completed Successfully';
        PRINT '==================================================';
        PRINT '📦 Total Batch Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
    END TRY

    BEGIN CATCH
        PRINT '==================================================';
        PRINT '❌ ERROR: Silver Layer Load Failed';
        PRINT 'Message       : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==================================================';
    END CATCH
END;
GO
