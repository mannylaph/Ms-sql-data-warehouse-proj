/*****************************************************************************************
    Stored Procedure :   bronze.load_bronze
    Description     :   Loads raw (bronze layer) staging tables from CSV files using 
                        BULK INSERT. The procedure truncates existing data and reloads 
                        from external flat files, recording load durations.

    Author          :   Emmanuel Iheukwumere
    Created On      :   2025-07-25
    Environment     :   Microsoft SQL Server

    Input Params    :   None

    Output          :   None (writes progress and duration to output using PRINT)

    How to Run      :   EXEC bronze.load_bronze;

    WARNING         :   ⚠️ This procedure will TRUNCATE all existing data in the bronze 
                        staging tables before inserting new records. Use only in 
                        development or during staging batch loads.
******************************************************************************************/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    -- Declare variables to capture timing for each load and the full batch
    DECLARE 
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '==================================================';
        PRINT '🚀 Starting Bronze Layer Data Load';
        PRINT '==================================================';

        /*********************************************************
         * Load CRM Source Tables
         *********************************************************/
        PRINT '--------------------------------------------------';
        PRINT '📁 Loading CRM Source Tables';
        PRINT '--------------------------------------------------';

        -- Load crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data into Table: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '............';

        -- Load crm_product_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_product_info';
        TRUNCATE TABLE bronze.crm_product_info;

        PRINT '>> Inserting Data into Table: bronze.crm_product_info';
        BULK INSERT bronze.crm_product_info
        FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '............';

        -- Load crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data into Table: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '............';

        /*********************************************************
         * Load ERP Source Tables
         *********************************************************/
        PRINT '--------------------------------------------------';
        PRINT '📁 Loading ERP Source Tables';
        PRINT '--------------------------------------------------';

        -- Load erp_customers
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_customers';
        TRUNCATE TABLE bronze.erp_customers;

        PRINT '>> Inserting Data into Table: bronze.erp_customers';
        BULK INSERT bronze.erp_customers
        FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '............';

        -- Load erp_location
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_location';
        TRUNCATE TABLE bronze.erp_location;

        PRINT '>> Inserting Data into Table: bronze.erp_location';
        BULK INSERT bronze.erp_location
        FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '............';

        -- Load erp_category
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_category';
        TRUNCATE TABLE bronze.erp_category;

        PRINT '>> Inserting Data into Table: bronze.erp_category';
        BULK INSERT bronze.erp_category
        FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @batch_end_time = GETDATE();

        PRINT '==================================================';
        PRINT '✅ Bronze Layer Load Completed Successfully';
        PRINT '==================================================';
        PRINT '📦 Total Batch Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
    END TRY

    BEGIN CATCH
        PRINT '==================================================';
        PRINT '❌ ERROR: Bronze Layer Load Failed';
        PRINT 'Message       : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==================================================';
    END CATCH
END;
GO
