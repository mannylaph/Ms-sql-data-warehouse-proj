/*****************************************************************************************
    Script Name :   Initialize_DataWarehouse.sql
    Description :   This script initializes the 'DataWarehouse' database on SQL Server.
                    It performs the following actions:
                    - Drops the existing 'DataWarehouse' database if it exists
                    - Recreates the 'DataWarehouse' database
                    - Creates schema layers: bronze, silver, and gold
                    
    Author      :   Emmanuel Iheukwumere
    Created On  :   2025-07-25
    Environment :   Microsoft SQL Server

    WARNING     :   ⚠️ This script will irreversibly DROP and RECREATE the entire 
                    'DataWarehouse' database. All existing data and objects will be lost.
                    Use with caution in development environments only.

    Note        :   Intended for initial setup and testing purposes only.
******************************************************************************************/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the new database
USE DataWarehouse;
GO

-- Create Bronze Schema
CREATE SCHEMA bronze;
GO

-- Create Silver Schema
CREATE SCHEMA silver;
GO

-- Create Gold Schema
CREATE SCHEMA gold;
GO

