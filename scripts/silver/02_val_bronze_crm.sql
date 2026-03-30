--Bronze Layer Data Quality Validation
--This script performs Data Quality (DQ) Checks on the bronze.crm_cust_info table to ensure data integrity before moving to the silver layer.

USE DataWarehouse
GO

-- Check for NULLS or Duplicates in Primary Key
-- Expectation: No result

SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) > 1 OR cst_id IS NULL


-- Check for unwanted spaces
-- Expectation: No results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Data Standardisaiton & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info
