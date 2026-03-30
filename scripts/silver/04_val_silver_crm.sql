-- Silver Layer Post-Transformation Validatation
-- This script serves as a Quality Assurance (QA) step to verify the results of the transformation process. It ensures that the data in the silver.crm_cust_info table meets the expected standards after cleaning

USE DataWarehouse
GO

-- Check for NULLS or Duplicates in Primary Key
-- Expectation: No result

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) > 1 OR cst_id IS NULL


-- Check for unwanted spaces
-- Expectation: No results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Data Standardisaiton & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info

--Check the whole silver layer crm_cust_info table
SELECT *
FROM silver.crm_cust_info
