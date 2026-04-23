--Bronze Layer Data Quality Validation
--This script performs Data Quality (DQ) Checks on the bronze level tables to ensure data integrity before moving to the silver layer.

USE DataWarehouse
GO

-- bronze.crm_cust_info table
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


-- bronze.crm_prd_info table
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No results

SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- check for unwanted spaces
-- Expactation: No results
SELECT
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--check for NULLS or Negative Numbers
-- Expactation: No results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data standardization and Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for Valid Date Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt


-- bronze layer table validation: bronze.erp_cust_az12
-- Identify out of range date (customer orlder than 100 years or birthday in the future)
-- Expectation: No results
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data standardization & Consistensy 
SELECT DISTINCT
gen
FROM bronze.erp_cust_az12

  
-- bronze layer table validation: bronze.erp_loc_a101
-- Identify invalid values and data normalisation
-- Compare cid
SELECT cid FROM bronze.erp_loc_a101
SELECT cst_key FROM silver.crm_cust_info
  
-- Handle invalid values: replace "-" to "" and compare the results
SELECT
REPLACE(cid,'-','') cid,
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-','') NOT IN
(SELECT cst_key FROM silver.crm_cust_info)

  -- Data Standardisation & Consistency
SELECT DISTINCT
cntry AS old_cntry,
CASE WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) = 'DE' THEN 'Germany'
     WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) = '' OR cntry IS NULL THEN 'n/a'
     ELSE TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry


-- bronze layer table validation: bronze.erp_px_cat_g1v2
--Check for unwanted spaces
--Expectation: No results
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

--Check Data Standardisation and Consistency
SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2
