--Silver Layer ETL: CRM Transformation
--This script automates the Cleaning, Standardization, and Deduplication of CRM customer data as it moves from the Bronze (Raw) layer to the Silver (Refined) layer.

USE DataWarehouse
GO

-- Customer Information Table Transformation

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date
)
SELECT
cst_id,
cst_key,
TRIM (cst_firstname) AS cst_firstname,
TRIM (cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
     WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
     ELSE 'n/a'
END cst_material_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1


-- silver.crm_prd_info Table Transformation
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info; 
CREATE TABLE silver.crm_prd_info(
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
     WHEN 'M' THEN 'Mountain'
     WHEN 'R' THEN 'Road'
     WHEN 'S' THEN 'Other Sales'
     WHEN 'T' THEN 'Touring'
     ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt ,
CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info


---- silver.erp_cust_az12 Table Transformation
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate
END AS bdate,
CASE 
    WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(10), ''), CHAR(13), ''))) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(10), ''), CHAR(13), ''))) IN ('M', 'MALE') THEN 'Male'
    ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

---- silver.erp_loc_a101 Table Transformation
INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT
REPLACE(cid, '-', '') AS cid,
CASE WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) = 'DE' THEN 'Germany'
     WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))
END AS cntry
FROM bronze.erp_loc_a101
