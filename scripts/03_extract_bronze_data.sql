CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
PRINT '===========================================';
PRINT 'Loading Bronze Layer';
PRINT '===========================================';

PRINT '-------------------------------------------';
PRINT 'Loading CRM Tables';
PRINT '-------------------------------------------';

PRINT '>> Trancating Table: bronze.crm_cust_info';
TRUNCATE TABLE bronze.crm_cust_info;

PRINT '>> Inserting Data Info: bronze.crm_cust_info';
BULK INSERT bronze.crm_cust_info
FROM '/var/opt/mssql/datasets/cust_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

PRINT '>> Trancating Table: bronze.crm_prd_info';
TRUNCATE TABLE bronze.crm_prd_info;

PRINT '>> Inserting Data Info: bronze.crm_prd_info';
BULK INSERT bronze.crm_prd_info
FROM '/var/opt/mssql/datasets/prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

PRINT '>> Trancating Table: bronze.crm_sales_details';
TRUNCATE TABLE bronze.crm_sales_details;

PRINT '>> Inserting Data Info: bronze.crm_sales_details';
BULK INSERT bronze.crm_sales_details
FROM '/var/opt/mssql/datasets/sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

PRINT '-------------------------------------------';
PRINT 'Loading ERP Tables';
PRINT '-------------------------------------------';

PRINT '>> Trancating Table: bronze.erp_cust_az12';
TRUNCATE TABLE bronze.erp_cust_az12;

PRINT '>> Inserting Data Info: bronze.erp_cust_az12';
BULK INSERT bronze.erp_cust_az12
FROM '/var/opt/mssql/datasets/CUST_AZ12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

PRINT '>> Trancating Table: bronze.erp_loc_a101';
TRUNCATE TABLE bronze.erp_loc_a101;

PRINT '>> Inserting Data Info: bronze.erp_loc_a101';
BULK INSERT bronze.erp_loc_a101
FROM '/var/opt/mssql/datasets/LOC_A101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

PRINT '>> Trancating Table: bronze.erp_px_cat_g1v2';
TRUNCATE TABLE bronze.erp_px_cat_g1v2;

PRINT '>> Inserting Data Info: bronze.erp_px_cat_g1v2';
BULK INSERT bronze.erp_px_cat_g1v2
FROM '/var/opt/mssql/datasets/PX_CAT_G1V2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
END
