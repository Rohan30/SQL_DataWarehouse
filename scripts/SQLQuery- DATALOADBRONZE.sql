--LOADING DATA INTO BRONZE LAYER TABLES
--DOING BULK INSERT from csv files instead of inserting ROW BY ROW

--loading into bronze.crm_cust_info(TRUNCATE+BULK INSERT)
TRUNCATE TABLE bronze.crm_cust_info
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\Rohan\Downloads\SQL Course - YT\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK);

--checking if data is present
SELECT * FROM bronze.crm_cust_info;
SELECT COUNT(*) AS row_count FROM bronze.crm_cust_info;

--loading into bronze.crm_prd_info(TRUNCATE+BULK INSERT)
TRUNCATE TABLE bronze.crm_prd_info
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\Rohan\Downloads\SQL Course - YT\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK);

--checking if data is present
SELECT * FROM bronze.crm_prd_info;
SELECT COUNT(*) AS row_count FROM bronze.crm_prd_info;


--loading into bronze.crm_sales_details(TRUNCATE+BULK INSERT)
TRUNCATE TABLE bronze.crm_sales_details
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\Rohan\Downloads\SQL Course - YT\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK);

--checking if data is present
SELECT * FROM bronze.crm_sales_details;
SELECT COUNT(*) AS row_count FROM bronze.crm_sales_details;

--loading into bronze.erp_cust_az12(TRUNCATE+BULK INSERT)
TRUNCATE TABLE bronze.erp_cust_az12
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\Rohan\Downloads\SQL Course - YT\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK);

--checking if data is present
SELECT * FROM bronze.erp_cust_az12;
SELECT COUNT(*) AS row_count FROM bronze.erp_cust_az12;


--loading into bronze.erp_loc_a101(TRUNCATE+BULK INSERT)
TRUNCATE TABLE bronze.erp_loc_a101
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\Rohan\Downloads\SQL Course - YT\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK);

--checking if data is present
SELECT * FROM bronze.erp_loc_a101;
SELECT COUNT(*) AS row_count FROM bronze.erp_loc_a101;


--loading into bronze.erp_px_cat_g1v2(TRUNCATE+BULK INSERT)
TRUNCATE TABLE bronze.erp_px_cat_g1v2
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\Rohan\Downloads\SQL Course - YT\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK);

--checking if data is present
SELECT * FROM bronze.erp_px_cat_g1v2;
SELECT COUNT(*) AS row_count FROM bronze.erp_px_cat_g1v2;

