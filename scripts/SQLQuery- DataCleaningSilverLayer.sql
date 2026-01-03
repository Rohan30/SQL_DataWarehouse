------1.DATA CLEANING of crm_cust_info table----------------------

-- doing the cleaning of bronze.crm_cust_info and then 
--loading into silver.crm_cust_info
INSERT INTO silver.crm_cust_info
(cst_id,
cst_key,
cst_firstname,cst_lastname,
cst_marital_status,cst_gender,cst_create_date)
SELECT cst_id,
cst_key,
-- remove trailing spaces
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
-- data standardization
CASE 
WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
ELSE 'n/a' END AS cst_marital_status,
-- data standardization
CASE 
WHEN UPPER(TRIM(cst_gender))='M' THEN 'Male'
WHEN UPPER(TRIM(cst_gender))='F' THEN 'Female'
ELSE 'n/a' END AS cst_gender,
cst_create_date
FROM(
SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as last_flag
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL)t
--to remove duplicate entries and keep only latest records
WHERE last_flag=1;


-- CHECKING IF DATA IS there in silver.crm_cust_info
SELECT *
FROM silver.crm_cust_info;


-------------2.CLEANING bronze.crm_prd_info table for loading into silver layer----------------
SELECT prd_id,
-- extracting first part of prd_key for join
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
-- extracting second part of prd_key for join
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
--treating nulls
ISNULL(prd_cost,0) AS prd_cost,
--data standardization 
CASE TRIM(UPPER(prd_line))
WHEN 'M' THEN 'Mountain'
WHEN 'R' THEN 'Road'
WHEN 'S' THEN 'Other Sales'
WHEN 'T' THEN 'Touring'
ELSE 'n/a' END AS prd_line,
--conv datetime to date
CAST(prd_start_dt AS DATE) AS prd_start_dt,
-- ensuring the start date < end date for each prd_key and casting to date
CAST(
LEAD(prd_start_dt)OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1
AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

--- checking each time with bronze table
SELECT *
FROM bronze.crm_prd_info;

-- as cols have changed to be loaded into silver layer for crm.prd_info table
-- have to change DDL OF silver.crm_prd_info
IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
DROP TABLE silver.crm_prd_info;
-- creating the TABLE again
CREATE TABLE silver.crm_prd_info
(prd_id INT,
cat_id NVARCHAR(50),
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE());

-- now doing insert into silver.crm_prd_info
INSERT INTO silver.crm_prd_info
(prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt)
SELECT prd_id,
-- extracting first part of prd_key for join
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
-- extracting second part of prd_key for join
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
--treating nulls
ISNULL(prd_cost,0) AS prd_cost,
--data standardization 
CASE TRIM(UPPER(prd_line))
WHEN 'M' THEN 'Mountain'
WHEN 'R' THEN 'Road'
WHEN 'S' THEN 'Other Sales'
WHEN 'T' THEN 'Touring'
ELSE 'n/a' END AS prd_line,
--conv datetime to date
CAST(prd_start_dt AS DATE) AS prd_start_dt,
-- ensuring the start date < end date for each prd_key and casting to date
CAST(
LEAD(prd_start_dt)OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1
AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

-- checking if cleaned data has been populated
select *
from silver.crm_prd_info;


------3. CLEANING bronze.crm_sales_details table to be loaded onto silver layer
select sls_ord_num,
sls_prd_key,
sls_cust_id,
-- date conversion
CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
     ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
--date conversion
CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
     ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
--date conversion
CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
     ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
--sales logic
CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales != ABS(sls_price)*sls_quantity
THEN ABS(sls_price)*sls_quantity
ELSE sls_sales END AS sls_sales,
sls_quantity,
--price logic
CASE WHEN sls_price IS NULL OR sls_price<=0
THEN sls_sales/NULLIF(sls_quantity,0)
ELSE sls_price END AS sls_price
FROM bronze.crm_sales_details;

-- as the date columns have changed, have to truncate silver.crm_sales_details
-- and create DDL again
IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
   DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details
(sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE());

-- now inserting into silver.crm_sales_details using INSERT INTO from cleaned table
INSERT INTO silver.crm_sales_details
(sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price)
SELECT sls_ord_num,
sls_prd_key,
sls_cust_id,
-- date conversion
CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
     ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
--date conversion
CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
     ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
--date conversion
CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
     ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
--sales logic
CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales != ABS(sls_price)*sls_quantity
THEN ABS(sls_price)*sls_quantity
ELSE sls_sales END AS sls_sales,
sls_quantity,
--price logic
CASE WHEN sls_price IS NULL OR sls_price<=0
THEN sls_sales/NULLIF(sls_quantity,0)
ELSE sls_price END AS sls_price
FROM bronze.crm_sales_details;

-- check if data is loaded properly into silver.crm_sales_details
select *
from silver.crm_sales_details;

----------------4. CLEANING erp_cust_az12 table------------------
SELECT
-- extracting specific strings from cid column for future joins
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
ELSE cid END AS cid,
-- Treating invalid dates
CASE WHEN bdate>GETDATE() THEN NULL
ELSE bdate END AS bdate,
-- Treating nulls and empty vals, data standardization
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
WHEN gen is NULL OR gen='' THEN 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

-- NO CHANGE IN DDL FOR THIS TABLE
-- so we insert the data into silver.erp_cust_az12 from the cleaned table
INSERT INTO silver.erp_cust_az12
(cid,bdate,gen)
SELECT
-- extracting specific strings from cid column for future joins
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
ELSE cid END AS cid,
-- Treating invalid dates
CASE WHEN bdate>GETDATE() THEN NULL
ELSE bdate END AS bdate,
-- Treating nulls and empty vals, data standardization
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
WHEN gen is NULL OR gen='' THEN 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

--checking the data in silver.erp_cust_az12 table
select *
from silver.erp_cust_az12;


------------5. cleaning the erp_loc_a101 table---------------------
SELECT REPLACE(cid,'-','') AS cid,
CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
WHEN TRIM(cntry) IN('USA','US') THEN 'United States'
WHEN TRIM(cntry) IS NULL OR TRIM(cntry)=''THEN 'n/a'
ELSE TRIM(cntry) END AS cntry
FROM bronze.erp_loc_a101;

-- NO DDL needed for silver.erp_loc_a101 table as DATA COLS haven't changed
-- so proceed with INSERT INTO to load data into silver.erp_loc_a101
INSERT INTO silver.erp_loc_a101(cid,cntry)
SELECT REPLACE(cid,'-','') AS cid,
CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
WHEN TRIM(cntry) IN('USA','US') THEN 'United States'
WHEN TRIM(cntry) IS NULL OR TRIM(cntry)=''THEN 'n/a'
ELSE TRIM(cntry) END AS cntry
FROM bronze.erp_loc_a101;

-- checking data in silver.erp_loc_a101 table
select *
from silver.erp_loc_a101;


----------6. cleaning erp_px_cat_g1v2 table---------------
--nothing to be cleaned and no DDL change
-- INSERT INTO silver.erp_px_cat_g1v2 table
INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
SELECT id,cat,subcat,maintenance
FROM bronze.erp_px_cat_g1v2;


--checking data in silver.erp_px_cat_g1v2
select *
from silver.erp_px_cat_g1v2;