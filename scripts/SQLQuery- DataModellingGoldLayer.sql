------------GOLD LAYER----------------
--1.CREATING CUSTOMERS TABLE IN VIEW FORMAT----------------
-- joining silver.crm_cust_info,silver.erp_loc_a101 and silver.erp_cust_az12
-- doing initial data display
select *
from silver.crm_cust_info;
select *
from silver.erp_loc_a101;
select *
from silver.erp_cust_az12;

-- doing the join and selecting the reqd cols
-- finally creating the view after making changes
CREATE VIEW gold.dim_customers AS
(
SELECT 
--generate a surrogate key (non-business primary key to identify the table)
ROW_NUMBER() OVER(ORDER BY ci.cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
-- ensuring cst_gender is followed as its the master data when there are similar cols
CASE WHEN ci.cst_gender!='n/a' THEN ci.cst_gender
ELSE COALESCE(ca.gen,'n/a') END AS gender,
ca.bdate as birth_date,
ci.cst_create_date as create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key=la.cid)

-- querying the view
SELECT *
FROM gold.dim_customers;


---2. CREATING PRODUCTS TABLE IN VIEW FORMAT-----------
-- creating view on top of table
CREATE VIEW gold.dim_products AS
(SELECT 
-- creating surrogate key to uniquely identify the table
ROW_NUMBER()OVER(ORDER BY pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE pn.prd_end_dt IS NULL) -- keeps latest info for each prd_id and removes hist data;

-- querying the view
SELECT *
FROM gold.dim_products;


---3. CREATING SALES TABLE IN VIEW FORMAT
-- joining the gold dimension prod and gold customer views
-- with silver sales details table to just see the surrogate keys
-- ACTUAL JOINING DOESN'T HAPPEN HERE
CREATE VIEW gold.fact_sales AS
(
SELECT sd.sls_ord_num as order_number,
-- we display the surrogate keys (product_key and customer_key from the dimension product and customer tables)
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
FROM silver.crm_sales_details sd
-- however the join is based on common keys with dim products table
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key=pr.product_number
-- however the join is based on common keys with dim customers table
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id=cu.customer_id)

-- querying the view
SELECT *
FROM gold.fact_sales;

---- DATA MODELLING AND CONNECTIONS CAN BE NOW DONE WITH GOLD FACT AND GOLD DIM TABLES
