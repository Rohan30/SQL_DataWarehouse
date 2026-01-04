--------ANALYTICAL QUERIES------------------
USE DataWarehouse;
----1. Changes over time analysis(yearly)
SELECT YEAR(order_date) as Year,
SUM(sales_amount) as Sales, 
COUNT(customer_key) as Total_Customers
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date);

-- analysis by Year and Month
SELECT YEAR(order_date) as Year,
DATENAME(month,order_date) as Month,
SUM(sales_amount) as Sales,
COUNT(customer_key) as Total_Customers
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date),DATENAME(month,order_date)
ORDER BY YEAR(order_date),DATENAME(month,order_date);


-- doing the proper sorting
SELECT YEAR(order_date) as Year,
DATENAME(month,order_date) as Month,
SUM(sales_amount) as Sales,
COUNT(customer_key) as Total_Customers,
CASE DATENAME(month,order_date)
WHEN 'January' THEN 1
WHEN 'February' THEN 2
WHEN 'March' THEN 3
WHEN 'April' THEN 4
WHEN 'May' THEN 5
WHEN 'June' THEN 6
WHEN 'July' THEN 7
WHEN 'August' THEN 8
WHEN 'September' THEN 9
WHEN 'October' THEN 10
WHEN 'November' THEN 11
WHEN 'December' THEN 12 END AS Month_Order
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date),DATENAME(month,order_date)
ORDER BY YEAR(order_date),
CASE DATENAME(month,order_date)
WHEN 'January' THEN 1
WHEN 'February' THEN 2
WHEN 'March' THEN 3
WHEN 'April' THEN 4
WHEN 'May' THEN 5
WHEN 'June' THEN 6
WHEN 'July' THEN 7
WHEN 'August' THEN 8
WHEN 'September' THEN 9
WHEN 'October' THEN 10
WHEN 'November' THEN 11
WHEN 'December' THEN 12 END;


---2. Cumulative Analysis
select *,
SUM(Sales) OVER (ORDER BY Year) as Running_Sales,
SUM(Tot_customers) OVER(ORDER BY Year) as Running_Tot_Customers,
AVG(Avg_Sales) OVER (ORDER BY Year) as Moving_Avg_Sales
from(
select YEAR(order_date) as Year,
SUM(sales_amount) as Sales,
COUNT((customer_key)) as Tot_customers,
AVG(sales_amount) as Avg_Sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date))t
ORDER BY Year;


---3. Comparitive Analysis
-- Compare the Total Yearly Sales of each product with Avg Sales and Prev Year's Sales
with yearly_products_cte as
(select YEAR(order_date) as Year,
p.product_name as prod_name,
SUM(sales_amount) as Tot_Sales
from gold.fact_sales s
left join gold.dim_products p
on s.product_key=p.product_key
where order_date IS NOT NULL
GROUP BY YEAR(order_date),p.product_name)
select Year,prod_name,Tot_Sales,
-- Avg Sales Comparison
AVG(Tot_Sales) OVER(PARTITION BY prod_name) as Avg_Sales,
Tot_Sales-AVG(Tot_Sales) OVER(PARTITION BY prod_name) as diff_frm_avg,
CASE WHEN Tot_Sales-AVG(Tot_Sales) OVER(PARTITION BY prod_name)>0 then 'Above_Avg'
     WHEN Tot_Sales-AVG(Tot_Sales) OVER(PARTITION BY prod_name)<0 then 'Below_Avg'
     ELSE 'Avg' END AS Avg_Perf,
LAG(Tot_Sales) OVER (PARTITION BY prod_name ORDER BY Year) as Prev_Tot_Sales,
Tot_Sales-LAG(Tot_Sales) OVER (PARTITION BY prod_name ORDER BY Year) as diff_frm_prev_yr,
-- Prev Yr Sales Comparison
CASE WHEN Tot_Sales-LAG(Tot_Sales) OVER (PARTITION BY prod_name ORDER BY Year)>0 then 'Above_Avg'
     WHEN Tot_Sales-LAG(Tot_Sales) OVER (PARTITION BY prod_name ORDER BY Year)<0 then 'Below_Avg'
     ELSE 'Avg' END AS Prev_Yr_Perf
from yearly_products_cte
ORDER BY prod_name,Year;

---4. Part to Whole/Proportional Analysis
-- Which Categories contribute the most to overall sales?
with tot_sales_category_cte as(
select category, SUM(sales_amount) as tot_sales
from gold.fact_sales s
left join gold.dim_products p
on s.product_key=p.product_key
GROUP BY category)
select *, SUM(tot_sales) OVER() as overall_sales,
ROUND(CAST(tot_sales AS FLOAT)/ SUM(tot_sales) OVER()*100,3) as fract_overall_sales
from tot_sales_category_cte
ORDER BY tot_sales DESC;

------5. Data Segmentation
-- grouping cost of products into segments and 
--counting how many products fall into each of these segments
with products_cost_range_cte as
(
select product_key,product_name,cost,
CASE WHEN cost<100 THEN 'Below 100'
WHEN cost BETWEEN 100 AND 500 THEN '100-500'
WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
ELSE 'Above 1000' END AS cost_range
from gold.dim_products)
select cost_range, count(product_key) as total_products
from products_cost_range_cte
group by cost_range;

-- group customers into 3 segments (VIP,Regular,New) 
-- VIP customers with atleast 12 months of history and spending more than 5000 in sales
-- Regular customers with atleast 12 months of history and spending of 5000 or less
-- New customers with less than 12 months of history
--and count the customers in each segment
with cust_sales_lifespan_cte as
(
select c.customer_key,
sum(s.sales_amount) as total_sales,
min(s.order_date) as first_order_date,
max(s.order_date) as last_order_date,
DATEDIFF(month,min(s.order_date),max(s.order_date)) as lifespan
from gold.fact_sales s
left join gold.dim_customers c
on s.customer_key=c.customer_key
group by c.customer_key),
lifespan_cte as(
select *,
case when lifespan>=12 and total_sales>5000 then 'VIP'
when lifespan>=12 and total_sales<=5000 then 'Regular'
else 'New' END AS cust_category
from cust_sales_lifespan_cte)
select cust_category,count(customer_key) as tot_customers
from lifespan_cte
group by cust_category;



