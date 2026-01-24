/*
=========================================================================================
DDL Script: Create Gold View - gold.vw_sales_wide
=========================================================================================
This script creates a single view object for the Gold layer in the Data Warehouse.
The Gold layer represents the final flat/wide view (Flat Schema)

Usage: 
This view can be queried directly for analytics and BI reporting.
=========================================================================================
*/


CREATE OR ALTER VIEW gold.vw_sales_wide AS
SELECT
    -- surrogate/dimension keys
    f.customer_key,
    f.product_key,
    
    -- Fact columns
    f.order_number,
    f.order_date,
    f.shipping_date,
    f.due_date,
    f.sales_amount,
    f.quantity,
    f.price,

    -- Customer attributes (exclude c.customer_key)
    c.customer_id,
    c.customer_number,
    c.first_name,
    c.last_name,
    c.country,
    c.marital_status,
    c.gender,
    c.birthdate,
    c.create_date AS customer_create_date,

    -- Product attributes (exclude p.product_key)
    p.product_id,
    p.product_number,
    p.product_name,
    p.category_id,
    p.category,
    p.subcategory,
    p.maintenance,
    p.cost,
    p.product_line,
    p.start_date
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products AS p
ON f.product_key = p.product_key;
