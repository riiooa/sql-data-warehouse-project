/*
===============================================================================
Quality Checks - Gold Layer
===============================================================================
Script Purpose:
    This script validates the integrity, consistency, and accuracy of the 
    Gold Layer (Star Schema). These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Connectivity of the data model for analytical reporting.

Usage Notes:
    - Run these checks after creating or updating the Gold Layer views.
    - Expectation: No results (Empty set) for errors.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key (Surrogate Key)
-- Expectation: No results (Each key must be unique)
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.dim_products'
-- ====================================================================
-- Check for Uniqueness of Product Key (Surrogate Key)
-- Expectation: No results (Each key must be unique)
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales' Referential Integrity
-- ====================================================================
-- Check if any sales records refer to non-existent customers or products
-- Expectation: No results (Every sale must link to a valid customer and product)
SELECT 
    f.order_number,
    f.customer_key AS fact_cust_key,
    c.customer_key AS dim_cust_key,
    f.product_key  AS fact_prod_key,
    p.product_key  AS dim_prod_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL 
   OR c.customer_key IS NULL;

-- ====================================================================
-- Final Validation: Sample Business Metric
-- ====================================================================
-- Quick check to see if the model produces data (Sanity Check)
SELECT 
    c.country,
    p.category,
    SUM(f.sales_amount) as total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers c ON c.customer_key = f.customer_key
JOIN gold.dim_products p ON p.product_key = f.product_key
GROUP BY c.country, p.category
ORDER BY total_sales DESC
LIMIT 5;
