-- ==========================================================
-- 1. CREATE SCHEMAS (DATA LAYERS)
-- ==========================================================
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ==========================================================
-- 2. CREATE BRONZE TABLES (RAW DATA)
-- ==========================================================

-- CRM Customer Table
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id             INT,
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr           VARCHAR(50), 
    cst_create_date    DATE
);

-- CRM Product Table
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt   TIMESTAMP
);

-- CRM Sales Details Table
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

-- ERP Location Table
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid   VARCHAR(50),
    cntry VARCHAR(50)
);

-- ERP Customer Table
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid   VARCHAR(50),
    bdate DATE,
    gen   VARCHAR(50)
);

-- ERP Category Table
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id          VARCHAR(50),
    cat         VARCHAR(50),
    subcat      VARCHAR(50),
    maintenance VARCHAR(50)
);

-- ==========================================================
-- 3. STORED PROCEDURE TO LOAD BRONZE LAYER
-- ==========================================================
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_batch_start_time TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '>>> STARTING BRONZE LAYER LOADING PROCESS <<<';

    -- --- LOADING CRM TABLES ---
    
    -- 1. crm_cust_info
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Loading Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    EXECUTE 'COPY bronze.crm_cust_info FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    RAISE NOTICE '>> cst_info load completed. Duration: %', clock_timestamp() - v_start_time;

    -- 2. crm_prd_info
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Loading Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    EXECUTE 'COPY bronze.crm_prd_info FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    RAISE NOTICE '>> prd_info load completed. Duration: %', clock_timestamp() - v_start_time;

    -- 3. crm_sales_details
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Loading Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    EXECUTE 'COPY bronze.crm_sales_details FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    RAISE NOTICE '>> sales_details load completed. Duration: %', clock_timestamp() - v_start_time;

    -- --- LOADING ERP TABLES ---

    -- 4. erp_loc_a101
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Loading Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    EXECUTE 'COPY bronze.erp_loc_a101 FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_erp/loc_a101.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    RAISE NOTICE '>> loc_a101 load completed. Duration: %', clock_timestamp() - v_start_time;

    -- 5. erp_cust_az12
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Loading Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    EXECUTE 'COPY bronze.erp_cust_az12 FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_erp/cust_az12.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    RAISE NOTICE '>> cust_az12 load completed. Duration: %', clock_timestamp() - v_start_time;

    -- 6. erp_px_cat_g1v2
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Loading Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    EXECUTE 'COPY bronze.erp_px_cat_g1v2 FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_erp/px_cat_g1v2.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    RAISE NOTICE '>> cat_g1v2 load completed. Duration: %', clock_timestamp() - v_start_time;

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'BRONZE PROCESS COMPLETED. Total Duration: %', clock_timestamp() - v_batch_start_time;
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '!!! AN ERROR OCCURRED !!!';
    RAISE NOTICE 'Error Message: %', SQLERRM;
END;
$$;

-- ==========================================================
-- 4. EXECUTION AND VERIFICATION
-- ==========================================================

-- 1. Run the Loading Procedure
CALL bronze.load_bronze();

-- 2. Verify if data has been loaded (Check one by one)
SELECT * FROM bronze.crm_cust_info LIMIT 10;
SELECT * FROM bronze.crm_prd_info LIMIT 10;
SELECT * FROM bronze.crm_sales_details LIMIT 10;
SELECT * FROM bronze.erp_cust_az12 LIMIT 10;
SELECT * FROM bronze.erp_loc_a101 LIMIT 10;
SELECT * FROM bronze.erp_px_cat_g1v2 LIMIT 10;
