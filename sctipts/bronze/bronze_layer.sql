-- ==========================================================
-- 1. MEMBUAT SKEMA (FOLDER BESAR)
-- ==========================================================
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ==========================================================
-- 2. MEMBUAT TABEL BRONZE (DATA MENTAH)
-- ==========================================================

-- Tabel Customer CRM
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

-- Tabel Produk CRM
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

-- Tabel Penjualan CRM (PENTING: Nama diseragamkan pakai 's')
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

-- Tabel Lokasi ERP
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid   VARCHAR(50),
    cntry VARCHAR(50)
);

-- Tabel Customer ERP
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid   VARCHAR(50),
    bdate DATE,
    gen   VARCHAR(50)
);

-- Tabel Kategori ERP (PENTING: Nama diseragamkan pakai 'g')
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id          VARCHAR(50),
    cat         VARCHAR(50),
    subcat      VARCHAR(50),
    maintenance VARCHAR(50)
);




-- ==========================================================
-- 3. PROSEDUR UNTUK MENGISI DATA BRONZE
-- ==========================================================
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_batch_start_time TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '>>> MEMULAI PROSES LOADING BRONZE LAYER <<<';

    -- --- LOADING CRM TABLES ---
    
    -- 1. crm_cust_info
    v_start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_cust_info;
    EXECUTE 'COPY bronze.crm_cust_info FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    RAISE NOTICE '>> cst_info selesai diisi. Durasi: %', clock_timestamp() - v_start_time;

    -- 2. crm_prd_info
    v_start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    EXECUTE 'COPY bronze.crm_prd_info FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    RAISE NOTICE '>> prd_info selesai diisi. Durasi: %', clock_timestamp() - v_start_time;

    -- 3. crm_sales_details
    v_start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    EXECUTE 'COPY bronze.crm_sales_details FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    RAISE NOTICE '>> sales_details selesai diisi. Durasi: %', clock_timestamp() - v_start_time;

    -- --- LOADING ERP TABLES ---

    -- 4. erp_loc_a101
    v_start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    EXECUTE 'COPY bronze.erp_loc_a101 FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_erp/loc_a101.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    RAISE NOTICE '>> loc_a101 selesai diisi. Durasi: %', clock_timestamp() - v_start_time;

    -- 5. erp_cust_az12
    v_start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    EXECUTE 'COPY bronze.erp_cust_az12 FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_erp/cust_az12.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    RAISE NOTICE '>> cust_az12 selesai diisi. Durasi: %', clock_timestamp() - v_start_time;

    -- 6. erp_px_cat_g1v2
    v_start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    EXECUTE 'COPY bronze.erp_px_cat_g1v2 FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_erp/px_cat_g1v2.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    RAISE NOTICE '>> cat_g1v2 selesai diisi. Durasi: %', clock_timestamp() - v_start_time;

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'PROSES BRONZE SELESAI. Total Waktu: %', clock_timestamp() - v_batch_start_time;
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '!!! TERJADI ERROR !!!';
    RAISE NOTICE 'Pesan Error: %', SQLERRM;
END;
$$;


-- 1. Jalankan Prosedur Loading
CALL bronze.load_bronze();

-- 2. Cek apakah data sudah ada (Lakukan satu per satu)
SELECT * FROM bronze.crm_cust_info LIMIT 10;
SELECT * FROM bronze.crm_prd_info LIMIT 10;
SELECT * FROM bronze.crm_sales_details LIMIT 10;
SELECT * FROM bronze.erp_cust_az12 LIMIT 10;
SELECT * FROM bronze.erp_loc_a101 LIMIT 10;
SELECT * FROM bronze.erp_px_cat_g1v2 LIMIT 10;


