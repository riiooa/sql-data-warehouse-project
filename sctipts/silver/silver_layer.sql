-- ==========================================================
-- 1. MEMBUAT TABEL SILVER (DATA BERSIH)
-- ==========================================================

-- Tabel Customer CRM
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id             INT,
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr           VARCHAR(50),
    cst_create_date    DATE,
    dwh_create_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabel Produk CRM
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          VARCHAR(50),
    prd_key         VARCHAR(50),
    prd_nm          VARCHAR(50),
    prd_cost        INT,
    prd_line        VARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabel Penjualan CRM
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num     VARCHAR(50),
    sls_prd_key     VARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabel Lokasi ERP
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid             VARCHAR(50),
    cntry           VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabel Customer ERP
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid             VARCHAR(50),
    bdate           DATE,
    gen             VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabel Kategori ERP (Menggunakan 'q1v2' sesuai Bronze)
DROP TABLE IF EXISTS silver.erp_px_cat_q1v2;
CREATE TABLE silver.erp_px_cat_q1v2 (
    id              VARCHAR(50),
    cat             VARCHAR(50),
    subcat          VARCHAR(50),
    maintenance     VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ==========================================================
-- 2. PROSEDUR UNTUK TRANSFORMASI DATA (BRONZE -> SILVER)
-- ==========================================================
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_batch_start_time TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '>>> MEMULAI PROSES LOADING SILVER LAYER <<<';

    -- --- CLEANING CRM TABLES ---

    -- 1. crm_cust_info (Hapus duplikat & normalisasi teks)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
    SELECT cst_id, cst_key, TRIM(cst_firstname), TRIM(cst_lastname),
           CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' ELSE 'n/a' END,
           CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' ELSE 'n/a' END,
           cst_create_date
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL) t 
    WHERE flag_last = 1;
    RAISE NOTICE '>> cst_info dibersihkan. Durasi: %', clock_timestamp() - v_start_time;

    -- 2. crm_prd_info (Pemisahan Key & Perbaikan Tanggal)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    SELECT prd_id, REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'), SUBSTRING(prd_key, 7, LENGTH(prd_key)), prd_nm, COALESCE(prd_cost, 0),
           CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain' WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road' WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales' WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring' ELSE 'n/a' END,
           prd_start_dt::DATE, (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::DATE
    FROM bronze.crm_prd_info;
    RAISE NOTICE '>> prd_info dibersihkan. Durasi: %', clock_timestamp() - v_start_time;

    -- 3. crm_sales_details (Konversi tipe data & kalkulasi ulang sales)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    SELECT sls_ord_num, sls_prd_key, sls_cust_id,
           CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL ELSE (sls_order_dt::TEXT)::DATE END,
           CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL ELSE (sls_ship_dt::TEXT)::DATE END,
           CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL ELSE (sls_due_dt::TEXT)::DATE END,
           CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price) ELSE sls_sales END,
           sls_quantity, CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0) ELSE sls_price END
    FROM bronze.crm_sales_details;
    RAISE NOTICE '>> sales_details dibersihkan. Durasi: %', clock_timestamp() - v_start_time;

    -- --- CLEANING ERP TABLES ---

    -- 4. erp_loc_a101
    v_start_time := clock_timestamp();
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (cid, cntry) 
    SELECT REPLACE(cid, '-', ''), CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany' WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States' ELSE COALESCE(NULLIF(TRIM(cntry), ''), 'n/a') END 
    FROM bronze.erp_loc_a101;
    RAISE NOTICE '>> loc_a101 dibersihkan. Durasi: %', clock_timestamp() - v_start_time;

    -- 5. erp_cust_az12
    v_start_time := clock_timestamp();
    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen) 
    SELECT CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) ELSE cid END, CASE WHEN bdate > CURRENT_DATE THEN NULL ELSE bdate END, CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' ELSE 'n/a' END 
    FROM bronze.erp_cust_az12;
    RAISE NOTICE '>> cust_az12 dibersihkan. Durasi: %', clock_timestamp() - v_start_time;

    -- 6. erp_px_cat_q1v2
    v_start_time := clock_timestamp();
    TRUNCATE TABLE silver.erp_px_cat_q1v2;
    INSERT INTO silver.erp_px_cat_q1v2 (id, cat, subcat, maintenance) 
    SELECT id, cat, subcat, maintenance FROM bronze.erp_px_cat_q1v2;
    RAISE NOTICE '>> cat_q1v2 dipindahkan. Durasi: %', clock_timestamp() - v_start_time;

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'PROSES SILVER SELESAI. Total Waktu: %', clock_timestamp() - v_batch_start_time;
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '!!! TERJADI ERROR PADA SILVER LAYER !!!';
    RAISE NOTICE 'Pesan Error: %', SQLERRM;
END;
$$;




-- 1. Jalankan Seluruh Pipeline
CALL bronze.load_bronze(); 
CALL silver.load_silver();

-- 2. Cek Hasil Akhir di Silver (Contoh 3 tabel utama)
SELECT * FROM silver.crm_cust_info LIMIT 10;
SELECT * FROM silver.crm_prd_info LIMIT 10;
SELECT * FROM silver.crm_sales_details LIMIT 10;
