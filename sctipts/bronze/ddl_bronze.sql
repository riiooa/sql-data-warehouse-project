
create table bronze.crm_cust_info (
	cst_id INT,
	cst_key VARCHAR (50),
	cst_firstname VARCHAR (50),
	cst_lastname varchar (50),
	cst_marital_status varchar (50),
	cst_gndr varchar (50), 
	cst_create_date DATE
);

create table bronze.crm_prd_info (
	prd_id INT,
	prd_key varchar (50),
	prd_nm varchar (50),
	prd_cost int,
	prd_line varchar (50),
	prd_start_dt timestamp,
	prd_end_dt timestamp
);

create table bronze.crm_sales_detail (
	sls_ord_num varchar (50),
	sls_prd_key varchar (50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);


create table bronze.erp_loc_a101 (
	cid varchar (50),
	cntry varchar (50)
);

create table bronze.erp_cust_az12 (
	cid varchar (50),
	bdate DATE,
	gen varchar (50)
);

create table bronze.erp_px_cat_q1v2 (
	id varchar (50),
	cat varchar (50),
	subcat varchar (50),
	maintenance varchar (50)
);


SELECT * FROM bronze.crm_cust_info;

SELECT * FROM bronze.crm_prd_info;

SELECT * FROM bronze.crm_sales_details csd ;

SELECT * FROM bronze.erp_cust_az12 eca  ;

SELECT * FROM bronze.erp_loc_a101 ela   ;

SELECT * FROM bronze.erp_px_cat_q1v2   ;



CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_batch_start_time TIMESTAMP;
    v_batch_end_time TIMESTAMP;
BEGIN
    v_batch_start_time := clock_timestamp();
    
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    ---------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- 1. crm_cust_info
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    EXECUTE 'COPY bronze.crm_cust_info FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;

    -- 2. crm_prd_info
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    EXECUTE 'COPY bronze.crm_prd_info FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;

    -- 3. crm_sales_detail
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    EXECUTE 'COPY bronze.crm_sales_details FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;

    ---------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- 4. erp_loc_a101
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    EXECUTE 'COPY bronze.erp_loc_a101 FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_erp/loc_a101.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;

    -- 5. erp_cust_az12
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    EXECUTE 'COPY bronze.erp_cust_az12 FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_erp/cust_az12.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;

    -- 6. erp_px_cat_q1v2
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_q1v2';
    TRUNCATE TABLE bronze.erp_px_cat_q1v2;
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_q1v2';
    EXECUTE 'COPY bronze.erp_px_cat_q1v2 FROM ''D:/1ProjectPorto/PROJECT-DATA-WAREHOUSE/sql-data-warehouse-project-main/datasets/source_erp/px_cat_g1v2.csv'' WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;

    v_batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: %', v_batch_end_time - v_batch_start_time;
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE '==========================================';
END;
$$;


call bronze.load_bronze();


