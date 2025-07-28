
CREATE OR ALTER PROCEDURE bronze.load_bronze as
begin
	Declare @start_time DATETIME, @end_time DATETIME;
	begin try	
		Print'===============================================================';
		Print'Loading Bronze Layer';
		Print'===============================================================';

		Print'---------------------------------';
		PRINT'loading CRM Layer';
		Print'---------------------------------';
		set @start_time= GetDATE();
		Print'>>Truncating Table:bronze.crm_cust_info'
		TRUNCATE TABLE DataWarehouse.bronze.crm_cust_info
		Print'>>Truncating Table:bronze.crm_cust_info'
		bulk insert DataWarehouse.bronze.crm_cust_info
		from 'C:\Users\SOWMICA\OneDrive\Desktop\Projects with Bara\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with
		(
		   FIRSTROW = 2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		set @end_time= GetDATE();
		print '>>Load Duration: ' + cast(DATEDIFF(second, @start_time,@end_time)) as NVARCHAR) +'seconds';

		set @start_time= GetDATE();
		Print'>>Truncating Table:bronze.crm_prd_info'
		TRUNCATE TABLE DataWarehouse.bronze.crm_prd_info
		Print'>>Inserting Table:bronze.crm_prd_info'
		bulk insert DataWarehouse.bronze.crm_prd_info
		from 'C:\Users\SOWMICA\OneDrive\Desktop\Projects with Bara\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with
		(
		   FIRSTROW = 2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		set @end_time= GetDATE();
		print '>>Load Duration: ' + cast(DATEDIFF(second, @start_time,@end_time)) as NVARCHAR) +'seconds';

		set @start_time= GetDATE();
		Print'>>Truncating Table:bronze.crm_sales_details'
		TRUNCATE TABLE DataWarehouse.bronze.crm_sales_details
		Print'>>Inserting Table:bronze.crm_sales_details'
		bulk insert DataWarehouse.bronze.crm_sales_details
		from 'C:\Users\SOWMICA\OneDrive\Desktop\Projects with Bara\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with
		(
		   FIRSTROW = 2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		set @end_time= GetDATE();
		print '>>Load Duration: ' + cast(DATEDIFF(second, @start_time,@end_time)) as NVARCHAR) +'seconds';

		Print'---------------------------------';
		PRINT'loading ERP Layer';
		Print'---------------------------------';

		set @start_time= GetDATE();
		Print'>>Truncating Table:bronze.erp_cust_az12'
		TRUNCATE TABLE DataWarehouse.bronze.erp_cust_az12
		Print'>>Inserting Table:bronze.erp_cust_az12'
		bulk insert DataWarehouse.bronze.erp_cust_az12
		from 'C:\Users\SOWMICA\OneDrive\Desktop\Projects with Bara\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with
		(
		   FIRSTROW = 2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		set @end_time= GetDATE();
		print '>>Load Duration: ' + cast(DATEDIFF(second, @start_time,@end_time)) as NVARCHAR) +'seconds';

		set @start_time= GetDATE();
		Print'>>Truncating Table:bronze.erp_loc_a101'
		TRUNCATE TABLE DataWarehouse.bronze.erp_loc_a101
		Print'>>Inserting Table:bronze.erp_loc_a101'
		bulk insert DataWarehouse.bronze.erp_loc_a101
		from 'C:\Users\SOWMICA\OneDrive\Desktop\Projects with Bara\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with
		(
		   FIRSTROW = 2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
        set @end_time= GetDATE();
		print '>>Load Duration: ' + cast(DATEDIFF(second, @start_time,@end_time)) as NVARCHAR) +'seconds';

		set @start_time= GetDATE();
		Print'>>Truncating Table:bronze.erp_cust_az12';
		TRUNCATE TABLE DataWarehouse.bronze.erp_px_cat_g1v2
		Print'>>Inserting Table:bronze.erp_cust_az12';
		bulk insert DataWarehouse.bronze.erp_px_cat_g1v2
		from 'C:\Users\SOWMICA\OneDrive\Desktop\Projects with Bara\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with
		(
		   FIRSTROW = 2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);

		set @end_time= GetDATE();
		print '>>Load Duration: ' + cast(DATEDIFF(second, @start_time,@end_time)) as NVARCHAR) +'seconds';
	end try
	begin catch
		print '========================================'
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		print 'Error Message' + ERROR_MESSAGE();
		print 'Error Message' + cast(ERROR_NUMBER() as NVARCHAR);
		print 'Error Message' + cast(ERROR_STATE() as NVARCHAR);
		print '========================================'
	end catch
end











