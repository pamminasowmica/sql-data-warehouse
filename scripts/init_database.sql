-- Create Database 'DataWarehouse'

use master;

create database DataWarehouse;

use DataWarehouse;

Go -- separater
create schema bronze;
Go
create schema silver;
Go
create schema gold;
Go
