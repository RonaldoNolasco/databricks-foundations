-- listando archivos en el HDFS con las magic words
%fs ls /mnt/training/ecommerce

-- listando archivos en el HDFS con el dbutils
files = dbutils.fs.ls("/mnt/training/ecommerce")
display(files)

-- creando tablas con sql con parquets
%sql
CREATE TABLE IF NOT EXISTS users USING parquet OPTIONS (path "/mnt/training/ecommerce/users/users.parquet");
CREATE TABLE IF NOT EXISTS sales USING parquet OPTIONS (path "/mnt/training/ecommerce/sales/sales.parquet");
CREATE TABLE IF NOT EXISTS products USING parquet OPTIONS (path "/mnt/training/ecommerce/products/products.parquet");

-- mostrando productos
%sql
select * from products

-- promedio de ventas
%sql
select avg (purchase_revenue_in_usd) from sales

-- nombres de evento diferentes
%sql
select distinct event_name from events

-- cerrando el aula
%run ./Includes/Classroom-Cleanup