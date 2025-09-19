--setup/setup_tables.sql
-- Tạo schema silver & gold nếu chưa tồn tại
CREATE SCHEMA IF NOT EXISTS nessie.silver;
CREATE SCHEMA IF NOT EXISTS nessie.gold;

-- Tạo bảng products cố định ở tầng silver
CREATE TABLE IF NOT EXISTS nessie.silver.products (
   product_id BIGINT,
   name VARCHAR,
   brand_name VARCHAR,
   price DOUBLE,
   original_price DOUBLE,
   rating_average DOUBLE,
   review_count INTEGER,
   quantity_sold INTEGER,
   available INTEGER,
   ingestion_date DATE
)
WITH (
   partitioning = ARRAY['day(ingestion_date)']
);