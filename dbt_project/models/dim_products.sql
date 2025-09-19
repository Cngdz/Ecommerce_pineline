-- dim_products.sql

{{ config(
    materialized="table",
    schema="gold"
) }}

select 
    product_id,
    name,
    brand_name,
    price,
    original_price,
    rating_average,
    review_count,
    quantity_sold,
    available
from {{ source('ecommerce_silver', 'products') }}