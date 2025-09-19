-- fact_product.sql

{{ config(
    materialized="table",
    schema="gold"
) }}

select
    price,
    original_price,
    rating_average,
    review_count,
    quantity_sold,
    available
from {{ source('ecommerce_silver', 'products') }}