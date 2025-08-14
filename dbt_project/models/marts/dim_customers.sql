-- models/marts/dim_customers.sql
select
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state

from {{ source('raw_data', 'raw_customers') }}