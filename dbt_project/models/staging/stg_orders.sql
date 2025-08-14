-- models/staging/stg_orders.sql
select
    order_id,
    customer_id,
    order_status,
    -- Chỉ chọn các cột cần thiết và đổi kiểu dữ liệu nếu cần
    cast(order_purchase_timestamp as timestamp) as purchase_at

from {{ source('raw_data', 'raw_orders') }}