with source as (
    select * from {{ source('raw_pos', 'order_header') }}
),

renamed as (
    select
        order_id,
        truck_id,
        location_id,
        customer_id,
        order_channel,
        order_ts,
        date_trunc('month', order_ts)  as order_month,
        order_amount,
        order_total
    from source
    where order_total > 0   -- exclude nulls / zero-value rows
)

select * from renamed