with transactions as (
    select * from {{ ref('stg_transactions') }}
),

devices as (
    select
        device_id,
        store_id
    from {{ ref('stg_devices') }}
)

select
    t.transaction_id,
    t.device_id,
    d.store_id,  
    t.product_sku,
    t.amount_eur,
    t.transaction_status,
    t.transaction_at,
    t.record_created_at,
    CURRENT_TIMESTAMP as dbt_updated_at
from transactions t
left join devices d on t.device_id = d.device_id
