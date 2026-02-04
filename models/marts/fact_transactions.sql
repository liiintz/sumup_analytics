{{
    config(
        materialized='table',
        unique_key='transaction_id',
        tags=['marts', 'facts']
    )
}}

with transactions as (
    select * from {{ ref('int_transactions_joined') }}
)

select
    transaction_id,
    device_id,
    store_id,
    product_sku,
    amount_eur,
    transaction_status,
    transaction_at,
    record_created_at,
    CURRENT_TIMESTAMP as dbt_updated_at
from transactions

