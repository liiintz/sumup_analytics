{{
    config(
        materialized='table',
        unique_key='device_id',
        tags=['marts', 'dimensions']
    )
}}

with devices as (
    select * from {{ ref('stg_devices') }}
)

select 
    device_id,
    device_type,
    store_id,
    CURRENT_TIMESTAMP as dbt_updated_at
from devices
