{{
    config(
        materialized='table',
        unique_key='store_id',
        tags=['marts', 'dimensions']
    )
}}

with stores as (
    select * from {{ ref('stg_stores') }}
)

select 
    store_id,
    store_name,
    store_address,
    store_city,
    store_country,
    store_typology,
    customer_id,
    created_at,
    CURRENT_TIMESTAMP as dbt_updated_at
from stores
