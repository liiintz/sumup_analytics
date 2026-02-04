with source as (
    select * from {{ source('sumup', 'devices') }}
),

renamed as (
    select
        id as device_id,
        type as device_type,
        store_id,
        CURRENT_TIMESTAMP as dbt_loaded_at
    from source
)

select * from renamed
