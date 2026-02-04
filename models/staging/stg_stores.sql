with source as (
    select * from {{ source('sumup', 'stores') }}
),

renamed as (
    select
        id as store_id,
        name as store_name,
        address as store_address,
        city as store_city,
        country as store_country,
        typology as store_typology,
        customer_id,
        created_at,
        CURRENT_TIMESTAMP as dbt_loaded_at
    from source
)

select * from renamed
