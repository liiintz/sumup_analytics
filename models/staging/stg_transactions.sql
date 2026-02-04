{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        on_schema_change='fail'
    )
}}

with source as (
    select * from {{ source('sumup', 'transactions') }}
    {% if is_incremental() %}
        where created_at > (select max(record_created_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        id as transaction_id,
        device_id,
        product_sku as product_sku_raw,
        {{ clean_product_sku('product_sku') }} as product_sku,
        cast(amount as decimal(18,2)) as amount_eur,
        case
            when status = 'accepted' then 'completed'
            when status = 'refused' then 'failed'
            else status
        end as transaction_status,
        happened_at as transaction_at,
        created_at as record_created_at,
        CURRENT_TIMESTAMP as dbt_loaded_at
    from source
)

select * from renamed
