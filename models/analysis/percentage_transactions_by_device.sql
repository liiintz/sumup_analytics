{{
    config(
        tags=['analysis']
    )
}}

-- Percentage of transactions per device type
-- Inline calculation for BI and reporting purposes
with device_stats as (
    select
        d.device_type,
        count(*) as device_count,
        sum(count(*)) over () as total_count
    from {{ ref('fact_transactions') }} f
    left join {{ ref('dim_devices') }} d on f.device_id = d.device_id
    group by 1
)

select
    device_type,
    device_count,
    round((cast(device_count as real) / total_count) * 100, 2) as percentage
from device_stats
order by percentage desc
