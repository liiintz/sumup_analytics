{{
    config(
        tags=['analysis']
    )
}}

-- Comprehensive device efficiency analysis
-- Answers: Which device types drive the most value for SumUp?
with device_metrics as (
    select
        d.device_type,
        
        -- Volume Metrics
        count(distinct f.store_id) as stores_using,
        count(distinct f.device_id) as unique_devices,
        count(distinct f.transaction_id) as total_transactions,
        
        -- Success Metrics
        count(distinct case when f.transaction_status = 'completed' 
              then f.transaction_id end) as successful_transactions,
        round(
            cast(count(distinct case when f.transaction_status = 'completed' 
                 then f.transaction_id end) as real) / 
            nullif(count(distinct f.transaction_id), 0) * 100, 
            2
        ) as success_rate_pct,
        
        -- Revenue Metrics
        sum(case when f.transaction_status = 'completed' 
            then f.amount_eur else 0 end) as total_revenue_eur,
        round(
            avg(case when f.transaction_status = 'completed' 
                then f.amount_eur end), 
            2
        ) as avg_transaction_value_eur,
        
        -- Efficiency Metrics (per device deployed)
        round(
            cast(count(distinct f.transaction_id) as real) / 
            nullif(count(distinct f.device_id), 0),
            1
        ) as avg_transactions_per_device,
        
        round(
            sum(case when f.transaction_status = 'completed' 
                then f.amount_eur else 0 end) / 
            nullif(count(distinct f.device_id), 0),
            2
        ) as avg_revenue_per_device_eur,
        
        -- Store Efficiency
        round(
            cast(count(distinct f.transaction_id) as real) / 
            nullif(count(distinct f.store_id), 0),
            1
        ) as avg_transactions_per_store,
        
        round(
            sum(case when f.transaction_status = 'completed' 
                then f.amount_eur else 0 end) / 
            nullif(count(distinct f.store_id), 0),
            2
        ) as avg_revenue_per_store_eur
        
    from {{ ref('fact_transactions') }} f
    left join {{ ref('dim_devices') }} d on f.device_id = d.device_id
    group by d.device_type
),

ranked_devices as (
    select
        device_type,
        stores_using,
        unique_devices,
        total_transactions,
        successful_transactions,
        success_rate_pct,
        total_revenue_eur,
        avg_transaction_value_eur,
        avg_transactions_per_device,
        avg_revenue_per_device_eur,
        avg_transactions_per_store,
        avg_revenue_per_store_eur
    from device_metrics
)

select
    device_type,
    
    -- Deployment Metrics
    stores_using,
    unique_devices,
    
    -- Volume Metrics
    total_transactions,
    successful_transactions,
    success_rate_pct,
    
    -- Revenue Metrics
    total_revenue_eur,
    avg_transaction_value_eur,
    
    -- Efficiency Metrics
    avg_transactions_per_device,
    avg_revenue_per_device_eur,
    avg_transactions_per_store,
    avg_revenue_per_store_eur

from ranked_devices
order by total_revenue_eur desc
