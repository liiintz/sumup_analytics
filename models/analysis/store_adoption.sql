-- models/analysis/store_adoption.sql
{{
    config(
        tags=['analysis']
    )
}}

-- How do stores progress in their first 90 days?
-- Answers: What's normal? Who's behind? Who's ahead?
with store_daily_transactions as (
    select
        f.store_id,
        s.created_at as store_created_at,
        date(f.transaction_at) as transaction_date,
        
        -- Days since onboarding
        cast(
            julianday(date(f.transaction_at)) - 
            julianday(date(s.created_at)) as integer
        ) as days_since_onboarding,
        
        -- Daily metrics
        count(distinct f.transaction_id) as daily_transactions,
        sum(case when f.transaction_status = 'completed' 
            then f.amount_eur else 0 end) as daily_revenue_eur
        
    from {{ ref('fact_transactions') }} f
    left join {{ ref('dim_stores') }} s on f.store_id = s.store_id
    where cast(
            julianday(date(f.transaction_at)) - 
            julianday(date(s.created_at)) as integer
          ) between 0 and 90  -- First 90 days only
    group by f.store_id, s.created_at, date(f.transaction_at)
),

cumulative_metrics as (
    select
        store_id,
        store_created_at,
        days_since_onboarding,
        
        -- Cumulative calculations
        sum(daily_transactions) over (
            partition by store_id 
            order by days_since_onboarding
            rows between unbounded preceding and current row
        ) as cumulative_transactions,
        
        sum(daily_revenue_eur) over (
            partition by store_id 
            order by days_since_onboarding
            rows between unbounded preceding and current row
        ) as cumulative_revenue_eur,
        
        count(*) over (
            partition by store_id 
            order by days_since_onboarding
            rows between unbounded preceding and current row
        ) as cumulative_active_days
        
    from store_daily_transactions
),

benchmark_milestones as (
    select
        days_since_onboarding as milestone_day,
        
        count(distinct store_id) as stores_in_sample,
        
        -- Transaction benchmarks
        round(avg(cumulative_transactions), 1) as avg_transactions,
        min(cumulative_transactions) as min_transactions,
        max(cumulative_transactions) as max_transactions,
        
        -- Revenue benchmarks
        round(avg(cumulative_revenue_eur), 2) as avg_revenue_eur,
        
        -- Activity benchmarks
        round(avg(cumulative_active_days), 1) as avg_active_days
        
    from cumulative_metrics
    group by days_since_onboarding
)

select
    milestone_day,
    stores_in_sample,
    
    -- Tendency
    avg_transactions,
    min_transactions,
    max_transactions,
    
    -- Revenue
    avg_revenue_eur,
    
    -- Engagement
    avg_active_days

from benchmark_milestones
order by milestone_day