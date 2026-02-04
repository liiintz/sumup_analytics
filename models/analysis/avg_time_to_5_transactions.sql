{{
    config(
        tags=['analysis']
    )
}}

-- Average time for a store to perform its first 5 transactions
-- Measures adoption speed: time between store creation and 5th transaction
with ranked_transactions as (
    select
        f.store_id,
        f.transaction_at,
        row_number() over (partition by f.store_id order by f.transaction_at asc) as tx_rank
    from {{ ref('fact_transactions') }} f
),

fifth_transaction as (
    select
        store_id,
        transaction_at as fifth_tx_at
    from ranked_transactions
    where tx_rank = 5
),

adoption_time as (
    select
        ft.store_id,
        ft.fifth_tx_at,
        s.created_at as store_created_at,
        cast(julianday(ft.fifth_tx_at) - julianday(s.created_at) as integer) as days_to_5th_transaction
    from fifth_transaction ft
    left join {{ ref('dim_stores') }} s on ft.store_id = s.store_id
    where ft.fifth_tx_at is not null 
        and s.created_at is not null
        and ft.fifth_tx_at >= s.created_at  -- Avoid negative days
)

select
    round(avg(days_to_5th_transaction), 2) as avg_days_to_5th_tx,
    min(days_to_5th_transaction) as min_days_to_5th_tx,
    max(days_to_5th_transaction) as max_days_to_5th_tx,
    count(*) as stores_with_5_tx
from adoption_time
where days_to_5th_transaction >= 0