{{
    config(
        tags=['analysis']
    )
}}

-- Average transacted amount per store typology and country
-- Lightweight aggregation compiled inline for BI tools
with transaction_summary as (
    select
        s.store_country,
        s.store_typology,
        count(*) as transaction_count,
        sum(f.amount_eur) as total_amount_eur,
        avg(f.amount_eur) as avg_amount_eur,
        min(f.amount_eur) as min_amount_eur,
        max(f.amount_eur) as max_amount_eur
    from {{ ref('fact_transactions') }} f
    left join {{ ref('dim_stores') }} s on f.store_id = s.store_id
    group by 1, 2
)

select
    store_country,
    store_typology,
    transaction_count,
    total_amount_eur,
    round(avg_amount_eur, 2) as avg_amount_eur,
    round(min_amount_eur, 2) as min_amount_eur,
    round(max_amount_eur, 2) as max_amount_eur
from transaction_summary
order by 1, 2
