{{
    config(
        tags=['analysis']
    )
}}

-- Top-N stores using window rank + configurable var:
-- - rank via DENSE_RANK() on total_amount_eur then transaction_count
-- - filter with default 10 or override with var('top_stores_n', 5) to switch to top 5/15/etc without code changes
with transaction_summary as (
    select
        f.store_id,
        count(*) as transaction_count,
        sum(f.amount_eur) as total_amount_eur,
        round(avg(f.amount_eur), 2) as avg_transaction_amount_eur
    from {{ ref('fact_transactions') }} f
    where f.transaction_status = 'completed'
    group by 1
),

ranked_stores as (
    select
        ts.*,
        dense_rank() over (order by ts.total_amount_eur desc, ts.transaction_count desc) as rank
    from transaction_summary ts
)

select
    s.store_id,
    s.store_name,
    s.store_country,
    s.store_typology,
    rs.transaction_count,
    rs.total_amount_eur,
    rs.avg_transaction_amount_eur,
    rs.rank
from ranked_stores rs
left join {{ ref('dim_stores') }} s on rs.store_id = s.store_id
where rs.rank <= {{ var('top_stores_n', 10) }}
order by rs.rank, rs.total_amount_eur desc, rs.transaction_count desc
