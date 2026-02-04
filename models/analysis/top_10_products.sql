{{ 
    config(
        tags=['analysis']
    )
}}

-- Top-N products using window rank + configurable var:
-- - rank via DENSE_RANK() on times_sold then total_revenue_eur
-- - filter N with var('top_products_n', 10) to switch to top 5/15/etc without code changes
-- Grain is (store_id, product_sku) to respect store-level SKU uniqueness
with product_sales as (
    select
        f.store_id,
        f.product_sku,
        count(*) as times_sold,
        sum(f.amount_eur) as total_revenue_eur,
        round(avg(f.amount_eur), 2) as avg_amount_eur
    from {{ ref('fact_transactions') }} f
    where f.transaction_status = 'completed'
    group by
        f.store_id,
        f.product_sku
),

ranked_products as (
    select
        store_id,
        product_sku,
        times_sold,
        total_revenue_eur,
        avg_amount_eur,
        dense_rank() over (order by total_revenue_eur desc, times_sold desc) as rank
    from product_sales
)

select
    store_id,
    product_sku,
    times_sold,
    total_revenue_eur,
    avg_amount_eur,
    rank
from ranked_products
where rank <= {{ var('top_products_n', 10) }}
order by rank, times_sold desc, total_revenue_eur desc
