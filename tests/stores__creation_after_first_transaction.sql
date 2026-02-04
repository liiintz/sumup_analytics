{{
  config(
    severity='warn'
  )
}}

-- Data test: warn when a store appears to be created
-- AFTER its first observed transaction.

with store_first_txn as (
    select
        f.store_id,
        min(f.transaction_at) as first_transaction_at
    from {{ ref('fact_transactions') }} f
    group by 1
),

store_qc as (
    select
        s.store_id,
        s.store_name,
        s.store_country,
        s.store_typology,
        s.created_at,
        t.first_transaction_at
    from {{ ref('dim_stores') }} s
    left join store_first_txn t
        on s.store_id = t.store_id
)

select
    *
from store_qc
where
    first_transaction_at is not null
    and created_at is not null
    and created_at > first_transaction_at
