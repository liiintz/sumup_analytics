{{
  config(
    severity='warn'
  )
}}

-- Test: transaction_at is not in the future
-- Ensures logical consistency of timestamps
select count(*) as future_transactions
from {{ ref('fact_transactions') }}
where transaction_at > datetime('now')
having count(*) > 0
