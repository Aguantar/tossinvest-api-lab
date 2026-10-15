-- 한 행 = 종목 × 거래일. 신용융자·대주 잔고.
select symbol, trade_date,
       (payload->'marginLoan'->>'balanceQuantity')::numeric as margin_loan_balance,
       (payload->'marginLoan'->>'balanceRate')::numeric     as margin_loan_balance_rate,
       (payload->'stockLoan'->>'balanceQuantity')::numeric  as stock_loan_balance,
       (payload->>'updatedAt')::timestamptz                 as updated_at,
       fetched_at
from {{ source('raw', 'trend_daily') }}
where endpoint = 'credit_trades'
