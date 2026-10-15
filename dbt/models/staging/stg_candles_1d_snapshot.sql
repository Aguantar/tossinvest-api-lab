-- 한 행 = 스냅샷일 × 종목 × 거래일 × adjusted.
select snapshot_date, symbol, trade_date, adjusted, open, high, low, close, volume, fetched_at
from {{ source('raw', 'candles_1d_snapshot') }}
