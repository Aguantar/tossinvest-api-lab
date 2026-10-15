-- 한 행 = 종목 × 1분봉 (장중 폴링 최종값). changes 는 폴링 사이에 값이 바뀐 횟수.
select trade_date, symbol, ts, open, high, low, close, volume, changes, first_seen_at, last_seen_at, last_changed_at
from {{ source('raw', 'candles_1m_intraday') }}
where {{ day_filter('trade_date') }}
