-- 한 행 = 거래일 × 종목. 수집기 명세.
select trade_date, symbol, bars, first_ts, last_ts,
       (first_ts at time zone 'Asia/Seoul')::time as first_bar_kst, (last_ts at time zone 'Asia/Seoul')::time as last_bar_kst,
       requests, retries_429, status, error, fetched_at
from {{ source('raw', 'sweep_manifest') }}
where {{ day_filter('trade_date') }}
