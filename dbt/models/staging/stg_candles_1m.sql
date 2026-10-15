-- 한 행 = 종목 × 1분봉. 세션 라벨은 봉 종료 시각(KST)으로: 09:00 이하 pre(08:01~09:00), 09:01~15:30 regular, 그 뒤 after.
--   근거: 10-14 봉 분포 - 08:01 봉 604종목(NXT 프리), 09:01·15:30 봉 4,304 전부, 15:31 이후 2,492 (docs/05 §1).
select
    trade_date,
    symbol,
    ts,
    (ts at time zone 'Asia/Seoul')                       as ts_kst,
    ((ts at time zone 'Asia/Seoul')::time)               as bar_time_kst,
    case when (ts at time zone 'Asia/Seoul')::time <= time '09:00' then 'pre'
         when (ts at time zone 'Asia/Seoul')::time <= time '15:30' then 'regular'
         else 'after' end                                 as session,
    open, high, low, close, volume,
    fetched_at
from {{ source('raw', 'candles_1m') }}
where {{ day_filter('trade_date') }}
