-- 한 행 = 스냅샷일 × 종목 (오늘 스냅샷과 직전 스냅샷에서 같은 거래일의 '수정주가 적용' 종가가 다른 종목).
--   = 그 사이에 기업행위(배당락·분할·분배락)가 반영된 종목. 정정 이력의 핵심 표. 스냅샷이 둘 이상 있어야 행이 생긴다 (10-15 저녁부터).
with s as (
    select snapshot_date, symbol, trade_date, close
    from {{ ref('stg_candles_1d_snapshot') }} where adjusted
),
pairs as (
    select distinct on (snapshot_date) snapshot_date,
           lag(snapshot_date) over (order by snapshot_date) as prev_snapshot_date
    from (select distinct snapshot_date from s) d
)
select
    cur.snapshot_date, p.prev_snapshot_date, cur.symbol,
    count(*)                                      as compared_days,
    count(*) filter (where cur.close <> prev.close) as changed_days,
    min(cur.trade_date) filter (where cur.close <> prev.close) as first_changed_trade_date,
    max(cur.trade_date) filter (where cur.close <> prev.close) as last_changed_trade_date,
    -- 비율: 바뀐 날들의 (오늘 종가 / 어제 스냅샷 종가) 중앙값. 분할·배당락이면 일정한 비율로 나온다
    percentile_cont(0.5) within group (order by cur.close / nullif(prev.close, 0)) filter (where cur.close <> prev.close) as adj_ratio_median
from s cur
join pairs p on p.snapshot_date = cur.snapshot_date and p.prev_snapshot_date is not null
join s prev on prev.snapshot_date = p.prev_snapshot_date and prev.symbol = cur.symbol and prev.trade_date = cur.trade_date
group by cur.snapshot_date, p.prev_snapshot_date, cur.symbol
having count(*) filter (where cur.close <> prev.close) > 0
