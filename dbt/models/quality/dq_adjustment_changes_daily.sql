-- 한 행 = 스냅샷일 × 종목 (오늘 스냅샷과 직전 스냅샷에서 같은 거래일의 종가가 다른 종목).
--   두 벌을 다 비교해 가른다: 수정주가 적용본만 바뀌었으면 기업행위(배당락·분할·분배락), **미적용본도 바뀌었으면 데이터 정정**이다.
--   2026-10-15 첫 행 078350: 59일 중 38일이 바뀌었는데 미적용본도 같이 바뀌었고 비율이 0.958~1.027 로 일정하지 않았다. 같은 날 nxt_supported 가 false→true.
--   NXT 편입으로 과거 일봉이 통합 기준으로 다시 계산된 것으로 보인다. FAQ 의 "과거 캔들 사후 정정 없음" 과 다르다 (docs/05 §5).
with s as (
    select snapshot_date, symbol, trade_date, adjusted, close
    from {{ ref('stg_candles_1d_snapshot') }}
),
pairs as (
    select snapshot_date, lag(snapshot_date) over (order by snapshot_date) as prev_snapshot_date
    from (select distinct snapshot_date from s) d
),
j as (
    select cur.snapshot_date, p.prev_snapshot_date, cur.symbol, cur.trade_date,
           max(cur.close) filter (where cur.adjusted)     as adj_cur,  max(prev.close) filter (where prev.adjusted)     as adj_prev,
           max(cur.close) filter (where not cur.adjusted) as raw_cur,  max(prev.close) filter (where not prev.adjusted) as raw_prev
    from s cur
    join pairs p on p.snapshot_date = cur.snapshot_date and p.prev_snapshot_date is not null
    join s prev on prev.snapshot_date = p.prev_snapshot_date and prev.symbol = cur.symbol and prev.trade_date = cur.trade_date and prev.adjusted = cur.adjusted
    group by 1, 2, 3, 4
)
select
    snapshot_date, prev_snapshot_date, symbol,
    count(*)                                                    as compared_days,
    count(*) filter (where adj_cur <> adj_prev)                 as adj_changed_days,
    count(*) filter (where raw_cur <> raw_prev)                 as raw_changed_days,
    case when count(*) filter (where raw_cur <> raw_prev) = 0 then 'corporate_action'   -- 적용본만
         when count(*) filter (where adj_cur <> adj_prev) = count(*) filter (where raw_cur <> raw_prev) then 'data_revision'  -- 둘 다 같은 날들
         else 'mixed' end                                       as change_kind,
    min(trade_date) filter (where adj_cur <> adj_prev or raw_cur <> raw_prev) as first_changed_trade_date,
    max(trade_date) filter (where adj_cur <> adj_prev or raw_cur <> raw_prev) as last_changed_trade_date,
    percentile_cont(0.5) within group (order by adj_cur / nullif(adj_prev, 0)) filter (where adj_cur <> adj_prev) as adj_ratio_median,
    percentile_cont(0.1) within group (order by adj_cur / nullif(adj_prev, 0)) filter (where adj_cur <> adj_prev) as adj_ratio_p10,
    percentile_cont(0.9) within group (order by adj_cur / nullif(adj_prev, 0)) filter (where adj_cur <> adj_prev) as adj_ratio_p90
from j
group by snapshot_date, prev_snapshot_date, symbol
having count(*) filter (where adj_cur <> adj_prev or raw_cur <> raw_prev) > 0
