-- 한 행 = 거래일. 장중 폴링 표와 마감 완결본의 셀(종목 × 봉) 대조. 10-14 첫 결과 2,387,640 셀 100% 일치 (docs/04 §7-2).
--   다른 셀이 생기면 "닫힌 봉이 나중에 바뀌었다" 는 뜻이고, 그때는 바뀐 봉의 시각 분포를 봐야 한다.
with f as (select trade_date, symbol, ts, open, high, low, close, volume from {{ ref('stg_candles_1m') }}),
i as (select trade_date, symbol, ts, open, high, low, close, volume, changes from {{ ref('stg_candles_1m_intraday') }}),
j as (
    select coalesce(f.trade_date, i.trade_date) as trade_date,
           (f.symbol is not null) as in_final, (i.symbol is not null) as in_intraday,
           (f.open, f.high, f.low, f.close, f.volume) is not distinct from (i.open, i.high, i.low, i.close, i.volume) as equal,
           coalesce(i.changes, 0) > 0 as changed_intraday
    from f full outer join i on f.trade_date = i.trade_date and f.symbol = i.symbol and f.ts = i.ts
)
select trade_date,
       count(*) filter (where in_final)                          as final_cells,
       count(*) filter (where in_intraday)                       as intraday_cells,
       count(*) filter (where in_final and in_intraday and equal)       as equal_cells,
       count(*) filter (where in_final and in_intraday and not equal)   as differ_cells,
       count(*) filter (where in_final and not in_intraday)      as final_only,
       count(*) filter (where in_intraday and not in_final)      as intraday_only,
       count(*) filter (where changed_intraday)                  as changed_intraday_cells,
       round(100.0 * count(*) filter (where in_final and in_intraday and equal) / nullif(count(*) filter (where in_final), 0), 4) as equal_pct
from j
group by trade_date
