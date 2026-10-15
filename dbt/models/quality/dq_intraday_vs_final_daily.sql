-- 한 행 = 거래일. 장중 폴링 표와 마감 완결본의 셀(종목 × 봉) 대조. 10-14 첫 결과 2,387,640 셀 100% 일치 (docs/04 §7-2).
--   2026-10-15 (docs/05 §5): 종목 집합이 다를 수 있다. 폴링은 전날 유니버스, 마감 순회는 그날 유니버스를 쓰므로 신규 상장은 마감에만, 그날 빠진 종목은 장중에만 있다.
--   그래서 "값이 다르다" 와 "한쪽에만 있다" 를 종목 집합 기준으로 가른다. 테스트는 양쪽에 다 있는 종목의 셀만 본다.
with f as (select trade_date, symbol, ts, open, high, low, close, volume from {{ ref('stg_candles_1m') }}),
i as (select trade_date, symbol, ts, open, high, low, close, volume, changes from {{ ref('stg_candles_1m_intraday') }}),
fs as (select distinct trade_date, symbol from f), isy as (select distinct trade_date, symbol from i),
both_sym as (select trade_date, symbol from fs intersect select trade_date, symbol from isy),
j as (
    select coalesce(f.trade_date, i.trade_date) as trade_date, coalesce(f.symbol, i.symbol) as symbol,
           (f.symbol is not null) as in_final, (i.symbol is not null) as in_intraday,
           (f.open, f.high, f.low, f.close, f.volume) is not distinct from (i.open, i.high, i.low, i.close, i.volume) as equal,
           coalesce(i.changes, 0) > 0 as changed_intraday
    from f full outer join i on f.trade_date = i.trade_date and f.symbol = i.symbol and f.ts = i.ts
)
select j.trade_date,
       count(*) filter (where in_final)                                    as final_cells,
       count(*) filter (where in_intraday)                                 as intraday_cells,
       count(*) filter (where in_final and in_intraday and equal)          as equal_cells,
       count(*) filter (where in_final and in_intraday and not equal)      as differ_cells,
       count(*) filter (where in_final and not in_intraday)                as final_only_cells,
       count(*) filter (where in_intraday and not in_final)                as intraday_only_cells,
       count(*) filter (where in_final and not in_intraday and b.symbol is not null)   as final_only_cells_common,     -- 양쪽에 있는 종목인데 봉이 빠짐 = 진짜 문제
       count(*) filter (where in_intraday and not in_final and b.symbol is not null)   as intraday_only_cells_common,
       (select count(*) from fs where fs.trade_date = j.trade_date)        as final_symbols,
       (select count(*) from isy where isy.trade_date = j.trade_date)      as intraday_symbols,
       (select count(*) from both_sym bs where bs.trade_date = j.trade_date) as common_symbols,
       count(*) filter (where changed_intraday)                            as changed_intraday_cells,
       round(100.0 * count(*) filter (where in_final and in_intraday and equal) / nullif(count(*) filter (where in_final and b.symbol is not null), 0), 4) as equal_pct_common
from j left join both_sym b on b.trade_date = j.trade_date and b.symbol = j.symbol
group by j.trade_date
