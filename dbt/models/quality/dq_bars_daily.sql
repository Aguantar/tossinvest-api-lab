-- 한 행 = 거래일. 빠짐없음 판정: 종목별 기대 봉 수(프로파일) 대비 실제, 수집 오류, 종목 안 빈틈.
with p as (select * from {{ ref('dim_symbol_day_profile') }}),
gaps as (
    select trade_date, count(*) as gap_cells, count(distinct symbol) as gap_symbols
    from (select trade_date, symbol, ts, lag(ts) over (partition by trade_date, symbol order by ts) as prev
          from {{ ref('stg_candles_1m') }}) x
    where prev is not null and ts - prev > interval '1 minute'
    group by trade_date
)
select
    p.trade_date,
    count(*)                                                        as symbols,
    sum(p.bars)                                                     as bars,
    count(*) filter (where p.collect_status <> 'ok')                as collect_errors,
    count(*) filter (where p.profile_observed is null)              as profile_unknown,
    count(*) filter (where p.profile_observed <> p.profile_expected) as profile_mismatch,
    count(*) filter (where p.bars < p.profile_observed)             as symbols_short,     -- 관측 프로파일보다 적게 받은 종목
    sum(greatest(p.profile_observed - p.bars, 0))                   as missing_bars,
    coalesce(max(g.gap_cells), 0)                                   as gap_cells,
    coalesce(max(g.gap_symbols), 0)                                 as gap_symbols,
    count(*) filter (where p.profile_observed = 720)                as n_720,
    count(*) filter (where p.profile_observed = 660)                as n_660,
    count(*) filter (where p.profile_observed = 390)                as n_390
from p left join gaps g using (trade_date)
group by p.trade_date
