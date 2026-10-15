-- 한 행 = 거래일. 시장 전체 집계 (종목 이름·가격 없음 → 공개 가능). 세션별 거래량과 봉 수, 프로파일 분포, 경고 종목 수.
with c as (
    select trade_date, session, count(*) as bars, sum(volume) as volume, count(distinct symbol) as symbols
    from {{ ref('stg_candles_1m') }} group by 1, 2
),
w as (select snapshot_date, count(distinct symbol) as warned_symbols from {{ ref('stg_warnings') }} group by 1),
b as (select * from {{ ref('dq_bars_daily') }})
select
    b.trade_date, b.symbols, b.bars, b.n_720, b.n_660, b.n_390,
    max(c.volume) filter (where c.session = 'pre')     as volume_pre,
    max(c.volume) filter (where c.session = 'regular') as volume_regular,
    max(c.volume) filter (where c.session = 'after')   as volume_after,
    max(c.symbols) filter (where c.session = 'pre')    as symbols_pre,
    max(c.symbols) filter (where c.session = 'after')  as symbols_after,
    coalesce(max(w.warned_symbols), 0)                 as warned_symbols
from b left join c using (trade_date) left join w on w.snapshot_date = b.trade_date
group by b.trade_date, b.symbols, b.bars, b.n_720, b.n_660, b.n_390
