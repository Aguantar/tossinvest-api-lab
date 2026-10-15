-- 한 행 = 거래일 × endpoint. 유니버스 대비 그날 행이 있는 종목 비율. "없는 날은 행이 없다" 의 크기를 매일 잰다 (신용은 하루 늦다).
with u as (select snapshot_date, count(*) as universe from {{ source('raw', 'universe_snapshot') }} group by 1),
t as (select endpoint, trade_date, count(distinct symbol) as symbols from {{ source('raw', 'trend_daily') }} group by 1, 2)
select t.trade_date, t.endpoint, t.symbols, u.universe,
       round(100.0 * t.symbols / nullif(u.universe, 0), 2) as coverage_pct
from t left join u on u.snapshot_date = (select max(snapshot_date) from u)
