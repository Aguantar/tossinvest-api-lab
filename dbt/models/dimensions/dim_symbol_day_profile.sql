-- 한 행 = 거래일 × 종목. 그날 받은 봉으로 세션 프로파일을 정하고, 그날의 종목 속성으로 기대 봉 수를 정한다.
--   프로파일은 세 값으로만 갈린다(docs/03 §3, 10-13~15 재확인): 720 = 08:01~20:00, 660 = 09:01~20:00, 390 = 09:01~15:30.
--   기대 봉 수의 근거는 종목 속성이다: nxt_supported 이고 그날 NXT 거래정지가 아니면 720 (10-14 에 604 = 604).
--   2026-10-15 (docs/05 §5): 속성은 반드시 **그날의 스냅샷**(as-of)으로 본다. 최신 속성으로 과거를 판정하니 078350 이 10-13·14 에 390 이었는데
--   720 을 기대해 틀렸다(그 이틀은 NXT 거래정지, 10-15 해제). 그날 스냅샷이 없으면(10-13) 기대값을 비워 테스트에서 제외한다.
with m as (select * from {{ ref('stg_sweep_manifest') }}),
info as (select snapshot_date, symbol, nxt_supported, nxt_trading_suspended, krx_trading_suspended, security_type from {{ ref('stg_stock_info') }})
select
    m.trade_date, m.symbol, m.bars, m.first_bar_kst, m.last_bar_kst, m.status as collect_status,
    case when m.first_bar_kst = time '08:01' and m.last_bar_kst = time '20:00' then 720
         when m.first_bar_kst = time '09:01' and m.last_bar_kst = time '20:00' then 660
         when m.first_bar_kst = time '09:01' and m.last_bar_kst = time '15:30' then 390
         else null end                                         as profile_observed,
    case when info.symbol is null then null
         when info.nxt_supported and not info.nxt_trading_suspended then 720
         when m.last_bar_kst = time '20:00' then 660
         when m.last_bar_kst = time '15:30' then 390
         else null end                                         as profile_expected,
    info.nxt_supported, info.nxt_trading_suspended, info.krx_trading_suspended, info.security_type,
    (info.symbol is not null)                                  as has_same_day_attr
from m left join info on info.symbol = m.symbol and info.snapshot_date = m.trade_date
