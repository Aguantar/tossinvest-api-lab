-- 한 행 = 거래일 × 종목. 그날 받은 봉으로 세션 프로파일을 정하고, 종목 속성(NXT 지원·거래정지)으로 기대 봉 수를 정한다.
--   프로파일은 세 값으로만 갈린다(docs/03 §3, 10-13·10-14 재확인): 720 = 08:01~20:00, 660 = 09:01~20:00, 390 = 09:01~15:30.
--   기대 봉 수의 근거는 종목 속성이다: nxt_supported 이면 720 (10-14 에 604 = 604). 그 외는 받은 마지막 봉으로 660/390 을 가른다 - 아직 속성으로 못 가르는 부분.
with m as (select * from {{ ref('stg_sweep_manifest') }}),
info as (
    select distinct on (symbol) symbol, nxt_supported, krx_trading_suspended, security_type
    from {{ ref('stg_stock_info') }} order by symbol, snapshot_date desc
)
select
    m.trade_date, m.symbol, m.bars, m.first_bar_kst, m.last_bar_kst, m.status as collect_status,
    case when m.first_bar_kst = time '08:01' and m.last_bar_kst = time '20:00' then 720
         when m.first_bar_kst = time '09:01' and m.last_bar_kst = time '20:00' then 660
         when m.first_bar_kst = time '09:01' and m.last_bar_kst = time '15:30' then 390
         else null end                                         as profile_observed,
    case when info.nxt_supported then 720
         when m.last_bar_kst = time '20:00' then 660
         when m.last_bar_kst = time '15:30' then 390
         else null end                                         as profile_expected,
    info.nxt_supported, info.krx_trading_suspended, info.security_type
from m left join info using (symbol)
