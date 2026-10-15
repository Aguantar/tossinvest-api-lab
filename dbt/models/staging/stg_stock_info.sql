-- 한 행 = 스냅샷일 × 종목. /stocks 기본정보를 열로. koreanMarketDetail 의 NXT 지원·거래정지가 세션 프로파일을 설명한다 (docs/05 §1).
select
    snapshot_date,
    symbol,
    payload->>'name'                                       as name,
    payload->>'englishName'                                as english_name,
    payload->>'market'                                     as market,
    payload->>'securityType'                               as security_type,
    (payload->>'isCommonShare')::boolean                   as is_common_share,
    payload->>'status'                                     as status,
    nullif(payload->>'listDate', '')::date                 as list_date,
    nullif(payload->>'delistDate', '')::date               as delist_date,
    (payload->>'sharesOutstanding')::numeric               as shares_outstanding,
    payload->>'isinCode'                                   as isin_code,
    payload->>'currency'                                   as currency,
    (payload->'koreanMarketDetail'->>'nxtSupported')::boolean          as nxt_supported,
    (payload->'koreanMarketDetail'->>'krxTradingSuspended')::boolean   as krx_trading_suspended,
    (payload->'koreanMarketDetail'->>'nxtTradingSuspended')::boolean   as nxt_trading_suspended,
    (payload->'koreanMarketDetail'->>'liquidationTrading')::boolean    as liquidation_trading,
    fetched_at
from {{ source('raw', 'stock_info_snapshot') }}
