-- 한 행 = 거래일 × 종목. 1분봉에서 만든 일봉(정규장)과 세션별 거래량, 투자자 순매수, 공매도 비중, 경고. 내부용(종목별 값 포함, 공개 금지).
with c as (
    select trade_date, symbol,
           (array_agg(open order by ts) filter (where session = 'regular'))[1]       as open_regular,
           max(high) filter (where session = 'regular')                              as high_regular,
           min(low) filter (where session = 'regular')                               as low_regular,
           (array_agg(close order by ts desc) filter (where session = 'regular'))[1] as close_regular,
           sum(volume) filter (where session = 'pre')     as volume_pre,
           sum(volume) filter (where session = 'regular') as volume_regular,
           sum(volume) filter (where session = 'after')   as volume_after,
           count(*) as bars
    from {{ ref('stg_candles_1m') }} group by 1, 2
)
select c.*, p.profile_observed, p.nxt_supported, p.security_type,
       it.individual_net, it.foreigner_net, it.institution_net, it.foreigner_holding_rate,
       ss.short_volume_rate,
       (w.symbol is not null) as has_warning
from c
left join {{ ref('dim_symbol_day_profile') }} p using (trade_date, symbol)
left join {{ ref('stg_investor_trading') }} it using (trade_date, symbol)
left join {{ ref('stg_short_selling') }} ss using (trade_date, symbol)
left join (select distinct snapshot_date, symbol from {{ ref('stg_warnings') }}) w on w.snapshot_date = c.trade_date and w.symbol = c.symbol
