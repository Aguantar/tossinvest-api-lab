-- 한 행 = 종목 × 거래일. 투자자별 순매수 거래량(주). 기관 세부는 payload 에 남겨 둔다.
select symbol, trade_date,
       (payload->'individual'->>'netBuyVolume')::numeric       as individual_net,
       (payload->'foreigner'->>'netBuyVolume')::numeric        as foreigner_net,
       (payload->'institution'->>'netBuyVolume')::numeric      as institution_net,
       (payload->'otherCorporation'->>'netBuyVolume')::numeric as other_corp_net,
       (payload->'individual'->>'buyVolume')::numeric + (payload->'individual'->>'sellVolume')::numeric as individual_gross,
       (payload->'foreignerHolding'->>'holdingRate')::numeric  as foreigner_holding_rate,
       (payload->>'updatedAt')::timestamptz                    as updated_at,
       fetched_at
from {{ source('raw', 'trend_daily') }}
where endpoint = 'investor_trading'
