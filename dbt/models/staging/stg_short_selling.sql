-- 한 행 = 종목 × 거래일. 공매도 거래량·비중.
select symbol, trade_date,
       (payload->>'shortSellingVolume')::numeric     as short_volume,
       (payload->>'shortSellingAmount')::numeric     as short_amount,
       (payload->>'shortSellingVolumeRate')::numeric as short_volume_rate,
       (payload->>'shortSellingAmountRate')::numeric as short_amount_rate,
       (payload->>'updatedAt')::timestamptz          as updated_at,
       fetched_at
from {{ source('raw', 'trend_daily') }}
where endpoint = 'short_selling'
