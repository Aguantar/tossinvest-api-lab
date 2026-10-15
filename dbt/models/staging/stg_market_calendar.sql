select trade_date, is_trading,
       (sessions->'preMarket'->>'startTime')::timestamptz     as pre_start,
       (sessions->'regularMarket'->>'startTime')::timestamptz as regular_start,
       (sessions->'regularMarket'->>'endTime')::timestamptz   as regular_end,
       (sessions->'afterMarket'->>'endTime')::timestamptz     as after_end,
       fetched_at
from {{ source('raw', 'market_calendar') }}
