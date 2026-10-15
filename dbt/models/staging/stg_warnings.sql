-- 한 행 = 스냅샷일 × 종목 × 경고. 경고 없는 종목은 행 없음.
select snapshot_date, symbol, seq,
       payload->>'warningType' as warning_type, payload->>'exchange' as exchange,
       nullif(payload->>'startDate', '')::date as start_date, nullif(payload->>'endDate', '')::date as end_date,
       fetched_at
from {{ source('raw', 'warnings_snapshot') }}
