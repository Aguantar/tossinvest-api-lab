-- 한 행 = 날짜. 거래일 여부와 세션 시각. "안 오는 게 정상" 의 근거.
select * from {{ ref('stg_market_calendar') }}
