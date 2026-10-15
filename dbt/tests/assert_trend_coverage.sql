-- 투자자별 매매는 1분봉을 받은 거래일마다 전 종목에 있어야 한다(10-14: 4,304/4,304). 나머지 넷은 없는 날이 정상이라 투자자별만 본다.
--   매일 count=10 으로 받으므로 그 창 밖의 과거 날짜는 부분(시험 실행 잔재)일 수 있어, 1분봉 수집일에 한정한다.
select c.* from {{ ref('dq_trend_coverage_daily') }} c
join (select distinct trade_date from {{ ref('stg_sweep_manifest') }}) d using (trade_date)
where c.endpoint = 'investor_trading' and c.coverage_pct < 99
