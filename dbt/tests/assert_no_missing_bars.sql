-- 빠짐없음: 거래일마다 관측 프로파일보다 적게 받은 종목·종목 안 빈틈·수집 오류가 0 이어야 한다.
--   관측 프로파일 기준이라 "그 종목이 원래 390 인데 390 다 왔나" 를 본다. 기대 프로파일과의 불일치는 아래 다른 테스트.
select * from {{ ref('dq_bars_daily') }}
where symbols_short > 0 or gap_cells > 0 or collect_errors > 0 or profile_unknown > 0
